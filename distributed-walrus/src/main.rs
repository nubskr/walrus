// Binary wiring only: construct the bucket (Walrus IO), metadata (Raft state machine), controller
// (routing), Kafka facade, monitor loop, and join/bootstrap helpers.
mod bucket;
mod config;
mod controller;
mod metadata;
mod monitor;
mod rpc;

use bucket::BucketService;
use clap::Parser;
use config::NodeConfig;
use controller::NodeController;
use metadata::{MetadataCmd, MetadataStateMachine};
use octopii::rpc::{RequestPayload, ResponsePayload};
use octopii::{Config as OctopiiConfig, OctopiiNode, OctopiiRuntime};
use rpc::{InternalOp, InternalResp};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let node_config = NodeConfig::parse();
    init_logging(&node_config)?;
    start_node(node_config).await?;
    tokio::signal::ctrl_c().await?;
    Ok(())
}

fn init_logging(node_config: &NodeConfig) -> anyhow::Result<()> {
    let subscriber = fmt::Subscriber::builder().with_env_filter(EnvFilter::from_default_env());
    if let Some(log_file_path) = &node_config.log_file {
        let file = std::fs::File::create(log_file_path)?;
        subscriber.with_writer(Arc::new(file)).init();
    } else {
        subscriber.init();
    }
    Ok(())
}

async fn start_node(node_config: NodeConfig) -> anyhow::Result<()> {
    info!("Node {} booting", node_config.node_id);
    let data_path = node_config.data_wal_dir();
    std::fs::create_dir_all(&data_path)?;
    let bucket = Arc::new(BucketService::new(data_path).await?);

    let metadata = Arc::new(MetadataStateMachine::new());

    let meta_path = node_config.meta_wal_dir();
    std::fs::create_dir_all(&meta_path)?;
    let has_existing_meta = std::fs::read_dir(&meta_path)
        .ok()
        .and_then(|mut it| it.next())
        .is_some();

    let advertise_host = node_config
        .raft_advertise_host
        .clone()
        .unwrap_or_else(|| node_config.raft_host.clone());
    let raft_bind_addr = format!("{}:{}", node_config.raft_host, node_config.raft_port);

    let mut join_target_resolved: Option<std::net::SocketAddr> = None;
    if let Some(join_target) = &node_config.join_addr {
        join_target_resolved = match join_target.parse() {
            Ok(addr) => Some(addr),
            Err(_) => {
                let mut resolved = tokio::net::lookup_host(join_target).await?;
                resolved.next()
            }
        };
    }

    let mut oct_peers = vec![];
    if node_config.node_id == 1 && node_config.join_addr.is_none() {
        oct_peers = node_config
            .initial_peers
            .iter()
            .map(|s| s.parse())
            .collect::<Result<_, _>>()?;
    } else if let Some(addr) = join_target_resolved {
        oct_peers.push(addr);
    }

    let oct_cfg = OctopiiConfig {
        node_id: node_config.node_id,
        bind_addr: raft_bind_addr.parse()?,
        peers: oct_peers,
        wal_dir: meta_path,
        is_initial_leader: node_config.node_id == 1
            && node_config.join_addr.is_none()
            && !has_existing_meta,
        ..Default::default()
    };

    let runtime = OctopiiRuntime::from_handle(tokio::runtime::Handle::current());
    let raft =
        Arc::new(OctopiiNode::new_with_state_machine(oct_cfg, runtime, metadata.clone()).await?);

    let controller = Arc::new(NodeController {
        node_id: node_config.node_id,
        bucket: bucket.clone(),
        metadata: metadata.clone(),
        raft: raft.clone(),
        offsets: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        test_fail_forward_read: std::sync::atomic::AtomicBool::new(false),
        test_fail_monitor: std::sync::atomic::AtomicBool::new(false),
        test_fail_dir_size: std::sync::atomic::AtomicBool::new(false),
        config: controller::ControllerConfig::from_env(),
    });

    let controller_rpc = controller.clone();
    // Use set_custom_rpc_handler so we don't clobber Octopii's Raft handler
    raft.set_custom_rpc_handler(move |req| {
        if let RequestPayload::Custom { operation, data } = req.payload {
            if operation == "Forward" {
                info!("Received Forward RPC, payload size: {}", data.len());
                match bincode::deserialize::<InternalOp>(&data) {
                    Ok(op) => {
                        let resp = tokio::task::block_in_place(|| {
                            tokio::runtime::Handle::current()
                                .block_on(async { controller_rpc.handle_rpc(op).await })
                        });
                        let success = !matches!(resp, InternalResp::Error(_));
                        let bytes = bincode::serialize(&resp).unwrap_or_default();
                        return ResponsePayload::CustomResponse {
                            success,
                            data: bytes.into(),
                        };
                    }
                    Err(e) => {
                        return ResponsePayload::Error {
                            message: format!("decode error: {e}"),
                        };
                    }
                }
            }
        }
        ResponsePayload::Error {
            message: "unsupported request".into(),
        }
    })
    .await;

    raft.start().await?;

    bootstrap_node_one(&controller, &raft, has_existing_meta).await?;
    attempt_join(&raft, &node_config, &advertise_host, join_target_resolved).await?;

    controller.sync_leases_now().await;
    let sync_controller = controller.clone();
    tokio::spawn(async move {
        sync_controller.run_lease_sync_loop().await;
    });

    // TODO: Spawn simple protocol server here
    // let simple_controller = controller.clone();
    // let simple_port = node_config.simple_port;
    // tokio::spawn(async move {
    //     if let Err(e) = simple::server::run_server(simple_port, simple_controller).await {
    //         error!("Simple protocol server exited: {e}");
    //     }
    // });

    let monitor_controller = controller.clone();
    let monitor_config = node_config.clone();
    tokio::spawn(async move {
        monitor::Monitor::new(monitor_controller, monitor_config)
            .run()
            .await;
    });

    info!("Node {} ready; waiting for ctrl-c", node_config.node_id);
    Ok(())
}

async fn bootstrap_node_one(
    controller: &Arc<NodeController>,
    raft: &Arc<OctopiiNode>,
    has_existing_meta: bool,
) -> anyhow::Result<()> {
    if controller.node_id != 1 {
        return Ok(());
    }

    info!(
        "Node 1: Campaigning for leadership (fresh_meta={})",
        !has_existing_meta
    );
    raft.campaign().await?;
    tokio::time::sleep(Duration::from_secs(20)).await;

    if has_existing_meta {
        info!("Existing metadata detected; skipping topic bootstrap");
        return Ok(());
    }

    info!("--- Create topic via Raft ---");
    let cmd = MetadataCmd::CreateTopic {
        name: "logs".into(),
        partitions: 2,
        initial_leader: 1,
    };

    let mut attempts = 0;
    loop {
        attempts += 1;
        match raft.propose(bincode::serialize(&cmd)?).await {
            Ok(res) => {
                info!("CreateTopic result: {:?}", String::from_utf8_lossy(&res));
                break;
            }
            Err(e) => {
                error!(
                    "CreateTopic failed (attempt {}): {}. Retrying in 2s...",
                    attempts, e
                );
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }

    // Always rollover to self (1) initially since we don't know if peers joined yet
    let roll = MetadataCmd::RolloverPartition {
        name: "logs".into(),
        partition: 0,
        new_leader: 1,
        sealed_segment_size_bytes: 0,
    };

    loop {
        match raft.propose(bincode::serialize(&roll)?).await {
            Ok(_) => break,
            Err(e) => {
                error!("Rollover failed: {}. Retrying...", e);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }

    // Allow time for state machine and sync leases
    tokio::time::sleep(Duration::from_secs(1)).await;

    controller.sync_leases_now().await;
    // TODO: Re-enable once we have simple protocol
    // controller
    //     .route_and_append("logs", 0, b"payload-from-node1".to_vec())
    //     .await?;
    // info!("Append routed through controller");

    // let (reads, high_watermark) = controller.route_and_read("logs", 0, 0, 1024).await?;
    // info!("Read {} entries (hw={})", reads.len(), high_watermark);
    // for (i, data) in reads.iter().enumerate() {
    //     info!("Entry {}: {:?}", i, String::from_utf8_lossy(data));
    // }

    Ok(())
}

async fn attempt_join(
    raft: &Arc<OctopiiNode>,
    node_config: &NodeConfig,
    advertise_host: &str,
    join_target_resolved: Option<std::net::SocketAddr>,
) -> anyhow::Result<()> {
    let Some(join_target) = &node_config.join_addr else {
        return Ok(());
    };

    info!("Joining cluster via {}", join_target);
    let my_addr = format!("{}:{}", advertise_host, node_config.raft_port);
    let op = InternalOp::JoinCluster {
        node_id: node_config.node_id,
        addr: my_addr,
    };
    let payload = bytes::Bytes::from(bincode::serialize(&op)?);
    let target_sock: std::net::SocketAddr = join_target_resolved
        .ok_or_else(|| anyhow::anyhow!("could not resolve join target {}", join_target))?;

    let rpc = raft.rpc_handler();
    let mut joined = false;
    for i in 0..10 {
        info!("Join attempt {}/10...", i + 1);
        match rpc
            .request(
                target_sock,
                RequestPayload::Custom {
                    operation: "Forward".into(),
                    data: payload.clone(),
                },
                Duration::from_secs(5),
            )
            .await
        {
            Ok(resp) => match resp.payload {
                ResponsePayload::CustomResponse { success, data } => {
                    if success {
                        info!("Successfully joined cluster");
                        joined = true;
                        break;
                    } else {
                        let err_msg = match bincode::deserialize::<InternalResp>(&data) {
                            Ok(InternalResp::Error(e)) => e,
                            _ => "unknown error".to_string(),
                        };
                        error!("Join failed: {}", err_msg);
                    }
                }
                _ => error!("Unexpected response payload"),
            },
            Err(e) => {
                error!("Join attempt failed: {}, retrying...", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
    if !joined {
        error!("Failed to join cluster after retries");
    }
    Ok(())
}
