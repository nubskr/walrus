// Binary wiring only: construct the bucket (Walrus IO), metadata (Raft state machine), controller
// (routing), Kafka facade, monitor loop, and join/bootstrap helpers.
mod auth;
mod bucket;
mod client;
mod config;
mod controller;
mod metadata;
mod monitor;
mod rpc;
mod token;

use bucket::Storage;
use clap::Parser;
use client::start_client_listener;
use config::NodeConfig;
use controller::NodeController;
use metadata::{Metadata, MetadataCmd};
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
    let bucket = Arc::new(Storage::new(data_path).await?);

    let metadata = Arc::new(Metadata::new());

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
    let advertised_addr = format!("{}:{}", advertise_host, node_config.raft_port);

    let advertised_socket_addr = match advertised_addr.parse::<std::net::SocketAddr>() {
        Ok(addr) => addr,
        Err(_) => tokio::net::lookup_host(&advertised_addr)
            .await?
            .next()
            .ok_or_else(|| {
                anyhow::anyhow!("could not resolve advertised addr {}", advertised_addr)
            })?,
    };

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

    let mut raft_peers = vec![];
    if node_config.node_id == 1 && node_config.join_addr.is_none() {
        raft_peers = node_config
            .initial_peers
            .iter()
            .map(|s| s.parse())
            .collect::<Result<_, _>>()?;
    } else if let Some(addr) = join_target_resolved {
        raft_peers.push(addr);
    }

    let raft_cfg = OctopiiConfig {
        node_id: node_config.node_id,
        bind_addr: raft_bind_addr.parse()?,
        public_addr: Some(advertised_socket_addr),
        peers: raft_peers,
        wal_dir: meta_path,
        is_initial_leader: node_config.node_id == 1
            && node_config.join_addr.is_none()
            && !has_existing_meta,
        ..Default::default()
    };

    let runtime = OctopiiRuntime::from_handle(tokio::runtime::Handle::current());
    let raft =
        Arc::new(OctopiiNode::new_with_state_machine(raft_cfg, runtime, metadata.clone()).await?);

    let controller = Arc::new(NodeController {
        node_id: node_config.node_id,
        bucket: bucket.clone(),
        metadata: metadata.clone(),
        raft: raft.clone(),
        offsets: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        read_cursors: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        test_fail_forward_read: std::sync::atomic::AtomicBool::new(false),
        test_fail_monitor: std::sync::atomic::AtomicBool::new(false),
        test_fail_dir_size: std::sync::atomic::AtomicBool::new(false),
    });

    // Register this node's address in metadata for routing (leader only; others are registered via JoinCluster).
    if node_config.node_id == 1 {
        let register_controller = controller.clone();
        let advertised_addr_clone = advertised_addr.clone();
        tokio::spawn(async move {
            for attempt in 1..=10 {
                match register_controller
                    .upsert_node(register_controller.node_id, advertised_addr_clone.clone())
                    .await
                {
                    Ok(_) => {
                        info!("Registered node address {}", advertised_addr_clone);
                        break;
                    }
                    Err(e) => {
                        error!(
                            "Failed to register node address (attempt {}): {}",
                            attempt, e
                        );
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
            }
        });
    }

    let client_addr = format!("{}:{}", node_config.client_host, node_config.client_port);
    let client_controller = controller.clone();
    tokio::spawn(async move {
        if let Err(e) = start_client_listener(client_controller, client_addr.clone()).await {
            error!("Client listener {} exited: {}", client_addr, e);
        }
    });

    let controller_rpc = controller.clone();
    // Use set_custom_rpc_handler so we don't clobber Octopii's Raft handler
    raft.set_custom_rpc_handler(move |req| {
        let controller_rpc = controller_rpc.clone();
        Box::pin(async move {
            if let RequestPayload::Custom { operation, data } = req.payload {
                if operation == "Forward" {
                    info!("Received Forward RPC, payload size: {}", data.len());
                    match bincode::deserialize::<InternalOp>(&data) {
                        Ok(op) => {
                            let resp = controller_rpc.handle_rpc(op).await;
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
    })
    .await;

    raft.start().await?;

    bootstrap_node_one(&controller, &raft, has_existing_meta).await?;
    attempt_join(&raft, &node_config, &advertised_addr, join_target_resolved).await?;

    // Initialize default admin user if no users exist
    initialize_default_admin(&controller).await?;

    controller.update_leases().await;
    let sync_controller = controller.clone();
    tokio::spawn(async move {
        sync_controller.run_lease_update_loop().await;
    });

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
    let roll = MetadataCmd::RolloverTopic {
        name: "logs".into(),
        new_leader: 1,
        sealed_segment_entry_count: 0,
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

    // Allow time for state machine and lease updates
    tokio::time::sleep(Duration::from_secs(1)).await;

    controller.update_leases().await;

    Ok(())
}

async fn attempt_join(
    raft: &Arc<OctopiiNode>,
    node_config: &NodeConfig,
    advertised_addr: &str,
    join_target_resolved: Option<std::net::SocketAddr>,
) -> anyhow::Result<()> {
    let Some(join_target) = &node_config.join_addr else {
        return Ok(());
    };

    info!("Joining cluster via {}", join_target);
    let op = InternalOp::JoinCluster {
        node_id: node_config.node_id,
        addr: advertised_addr.to_string(),
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

async fn initialize_default_admin(controller: &Arc<NodeController>) -> anyhow::Result<()> {
    // Wait for metadata to be ready
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Check if any users exist
    if !controller.metadata.has_users() {
        info!("====================================================================");
        info!("No users found. Creating default users...");
        info!("====================================================================");

        // Read passwords from environment variables or use defaults
        let admin_password = std::env::var("WALRUS_ADMIN_PASSWORD")
            .unwrap_or_else(|_| "admin123".to_string());
        let producer_password = std::env::var("WALRUS_PRODUCER_PASSWORD")
            .unwrap_or_else(|_| "producer123".to_string());
        let consumer_password = std::env::var("WALRUS_CONSUMER_PASSWORD")
            .unwrap_or_else(|_| "consumer123".to_string());

        // Warn if using default passwords
        let using_defaults = !std::env::var("WALRUS_ADMIN_PASSWORD").is_ok();
        if using_defaults {
            info!("⚠️  WARNING: Using default passwords!");
            info!("⚠️  For production, set environment variables:");
            info!("    WALRUS_ADMIN_PASSWORD=<strong_password>");
            info!("    WALRUS_PRODUCER_PASSWORD=<strong_password>");
            info!("    WALRUS_CONSUMER_PASSWORD=<strong_password>");
            info!("");
        }

        // Create admin user
        match controller.add_admin_user("admin", &admin_password).await {
            Ok(_) => {
                info!("✓ Admin user created");
                info!("  Username: admin");
                if using_defaults {
                    info!("  Password: {} (DEFAULT - CHANGE THIS!)", admin_password);
                } else {
                    info!("  Password: <from WALRUS_ADMIN_PASSWORD>");
                }
                info!("  Role: Admin (full access)");
            }
            Err(e) => {
                error!("Failed to create default admin user: {}", e);
                return Err(e);
            }
        }

        // Create producer user
        match controller.add_producer("producer1", &producer_password).await {
            Ok(api_key) => {
                info!("✓ Producer user created");
                info!("  Username: producer1");
                if using_defaults {
                    info!("  Password: {} (DEFAULT)", producer_password);
                } else {
                    info!("  Password: <from WALRUS_PRODUCER_PASSWORD>");
                }
                info!("  Role: Producer (can publish messages)");
                info!("  API_KEY: {}", api_key);
            }
            Err(e) => {
                error!("Failed to create producer user: {}", e);
            }
        }

        // Create consumer user
        match controller.add_consumer("consumer1", &consumer_password).await {
            Ok(api_key) => {
                info!("✓ Consumer user created");
                info!("  Username: consumer1");
                if using_defaults {
                    info!("  Password: {} (DEFAULT)", consumer_password);
                } else {
                    info!("  Password: <from WALRUS_CONSUMER_PASSWORD>");
                }
                info!("  Role: Consumer (can read messages)");
                info!("  API_KEY: {}", api_key);
            }
            Err(e) => {
                error!("Failed to create consumer user: {}", e);
            }
        }

        info!("====================================================================");
        info!("Usage:");
        info!("  1. First time: LOGIN <username> <password>");
        info!("     This returns your permanent API key");
        info!("  2. Save the API key for long-term use");
        info!("  3. Future connections: APIKEY <your_api_key>");
        if using_defaults {
            info!("");
            info!("⚠️  SECURITY WARNING:");
            info!("    Default passwords are in use. Set environment variables");
            info!("    or change passwords immediately after first login!");
        }
        info!("====================================================================");
    } else {
        info!("Users already exist, skipping default user creation");
    }

    Ok(())
}
