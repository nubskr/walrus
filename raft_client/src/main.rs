use anyhow::Result;
use octopii::{Config as OctopiiConfig, OctopiiNode, OctopiiRuntime};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::{info, Level};
use tracing_subscriber::{fmt, EnvFilter};
use tempfile;
use openraft::impls::BasicNode;

#[tokio::main]
async fn main() -> Result<()> {
    fmt::Subscriber::builder()
        .with_env_filter(EnvFilter::from_default_env().add_directive(Level::INFO.into()))
        .init();

    let node_id = 1;
    let raft_port = 5001;
    let bind_addr: SocketAddr = format!("127.0.0.1:{}", raft_port).parse()?;

    info!("Raft Client: Connecting to Node {} at {}", node_id, bind_addr);

    let oct_cfg = OctopiiConfig {
        node_id,
        bind_addr,
        peers: vec![], // No peers needed for a client that only queries
        wal_dir: tempfile::tempdir()?.path().to_path_buf(), // Temp dir for client's WAL
        is_initial_leader: false,
        ..Default::default()
    };

    let runtime = OctopiiRuntime::from_handle(tokio::runtime::Handle::current());

    let node = Arc::new(OctopiiNode::new(oct_cfg, runtime.clone()).await?);

    // Give the cluster some time to stabilize
    sleep(Duration::from_secs(5)).await;

    // Query Node 1's metrics
    let metrics = node.raft_metrics();

    info!("Node {} Raft Metrics: ", node_id);
    info!("  Current Leader: {:?}", metrics.current_leader);
    info!("  State: {:?}", metrics.state);

    let current_nodes: BTreeMap<u64, BasicNode> = metrics.membership_config.nodes().map(|(id, node_info)| (*id, node_info.clone())).collect();
    info!("  Nodes: {:?}", current_nodes);

    // Verify expected voters (Nodes 1, 2, 3)
    let expected_voters: BTreeMap<u64, String> = BTreeMap::from([
        (1, "127.0.0.1:5001".to_string()),
        (2, "127.0.0.1:5002".to_string()),
        (3, "127.0.0.1:5003".to_string()),
    ]);

    let mut all_voters_present = true;

    for (expected_id, expected_addr) in &expected_voters {
        if let Some(node_info) = current_nodes.get(expected_id) {
            if node_info.addr != *expected_addr {
                info!("Mismatch for Node {}: Expected addr {}, got {}", expected_id, expected_addr, node_info.addr);
                all_voters_present = false;
            }
        } else {
            info!("Node {} not found in cluster membership", expected_id);
            all_voters_present = false;
        }
    }

    if all_voters_present {
        info!("All expected nodes (1, 2, 3) are present in the cluster membership.");
    }

    if metrics.current_leader == Some(node_id) {
        info!("Node {} is the current leader.", node_id);
    } else {
        info!("Node {} is NOT the current leader (leader is {:?}).", node_id, metrics.current_leader);
    }

    Ok(())
}