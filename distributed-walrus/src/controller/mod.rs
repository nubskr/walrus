//! Controller: glues together metadata (who owns what), the bucket (Walrus IO behind leases),
//! and RPC surfaces (Kafka facade + internal forwards). Its only jobs are routing appends/reads,
//! mapping logical offsets onto Walrus’ physical offsets, and keeping leases in sync with
//! metadata. It does not parse Kafka or manage retention—that lives elsewhere.

use crate::bucket::BucketService;
use crate::metadata::{MetadataStateMachine, NodeId};
use crate::rpc::TestControl;
use crate::rpc::{InternalOp, InternalResp};
use anyhow::{anyhow, Result};
use octopii::OctopiiNode;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
mod internal;
mod topics;
mod types;
pub use types::wal_key;

#[derive(Clone, Copy)]
pub struct ControllerConfig {
    pub forward_timeout: Duration,
}

impl ControllerConfig {
    pub fn from_env() -> Self {
        let forward_timeout = std::env::var("WALRUS_RPC_TIMEOUT_MS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or_else(|| Duration::from_millis(2_000));
        Self { forward_timeout }
    }
}

pub struct NodeController {
    pub node_id: NodeId,
    pub bucket: Arc<BucketService>,
    pub metadata: Arc<MetadataStateMachine>,
    pub raft: Arc<OctopiiNode>,
    pub offsets: Arc<RwLock<HashMap<String, u64>>>,
    pub test_fail_forward_read: AtomicBool,
    pub test_fail_monitor: AtomicBool,
    pub test_fail_dir_size: AtomicBool,
    pub config: ControllerConfig,
}

impl NodeController {
    // Each WAL entry reserves a fixed prefix (matches wal::config::PREFIX_META_SIZE).
    const ENTRY_OVERHEAD: u64 = 256;
    const SCAN_WINDOW: usize = 16 * 1024 * 1024;

    pub async fn handle_rpc(&self, op: InternalOp) -> InternalResp {
        tracing::info!("handle_rpc: processing op {:?}", op);
        match op {
            InternalOp::ForwardAppend { wal_key, data } => self.forward_append(wal_key, data).await,
            InternalOp::ForwardRead {
                wal_key,
                start_offset,
                max_bytes,
            } => {
                // TODO: Implement with new simple protocol
                InternalResp::Error("ForwardRead not implemented yet".to_string())
            }
            InternalOp::JoinCluster { node_id, addr } => {
                self.handle_join_cluster(node_id, addr).await
            }
            InternalOp::TestControl(cmd) => match cmd {
                TestControl::ForceForwardReadError(flag) => {
                    self.test_fail_forward_read.store(flag, Ordering::Relaxed);
                    InternalResp::Ok
                }
                TestControl::RevokeLeases { topic, partition } => {
                    let expected = HashSet::new();
                    self.bucket.sync_leases(&expected).await;
                    let key = wal_key(&topic, partition, 1);
                    let mut guard = self.offsets.write().await;
                    guard.remove(&key);
                    InternalResp::Ok
                }
                TestControl::SyncLeases => {
                    self.sync_leases_now().await;
                    InternalResp::Ok
                }
                TestControl::TriggerJoin { node_id, addr } => {
                    self.handle_join_cluster(node_id, addr).await
                }
                TestControl::ForceMonitorError => {
                    self.test_fail_monitor.store(true, Ordering::Relaxed);
                    InternalResp::Ok
                }
                TestControl::ForceDirSizeError => {
                    self.test_fail_dir_size.store(true, Ordering::Relaxed);
                    InternalResp::Ok
                }
            },
        }
    }

    // TODO: Implement with new simple protocol
    // pub async fn route_and_read(
    //     &self,
    //     topic: &str,
    //     partition: u32,
    //     req_offset: u64,
    //     max_bytes: usize,
    // ) -> Result<(Vec<Vec<u8>>, u64)> {
    //     unimplemented!("route_and_read removed with Kafka protocol")
    // }

    pub async fn sync_leases_now(&self) {
        let expected: HashSet<String> = self
            .metadata
            .assignments_for_node(self.node_id)
            .into_iter()
            .map(|(topic, part, gen)| wal_key(&topic, part, gen))
            .collect();
        tracing::info!(
            "sync_leases_now node={} leases={:?}",
            self.node_id,
            expected
        );
        self.bucket.sync_leases(&expected).await;
    }

    pub async fn run_lease_sync_loop(self: Arc<Self>) {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            interval.tick().await;
            self.sync_leases_now().await;
        }
    }

    // TODO: All logical_wal methods removed - will be replaced with message-based reading

    pub async fn partition_high_watermark(&self, _topic: &str, _partition: u32) -> u64 {
        // TODO: Implement with message-based counting
        0
    }

    pub(super) async fn tracked_len(&self, wal_key: &str) -> u64 {
        let guard = self.offsets.read().await;
        guard.get(wal_key).copied().unwrap_or(0)
    }

    pub(super) async fn set_tracked_len(&self, wal_key: &str, bytes: u64) {
        let mut guard = self.offsets.write().await;
        let entry = guard.entry(wal_key.to_string()).or_insert(0);
        *entry = bytes;
    }

    async fn record_append(&self, wal_key: &str, bytes: u64) -> u64 {
        let mut guard = self.offsets.write().await;
        let entry = guard.entry(wal_key.to_string()).or_insert(0);
        *entry += bytes;
        *entry
    }

    async fn append_with_retry(&self, wal_key: &str, data: Vec<u8>) -> Result<()> {
        let mut last_err = None;
        for attempt in 0..2 {
            if attempt > 0 {
                tracing::warn!(
                    "append retry for {} on node {} (attempt {})",
                    wal_key,
                    self.node_id,
                    attempt + 1
                );
                self.sync_leases_now().await;
            }
            match self.bucket.append_by_key(wal_key, data.clone()).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    tracing::warn!(
                        "append attempt {} for {} failed: {}",
                        attempt + 1,
                        wal_key,
                        e
                    );
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("append failed for {}", wal_key)))
    }
    async fn handle_join_cluster(&self, node_id: u64, addr: String) -> InternalResp {
        tracing::info!(
            "Received JoinCluster request from node {} at {}",
            node_id,
            addr
        );
        let socket_addr = match addr.parse::<std::net::SocketAddr>() {
            Ok(socket_addr) => socket_addr,
            Err(parse_err) => {
                tracing::info!(
                    "JoinCluster: resolving hostname {} due to parse error {}",
                    addr,
                    parse_err
                );
                match tokio::net::lookup_host(&addr).await {
                    Ok(mut hosts) => match hosts.next() {
                        Some(resolved) => resolved,
                        None => {
                            return InternalResp::Error(format!(
                                "Could not resolve join addr {}",
                                addr
                            ))
                        }
                    },
                    Err(e) => {
                        return InternalResp::Error(format!(
                            "Failed to resolve addr {}: {}",
                            addr, e
                        ))
                    }
                }
            }
        };

        match self.raft.add_learner(node_id, socket_addr).await {
            Ok(_) => {
                tracing::info!("Node {}: Added learner {}", self.node_id, node_id);
                let raft_clone = self.raft.clone();
                let node_id_clone = node_id;
                tokio::spawn(async move {
                    for i in 0..120 {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        match raft_clone.is_learner_caught_up(node_id_clone).await {
                            Ok(true) => {
                                tracing::info!("Node {} caught up, promoting...", node_id_clone);
                                match raft_clone.promote_learner(node_id_clone).await {
                                    Ok(_) => {
                                        tracing::info!(
                                            "Node {}: Promoted learner {}",
                                            raft_clone.id(),
                                            node_id_clone
                                        );
                                        return;
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            "Failed to promote node {}: {}. Retrying...",
                                            node_id_clone,
                                            e
                                        );
                                    }
                                }
                            }
                            Ok(false) => {
                                if i % 10 == 0 {
                                    tracing::debug!("Node {} not caught up yet (attempt {}/120)", node_id_clone, i);
                                }
                            }
                            Err(e) => {
                                tracing::warn!("Error checking learner status: {}", e);
                            }
                        }
                    }
                    tracing::warn!("Timeout waiting for node {} to catch up", node_id_clone);
                });
                InternalResp::Ok
            }
            Err(e) => InternalResp::Error(format!("Failed to add learner: {}", e)),
        }
    }
}
