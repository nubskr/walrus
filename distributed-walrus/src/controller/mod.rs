//! Controller: glues together metadata (who owns what), the bucket (Walrus IO behind leases),
//! and RPC surfaces (Kafka facade + internal forwards). Its only jobs are routing appends/reads,
//! mapping logical offsets onto Walrus’ physical offsets, and keeping leases in sync with
//! metadata. It does not parse Kafka or manage retention—that lives elsewhere.

use crate::bucket::Storage;
use crate::metadata::{Metadata, NodeId};
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

pub struct NodeController {
    pub node_id: NodeId,
    pub bucket: Arc<Storage>,
    pub metadata: Arc<Metadata>,
    pub raft: Arc<OctopiiNode>,
    pub offsets: Arc<RwLock<HashMap<String, u64>>>,
    pub test_fail_forward_read: AtomicBool,
    pub test_fail_monitor: AtomicBool,
    pub test_fail_dir_size: AtomicBool,
}

impl NodeController {
    pub async fn handle_rpc(&self, op: InternalOp) -> InternalResp {
        tracing::info!("handle_rpc: processing op {:?}", op);
        match op {
            InternalOp::ForwardAppend { wal_key, data } => self.forward_append(wal_key, data).await,
            InternalOp::ForwardRead { .. } => {
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
                TestControl::RevokeLeases { topic } => {
                    let expected = HashSet::new();
                    self.bucket.update_leases(&expected).await;
                    let key = wal_key(&topic, 1);
                    let mut guard = self.offsets.write().await;
                    guard.remove(&key);
                    InternalResp::Ok
                }
                TestControl::SyncLeases => {
                    self.update_leases().await;
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

    pub async fn update_leases(&self) {
        let expected: HashSet<String> = self
            .metadata
            .owned_topics(self.node_id)
            .into_iter()
            .map(|(topic, seg)| wal_key(&topic, seg))
            .collect();
        tracing::info!(
            "update_leases node={} leases={:?}",
            self.node_id,
            expected
        );
        self.bucket.update_leases(&expected).await;
    }

    pub async fn run_lease_update_loop(self: Arc<Self>) {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            interval.tick().await;
            self.update_leases().await;
        }
    }

    pub(crate) async fn tracked_entry_count(&self, wal_key: &str) -> u64 {
        let guard = self.offsets.read().await;
        guard.get(wal_key).copied().unwrap_or(0)
    }

    async fn record_append(&self, wal_key: &str, num_entries: u64) -> u64 {
        let mut guard = self.offsets.write().await;
        let entry = guard.entry(wal_key.to_string()).or_insert(0);
        *entry += num_entries;
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
                self.update_leases().await;
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
