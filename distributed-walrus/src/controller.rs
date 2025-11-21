use crate::bucket::BucketService;
use crate::metadata::{MetadataStateMachine, NodeId};
use crate::rpc::TestControl;
use crate::rpc::{InternalOp, InternalResp};
use anyhow::{anyhow, bail, Result};
use bytes::Bytes;
use octopii::rpc::{RequestPayload, ResponsePayload};
use octopii::OctopiiNode;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::timeout;
use tracing::warn;

pub struct NodeController {
    pub node_id: NodeId,
    pub bucket: Arc<BucketService>,
    pub metadata: Arc<MetadataStateMachine>,
    pub raft: Arc<OctopiiNode>,
    pub offsets: Arc<RwLock<HashMap<String, u64>>>,
    pub test_fail_forward_read: AtomicBool,
    pub test_fail_monitor: AtomicBool,
    pub test_fail_dir_size: AtomicBool,
    pub test_fail_gc: AtomicBool,
}

impl NodeController {
    fn forward_timeout() -> Duration {
        let ms = std::env::var("WALRUS_RPC_TIMEOUT_MS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(2_000);
        Duration::from_millis(ms)
    }

    pub async fn handle_rpc(&self, op: InternalOp) -> InternalResp {
        tracing::info!("handle_rpc: processing op {:?}", op);
        match op {
            InternalOp::ForwardAppend { wal_key, data } => {
                self.sync_leases_now().await;
                match self.append_with_retry(&wal_key, data).await {
                    Ok(_) => {
                        tracing::info!("handle_rpc: append success for {}", wal_key);
                        self.record_append(&wal_key).await;
                        InternalResp::Ok
                    }
                    Err(e) => {
                        tracing::error!("handle_rpc: append failed for {}: {}", wal_key, e);
                        InternalResp::Error(e.to_string())
                    }
                }
            }
            InternalOp::ForwardRead { wal_key, max_bytes } => {
                if self.test_fail_forward_read.load(Ordering::Relaxed) {
                    return InternalResp::Error("forced forward read failure".into());
                }
                match self.bucket.read_by_key(&wal_key, max_bytes).await {
                    Ok(entries) => {
                        let high_watermark = self.current_high_watermark(&wal_key).await;
                        let data = entries.into_iter().map(|e| e.data).collect();
                        InternalResp::ReadResult {
                            data,
                            high_watermark,
                        }
                    }
                    Err(e) => InternalResp::Error(e.to_string()),
                }
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
                TestControl::ForceGcError => {
                    self.test_fail_gc.store(true, Ordering::Relaxed);
                    InternalResp::Ok
                }
            },
        }
    }

    pub async fn create_topic(&self, name: String, partitions: u32) -> Result<()> {
        if !self.raft.is_leader().await {
            anyhow::bail!("not leader");
        }
        let cmd = crate::metadata::MetadataCmd::CreateTopic {
            name,
            partitions,
            initial_leader: self.node_id,
        };
        self.raft
            .propose(bincode::serialize(&cmd)?)
            .await
            .map(|_| ())
            .map_err(|e| anyhow!(e))
    }

    pub async fn remove_node_from_membership(&self, removed: NodeId) -> Result<()> {
        if !self.raft.is_leader().await {
            bail!("not leader");
        }
        // Raft membership changes are not exposed here; we still reassign leaders away from the removed node.
        self.metadata.reassign_leader(removed, self.node_id);
        Ok(())
    }

    pub async fn route_and_append(&self, topic: &str, partition: u32, data: Vec<u8>) -> Result<()> {
        let (leader, generation) = self
            .metadata
            .get_partition_leader(topic, partition)
            .ok_or_else(|| anyhow!("unknown topic/partition"))?;
        let key = wal_key(topic, partition, generation);
        tracing::info!(
            "route_and_append topic={} partition={} leader={} gen={} key={}",
            topic,
            partition,
            leader,
            generation,
            key
        );
        if leader == self.node_id {
            tracing::info!("writing locally on node {}", self.node_id);
            // Ensure local bucket has the latest leases from metadata before appending.
            self.sync_leases_now().await;
            self.append_with_retry(&key, data).await?;
            self.record_append(&key).await;
        } else {
            let addr = self
                .raft
                .peer_addr_for(leader)
                .await
                .ok_or_else(|| anyhow!("missing peer address for {}", leader))?;
            tracing::info!("forwarding to leader {} at {}", leader, addr);
            let op = InternalOp::ForwardAppend { wal_key: key, data };
            let payload = Bytes::from(bincode::serialize(&op)?);
            let resp = self
                .raft
                .rpc_handler()
                .request(
                    addr,
                    RequestPayload::Custom {
                        operation: "Forward".into(),
                        data: payload,
                    },
                    Self::forward_timeout(),
                )
                .await?;
            match resp.payload {
                ResponsePayload::CustomResponse { data, .. } => {
                    match bincode::deserialize::<InternalResp>(&data) {
                        Ok(InternalResp::Ok) => {}
                        Ok(InternalResp::Error(e)) => return Err(anyhow!(e)),
                        Ok(other) => {
                            warn!("unexpected response {:?}", other);
                        }
                        Err(e) => return Err(anyhow!(e)),
                    }
                }
                other => {
                    return Err(anyhow!("unexpected response: {:?}", other));
                }
            }
        }
        Ok(())
    }

    pub async fn route_and_read(
        &self,
        topic: &str,
        partition: u32,
        max_bytes: usize,
    ) -> Result<(Vec<Vec<u8>>, u64)> {
        let (leader, generation) = self
            .metadata
            .get_partition_leader(topic, partition)
            .ok_or_else(|| anyhow!("unknown topic/partition"))?;
        let key = wal_key(topic, partition, generation);
        tracing::info!(
            "route_and_read topic={} partition={} leader={} gen={} key={}",
            topic,
            partition,
            leader,
            generation,
            key
        );

        if leader == self.node_id {
            tracing::info!("reading locally on node {}", self.node_id);
            let entries = self.bucket.read_by_key(&key, max_bytes).await?;
            let watermark = self
                .current_high_watermark(&key)
                .await
                .max(entries.len() as u64);
            Ok((entries.into_iter().map(|e| e.data).collect(), watermark))
        } else {
            let addr = self
                .raft
                .peer_addr_for(leader)
                .await
                .ok_or_else(|| anyhow!("missing peer address for {}", leader))?;
            tracing::info!("forwarding read to leader {} at {}", leader, addr);
            let fwd_key = key.clone();
            let op = InternalOp::ForwardRead {
                wal_key: fwd_key,
                max_bytes,
            };
            let payload = Bytes::from(bincode::serialize(&op)?);
            let forwarded = timeout(
                Self::forward_timeout(),
                self.raft.rpc_handler().request(
                    addr,
                    RequestPayload::Custom {
                        operation: "Forward".into(),
                        data: payload,
                    },
                    Self::forward_timeout(),
                ),
            )
            .await;
            let resp = match forwarded {
                Ok(Ok(resp)) => resp,
                Ok(Err(e)) => {
                    warn!(
                        "forward read to leader {} at {} failed: {}",
                        leader, addr, e
                    );
                    let watermark = self.current_high_watermark(&key).await;
                    return Ok((Vec::new(), watermark));
                }
                Err(_) => {
                    warn!(
                        "forward read to leader {} at {} timed out after {:?}",
                        leader,
                        addr,
                        Self::forward_timeout()
                    );
                    let watermark = self.current_high_watermark(&key).await;
                    return Ok((Vec::new(), watermark));
                }
            };
            match resp.payload {
                ResponsePayload::CustomResponse { data, .. } => {
                    match bincode::deserialize::<InternalResp>(&data) {
                        Ok(InternalResp::ReadResult {
                            data,
                            high_watermark,
                        }) => Ok((data, high_watermark)),
                        Ok(InternalResp::Error(e)) => {
                            warn!(
                                "forward read to leader {} at {} returned error: {}",
                                leader, addr, e
                            );
                            Ok((Vec::new(), self.current_high_watermark(&key).await))
                        }
                        Ok(other) => {
                            warn!("unexpected response {:?}", other);
                            Ok((Vec::new(), self.current_high_watermark(&key).await))
                        }
                        Err(e) => {
                            warn!("failed to decode forward read response: {}", e);
                            Ok((Vec::new(), self.current_high_watermark(&key).await))
                        }
                    }
                }
                other => {
                    warn!("unexpected response: {:?}", other);
                    Ok((Vec::new(), self.current_high_watermark(&key).await))
                }
            }
        }
    }

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

    async fn record_append(&self, wal_key: &str) -> u64 {
        let mut guard = self.offsets.write().await;
        let entry = guard.entry(wal_key.to_string()).or_insert(0);
        *entry += 1;
        *entry
    }

    async fn current_high_watermark(&self, wal_key: &str) -> u64 {
        let guard = self.offsets.read().await;
        if let Some(hw) = guard.get(wal_key) {
            return *hw;
        }
        if let Some((topic, partition, _)) = parse_wal_key(wal_key) {
            let prefix = format!("t_{}_p_{}", topic, partition);
            if let Some(max_seen) = guard
                .iter()
                .filter(|(key, _)| key.starts_with(&prefix))
                .map(|(_, v)| *v)
                .max()
            {
                return max_seen;
            }
        }
        0
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
                    for _ in 0..20 {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        match raft_clone.is_learner_caught_up(node_id_clone).await {
                            Ok(true) => {
                                tracing::info!("Node {} caught up, promoting...", node_id_clone);
                                if let Err(e) = raft_clone.promote_learner(node_id_clone).await {
                                    tracing::error!(
                                        "Failed to promote node {}: {}",
                                        node_id_clone,
                                        e
                                    );
                                } else {
                                    tracing::info!(
                                        "Node {}: Promoted learner {}",
                                        raft_clone.id(),
                                        node_id_clone
                                    );
                                }
                                return;
                            }
                            Ok(false) => {
                                tracing::debug!("Node {} not caught up yet", node_id_clone);
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

pub fn wal_key(topic: &str, partition: u32, generation: u64) -> String {
    format!("t_{}_p_{}_g_{}", topic, partition, generation)
}

fn parse_wal_key(key: &str) -> Option<(String, u32, u64)> {
    if !key.starts_with("t_") {
        return None;
    }
    let rest = &key[2..];
    let p_idx = rest.rfind("_p_")?;
    let g_idx = rest.rfind("_g_")?;
    if p_idx >= g_idx {
        return None;
    }
    let topic = &rest[..p_idx];
    let partition = rest[p_idx + 3..g_idx].parse().ok()?;
    let generation = rest[g_idx + 3..].parse().ok()?;
    Some((topic.to_string(), partition, generation))
}
