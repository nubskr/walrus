//! Controller: glues together metadata (who owns what), the bucket (Walrus IO behind leases),
//! and RPC surfaces (Kafka facade + internal forwards). Its only jobs are routing appends/reads,
//! mapping logical offsets onto Walrus’ physical offsets, and keeping leases in sync with
//! metadata. It does not parse Kafka or manage retention—that lives elsewhere.

use crate::auth::{User, UserRole};
use crate::bucket::Storage;
use crate::metadata::{Metadata, MetadataCmd, NodeId};
use crate::monitor::max_segment_entries;
use crate::rpc::TestControl;
use crate::rpc::{InternalOp, InternalResp};
use anyhow::{anyhow, Result};
use bincode;
use octopii::rpc::{RequestPayload, ResponsePayload};
use octopii::OctopiiNode;
use serde_json;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tracing::info;
mod internal;
mod topics;
mod types;
pub use types::parse_wal_key;
pub use types::wal_key;
pub use types::ReadCursor;

pub struct NodeController {
    pub node_id: NodeId,
    pub bucket: Arc<Storage>,
    pub metadata: Arc<Metadata>,
    pub raft: Arc<OctopiiNode>,
    pub offsets: Arc<RwLock<HashMap<String, u64>>>,
    pub read_cursors: Arc<Mutex<HashMap<String, ReadCursor>>>,
    pub test_fail_forward_read: AtomicBool,
    pub test_fail_monitor: AtomicBool,
    pub test_fail_dir_size: AtomicBool,
}

impl NodeController {
    pub async fn handle_rpc(&self, op: InternalOp) -> InternalResp {
        tracing::info!("handle_rpc: processing op {:?}", op);
        match op {
            InternalOp::ForwardAppend { wal_key, data } => self.forward_append(wal_key, data).await,
            InternalOp::ForwardRead {
                wal_key,
                max_entries,
            } => self.forward_read(&wal_key, max_entries).await,
            InternalOp::ForwardMetadata { cmd } => match self.propose_metadata(cmd).await {
                Ok(_) => InternalResp::Ok,
                Err(e) => InternalResp::Error(e.to_string()),
            },
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
        self.sync_peer_addrs_from_metadata().await;
        tracing::debug!("update_leases node={} leases={:?}", self.node_id, expected);
        self.bucket.update_leases(&expected).await;
    }

    async fn sync_peer_addrs_from_metadata(&self) {
        for (node_id, addr) in self.metadata.all_node_addrs() {
            if let Some(sock) = self.resolve_node_addr(&addr).await {
                if self.raft.peer_addr_for(node_id).await != Some(sock) {
                    self.raft.update_peer_addr(node_id, sock).await;
                }
            }
        }
    }

    async fn resolve_node_addr(&self, addr: &str) -> Option<SocketAddr> {
        match addr.parse() {
            Ok(sock) => Some(sock),
            Err(_) => tokio::net::lookup_host(addr)
                .await
                .ok()
                .and_then(|mut it| it.next()),
        }
    }

    /// Create the topic in Raft if it does not already exist.
    pub async fn ensure_topic(&self, topic: &str) -> Result<()> {
        if self.metadata.get_topic_state(topic).is_some() {
            return Ok(());
        }

        let mut all_nodes = self.metadata.all_node_addrs();
        all_nodes.sort_by_key(|(id, _)| *id); // deterministic order

        let initial_leader = if all_nodes.is_empty() {
            self.node_id
        } else {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            topic.hash(&mut hasher);
            let hash = hasher.finish();
            let idx = (hash as usize) % all_nodes.len();
            all_nodes[idx].0
        };

        let cmd = MetadataCmd::CreateTopic {
            name: topic.to_string(),
            initial_leader,
        };

        self.propose_metadata(cmd).await?;
        tracing::info!(
            "ensure_topic: CreateTopic proposed for {} with leader {}",
            topic,
            initial_leader
        );
        // Raft apply will update Metadata, but refresh leases eagerly.
        self.update_leases().await;
        Ok(())
    }

    pub async fn upsert_node(&self, node_id: NodeId, addr: String) -> Result<()> {
        let cmd = MetadataCmd::UpsertNode { node_id, addr };
        self.propose_metadata(cmd).await?;
        Ok(())
    }

    /// Add an admin user (for initial setup)
    pub async fn add_admin_user(&self, username: &str, password: &str) -> Result<()> {
        let user = User::new(username.to_string(), password, UserRole::Admin)?;
        let cmd = MetadataCmd::AddUser { user };
        self.propose_metadata(cmd).await?;
        Ok(())
    }

    /// Add a producer user (can publish messages) and return API key
    pub async fn add_producer(&self, username: &str, password: &str) -> Result<String> {
        let user = User::new(username.to_string(), password, UserRole::Producer)?;
        let api_key = user.api_key.clone();
        let cmd = MetadataCmd::AddUser { user };
        self.propose_metadata(cmd).await?;
        Ok(api_key)
    }

    /// Add a consumer user (can read messages) and return API key
    pub async fn add_consumer(&self, username: &str, password: &str) -> Result<String> {
        let user = User::new(username.to_string(), password, UserRole::Consumer)?;
        let api_key = user.api_key.clone();
        let cmd = MetadataCmd::AddUser { user };
        self.propose_metadata(cmd).await?;
        Ok(api_key)
    }

    /// Append data for the given topic. Only succeeds if this node is the leader.
    pub async fn append_for_topic(&self, topic: &str, data: Vec<u8>) -> Result<()> {
        let Some(topic_state) = self.metadata.get_topic_state(topic) else {
            return Err(anyhow!("unknown topic {}", topic));
        };

        let key = wal_key(topic, topic_state.current_segment);
        tracing::info!(
            "append_for_topic topic={} leader={} self={}",
            topic,
            topic_state.leader_node,
            self.node_id
        );
        if topic_state.leader_node == self.node_id {
            match self.forward_append(key.clone(), data).await {
                InternalResp::Ok => Ok(()),
                InternalResp::Error(e) => Err(anyhow!(e)),
                other => Err(anyhow!("unexpected append response: {:?}", other)),
            }
        } else {
            self.forward_append_remote(topic_state.leader_node, key, data)
                .await
        }
    }

    pub fn topic_snapshot(&self, topic: &str) -> Result<String> {
        let Some(state) = self.metadata.get_topic_state(topic) else {
            return Err(anyhow!("unknown topic {}", topic));
        };
        let json = serde_json::to_string(&state)?;
        Ok(json)
    }

    /// Read a single entry for the topic using the provided cursor. Advances the cursor
    /// across sealed segments based on sealed counts in metadata.
    pub async fn read_one_for_topic(
        &self,
        topic: &str,
        cursor: &mut ReadCursor,
    ) -> Result<Option<Vec<u8>>> {
        loop {
            let Some(topic_state) = self.metadata.get_topic_state(topic) else {
                return Err(anyhow!("unknown topic {}", topic));
            };

            if cursor.segment == 0 {
                cursor.segment = 1;
            }

            let current_segment = topic_state.current_segment;
            if cursor.segment < current_segment {
                let sealed_count = self
                    .metadata
                    .sealed_count(topic, cursor.segment)
                    .unwrap_or(0);
                if cursor.delivered_in_segment >= sealed_count {
                    cursor.segment += 1;
                    cursor.delivered_in_segment = 0;
                    continue;
                }
            }

            let leader = if cursor.segment == current_segment {
                topic_state.leader_node
            } else {
                self.metadata
                    .segment_leader(topic, cursor.segment)
                    .unwrap_or(topic_state.leader_node)
            };

            let wal = wal_key(topic, cursor.segment);
            let data_vec = if leader == self.node_id {
                match self.forward_read(&wal, 1).await {
                    InternalResp::ReadResult { data, .. } => data,
                    InternalResp::Error(e) => return Err(anyhow!(e)),
                    other => return Err(anyhow!("unexpected read response: {:?}", other)),
                }
            } else {
                self.forward_read_remote(leader, wal).await?
            };

            if let Some(entry) = data_vec.into_iter().next() {
                cursor.delivered_in_segment += 1;
                return Ok(Some(entry));
            }

            // No entry available: if this segment is sealed and we've consumed it, advance;
            // otherwise return None to signal empty.
            if cursor.segment < current_segment {
                let sealed_count = self
                    .metadata
                    .sealed_count(topic, cursor.segment)
                    .unwrap_or(0);
                if cursor.delivered_in_segment < sealed_count {
                    cursor.delivered_in_segment = sealed_count;
                }
                if cursor.delivered_in_segment >= sealed_count {
                    cursor.segment += 1;
                    cursor.delivered_in_segment = 0;
                    continue;
                }
            }
            return Ok(None);
        }
    }

    /// Shared read path that preserves a per-topic cursor across client connections.
    pub async fn read_one_for_topic_shared(&self, topic: &str) -> Result<Option<Vec<u8>>> {
        let mut guard = self.read_cursors.lock().await;
        let cursor = guard.entry(topic.to_string()).or_default();
        let res = self.read_one_for_topic(topic, cursor).await?;
        Ok(res)
    }

    pub fn get_metrics(&self) -> Result<String> {
        let metrics = self.raft.raft_metrics();
        let json = serde_json::to_string_pretty(&metrics)?;
        Ok(json)
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

    async fn forward_read(&self, wal_key: &str, _max_entries: usize) -> InternalResp {
        if self.test_fail_forward_read.load(Ordering::Relaxed) {
            return InternalResp::Error("forced forward read failure".into());
        }
        match self.bucket.read_one(wal_key).await {
            Ok(Some(data)) => InternalResp::ReadResult {
                data: vec![data],
                high_watermark: self.tracked_entry_count(wal_key).await,
            },
            Ok(None) => InternalResp::ReadResult {
                data: Vec::new(),
                high_watermark: self.tracked_entry_count(wal_key).await,
            },
            Err(e) => InternalResp::Error(e.to_string()),
        }
    }

    async fn forward_append_remote(
        &self,
        leader_node: NodeId,
        wal_key: String,
        data: Vec<u8>,
    ) -> Result<()> {
        let Some(addr) = self.metadata.get_node_addr(leader_node) else {
            return Err(anyhow!("unknown addr for node {}", leader_node));
        };
        tracing::info!(
            "forward_append_remote leader={} addr={} key={}",
            leader_node,
            addr,
            wal_key
        );
        let payload = InternalOp::ForwardAppend { wal_key, data };
        let bytes = bincode::serialize(&payload)?;
        let rpc = self.raft.rpc_handler();
        let target_sock = match addr.parse() {
            Ok(sock) => sock,
            Err(_) => tokio::net::lookup_host(addr.clone())
                .await?
                .next()
                .ok_or_else(|| anyhow!("could not resolve addr {}", addr))?,
        };
        let resp = rpc
            .request(
                target_sock,
                RequestPayload::Custom {
                    operation: "Forward".into(),
                    data: bytes.into(),
                },
                Duration::from_secs(15),
            )
            .await?;

        match resp.payload {
            ResponsePayload::CustomResponse { success, data: _ } if success => Ok(()),
            ResponsePayload::CustomResponse { data, .. } => {
                let err = bincode::deserialize::<InternalResp>(&data)
                    .ok()
                    .and_then(|r| match r {
                        InternalResp::Error(e) => Some(e),
                        _ => None,
                    })
                    .unwrap_or_else(|| "unknown error".to_string());
                Err(anyhow!("forward append failed: {}", err))
            }
            other => Err(anyhow!("unexpected response: {:?}", other)),
        }
    }

    pub(crate) async fn propose_metadata(&self, cmd: MetadataCmd) -> Result<()> {
        if self.raft.is_leader().await {
            let payload = bincode::serialize(&cmd)?;
            let _ = tokio::time::timeout(Duration::from_secs(10), self.raft.propose(payload))
                .await
                .map_err(|_| anyhow!("raft propose timed out"))??;
            return Ok(());
        }

        let mut leader_id = None;
        let mut attempts = 0;
        let max_attempts = 50; // 5 seconds with 100ms sleep
        while leader_id.is_none() && attempts < max_attempts {
            let metrics = self.raft.raft_metrics();
            leader_id = metrics.current_leader;
            tracing::debug!(
                "propose_metadata retry (attempt {}): current_leader={:?}, state={:?}, last_log_index={:?}",
                attempts,
                metrics.current_leader,
                metrics.state,
                metrics.last_log_index
            );
            if leader_id.is_none() {
                tokio::time::sleep(Duration::from_millis(100)).await;
                attempts += 1;
            }
        }

        let Some(leader_id) = leader_id else {
            return Err(anyhow!(
                "no raft leader known for metadata proposal after retries"
            ));
        };

        if leader_id == self.node_id {
            let payload = bincode::serialize(&cmd)?;
            let _ = tokio::time::timeout(Duration::from_secs(10), self.raft.propose(payload))
                .await
                .map_err(|_| anyhow!("raft propose timed out"))??;
            return Ok(());
        }

        let Some(addr) = self.metadata.get_node_addr(leader_id) else {
            return Err(anyhow!("unknown addr for raft leader {}", leader_id));
        };

        let payload = InternalOp::ForwardMetadata { cmd };
        let bytes = bincode::serialize(&payload)?;
        let rpc = self.raft.rpc_handler();
        let target_sock = match addr.parse() {
            Ok(sock) => sock,
            Err(_) => tokio::net::lookup_host(addr.clone())
                .await?
                .next()
                .ok_or_else(|| anyhow!("could not resolve addr {}", addr))?,
        };

        let resp = rpc
            .request(
                target_sock,
                RequestPayload::Custom {
                    operation: "Forward".into(),
                    data: bytes.into(),
                },
                Duration::from_secs(15),
            )
            .await?;

        match resp.payload {
            ResponsePayload::CustomResponse { success, data: _ } if success => Ok(()),
            ResponsePayload::CustomResponse { data, .. } => {
                let err = bincode::deserialize::<InternalResp>(&data)
                    .ok()
                    .and_then(|r| match r {
                        InternalResp::Error(e) => Some(e),
                        _ => None,
                    })
                    .unwrap_or_else(|| "unknown error".to_string());
                Err(anyhow!("forwarded metadata failed: {}", err))
            }
            other => Err(anyhow!("unexpected response: {:?}", other)),
        }
    }

    async fn maybe_rollover(&self, topic: &str, segment: u64) -> Result<()> {
        let wal = wal_key(topic, segment);
        let count = self.tracked_entry_count(&wal).await;
        if count < max_segment_entries() {
            return Ok(());
        }

        let metrics = self.raft.raft_metrics();
        let membership = metrics.membership_config.membership();
        let mut voters = std::collections::BTreeSet::new();
        for config in membership.get_joint_config() {
            voters.extend(config.iter());
        }
        let nodes: Vec<u64> = voters.into_iter().copied().collect();

        let current_idx = nodes.iter().position(|&id| id == self.node_id).unwrap_or(0);

        let next_leader = if nodes.is_empty() {
            self.node_id
        } else {
            nodes[(current_idx + 1) % nodes.len()]
        };

        info!(
            "maybe_rollover: proposing rollover for {} segment {} entries={} next={}",
            topic, segment, count, next_leader
        );

        let cmd = MetadataCmd::RolloverTopic {
            name: topic.to_string(),
            new_leader: next_leader,
            sealed_segment_entry_count: count,
        };
        self.propose_metadata(cmd).await?;
        Ok(())
    }

    async fn forward_read_remote(
        &self,
        leader_node: NodeId,
        wal_key: String,
    ) -> Result<Vec<Vec<u8>>> {
        let Some(addr) = self.metadata.get_node_addr(leader_node) else {
            return Err(anyhow!("unknown addr for node {}", leader_node));
        };
        let payload = InternalOp::ForwardRead {
            wal_key,
            max_entries: 1,
        };
        let bytes = bincode::serialize(&payload)?;
        let rpc = self.raft.rpc_handler();
        let target_sock = match addr.parse() {
            Ok(sock) => sock,
            Err(_) => tokio::net::lookup_host(addr.clone())
                .await?
                .next()
                .ok_or_else(|| anyhow!("could not resolve addr {}", addr))?,
        };
        let resp = rpc
            .request(
                target_sock,
                RequestPayload::Custom {
                    operation: "Forward".into(),
                    data: bytes.into(),
                },
                Duration::from_secs(15),
            )
            .await?;

        match resp.payload {
            ResponsePayload::CustomResponse { data, .. } => {
                let decoded: InternalResp =
                    bincode::deserialize(&data).map_err(|e| anyhow!("decode read: {e}"))?;
                match decoded {
                    InternalResp::ReadResult { data, .. } => Ok(data),
                    InternalResp::Error(e) => Err(anyhow!(e)),
                    other => Err(anyhow!("unexpected read response: {:?}", other)),
                }
            }
            other => Err(anyhow!("unexpected response: {:?}", other)),
        }
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
                if let Err(e) = self.upsert_node(node_id, addr.clone()).await {
                    tracing::warn!("Failed to record node {} address {}: {}", node_id, addr, e);
                }
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
                                    tracing::debug!(
                                        "Node {} not caught up yet (attempt {}/120)",
                                        node_id_clone,
                                        i
                                    );
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
