#![cfg(feature = "openraft")]

use crate::config::Config;
use crate::error::Result;
#[cfg(feature = "openraft-filters")]
use crate::openraft::network::OpenRaftFilters;
use crate::openraft::network::{QuinnNetwork, QuinnNetworkFactory};
use crate::openraft::storage::new_mem_state_machine;
use crate::openraft::storage::new_wal_log_store;
use crate::openraft::types::{AppEntry, AppNodeId, AppResponse, AppTypeConfig};
use crate::runtime::OctopiiRuntime;
use crate::state_machine::{KvStateMachine, StateMachine};
use crate::wal::WriteAheadLog;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::FutureExt;
use once_cell::sync::Lazy;
use openraft::impls::BasicNode;
use openraft::metrics::RaftMetrics;
use openraft::{Config as RaftConfig, Raft};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock as StdRwLock};
use tokio::sync::RwLock;
use tokio::time::Duration;

pub(crate) static GLOBAL_PEER_ADDRS: Lazy<StdRwLock<HashMap<String, HashMap<u64, SocketAddr>>>> =
    Lazy::new(|| StdRwLock::new(HashMap::new()));

fn namespace_key_from_path(path: &std::path::Path) -> String {
    path.to_string_lossy().to_string()
}

fn cluster_namespace_from_wal_dir(_wal_dir: &std::path::Path) -> String {
    "distributed_walrus_cluster".to_string()
}

pub fn peer_namespace_from_base(path: &std::path::Path) -> String {
    namespace_key_from_path(path)
}

pub(crate) fn register_global_peer_addr(namespace: &str, node_id: u64, addr: SocketAddr) {
    let mut map = GLOBAL_PEER_ADDRS.write().unwrap();
    map.entry(namespace.to_string())
        .or_default()
        .insert(node_id, addr);
}

pub(crate) fn global_peer_addr(namespace: &str, peer_id: u64) -> Option<SocketAddr> {
    GLOBAL_PEER_ADDRS
        .read()
        .unwrap()
        .get(namespace)
        .and_then(|m| m.get(&peer_id).copied())
}

pub fn clear_global_peer_addrs_for(namespace: &str) {
    GLOBAL_PEER_ADDRS.write().unwrap().remove(namespace);
}

pub fn clear_global_peer_addrs() {
    GLOBAL_PEER_ADDRS.write().unwrap().clear();
}

#[derive(Serialize, Deserialize)]
struct PeerAddrRecord {
    peer_id: u64,
    addr: SocketAddr,
}

async fn load_peer_addr_records(wal: &Arc<WriteAheadLog>) -> HashMap<u64, SocketAddr> {
    let mut map = HashMap::new();
    if let Ok(entries) = wal.read_all().await {
        for raw in entries {
            if let Ok(record) = bincode::deserialize::<PeerAddrRecord>(&raw) {
                map.insert(record.peer_id, record.addr);
            }
        }
    }
    map
}

async fn append_peer_addr_record(
    wal: &Arc<WriteAheadLog>,
    peer_id: u64,
    addr: SocketAddr,
) -> Result<()> {
    let bytes = bincode::serialize(&PeerAddrRecord { peer_id, addr })
        .map_err(|e| crate::error::OctopiiError::Wal(format!("peer addr encode: {e}")))?;
    wal.append(Bytes::from(bytes)).await?;
    Ok(())
}

/// OpenRaft-based node
pub struct OpenRaftNode {
    runtime: OctopiiRuntime,
    config: Config,
    rpc: Arc<crate::rpc::RpcHandler>,
    transport: Arc<crate::transport::QuicTransport>,
    raft: Arc<Raft<AppTypeConfig>>,
    state_machine: StateMachine,
    peer_addrs: Arc<RwLock<std::collections::HashMap<u64, SocketAddr>>>,
    peer_addr_wal: Arc<WriteAheadLog>,
    peer_namespace: Arc<String>,
    custom_rpc_handler: Arc<RwLock<Option<Arc<dyn Fn(crate::rpc::RpcRequest) -> BoxFuture<'static, crate::rpc::ResponsePayload> + Send + Sync>>>>,
    #[cfg(feature = "openraft-filters")]
    filters: Arc<OpenRaftFilters>,
}

/// Minimal configuration state used by tests
pub struct ConfStateCompat {
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
}

impl OpenRaftNode {
    async fn new_with_optional_state_machine(
        config: Config,
        runtime: OctopiiRuntime,
        custom_state_machine: Option<StateMachine>,
    ) -> Result<Self> {
        let transport = Arc::new(crate::transport::QuicTransport::new(config.bind_addr).await?);
        let rpc = Arc::new(crate::rpc::RpcHandler::new(Arc::clone(&transport)));

        std::fs::create_dir_all(&config.wal_dir).map_err(|e| crate::error::OctopiiError::Io(e))?;

        let cluster_namespace = Arc::new(cluster_namespace_from_wal_dir(&config.wal_dir));

        let my_addr = config.public_addr.unwrap_or(config.bind_addr);
        register_global_peer_addr(cluster_namespace.as_str(), config.node_id, my_addr);

        let flush_interval = Duration::from_millis(config.wal_flush_interval_ms);
        let log_store = new_wal_log_store(Arc::new(
            WriteAheadLog::new(
                config.wal_dir.join("openraft_log"),
                config.wal_batch_size,
                flush_interval,
            )
            .await?,
        ))
        .await?;

        let peer_addr_wal = Arc::new(
            WriteAheadLog::new(
                config.wal_dir.join("peer_addrs"),
                config.wal_batch_size,
                flush_interval,
            )
            .await?,
        );
        let mut initial_peer_map = load_peer_addr_records(&peer_addr_wal).await;
        initial_peer_map.insert(config.node_id, config.bind_addr);

        for peer_addr in config.peers.iter() {
            let peer_id = (peer_addr.port() % 10) as u64;
            if peer_id != config.node_id && peer_id > 0 {
                if initial_peer_map.get(&peer_id).copied() != Some(*peer_addr) {
                    append_peer_addr_record(&peer_addr_wal, peer_id, *peer_addr).await?;
                    initial_peer_map.insert(peer_id, *peer_addr);
                }
            }
        }
        let peer_addrs = Arc::new(RwLock::new(initial_peer_map));
        {
            let map = peer_addrs.read().await;
            for (peer_id, addr) in map.iter() {
                register_global_peer_addr(cluster_namespace.as_str(), *peer_id, *addr);
            }
        }

        // Start accepting incoming connections
        let rpc_clone = Arc::clone(&rpc);
        let transport_clone = Arc::clone(&transport);
        tokio::spawn(async move {
            loop {
                match transport_clone.accept().await {
                    Ok((addr, peer)) => {
                        tracing::debug!("Accepted connection from {}", addr);
                        // Spawn a task to handle messages from this peer
                        let rpc_inner = Arc::clone(&rpc_clone);
                        tokio::spawn(async move {
                            loop {
                                match peer.recv().await {
                                    Ok(Some(data)) => {
                                        match crate::rpc::deserialize::<crate::rpc::RpcMessage>(
                                            &data,
                                        ) {
                                            Ok(msg) => {
                                                rpc_inner
                                                    .notify_message(
                                                        addr,
                                                        msg,
                                                        Some(Arc::clone(&peer)),
                                                    )
                                                    .await;
                                            }
                                            Err(e) => {
                                                tracing::error!(
                                                    "Failed to deserialize RPC message from {}: {}",
                                                    addr,
                                                    e
                                                );
                                            }
                                        }
                                    }
                                    Ok(None) => {
                                        tracing::debug!("Peer {} closed connection", addr);
                                        break;
                                    }
                                    Err(e) => {
                                        tracing::debug!("Peer {} recv error: {}", addr, e);
                                        break;
                                    }
                                }
                            }
                        });
                    }
                    Err(e) => {
                        // Don't break the loop on accept errors - just log and continue
                        // This can happen during normal operation (e.g., connection refused, handshake failures)
                        tracing::debug!("Failed to accept connection: {}", e);
                        // Small delay to avoid tight loop on persistent errors
                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    }
                }
            }
        });

        #[cfg(feature = "openraft-filters")]
        let filters = Arc::new(OpenRaftFilters::new());

        let state_machine: StateMachine =
            custom_state_machine.unwrap_or_else(|| Arc::new(KvStateMachine::in_memory()));
        let state_machine_store = new_mem_state_machine(state_machine.clone());

        // Create network factory
        #[cfg(feature = "openraft-filters")]
        let network_factory = QuinnNetworkFactory::new(
            Arc::clone(&rpc),
            Arc::clone(&peer_addrs),
            config.node_id,
            Arc::clone(&cluster_namespace),
            Arc::clone(&filters),
        );

        #[cfg(not(feature = "openraft-filters"))]
        let network_factory = QuinnNetworkFactory::new(
            Arc::clone(&rpc),
            Arc::clone(&peer_addrs),
            config.node_id,
            Arc::clone(&cluster_namespace),
        );

        // Create Raft config
        let mut raft_config = RaftConfig::default();
        raft_config.election_timeout_min = 5000;
        raft_config.election_timeout_max = 10000;
        raft_config.heartbeat_interval = 1000;
        let raft_config = Arc::new(
            raft_config
                .validate()
                .map_err(|e| crate::error::OctopiiError::Rpc(format!("raft config: {e}")))?,
        );

        // Create Raft instance
        let raft = Raft::new(
            config.node_id,
            raft_config,
            network_factory,
            log_store,
            state_machine_store.clone(),
        )
        .await
        .map_err(|e| crate::error::OctopiiError::Rpc(format!("raft new: {e}")))?;

        Ok(Self {
            runtime,
            config,
            rpc,
            transport,
            raft: Arc::new(raft),
            state_machine,
            peer_addrs,
            peer_addr_wal,
            peer_namespace: Arc::clone(&cluster_namespace),
            custom_rpc_handler: Arc::new(RwLock::new(None)),
            #[cfg(feature = "openraft-filters")]
            filters,
        })
    }

    pub async fn new(config: Config, runtime: OctopiiRuntime) -> Result<Self> {
        Self::new_with_optional_state_machine(config, runtime, None).await
    }

    pub fn new_blocking(config: Config) -> Result<Self> {
        let runtime = OctopiiRuntime::new(config.worker_threads);
        let handle = runtime.handle();
        let config_clone = config.clone();
        let runtime_clone = runtime.clone();
        let node = std::thread::scope(|s| {
            s.spawn(move || {
                handle.block_on(async move { Self::new(config_clone, runtime_clone).await })
            })
            .join()
            .expect("thread panicked")
        })?;
        Ok(node)
    }

    pub async fn new_with_state_machine(
        config: Config,
        runtime: OctopiiRuntime,
        state_machine: StateMachine,
    ) -> Result<Self> {
        Self::new_with_optional_state_machine(config, runtime, Some(state_machine)).await
    }

    pub fn rpc_handler(&self) -> Arc<crate::rpc::RpcHandler> {
        Arc::clone(&self.rpc)
    }

    async fn persist_peer_addr_if_needed(&self, peer_id: u64, addr: SocketAddr) -> Result<()> {
        let mut needs_persist = false;
        {
            let mut map = self.peer_addrs.write().await;
            if map.get(&peer_id).copied() != Some(addr) {
                map.insert(peer_id, addr);
                needs_persist = true;
            }
        }
        register_global_peer_addr(self.peer_namespace.as_str(), peer_id, addr);
        if needs_persist {
            append_peer_addr_record(&self.peer_addr_wal, peer_id, addr).await?;
        }
        Ok(())
    }

    pub async fn set_custom_rpc_handler<F>(&self, handler: F)
    where
        F: Fn(crate::rpc::RpcRequest) -> BoxFuture<'static, crate::rpc::ResponsePayload> + Send + Sync + 'static,
    {
        let mut h = self.custom_rpc_handler.write().await;
        *h = Some(Arc::new(handler));
    }

    pub async fn start(&self) -> Result<()> {
        // Populate peer_addrs map for all nodes (not just initial leader)
        // HACK: Use last digit of port as node ID (e.g., :9321 -> node 1, :9322 -> node 2)
        // This works for the test suite but is not production-ready
        for peer_addr in self.config.peers.iter() {
            let peer_id = (peer_addr.port() % 10) as u64;
            if peer_id != self.config.node_id && peer_id > 0 {
                tracing::info!(
                    "Node {}: Adding peer {} at address {}",
                    self.config.node_id,
                    peer_id,
                    peer_addr
                );
                self.persist_peer_addr_if_needed(peer_id, *peer_addr)
                    .await?;
            }
        }

        // Log final peer_addrs state
        let peers = self.peer_addrs.read().await;
        tracing::info!(
            "Node {}: Initialized with peers: {:?}",
            self.config.node_id,
            peers
                .iter()
                .map(|(id, addr)| format!("{}@{}", id, addr))
                .collect::<Vec<_>>()
        );
        drop(peers);

        // Register RPC handler for OpenRaft messages BEFORE initialization
        // This is critical: nodes 2 and 3 need to be able to receive RPCs when node 1 initializes
        let raft_clone = self.raft.clone();
        let node_id = self.config.node_id;
        let custom_handler = Arc::clone(&self.custom_rpc_handler);
        self.rpc
            .set_request_handler(move |req| {
                let raft = raft_clone.clone();
                let custom_handler = custom_handler.clone();

                async move {
                    match req.payload {
                        crate::rpc::RequestPayload::OpenRaft { kind, data } => {
                            tracing::info!(
                                "Node {}: OpenRaft RPC handler received: kind={}",
                                node_id,
                                kind
                            );
                            let response_data = match kind.as_str() {
                                "append_entries" => {
                                    if let Ok(req) = bincode::deserialize::<
                                        openraft::raft::AppendEntriesRequest<AppTypeConfig>,
                                    >(&data)
                                    {
                                        if let Ok(resp) = raft.append_entries(req).await {
                                            bincode::serialize(&resp).unwrap_or_default()
                                        } else {
                                            Vec::new()
                                        }
                                    } else {
                                        Vec::new()
                                    }
                                }
                                "vote" => {
                                    if let Ok(req) = bincode::deserialize::<
                                        openraft::raft::VoteRequest<AppTypeConfig>,
                                    >(&data)
                                    {
                                        if let Ok(resp) = raft.vote(req).await {
                                            bincode::serialize(&resp).unwrap_or_default()
                                        } else {
                                            Vec::new()
                                        }
                                    } else {
                                        Vec::new()
                                    }
                                }
                                "install_snapshot" => {
                                    if let Ok(req) = bincode::deserialize::<
                                        openraft::raft::InstallSnapshotRequest<AppTypeConfig>,
                                    >(&data)
                                    {
                                        if let Ok(resp) = raft.install_snapshot(req).await {
                                            bincode::serialize(&resp).unwrap_or_default()
                                        } else {
                                            Vec::new()
                                        }
                                    } else {
                                        Vec::new()
                                    }
                                }
                                _ => Vec::new(),
                            };

                            crate::rpc::ResponsePayload::OpenRaft {
                                kind,
                                data: bytes::Bytes::from(response_data),
                            }
                        }
                        _ => {
                            let guard = custom_handler.read().await;
                            if let Some(h) = guard.as_ref() {
                                h(req).await
                            } else {
                                crate::rpc::ResponsePayload::CustomResponse {
                                    success: false,
                                    data: bytes::Bytes::new(),
                                }
                            }
                        }
                    }
                }
                .boxed()
            })
            .await;

        // NOW initialize the cluster after all RPC handlers are set up
        // Only initialize if this is the initial leader and cluster is not initialized
        // For multi-node clusters, only the initial leader should call initialize
        if self.config.is_initial_leader
            && !self
                .raft
                .is_initialized()
                .await
                .map_err(|e| crate::error::OctopiiError::Rpc(format!("is_initialized: {e}")))?
        {
            tracing::info!(
                "Node {}: Initializing cluster as initial leader",
                self.config.node_id
            );
            let mut nodes = BTreeMap::new();
            let my_addr = self.config.public_addr.unwrap_or(self.config.bind_addr);
            nodes.insert(
                self.config.node_id,
                BasicNode {
                    addr: my_addr.to_string(),
                },
            );

            // Add peer nodes to the initial cluster membership
            for peer_addr in self.config.peers.iter() {
                let peer_id = (peer_addr.port() % 10) as u64;
                if peer_id != self.config.node_id && peer_id > 0 {
                    tracing::info!(
                        "Node {}: Adding peer {} at address {} to initial membership",
                        self.config.node_id,
                        peer_id,
                        peer_addr
                    );
                    nodes.insert(
                        peer_id,
                        BasicNode {
                            addr: peer_addr.to_string(),
                        },
                    );
                }
            }
            tracing::info!("Node {}: Calling raft.initialize with nodes: {:?}", self.config.node_id, nodes);
            self.raft
                .initialize(nodes)
                .await
                .map_err(|e| crate::error::OctopiiError::Rpc(format!("initialize: {e}")))?;
            tracing::info!(
                "Node {}: Cluster initialization complete",
                self.config.node_id
            );
        }

        Ok(())
    }

    pub async fn propose(&self, command: Vec<u8>) -> Result<Bytes> {
        let resp = self
            .raft
            .client_write(AppEntry(command))
            .await
            .map_err(|e| crate::error::OctopiiError::Rpc(format!("client_write: {e}")))?;
        Ok(Bytes::from(resp.data.0))
    }

    pub async fn query(&self, command: &[u8]) -> Result<Bytes> {
        self.state_machine
            .apply(command)
            .map_err(|e| crate::error::OctopiiError::Rpc(e))
    }

    pub async fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        tracing::debug!(
            "Node {}: is_leader check - current_leader={:?}, server_state={:?}, membership={:?}",
            self.config.node_id,
            metrics.current_leader,
            metrics.state,
            metrics.membership_config.membership()
        );
        metrics.current_leader == Some(self.config.node_id)
    }

    pub async fn has_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader.is_some()
    }

    pub async fn campaign(&self) -> Result<()> {
        self.raft
            .trigger()
            .elect()
            .await
            .map_err(|e| crate::error::OctopiiError::Rpc(format!("elect: {e}")))?;
        Ok(())
    }

    pub async fn transfer_leader(&self, _target_id: u64) -> Result<()> {
        // OpenRaft doesn't have a direct transfer_leader API in 0.9
        // This would need to be implemented via membership changes
        Ok(())
    }

    pub async fn read_index(&self, _ctx: Vec<u8>) -> Result<()> {
        // In OpenRaft 0.10, we check leadership via metrics
        let metrics = self.raft.metrics().borrow().clone();
        if metrics.current_leader != Some(self.config.node_id) {
            return Err(crate::error::OctopiiError::Rpc("not leader".to_string()));
        }
        Ok(())
    }

    pub async fn add_learner(&self, peer_id: u64, addr: SocketAddr) -> Result<()> {
        self.persist_peer_addr_if_needed(peer_id, addr).await?;
        let node = BasicNode {
            addr: addr.to_string(),
        };
        self.raft
            .add_learner(peer_id, node, true)
            .await
            .map_err(|e| crate::error::OctopiiError::Rpc(format!("add_learner: {e}")))?;
        Ok(())
    }

    pub async fn promote_learner(&self, peer_id: u64) -> Result<()> {
        // Get current membership and add the new voter to it
        let metrics = self.raft.metrics().borrow().clone();
        let current_membership = metrics.membership_config.membership();

        // Get all current voter IDs
        let mut members = BTreeSet::new();
        for config in current_membership.get_joint_config() {
            members.extend(config.iter().copied());
        }

        // Add the new voter
        members.insert(peer_id);

        tracing::info!(
            "Node {}: Promoting learner {} to voter, new membership: {:?}",
            self.config.node_id,
            peer_id,
            members
        );

        self.raft
            .change_membership(members, true)
            .await
            .map_err(|e| crate::error::OctopiiError::Rpc(format!("change_membership: {e}")))?;
        Ok(())
    }

    pub async fn is_learner_caught_up(&self, peer_id: u64) -> Result<bool> {
        let metrics = self.raft.metrics().borrow().clone();
        if let Some(last_log) = metrics.last_log_index {
            if let Some(replication) = metrics.replication {
                if let Some(repl_log_id_opt) = replication.get(&peer_id) {
                    let matched = repl_log_id_opt.as_ref().map_or(0, |log_id| log_id.index);
                    let distance = last_log.saturating_sub(matched);
                    return Ok(distance <= self.raft.config().replication_lag_threshold);
                }
            }
        }
        Ok(false)
    }

    pub async fn conf_state(&self) -> ConfStateCompat {
        let map = self.peer_addrs.read().await;
        let mut voters: Vec<u64> = map.keys().copied().collect();
        if !voters.contains(&self.config.node_id) {
            voters.push(self.config.node_id);
        }
        voters.sort_unstable();
        ConfStateCompat {
            voters,
            learners: Vec::new(),
        }
    }

    pub async fn force_snapshot_to_peer(&self, _peer_id: u64) -> Result<()> {
        self.raft
            .trigger()
            .snapshot()
            .await
            .map_err(|e| crate::error::OctopiiError::Rpc(format!("snapshot: {e}")))?;
        Ok(())
    }

    pub async fn peer_progress(&self, peer_id: u64) -> Option<(u64, u64)> {
        let metrics = self.raft.metrics().borrow().clone();
        if let Some(last_log) = metrics.last_log_index {
            if let Some(replication) = metrics.replication {
                if let Some(repl_log_id_opt) = replication.get(&peer_id) {
                    let matched = repl_log_id_opt.as_ref().map_or(0, |log_id| log_id.index);
                    return Some((matched, last_log));
                }
            }
        }
        None
    }

    pub async fn update_peer_addr(&self, peer_id: u64, addr: SocketAddr) {
        let _ = self.persist_peer_addr_if_needed(peer_id, addr).await;
    }

    pub async fn peer_addr_for(&self, peer_id: u64) -> Option<SocketAddr> {
        if let Some(addr) = self.peer_addrs.read().await.get(&peer_id).copied() {
            return Some(addr);
        }

        if let Some(addr) = global_peer_addr(self.peer_namespace.as_str(), peer_id) {
            let _ = self.persist_peer_addr_if_needed(peer_id, addr).await;
            return Some(addr);
        }

        None
    }

    pub fn raft_metrics(&self) -> RaftMetrics<AppTypeConfig> {
        self.raft.metrics().borrow().clone()
    }

    pub fn id(&self) -> u64 {
        self.config.node_id
    }

    pub fn shutdown(&self) {
        let _ = self.raft.shutdown();
    }

    pub fn shipping_lane(&self) -> crate::shipping_lane::ShippingLane {
        crate::shipping_lane::ShippingLane::new(Arc::clone(&self.transport))
    }

    #[cfg(feature = "openraft-filters")]
    pub async fn clear_send_filters(&self) {
        self.filters.clear().await;
    }

    #[cfg(feature = "openraft-filters")]
    pub async fn add_send_drop_to(&self, to: u64) {
        self.filters
            .drop_pairs
            .write()
            .await
            .insert((self.config.node_id, to));
    }

    #[cfg(feature = "openraft-filters")]
    pub async fn add_send_delay_to(&self, to: u64, delay: Duration) {
        self.filters
            .delay_pairs
            .write()
            .await
            .insert((self.config.node_id, to), delay);
    }

    #[cfg(feature = "openraft-filters")]
    pub async fn add_partition(&self, group1: Vec<u64>, group2: Vec<u64>) {
        let g1: std::collections::HashSet<u64> = group1.into_iter().collect();
        let g2: std::collections::HashSet<u64> = group2.into_iter().collect();
        self.filters.partitions.write().await.push((g1, g2));
    }
}
