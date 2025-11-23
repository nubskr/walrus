#![cfg(feature = "openraft")]

use crate::openraft::node::global_peer_addr;
use crate::openraft::types::{AppNodeId, AppTypeConfig};
use crate::rpc::{RequestPayload, ResponsePayload, RpcHandler};
use openraft::{
    error::RPCError,
    network::{RaftNetwork, RaftNetworkFactory},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::Duration;

/// QUIC-backed network for OpenRaft messages
pub struct QuinnNetwork {
    rpc: Arc<RpcHandler>,
    peer_addrs: Arc<tokio::sync::RwLock<std::collections::HashMap<AppNodeId, SocketAddr>>>,
    self_id: AppNodeId,
    target: AppNodeId,
    cluster_namespace: Arc<String>,
    #[cfg(feature = "openraft-filters")]
    pub(crate) filters: Arc<OpenRaftFilters>,
}

impl QuinnNetwork {
    pub fn new(
        rpc: Arc<RpcHandler>,
        peer_addrs: Arc<tokio::sync::RwLock<std::collections::HashMap<AppNodeId, SocketAddr>>>,
        self_id: AppNodeId,
        target: AppNodeId,
        cluster_namespace: Arc<String>,
        #[cfg(feature = "openraft-filters")] filters: Arc<OpenRaftFilters>,
    ) -> Self {
        Self {
            rpc,
            peer_addrs,
            self_id,
            target,
            cluster_namespace,
            #[cfg(feature = "openraft-filters")]
            filters,
        }
    }

    async fn peer_addr(&self) -> Option<SocketAddr> {
        let g = self.peer_addrs.read().await;
        if let Some(addr) = g.get(&self.target).copied() {
            return Some(addr);
        }
        tracing::warn!(
            "QuinnNetwork: peer_addr lookup failed for target {} (self={}), local peers: {:?}",
            self.target,
            self.self_id,
            g.keys().collect::<Vec<_>>()
        );
        drop(g);

        // Fall back to the global map so nodes can still communicate after dynamic
        // membership changes or leadership loss.
        if let Some(addr) = global_peer_addr(self.cluster_namespace.as_str(), self.target) {
            tracing::info!(
                "QuinnNetwork: using global peer addr for target {} -> {}",
                self.target,
                addr
            );
            self.peer_addrs.write().await.insert(self.target, addr);
            return Some(addr);
        }
        None
    }

    async fn send_openraft(
        &self,
        kind: &str,
        data: Vec<u8>,
    ) -> Result<ResponsePayload, anyhow::Error> {
        #[cfg(feature = "openraft-filters")]
        {
            // Partition check
            for (g1, g2) in self.filters.partitions.read().await.iter() {
                if (g1.contains(&self.self_id) && g2.contains(&self.target))
                    || (g2.contains(&self.self_id) && g1.contains(&self.target))
                {
                    anyhow::bail!(
                        "openraft-filters: partition drop {}->{}",
                        self.self_id,
                        self.target
                    );
                }
            }
            // Pair-specific drop
            if self
                .filters
                .drop_pairs
                .read()
                .await
                .contains(&(self.self_id, self.target))
            {
                anyhow::bail!(
                    "openraft-filters: drop pair {}->{}",
                    self.self_id,
                    self.target
                );
            }
            // Delay if configured
            if let Some(d) = self
                .filters
                .delay_pairs
                .read()
                .await
                .get(&(self.self_id, self.target))
                .copied()
            {
                tokio::time::sleep(d).await;
            }
        }

        let Some(addr) = self.peer_addr().await else {
            tracing::warn!(
                "QuinnNetwork: no address for peer {} (self={})",
                self.target,
                self.self_id
            );
            anyhow::bail!("no address for peer {}", self.target);
        };

        tracing::debug!(
            "QuinnNetwork: sending {} from {} to {} at {}",
            kind,
            self.self_id,
            self.target,
            addr
        );

        let payload = RequestPayload::OpenRaft {
            kind: kind.to_string(),
            data: bytes::Bytes::from(data),
        };

        let resp = self
            .rpc
            .request(addr, payload, Duration::from_secs(5))
            .await?;

        Ok(resp.payload)
    }
}

impl openraft::network::v2::RaftNetworkV2<AppTypeConfig> for QuinnNetwork {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<AppTypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<AppendEntriesResponse<AppTypeConfig>, RPCError<AppTypeConfig>> {
        let data = bincode::serialize(&req)
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        let resp_payload = self
            .send_openraft("append_entries", data)
            .await
            .map_err(|e| {
                RPCError::Unreachable(openraft::error::Unreachable::new(&io::Error::new(
                    io::ErrorKind::Other,
                    e,
                )))
            })?;

        match resp_payload {
            ResponsePayload::OpenRaft { kind, data } if kind == "append_entries" => {
                bincode::deserialize(&data)
                    .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))
            }
            other => Err(RPCError::Unreachable(openraft::error::Unreachable::new(
                &io::Error::new(
                    io::ErrorKind::Other,
                    format!("unexpected response: {:?}", other),
                ),
            ))),
        }
    }

    async fn full_snapshot(
        &mut self,
        _vote: openraft::Vote<AppTypeConfig>,
        _snapshot: openraft::storage::Snapshot<AppTypeConfig>,
        _cancel: impl std::future::Future<Output = openraft::error::ReplicationClosed>
            + openraft::OptionalSend
            + 'static,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::SnapshotResponse<AppTypeConfig>,
        openraft::error::StreamingError<AppTypeConfig>,
    > {
        // For now, return an error - full snapshot streaming not yet implemented
        Err(openraft::error::StreamingError::Unreachable(
            openraft::error::Unreachable::new(&io::Error::new(
                io::ErrorKind::Other,
                "full_snapshot not yet implemented",
            )),
        ))
    }

    async fn vote(
        &mut self,
        req: VoteRequest<AppTypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<VoteResponse<AppTypeConfig>, RPCError<AppTypeConfig>> {
        tracing::info!(
            "QuinnNetwork::vote() called: self_id={} target={} vote={:?}",
            self.self_id,
            self.target,
            req.vote
        );

        let data = bincode::serialize(&req).map_err(|e| {
            tracing::error!(
                "Failed to serialize vote request {}->{}: {}",
                self.self_id,
                self.target,
                e
            );
            RPCError::Network(openraft::error::NetworkError::new(&e))
        })?;

        tracing::debug!("Sending vote RPC {}->{}", self.self_id, self.target);
        let resp_payload = self.send_openraft("vote", data).await.map_err(|e| {
            tracing::error!("Vote RPC {}->{} failed: {}", self.self_id, self.target, e);
            RPCError::Unreachable(openraft::error::Unreachable::new(&io::Error::new(
                io::ErrorKind::Other,
                e,
            )))
        })?;

        tracing::debug!("Received vote response {}->{}", self.self_id, self.target);
        match resp_payload {
            ResponsePayload::OpenRaft { kind, data } if kind == "vote" => {
                bincode::deserialize(&data).map_err(|e| {
                    tracing::error!(
                        "Failed to deserialize vote response {}->{}: {}",
                        self.self_id,
                        self.target,
                        e
                    );
                    RPCError::Network(openraft::error::NetworkError::new(&e))
                })
            }
            other => {
                tracing::error!(
                    "Unexpected vote response {}->{}: {:?}",
                    self.self_id,
                    self.target,
                    other
                );
                Err(RPCError::Unreachable(openraft::error::Unreachable::new(
                    &io::Error::new(
                        io::ErrorKind::Other,
                        format!("unexpected response: {:?}", other),
                    ),
                )))
            }
        }
    }
}

pub struct QuinnNetworkFactory {
    rpc: Arc<RpcHandler>,
    peer_addrs: Arc<tokio::sync::RwLock<std::collections::HashMap<AppNodeId, SocketAddr>>>,
    self_id: AppNodeId,
    cluster_namespace: Arc<String>,
    #[cfg(feature = "openraft-filters")]
    filters: Arc<OpenRaftFilters>,
}

impl QuinnNetworkFactory {
    pub fn new(
        rpc: Arc<RpcHandler>,
        peer_addrs: Arc<tokio::sync::RwLock<std::collections::HashMap<AppNodeId, SocketAddr>>>,
        self_id: AppNodeId,
        cluster_namespace: Arc<String>,
        #[cfg(feature = "openraft-filters")] filters: Arc<OpenRaftFilters>,
    ) -> Self {
        Self {
            rpc,
            peer_addrs,
            self_id,
            cluster_namespace,
            #[cfg(feature = "openraft-filters")]
            filters,
        }
    }
}

impl RaftNetworkFactory<AppTypeConfig> for QuinnNetworkFactory {
    type Network = QuinnNetwork;

    async fn new_client(
        &mut self,
        target: AppNodeId,
        _node: &openraft::impls::BasicNode,
    ) -> Self::Network {
        QuinnNetwork::new(
            Arc::clone(&self.rpc),
            Arc::clone(&self.peer_addrs),
            self.self_id,
            target,
            Arc::clone(&self.cluster_namespace),
            #[cfg(feature = "openraft-filters")]
            Arc::clone(&self.filters),
        )
    }
}

#[cfg(feature = "openraft-filters")]
pub struct OpenRaftFilters {
    pub(crate) drop_pairs: tokio::sync::RwLock<std::collections::HashSet<(AppNodeId, AppNodeId)>>,
    pub(crate) delay_pairs:
        tokio::sync::RwLock<std::collections::HashMap<(AppNodeId, AppNodeId), Duration>>,
    pub(crate) partitions: tokio::sync::RwLock<
        Vec<(
            std::collections::HashSet<AppNodeId>,
            std::collections::HashSet<AppNodeId>,
        )>,
    >,
}

#[cfg(feature = "openraft-filters")]
impl OpenRaftFilters {
    pub fn new() -> Self {
        Self {
            drop_pairs: tokio::sync::RwLock::new(Default::default()),
            delay_pairs: tokio::sync::RwLock::new(Default::default()),
            partitions: tokio::sync::RwLock::new(vec![]),
        }
    }

    pub async fn clear(&self) {
        self.drop_pairs.write().await.clear();
        self.delay_pairs.write().await.clear();
        self.partitions.write().await.clear();
    }
}
