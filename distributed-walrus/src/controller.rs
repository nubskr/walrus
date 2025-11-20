use crate::bucket::BucketService;
use crate::metadata::{MetadataStateMachine, NodeId};
use crate::rpc::{InternalOp, InternalResp};
use anyhow::Result;
use bytes::Bytes;
use octopii::rpc::{RequestPayload, ResponsePayload};
use octopii::OctopiiNode;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

pub struct NodeController {
    pub node_id: NodeId,
    pub bucket: Arc<BucketService>,
    pub metadata: Arc<MetadataStateMachine>,
    pub raft: Arc<OctopiiNode>,
}

impl NodeController {
    pub async fn handle_rpc(&self, op: InternalOp) -> InternalResp {
        tracing::info!("handle_rpc: processing op {:?}", op);
        match op {
            InternalOp::ForwardAppend { wal_key, data } => {
                self.sync_leases_now().await;
                match self.bucket.append_by_key(&wal_key, data).await {
                    Ok(_) => {
                        tracing::info!("handle_rpc: append success for {}", wal_key);
                        InternalResp::Ok
                    }
                    Err(e) => {
                        tracing::error!("handle_rpc: append failed for {}: {}", wal_key, e);
                        InternalResp::Error(e.to_string())
                    }
                }
            }
            InternalOp::ForwardRead { wal_key, max_bytes } => {
                match self.bucket.read_by_key(&wal_key, max_bytes).await {
                    Ok(entries) => {
                        let data = entries.into_iter().map(|e| e.data).collect();
                        InternalResp::ReadResult(data)
                    }
                    Err(e) => InternalResp::Error(e.to_string()),
                }
            }
            InternalOp::JoinCluster { node_id, addr } => {
                tracing::info!("Received JoinCluster request from node {} at {}", node_id, addr);
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
                        // Spawn task to promote learner once caught up
                        let raft_clone = self.raft.clone();
                        let node_id_clone = node_id;
                        tokio::spawn(async move {
                            // Poll until caught up
                            for _ in 0..20 {
                                // check every 500ms for 10s
                                tokio::time::sleep(Duration::from_millis(500)).await;
                                match raft_clone.is_learner_caught_up(node_id_clone).await {
                                    Ok(true) => {
                                        tracing::info!("Node {} caught up, promoting...", node_id_clone);
                                        if let Err(e) = raft_clone.promote_learner(node_id_clone).await {
                                            tracing::error!("Failed to promote node {}: {}", node_id_clone, e);
                                        } else {
                                            tracing::info!("Node {}: Promoted learner {}", raft_clone.id(), node_id_clone);
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
    }

    pub async fn route_and_append(&self, topic: &str, partition: u32, data: Vec<u8>) -> Result<()> {
        let (leader, generation) = self
            .metadata
            .get_partition_leader(topic, partition)
            .ok_or_else(|| anyhow::anyhow!("unknown topic/partition"))?;
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
            if let Err(e) = self.bucket.append_by_key(&key, data).await {
                tracing::error!("local append failed: {}", e);
                return Err(e);
            }
        } else {
            let addr = self
                .raft
                .peer_addr_for(leader)
                .await
                .ok_or_else(|| anyhow::anyhow!("missing peer address for {}", leader))?;
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
                    Duration::from_secs(5),
                )
                .await?;
            match resp.payload {
                ResponsePayload::CustomResponse { data, .. } => {
                    match bincode::deserialize::<InternalResp>(&data) {
                        Ok(InternalResp::Ok) => {}
                        Ok(InternalResp::Error(e)) => return Err(anyhow::anyhow!(e)),
                        Ok(other) => {
                            warn!("unexpected response {:?}", other);
                        }
                        Err(e) => return Err(anyhow::anyhow!(e)),
                    }
                }
                other => {
                    return Err(anyhow::anyhow!("unexpected response: {:?}", other));
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
    ) -> Result<Vec<Vec<u8>>> {
        let (leader, generation) = self
            .metadata
            .get_partition_leader(topic, partition)
            .ok_or_else(|| anyhow::anyhow!("unknown topic/partition"))?;
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
            Ok(entries.into_iter().map(|e| e.data).collect())
        } else {
            let addr = self
                .raft
                .peer_addr_for(leader)
                .await
                .ok_or_else(|| anyhow::anyhow!("missing peer address for {}", leader))?;
            tracing::info!("forwarding read to leader {} at {}", leader, addr);
            let op = InternalOp::ForwardRead {
                wal_key: key,
                max_bytes,
            };
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
                    Duration::from_secs(5),
                )
                .await?;
            match resp.payload {
                ResponsePayload::CustomResponse { data, .. } => {
                    match bincode::deserialize::<InternalResp>(&data) {
                        Ok(InternalResp::ReadResult(data)) => Ok(data),
                        Ok(InternalResp::Error(e)) => Err(anyhow::anyhow!(e)),
                        Ok(other) => {
                            warn!("unexpected response {:?}", other);
                            Err(anyhow::anyhow!("unexpected response {:?}", other))
                        }
                        Err(e) => Err(anyhow::anyhow!(e)),
                    }
                }
                other => Err(anyhow::anyhow!("unexpected response: {:?}", other)),
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
}

pub fn wal_key(topic: &str, partition: u32, generation: u64) -> String {
    format!("t_{}_p_{}_g_{}", topic, partition, generation)
}
