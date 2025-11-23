use anyhow::Result;
use bytes::Bytes;
use octopii::rpc::RequestPayload;

use crate::controller::{NodeController, TopicPartition};
use crate::rpc::{InternalOp, InternalResp};

impl NodeController {
    pub async fn route_and_append(&self, topic: &str, partition: u32, data: Vec<u8>) -> Result<()> {
        let tp = TopicPartition::new(topic, partition);
        let (leader, generation) = self
            .metadata
            .get_partition_leader(topic, partition)
            .ok_or_else(|| anyhow::anyhow!("unknown topic/partition"))?;
        let key = tp.wal_key(generation);
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
            self.sync_leases_now().await;
            let bytes = data.len() as u64;
            self.append_with_retry(&key, data).await?;
            self.record_append(&key, bytes).await;
            return Ok(());
        }

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
                self.config.forward_timeout,
            )
            .await?;
        match resp.payload {
            octopii::rpc::ResponsePayload::CustomResponse { data, .. } => {
                match bincode::deserialize::<InternalResp>(&data) {
                    Ok(InternalResp::Ok) => Ok(()),
                    Ok(InternalResp::Error(e)) => Err(anyhow::anyhow!(e)),
                    Ok(other) => {
                        tracing::warn!("unexpected response {:?}", other);
                        Ok(())
                    }
                    Err(e) => Err(anyhow::anyhow!(e)),
                }
            }
            other => Err(anyhow::anyhow!("unexpected response: {:?}", other)),
        }
    }
}
