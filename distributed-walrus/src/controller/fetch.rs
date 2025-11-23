use crate::controller::{NodeController, TopicPartition};
use crate::metadata::PartitionState;
use crate::rpc::{InternalOp, InternalResp};
use anyhow::Result;
use bytes::Bytes;
use octopii::rpc::RequestPayload;
use tracing::warn;

impl NodeController {
    pub async fn handle_fetch(
        &self,
        topic: &str,
        partition: u32,
        req_offset: u64,
        max_bytes: usize,
    ) -> Result<(Vec<Vec<u8>>, u64)> {
        let tp = TopicPartition::new(topic, partition);
        let state = self
            .metadata
            .get_partition_state(topic, partition)
            .ok_or_else(|| anyhow::anyhow!("unknown topic/partition"))?;

        let mut history_end = 0u64;
        for seg in &state.history {
            let seg_len = self.segment_logical_len(tp, seg).await;
            let seg_end = seg.start_offset + seg_len;
            history_end = history_end.max(seg_end);

            if req_offset >= seg.start_offset && req_offset < seg_end {
                let local_offset = req_offset - seg.start_offset;
                return self
                    .read_historical_segment(tp, seg, local_offset, max_bytes)
                    .await;
            }
        }

        if req_offset < history_end {
            let watermark = self.partition_high_watermark(tp.topic, tp.partition).await;
            return Ok((Vec::new(), watermark));
        }

        let local_offset = req_offset - history_end;
        self.read_active_head(tp, &state, local_offset, max_bytes, history_end)
            .await
    }

    pub(super) async fn forward_read(
        &self,
        wal_key: &str,
        start_offset: u64,
        max_bytes: usize,
    ) -> InternalResp {
        if self
            .test_fail_forward_read
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return InternalResp::Error("forced forward read failure".into());
        }
        let Some(parts) = crate::controller::parse_wal_key(wal_key) else {
            return InternalResp::Error("invalid wal_key".into());
        };
        match self
            .logical_wal(wal_key)
            .read(start_offset, max_bytes)
            .await
        {
            Ok(entries) => {
                let high_watermark = self
                    .partition_high_watermark(&parts.topic, parts.partition)
                    .await;
                InternalResp::ReadResult {
                    data: entries,
                    high_watermark,
                }
            }
            Err(e) => InternalResp::Error(e.to_string()),
        }
    }

    pub(super) async fn read_active_head(
        &self,
        tp: TopicPartition<'_>,
        state: &PartitionState,
        local_offset: u64,
        max_bytes: usize,
        head_start: u64,
    ) -> Result<(Vec<Vec<u8>>, u64)> {
        let wal_key = tp.wal_key(state.current_generation);
        tracing::info!(
            "route_and_read head topic={} partition={} leader={} gen={} key={} offset={}",
            tp.topic,
            tp.partition,
            state.leader_node,
            state.current_generation,
            wal_key,
            local_offset
        );

        if state.leader_node == self.node_id {
            return self
                .read_local_logical(&wal_key, local_offset, max_bytes, head_start)
                .await;
        }

        self.forward_read_from(state.leader_node, &wal_key, local_offset, max_bytes, tp)
            .await
    }

    pub(super) async fn read_historical_segment(
        &self,
        tp: TopicPartition<'_>,
        segment: &crate::metadata::SegmentRecord,
        local_offset: u64,
        max_bytes: usize,
    ) -> Result<(Vec<Vec<u8>>, u64)> {
        let wal_key = tp.wal_key(segment.generation);
        tracing::info!(
            "route_and_read history topic={} partition={} gen={} node={} key={} offset={} end_offset={}",
            tp.topic,
            tp.partition,
            segment.generation,
            segment.stored_on_node,
            wal_key,
            local_offset,
            segment.end_offset
        );

        if segment.stored_on_node == self.node_id {
            return self
                .read_local_logical(&wal_key, local_offset, max_bytes, segment.start_offset)
                .await;
        }

        self.forward_read_from(
            segment.stored_on_node,
            &wal_key,
            local_offset,
            max_bytes,
            tp,
        )
        .await
    }

    async fn forward_read_from(
        &self,
        target: crate::metadata::NodeId,
        wal_key: &str,
        start_offset: u64,
        max_bytes: usize,
        tp: TopicPartition<'_>,
    ) -> Result<(Vec<Vec<u8>>, u64)> {
        let Some(addr) = self.raft.peer_addr_for(target).await else {
            anyhow::bail!("missing peer address for {}", target);
        };
        let fallback_hw = self.partition_high_watermark(tp.topic, tp.partition).await;

        let op = InternalOp::ForwardRead {
            wal_key: wal_key.to_string(),
            start_offset,
            max_bytes,
        };
        let payload = Bytes::from(bincode::serialize(&op)?);
        let forwarded = tokio::time::timeout(
            self.config.forward_timeout,
            self.raft.rpc_handler().request(
                addr,
                RequestPayload::Custom {
                    operation: "Forward".into(),
                    data: payload,
                },
                self.config.forward_timeout,
            ),
        )
        .await;

        let resp = match forwarded {
            Ok(Ok(resp)) => resp,
            Ok(Err(e)) => {
                warn!("forward read to node {} at {} failed: {}", target, addr, e);
                return Ok((Vec::new(), fallback_hw));
            }
            Err(_) => {
                warn!(
                    "forward read to node {} at {} timed out after {:?}",
                    target, addr, self.config.forward_timeout
                );
                return Ok((Vec::new(), fallback_hw));
            }
        };

        match resp.payload {
            octopii::rpc::ResponsePayload::CustomResponse { data, .. } => {
                match bincode::deserialize::<InternalResp>(&data) {
                    Ok(InternalResp::ReadResult {
                        data,
                        high_watermark,
                    }) => Ok((data, high_watermark)),
                    Ok(InternalResp::Error(e)) => {
                        warn!(
                            "forward read to node {} at {} returned error: {}",
                            target, addr, e
                        );
                        Ok((Vec::new(), fallback_hw))
                    }
                    Ok(other) => {
                        warn!("unexpected response {:?}", other);
                        Ok((Vec::new(), fallback_hw))
                    }
                    Err(e) => {
                        warn!("failed to decode forward read response: {}", e);
                        Ok((Vec::new(), fallback_hw))
                    }
                }
            }
            other => {
                warn!("unexpected response: {:?}", other);
                Ok((Vec::new(), fallback_hw))
            }
        }
    }
}
