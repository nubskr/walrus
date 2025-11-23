use crate::bucket::BucketService;
use crate::metadata::{MetadataStateMachine, NodeId, PartitionState, SegmentRecord};
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

    // Each WAL entry reserves a fixed prefix (matches wal::config::PREFIX_META_SIZE).
    const ENTRY_OVERHEAD: u64 = 256;

    pub async fn handle_rpc(&self, op: InternalOp) -> InternalResp {
        tracing::info!("handle_rpc: processing op {:?}", op);
        match op {
            InternalOp::ForwardAppend { wal_key, data } => {
                self.sync_leases_now().await;
                let data_len = data.len() as u64 + Self::ENTRY_OVERHEAD;
                match self.append_with_retry(&wal_key, data).await {
                    Ok(_) => {
                        tracing::info!("handle_rpc: append success for {}", wal_key);
                        self.record_append(&wal_key, data_len).await;
                        InternalResp::Ok
                    }
                    Err(e) => {
                        tracing::error!("handle_rpc: append failed for {}: {}", wal_key, e);
                        InternalResp::Error(e.to_string())
                    }
                }
            }
            InternalOp::ForwardRead {
                wal_key,
                start_offset,
                max_bytes,
            } => {
                if self.test_fail_forward_read.load(Ordering::Relaxed) {
                    return InternalResp::Error("forced forward read failure".into());
                }
                let Some((topic, partition, _)) = parse_wal_key(&wal_key) else {
                    return InternalResp::Error("invalid wal_key".into());
                };
                match self
                    .read_entries_from_logical_offset(&wal_key, start_offset, max_bytes)
                    .await
                {
                    Ok(entries) => {
                        let high_watermark = self.partition_high_watermark(&topic, partition).await;
                        InternalResp::ReadResult {
                            data: entries,
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
            let bytes = data.len() as u64 + Self::ENTRY_OVERHEAD;
            self.append_with_retry(&key, data).await?;
            self.record_append(&key, bytes).await;
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
        req_offset: u64,
        max_bytes: usize,
    ) -> Result<(Vec<Vec<u8>>, u64)> {
        let state = self
            .metadata
            .get_partition_state(topic, partition)
            .ok_or_else(|| anyhow!("unknown topic/partition"))?;

        // Snapshot the observed end offset (metadata vs. actual bytes) for each history segment on this node.
        let mut history_actual_ends: Vec<u64> = Vec::with_capacity(state.history.len());
        for seg in &state.history {
            let end = if seg.stored_on_node == self.node_id {
                let key = wal_key(topic, partition, seg.generation);
                let actual = self.bucket.get_topic_size_blocking(&key);
                self.update_tracked_if_smaller(&key, actual).await;
                seg.start_offset + actual
            } else {
                seg.end_offset
            };
            history_actual_ends.push(end);
        }

        // Check history segments
        for (i, seg) in state.history.iter().enumerate() {
            // Adjust the logical start of this segment if the previous segment (on this node) actually extended further.
            let mut prev_actual_end: Option<u64> = None;
            if i > 0 {
                prev_actual_end = Some(history_actual_ends[i - 1]);
            }

            // Compute this segment's actual end if we have local bytes; otherwise fall back to metadata.
            let seg_len = if seg.stored_on_node == self.node_id {
                history_actual_ends[i].saturating_sub(seg.start_offset)
            } else {
                seg.end_offset.saturating_sub(seg.start_offset)
            };
            let actual_end = seg.start_offset + seg_len;
            let corrected_start = seg.start_offset.max(prev_actual_end.unwrap_or(0));

            // If the previous segment completely covers this one, skip it.
            if corrected_start >= actual_end {
                continue;
            }

            // If the previous segment actually extends into this one's metadata start, redirect before the normal range check.
            if let Some(prev_end) = prev_actual_end {
                if req_offset < prev_end && req_offset >= state.history[i - 1].start_offset {
                    tracing::info!(
                        "Gap Fix (History pre-range): req_offset={} < prev_actual_end={} (prev_gen={}, start={}, meta_end={})",
                        req_offset,
                        prev_end,
                        state.history[i - 1].generation,
                        state.history[i - 1].start_offset,
                        state.history[i - 1].end_offset
                    );
                    let local_offset = req_offset - state.history[i - 1].start_offset;
                    return self
                        .read_historical_segment(
                            topic,
                            partition,
                            &state,
                            &state.history[i - 1],
                            local_offset,
                            max_bytes,
                        )
                        .await;
                }
            }

            if req_offset >= corrected_start && req_offset < actual_end {
                let local_offset = req_offset - corrected_start;
                return self
                    .read_historical_segment(topic, partition, &state, seg, local_offset, max_bytes)
                    .await;
            }
        }

        let head_meta_end = state.history.last().map(|s| s.end_offset).unwrap_or(0);
        let last_actual_end = if let Some(last_seg) = state.history.last() {
            if last_seg.stored_on_node == self.node_id {
                let key = wal_key(topic, partition, last_seg.generation);
                let last_len = self.bucket.get_topic_size_blocking(&key);
                self.update_tracked_if_smaller(&key, last_len).await;
                let mut end = last_seg.start_offset + last_len;
                if state.history.len() >= 2 {
                    let prev = &state.history[state.history.len() - 2];
                    let prev_key = wal_key(topic, partition, prev.generation);
                    let prev_len = if prev.stored_on_node == self.node_id {
                        let len = self.bucket.get_topic_size_blocking(&prev_key);
                        self.update_tracked_if_smaller(&prev_key, len).await;
                        len
                    } else {
                        prev.end_offset.saturating_sub(prev.start_offset)
                    };
                    let prev_end = prev.start_offset + prev_len;
                    if prev_end > end {
                        end = prev_end;
                    }
                }
                Some(end)
            } else {
                None
            }
        } else {
            None
        };

        if req_offset >= head_meta_end {
            // Gap Fix for Head
            if let Some(last_seg) = state.history.last() {
                if last_seg.stored_on_node == self.node_id {
                    let actual_end = last_actual_end.unwrap_or_else(|| {
                        last_seg.start_offset + (last_seg.end_offset - last_seg.start_offset)
                    });

                    if req_offset < actual_end {
                        tracing::info!("Gap Fix (Head): req_offset={} falls in gap of last_gen={} (start={}, meta_end={}, actual_end={}). Redirecting.", 
                            req_offset, last_seg.generation, last_seg.start_offset, last_seg.end_offset, actual_end);
                        let local_offset = req_offset - last_seg.start_offset;
                        return self
                            .read_historical_segment(
                                topic,
                                partition,
                                &state,
                                last_seg,
                                local_offset,
                                max_bytes,
                            )
                            .await;
                    } else {
                        tracing::debug!(
                            "Gap Fix (Head): req_offset={} NOT in last_gen={} (actual_end={}).",
                            req_offset,
                            last_seg.generation,
                            actual_end
                        );
                    }
                }
            }

            let adjusted_head_start = last_actual_end.unwrap_or(head_meta_end).max(head_meta_end);
            let local_offset = req_offset - adjusted_head_start;
            return self
                .read_active_head(topic, partition, &state, local_offset, max_bytes)
                .await;
        }

        Ok((
            Vec::new(),
            self.partition_high_watermark(topic, partition).await,
        ))
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

    async fn read_active_head(
        &self,
        topic: &str,
        partition: u32,
        state: &PartitionState,
        local_offset: u64,
        max_bytes: usize,
    ) -> Result<(Vec<Vec<u8>>, u64)> {
        let leader = state.leader_node;
        let wal_key = wal_key(topic, partition, state.current_generation);
        tracing::info!(
            "route_and_read head topic={} partition={} leader={} gen={} key={} offset={}",
            topic,
            partition,
            leader,
            state.current_generation,
            wal_key,
            local_offset
        );

        if leader == self.node_id {
            tracing::info!("reading head locally on node {}", self.node_id);
            let entries = self
                .read_entries_from_logical_offset(&wal_key, local_offset, max_bytes)
                .await?;
            let watermark = self.partition_high_watermark(topic, partition).await;
            return Ok((entries, watermark));
        }

        let addr = self
            .raft
            .peer_addr_for(leader)
            .await
            .ok_or_else(|| anyhow!("missing peer address for {}", leader))?;
        tracing::info!("forwarding read to leader {} at {}", leader, addr);
        let op = InternalOp::ForwardRead {
            wal_key: wal_key.clone(),
            start_offset: local_offset,
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
                let watermark = self.partition_high_watermark(topic, partition).await;
                return Ok((Vec::new(), watermark));
            }
            Err(_) => {
                warn!(
                    "forward read to leader {} at {} timed out after {:?}",
                    leader,
                    addr,
                    Self::forward_timeout()
                );
                let watermark = self.partition_high_watermark(topic, partition).await;
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
                        Ok((
                            Vec::new(),
                            self.partition_high_watermark(topic, partition).await,
                        ))
                    }
                    Ok(other) => {
                        warn!("unexpected response {:?}", other);
                        Ok((
                            Vec::new(),
                            self.partition_high_watermark(topic, partition).await,
                        ))
                    }
                    Err(e) => {
                        warn!("failed to decode forward read response: {}", e);
                        Ok((
                            Vec::new(),
                            self.partition_high_watermark(topic, partition).await,
                        ))
                    }
                }
            }
            other => {
                warn!("unexpected response: {:?}", other);
                Ok((
                    Vec::new(),
                    self.partition_high_watermark(topic, partition).await,
                ))
            }
        }
    }

    async fn read_historical_segment(
        &self,
        topic: &str,
        partition: u32,
        _state: &PartitionState,
        segment: &SegmentRecord,
        local_offset: u64,
        max_bytes: usize,
    ) -> Result<(Vec<Vec<u8>>, u64)> {
        let wal_key = wal_key(topic, partition, segment.generation);
        tracing::info!(
            "route_and_read history topic={} partition={} gen={} node={} key={} offset={} end_offset={}",
            topic,
            partition,
            segment.generation,
            segment.stored_on_node,
            wal_key,
            local_offset,
            segment.end_offset
        );

        if segment.stored_on_node == self.node_id {
            let entries = self
                .read_entries_from_logical_offset(&wal_key, local_offset, max_bytes)
                .await?;
            tracing::info!(
                "read_historical_segment local: key={} entries={}",
                wal_key,
                entries.len()
            );
            let watermark = self.partition_high_watermark(topic, partition).await;
            return Ok((entries, watermark));
        }

        let addr = self
            .raft
            .peer_addr_for(segment.stored_on_node)
            .await
            .ok_or_else(|| anyhow!("missing peer address for {}", segment.stored_on_node))?;

        let op = InternalOp::ForwardRead {
            wal_key: wal_key.clone(),
            start_offset: local_offset,
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
                    "forward read to node {} at {} failed: {}",
                    segment.stored_on_node, addr, e
                );
                let watermark = self.partition_high_watermark(topic, partition).await;
                return Ok((Vec::new(), watermark));
            }
            Err(_) => {
                warn!(
                    "forward read to node {} at {} timed out after {:?}",
                    segment.stored_on_node,
                    addr,
                    Self::forward_timeout()
                );
                let watermark = self.partition_high_watermark(topic, partition).await;
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
                            "forward read to node {} at {} returned error: {}",
                            segment.stored_on_node, addr, e
                        );
                        Ok((
                            Vec::new(),
                            self.partition_high_watermark(topic, partition).await,
                        ))
                    }
                    Ok(other) => {
                        warn!("unexpected response {:?}", other);
                        Ok((
                            Vec::new(),
                            self.partition_high_watermark(topic, partition).await,
                        ))
                    }
                    Err(e) => {
                        warn!("failed to decode forward read response: {}", e);
                        Ok((
                            Vec::new(),
                            self.partition_high_watermark(topic, partition).await,
                        ))
                    }
                }
            }
            other => {
                warn!("unexpected response: {:?}", other);
                Ok((
                    Vec::new(),
                    self.partition_high_watermark(topic, partition).await,
                ))
            }
        }
    }

    pub async fn partition_high_watermark(&self, topic: &str, partition: u32) -> u64 {
        let Some(state) = self.metadata.get_partition_state(topic, partition) else {
            return 0;
        };
        let head_start_meta = state.history.last().map(|s| s.end_offset).unwrap_or(0);
        let head_start = if let Some(last_seg) = state.history.last() {
            if last_seg.stored_on_node == self.node_id {
                let last_key = wal_key(topic, partition, last_seg.generation);
                let last_len = self.bucket.get_topic_size_blocking(&last_key);
                self.update_tracked_if_smaller(&last_key, last_len).await;
                let mut corrected_start = last_seg.start_offset;
                let mut actual_end = last_seg.start_offset + last_len;
                if state.history.len() >= 2 {
                    let prev = &state.history[state.history.len() - 2];
                    if prev.stored_on_node == self.node_id {
                        let prev_key = wal_key(topic, partition, prev.generation);
                        let prev_len = self.bucket.get_topic_size_blocking(&prev_key);
                        self.update_tracked_if_smaller(&prev_key, prev_len).await;
                        let prev_end = prev.start_offset + prev_len;
                        corrected_start = corrected_start.max(prev_end);
                        actual_end = actual_end.max(prev_end);
                    }
                }

                actual_end.max(head_start_meta)
            } else {
                head_start_meta
            }
        } else {
            head_start_meta
        };
        let key = wal_key(topic, partition, state.current_generation);
        let active_bytes = self.active_bytes_for_key(&key).await;
        head_start + active_bytes
    }

    async fn map_logical_to_physical_offset(
        &self,
        wal_key: &str,
        logical_offset: u64,
    ) -> Result<(u64, usize)> {
        if logical_offset == 0 {
            return Ok((0, 0));
        }

        let total_physical = self.bucket.get_topic_size_blocking(wal_key);
        let mut physical_cursor = 0u64;
        let mut logical_cursor = 0u64;

        // Cap per-read window to avoid loading huge logs at once while still progressing.
        const SCAN_WINDOW: usize = 16 * 1024 * 1024;

        while physical_cursor < total_physical {
            let remaining = (total_physical - physical_cursor) as usize;
            let max_bytes = remaining
                .min(SCAN_WINDOW)
                .max(Self::ENTRY_OVERHEAD as usize + 1);
            let entries = self
                .bucket
                .read_by_key_from_offset(wal_key, physical_cursor, max_bytes)
                .await?;
            if entries.is_empty() {
                break;
            }

            for entry in entries {
                let payload_len = entry.data.len() as u64;

                if logical_offset < logical_cursor + payload_len {
                    let trim = (logical_offset - logical_cursor) as usize;
                    return Ok((physical_cursor, trim));
                }

                logical_cursor += payload_len;
                physical_cursor += payload_len + Self::ENTRY_OVERHEAD;

                if logical_offset == logical_cursor {
                    return Ok((physical_cursor, 0));
                }
            }
        }

        Ok((physical_cursor, 0))
    }

    async fn read_entries_from_logical_offset(
        &self,
        wal_key: &str,
        logical_offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<Vec<u8>>> {
        let (physical_offset, trim) = self
            .map_logical_to_physical_offset(wal_key, logical_offset)
            .await?;

        let mut entries = self
            .bucket
            .read_by_key_from_offset(wal_key, physical_offset, max_bytes)
            .await?;

        if trim > 0 {
            if let Some(first) = entries.first_mut() {
                if trim >= first.data.len() {
                    first.data.clear();
                } else {
                    first.data.drain(0..trim);
                }
            }
        }

        Ok(entries.into_iter().map(|e| e.data).collect())
    }

    async fn active_bytes_for_key(&self, wal_key: &str) -> u64 {
        let tracked = {
            let guard = self.offsets.read().await;
            guard.get(wal_key).copied().unwrap_or(0)
        };
        // Trust the WAL engine as the source of truth; the tracked map can drift if rollovers race with writes.
        let actual = self.bucket.get_topic_size_blocking(wal_key);
        let best = actual.max(tracked);
        if actual > tracked {
            self.update_tracked_if_smaller(wal_key, actual).await;
        }
        best
    }

    async fn record_append(&self, wal_key: &str, bytes: u64) -> u64 {
        let mut guard = self.offsets.write().await;
        let entry = guard.entry(wal_key.to_string()).or_insert(0);
        *entry += bytes;
        *entry
    }

    async fn update_tracked_if_smaller(&self, wal_key: &str, bytes: u64) {
        let mut guard = self.offsets.write().await;
        let entry = guard.entry(wal_key.to_string()).or_insert(0);
        if *entry < bytes {
            *entry = bytes;
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
