use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::fs;
use tracing::{error, info};

// Monitor: background loop that watches segment sizes (rollover) and retention (GC) using
// metadata for ownership and controller for watermarks. It never mutates data directly—only
// proposes metadata commands and deletes expired segment dirs.

use crate::bucket::DATA_NAMESPACE;
use crate::config::NodeConfig;
use crate::controller::{wal_key, NodeController};
use crate::fs_utils::{dir_size, remove_segment_dir, walrus_path_for_key};
use crate::metadata::MetadataCmd;

const DEFAULT_CHECK_INTERVAL: Duration = Duration::from_secs(10);
const DEFAULT_MAX_SEGMENT_SIZE: u64 = 1_000_000_000;
const DEFAULT_RETENTION_BYTES: u64 = 1_000_000_000;

#[derive(Clone, Copy)]
struct MonitorLimits {
    check_interval: Duration,
    max_segment_size: u64,
    retention_bytes: u64,
}

impl MonitorLimits {
    fn load() -> Self {
        Self {
            check_interval: check_interval(),
            max_segment_size: max_segment_size(),
            retention_bytes: retention_bytes(),
        }
    }
}

pub struct Monitor {
    controller: Arc<NodeController>,
    config: NodeConfig,
    limits: MonitorLimits,
}

impl Monitor {
    pub fn new(controller: Arc<NodeController>, config: NodeConfig) -> Self {
        Self {
            controller,
            config,
            limits: MonitorLimits::load(),
        }
    }

    pub async fn run(self) {
        info!("Monitor loop started");
        let mut interval = tokio::time::interval(self.limits.check_interval);
        loop {
            interval.tick().await;
            if let Err(e) = self.tick().await {
                error!("monitor tick failed: {e}");
            }
        }
    }

    async fn tick(&self) -> Result<()> {
        if self
            .controller
            .test_fail_monitor
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            anyhow::bail!("monitor in forced error mode");
        }
        self.check_rollovers().await?;
        self.run_gc().await
    }

    async fn check_rollovers(&self) -> Result<()> {
        if self
            .controller
            .test_fail_dir_size
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            error!("dir_size forced failure");
            return Ok(());
        }

        let metrics = self.controller.raft.raft_metrics();
        let membership = metrics.membership_config.membership();
        let mut voters = std::collections::BTreeSet::new();
        for config in membership.get_joint_config() {
            voters.extend(config.iter());
        }
        let nodes: Vec<u64> = voters.into_iter().copied().collect();

        let assignments = self
            .controller
            .metadata
            .assignments_for_node(self.controller.node_id);

        for (topic, partition, generation) in assignments {
            let wal = wal_key(&topic, partition, generation);

            let disk_size = self.controller.bucket.get_topic_size_blocking(&wal);

            let logical_head_start = self
                .controller
                .metadata
                .get_partition_state(&topic, partition)
                .and_then(|p| p.history.last().map(|h| h.end_offset))
                .unwrap_or(0);
            let logical_watermark = self
                .controller
                .partition_high_watermark(&topic, partition)
                .await;
            let logical_size = logical_watermark.saturating_sub(logical_head_start);

            // Use the larger of disk vs. logical offsets to decide rollover and record size.
            let size = disk_size.max(logical_size);

            if size <= self.limits.max_segment_size {
                continue;
            }

            info!(
                "Segment {} size {} exceeds limit {}, proposing rollover",
                wal, size, self.limits.max_segment_size,
            );

            let current_idx = nodes
                .iter()
                .position(|&id| id == self.controller.node_id)
                .unwrap_or(0);
            
            let next_leader = if nodes.is_empty() {
                self.controller.node_id
            } else {
                nodes[(current_idx + 1) % nodes.len()]
            };

            tracing::info!(
                "Rollover for {} p{}: voters={:?}, current={} idx={}, next_leader={}",
                topic,
                partition,
                nodes,
                self.controller.node_id,
                current_idx,
                next_leader
            );

            let cmd = MetadataCmd::RolloverPartition {
                name: topic.clone(),
                partition,
                new_leader: next_leader,
                sealed_segment_size_bytes: size,
            };
            let payload = bincode::serialize(&cmd)?;
            self.controller.raft.propose(payload).await?;
        }

        Ok(())
    }

    async fn run_gc(&self) -> Result<()> {
        if self
            .controller
            .test_fail_gc
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Err(anyhow::anyhow!("forced gc failure"));
        }
        let state = self.controller.metadata.snapshot_state();
        let root = self.wal_root();

        for (topic, info) in state.topics {
            for (partition, part_state) in info.partition_states {
                let high_watermark = self
                    .controller
                    .partition_high_watermark(&topic, partition)
                    .await;
                if high_watermark == 0 {
                    continue;
                }
                let cutoff = high_watermark.saturating_sub(self.limits.retention_bytes);
                let mut max_trim: Option<u64> = None;
                for segment in part_state.history.iter().filter(|seg| {
                    seg.stored_on_node == self.controller.node_id && seg.end_offset < cutoff
                }) {
                    let key = wal_key(&topic, partition, segment.generation);
                    let path = walrus_path_for_key(&root, &key);
                    if fs::metadata(&path).await.is_ok() {
                        info!(
                            "Removing expired Walrus segment {} (end_offset {}, cutoff {})",
                            key, segment.end_offset, cutoff
                        );
                        remove_segment_dir(&path).await?;
                        let candidate = segment.generation;
                        max_trim = Some(max_trim.map(|g| g.max(candidate)).unwrap_or(candidate));
                    }
                }

                if let Some(up_to) = max_trim {
                    let cmd = MetadataCmd::TrimHistory {
                        name: topic.clone(),
                        partition,
                        up_to_generation: up_to,
                    };
                    let payload = bincode::serialize(&cmd)?;
                    let _ = self.controller.raft.propose(payload).await;
                }
            }
        }
        Ok(())
    }

    fn wal_root(&self) -> std::path::PathBuf {
        self.config.data_wal_dir().join(DATA_NAMESPACE)
    }
}

fn check_interval() -> Duration {
    if let Ok(ms) = std::env::var("WALRUS_MONITOR_CHECK_MS") {
        if let Ok(parsed) = ms.parse::<u64>() {
            return Duration::from_millis(parsed.max(10));
        }
    }
    DEFAULT_CHECK_INTERVAL
}

fn max_segment_size() -> u64 {
    if let Ok(val) = std::env::var("WALRUS_MAX_SEGMENT_BYTES") {
        if let Ok(parsed) = val.parse::<u64>() {
            return parsed.max(1);
        }
    }
    DEFAULT_MAX_SEGMENT_SIZE
}

fn retention_bytes() -> u64 {
    if let Ok(val) = std::env::var("WALRUS_RETENTION_BYTES") {
        if let Ok(parsed) = val.parse::<u64>() {
            return parsed;
        }
    }
    if let Ok(val) = std::env::var("WALRUS_RETENTION_GENERATIONS") {
        if let Ok(parsed) = val.parse::<u64>() {
            return parsed.saturating_mul(max_segment_size());
        }
    }
    DEFAULT_RETENTION_BYTES
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn check_interval_and_limits_respect_env_overrides() {
        let _guard = env_lock().lock().unwrap();
        std::env::set_var("WALRUS_MONITOR_CHECK_MS", "5");
        std::env::set_var("WALRUS_MAX_SEGMENT_BYTES", "42");
        std::env::set_var("WALRUS_RETENTION_BYTES", "3");

        assert_eq!(check_interval(), Duration::from_millis(10)); // clamped to minimum
        assert_eq!(max_segment_size(), 42);
        assert_eq!(retention_bytes(), 3);

        std::env::remove_var("WALRUS_MONITOR_CHECK_MS");
        std::env::remove_var("WALRUS_MAX_SEGMENT_BYTES");
        std::env::remove_var("WALRUS_RETENTION_BYTES");
    }

    #[test]
    fn defaults_apply_for_invalid_env_values() {
        let _guard = env_lock().lock().unwrap();
        std::env::set_var("WALRUS_MONITOR_CHECK_MS", "bogus");
        std::env::set_var("WALRUS_MAX_SEGMENT_BYTES", "notanumber");
        std::env::set_var("WALRUS_RETENTION_BYTES", "-1");

        assert_eq!(check_interval(), DEFAULT_CHECK_INTERVAL);
        assert_eq!(max_segment_size(), DEFAULT_MAX_SEGMENT_SIZE);
        assert_eq!(retention_bytes(), DEFAULT_RETENTION_BYTES);

        std::env::remove_var("WALRUS_MONITOR_CHECK_MS");
        std::env::remove_var("WALRUS_MAX_SEGMENT_BYTES");
        std::env::remove_var("WALRUS_RETENTION_BYTES");
    }
}
