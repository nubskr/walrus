use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tracing::{error, info};

// Monitor: background loop that watches segment sizes (rollover) using
// metadata for ownership and controller for watermarks. It never mutates data directly—only
// proposes metadata commands.

use crate::config::NodeConfig;
use crate::controller::{wal_key, NodeController};
use crate::metadata::MetadataCmd;

const DEFAULT_CHECK_INTERVAL: Duration = Duration::from_secs(10);
const DEFAULT_MAX_SEGMENT_ENTRIES: u64 = 1_000_000;

#[derive(Clone, Copy)]
struct MonitorLimits {
    check_interval: Duration,
    max_segment_entries: u64,
}

impl MonitorLimits {
    fn load() -> Self {
        Self {
            check_interval: check_interval(),
            max_segment_entries: max_segment_entries(),
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
        Ok(())
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

        let owned = self
            .controller
            .metadata
            .owned_topics(self.controller.node_id);

        for (topic, segment) in owned {
            let wal = wal_key(&topic, segment);

            let sealed_entry_count = self
                .controller
                .metadata
                .get_topic_state(&topic)
                .map(|p| p.last_sealed_entry_offset)
                .unwrap_or(0);

            let current_entry_count = self
                .controller
                .tracked_entry_count(&wal)
                .await;

            let entries_in_segment = current_entry_count.saturating_sub(sealed_entry_count);

            if entries_in_segment <= self.limits.max_segment_entries {
                continue;
            }

            info!(
                "Segment {} has {} entries (exceeds limit {}), proposing rollover",
                wal, entries_in_segment, self.limits.max_segment_entries
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
                "Rollover for {}: voters={:?}, current={} idx={}, next_leader={}",
                topic,
                nodes,
                self.controller.node_id,
                current_idx,
                next_leader
            );

            let cmd = MetadataCmd::RolloverTopic {
                name: topic.clone(),
                new_leader: next_leader,
                sealed_segment_entry_count: entries_in_segment,
            };
            let payload = bincode::serialize(&cmd)?;
            self.controller.raft.propose(payload).await?;
        }

        Ok(())
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

fn max_segment_entries() -> u64 {
    if let Ok(val) = std::env::var("WALRUS_MAX_SEGMENT_ENTRIES") {
        if let Ok(parsed) = val.parse::<u64>() {
            return parsed.max(1);
        }
    }
    DEFAULT_MAX_SEGMENT_ENTRIES
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
        std::env::set_var("WALRUS_MAX_SEGMENT_ENTRIES", "42");

        assert_eq!(check_interval(), Duration::from_millis(10)); // clamped to minimum
        assert_eq!(max_segment_entries(), 42);

        std::env::remove_var("WALRUS_MONITOR_CHECK_MS");
        std::env::remove_var("WALRUS_MAX_SEGMENT_ENTRIES");
    }

    #[test]
    fn defaults_apply_for_invalid_env_values() {
        let _guard = env_lock().lock().unwrap();
        std::env::set_var("WALRUS_MONITOR_CHECK_MS", "bogus");
        std::env::set_var("WALRUS_MAX_SEGMENT_ENTRIES", "notanumber");

        assert_eq!(check_interval(), DEFAULT_CHECK_INTERVAL);
        assert_eq!(max_segment_entries(), DEFAULT_MAX_SEGMENT_ENTRIES);

        std::env::remove_var("WALRUS_MONITOR_CHECK_MS");
        std::env::remove_var("WALRUS_MAX_SEGMENT_ENTRIES");
    }
}
