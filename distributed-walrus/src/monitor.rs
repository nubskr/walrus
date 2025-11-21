use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::fs;
use tracing::{error, info};

use crate::bucket::DATA_NAMESPACE;
use crate::config::NodeConfig;
use crate::controller::{wal_key, NodeController};
use crate::fs_utils::{dir_size, remove_segment_dir, walrus_path_for_key};
use crate::metadata::MetadataCmd;

const DEFAULT_CHECK_INTERVAL: Duration = Duration::from_secs(10);
const DEFAULT_MAX_SEGMENT_SIZE: u64 = 1_000_000_000;
const DEFAULT_RETENTION_GENERATIONS: u64 = 10;

pub struct Monitor {
    controller: Arc<NodeController>,
    config: NodeConfig,
}

impl Monitor {
    pub fn new(controller: Arc<NodeController>, config: NodeConfig) -> Self {
        Self { controller, config }
    }

    pub async fn run(self) {
        info!("Monitor loop started");
        let mut interval = tokio::time::interval(check_interval());
        loop {
            interval.tick().await;
            if self
                .controller
                .test_fail_monitor
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                error!("monitor in forced error mode");
                continue;
            }
            if let Err(e) = self.check_rollovers().await {
                error!("rollover check failed: {e}");
            }
            if let Err(e) = self.run_gc().await {
                error!("gc check failed: {e}");
            }
        }
    }

    async fn check_rollovers(&self) -> Result<()> {
        let root = self.wal_root();
        let size = if self
            .controller
            .test_fail_dir_size
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            error!("dir_size forced failure for {:?}", root);
            return Ok(());
        } else {
            match dir_size(&root).await {
                Ok(bytes) => bytes,
                Err(e) => {
                    error!("dir_size failed for {:?}: {}", root, e);
                    return Ok(());
                }
            }
        };
        if size <= max_segment_size() {
            return Ok(());
        }

        let assignments = self
            .controller
            .metadata
            .assignments_for_node(self.controller.node_id);
        for (topic, partition, _) in assignments {
            info!(
                "Namespace size {} exceeds limit {}, proposing rollover for {}-{}",
                size,
                max_segment_size(),
                topic,
                partition
            );
            let cmd = MetadataCmd::RolloverPartition {
                name: topic.clone(),
                partition,
                new_leader: self.controller.node_id,
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
                if part_state.current_generation <= retention_generations() {
                    continue;
                }
                let cutoff = part_state.current_generation - retention_generations();
                for generation in 1..cutoff {
                    let key = wal_key(&topic, partition, generation);
                    let path = walrus_path_for_key(&root, &key);
                    if fs::metadata(&path).await.is_ok() {
                        info!("Removing expired Walrus segment {}", key);
                        remove_segment_dir(&path).await?;
                    }
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

fn retention_generations() -> u64 {
    if let Ok(val) = std::env::var("WALRUS_RETENTION_GENERATIONS") {
        if let Ok(parsed) = val.parse::<u64>() {
            return parsed;
        }
    }
    DEFAULT_RETENTION_GENERATIONS
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
        std::env::set_var("WALRUS_RETENTION_GENERATIONS", "3");

        assert_eq!(check_interval(), Duration::from_millis(10)); // clamped to minimum
        assert_eq!(max_segment_size(), 42);
        assert_eq!(retention_generations(), 3);

        std::env::remove_var("WALRUS_MONITOR_CHECK_MS");
        std::env::remove_var("WALRUS_MAX_SEGMENT_BYTES");
        std::env::remove_var("WALRUS_RETENTION_GENERATIONS");
    }

    #[test]
    fn defaults_apply_for_invalid_env_values() {
        let _guard = env_lock().lock().unwrap();
        std::env::set_var("WALRUS_MONITOR_CHECK_MS", "bogus");
        std::env::set_var("WALRUS_MAX_SEGMENT_BYTES", "notanumber");
        std::env::set_var("WALRUS_RETENTION_GENERATIONS", "-1");

        assert_eq!(check_interval(), DEFAULT_CHECK_INTERVAL);
        assert_eq!(max_segment_size(), DEFAULT_MAX_SEGMENT_SIZE);
        assert_eq!(retention_generations(), DEFAULT_RETENTION_GENERATIONS);

        std::env::remove_var("WALRUS_MONITOR_CHECK_MS");
        std::env::remove_var("WALRUS_MAX_SEGMENT_BYTES");
        std::env::remove_var("WALRUS_RETENTION_GENERATIONS");
    }
}
