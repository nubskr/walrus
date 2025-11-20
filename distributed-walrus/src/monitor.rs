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

const CHECK_INTERVAL: Duration = Duration::from_secs(10);
const MAX_SEGMENT_SIZE: u64 = 1_000_000_000;
const RETENTION_GENERATIONS: u64 = 10;

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
        let mut interval = tokio::time::interval(CHECK_INTERVAL);
        loop {
            interval.tick().await;
            if let Err(e) = self.check_rollovers().await {
                error!("rollover check failed: {e}");
            }
            if let Err(e) = self.run_gc().await {
                error!("gc check failed: {e}");
            }
        }
    }

    async fn check_rollovers(&self) -> Result<()> {
        let assignments = self
            .controller
            .metadata
            .assignments_for_node(self.controller.node_id);
        let root = self.wal_root();

        for (topic, partition, generation) in assignments {
            let key = wal_key(&topic, partition, generation);
            let path = walrus_path_for_key(&root, &key);
            if fs::metadata(&path).await.is_err() {
                continue;
            }

            let size = match dir_size(&path).await {
                Ok(bytes) => bytes,
                Err(e) => {
                    error!("dir_size failed for {:?}: {}", path, e);
                    continue;
                }
            };

            if size > MAX_SEGMENT_SIZE {
                info!(
                    "Segment {} is {} bytes (limit {}), proposing rollover",
                    key, size, MAX_SEGMENT_SIZE
                );
                let cmd = MetadataCmd::RolloverPartition {
                    name: topic.clone(),
                    partition,
                    new_leader: self.controller.node_id,
                };
                let payload = bincode::serialize(&cmd)?;
                self.controller.raft.propose(payload).await?;
            }
        }

        Ok(())
    }

    async fn run_gc(&self) -> Result<()> {
        let state = self.controller.metadata.snapshot_state();
        let root = self.wal_root();

        for (topic, info) in state.topics {
            for (partition, part_state) in info.partition_states {
                if part_state.current_generation <= RETENTION_GENERATIONS {
                    continue;
                }
                let cutoff = part_state.current_generation - RETENTION_GENERATIONS;
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
