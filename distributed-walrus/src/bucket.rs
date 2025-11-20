use anyhow::{bail, Result};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;
use walrus_rust::{disable_fd_backend, Entry, Walrus};

pub const DATA_NAMESPACE: &str = "data_plane";

/// Logical identifier for a distributed Walrus partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PartitionId {
    pub topic_id: u64,
    pub partition_id: u32,
    pub generation_id: u64,
}

impl PartitionId {
    /// Convert the logical identifier into Walrus' topic naming scheme.
    pub fn to_wal_key(&self) -> String {
        format!(
            "t_{}_p_{}_g_{}",
            self.topic_id, self.partition_id, self.generation_id
        )
    }
}

/// Wraps Walrus with fencing/lease awareness.
pub struct BucketService {
    engine: Arc<Walrus>,
    active_leases: RwLock<HashSet<String>>,
}

impl BucketService {
    /// Construct a new bucket service rooted at the provided storage directory.
    pub async fn new(storage_path: PathBuf) -> Result<Self> {
        if let Some(parent) = storage_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::create_dir_all(&storage_path)?;

        // io_uring is unavailable in many containerized environments; allow opting into mmap.
        if std::env::var("WALRUS_DISABLE_IO_URING").is_ok() {
            disable_fd_backend();
        }
        std::env::set_var("WALRUS_DATA_DIR", &storage_path);

        let engine = Arc::new(Walrus::new_for_key(DATA_NAMESPACE)?);

        Ok(Self {
            engine,
            active_leases: RwLock::new(HashSet::new()),
        })
    }

    /// Allow future writes for the partition/generation.
    pub async fn grant_lease(&self, pid: PartitionId) {
        self.grant_key(pid.to_wal_key()).await;
    }

    /// Revoke write permission for the partition/generation.
    pub async fn revoke_lease(&self, pid: PartitionId) {
        self.revoke_key(pid.to_wal_key()).await;
    }

    /// Append bytes to the topic corresponding to the partition.
    pub async fn append(&self, pid: PartitionId, data: Vec<u8>) -> Result<()> {
        let key = pid.to_wal_key();
        {
            let leases = self.active_leases.read().await;
            if !leases.contains(&key) {
                tracing::warn!("Write rejected for {} (missing lease)", key);
                bail!("NotLeaderForPartition: {}", key);
            }
        }

        let engine = self.engine.clone();
        tokio::task::spawn_blocking(move || engine.batch_append_for_topic(&key, &[&data]))
            .await??;

        Ok(())
    }

    /// Read up to `max_bytes` from the partition.
    pub async fn read(&self, pid: PartitionId, max_bytes: usize) -> Result<Vec<Entry>> {
        let key = pid.to_wal_key();
        let engine = self.engine.clone();
        let entries = tokio::task::spawn_blocking(move || {
            engine.batch_read_for_topic(&key, max_bytes, false)
        })
        .await??;
        Ok(entries)
    }

    pub async fn append_by_key(&self, wal_key: &str, data: Vec<u8>) -> Result<()> {
        {
            let leases = self.active_leases.read().await;
            if !leases.contains(wal_key) {
                bail!("NotLeaderForPartition: {}", wal_key);
            }
        }
        let engine = self.engine.clone();
        let key = wal_key.to_string();
        tokio::task::spawn_blocking(move || engine.batch_append_for_topic(&key, &[&data]))
            .await??;
        Ok(())
    }

    pub async fn read_by_key(&self, wal_key: &str, max_bytes: usize) -> Result<Vec<Entry>> {
        let engine = self.engine.clone();
        let key = wal_key.to_string();
        let entries = tokio::task::spawn_blocking(move || {
            engine.batch_read_for_topic(&key, max_bytes, false)
        })
        .await??;
        Ok(entries)
    }

    pub async fn sync_leases(&self, expected: &HashSet<String>) {
        let mut leases = self.active_leases.write().await;
        leases.retain(|key| expected.contains(key));
        for key in expected.iter() {
            leases.insert(key.clone());
        }
    }

    async fn grant_key(&self, key: String) {
        info!("Granting lease for {}", key);
        let mut leases = self.active_leases.write().await;
        leases.insert(key);
    }

    async fn revoke_key(&self, key: String) {
        info!("Revoking lease for {}", key);
        let mut leases = self.active_leases.write().await;
        leases.remove(&key);
    }
}
