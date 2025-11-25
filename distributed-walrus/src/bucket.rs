//! Storage: thin Walrus wrapper that enforces leases (who may write), handles per-key
//! mutexes for concurrent writers, and exposes read/append by wal key.

use anyhow::{bail, Result};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::warn;
use walrus_rust::Walrus;

pub const DATA_NAMESPACE: &str = "data_plane";

/// Wraps Walrus with fencing/lease awareness.
pub struct Storage {
    engine: Arc<Walrus>,
    active_leases: RwLock<HashSet<String>>,
    write_locks: RwLock<HashMap<String, Arc<Mutex<()>>>>,
}

impl Storage {
    /// Construct a new storage instance rooted at the provided storage directory.
    pub async fn new(storage_path: PathBuf) -> Result<Self> {
        if let Some(parent) = storage_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::create_dir_all(&storage_path)?;

        // io_uring is unavailable in many containerized environments; allow opting into mmap.
        if std::env::var("WALRUS_DISABLE_IO_URING").is_ok() {
            walrus_rust::disable_fd_backend();
        }
        std::env::set_var("WALRUS_DATA_DIR", &storage_path);

        let engine = Arc::new(Walrus::new_for_key(DATA_NAMESPACE)?);

        Ok(Self {
            engine,
            active_leases: RwLock::new(HashSet::new()),
            write_locks: RwLock::new(HashMap::new()),
        })
    }

    pub async fn append_by_key(&self, wal_key: &str, data: Vec<u8>) -> Result<()> {
        let _guard = BucketGuard::lock(self, wal_key).await?;
        let engine = self.engine.clone();
        let key = wal_key.to_string();
        tokio::task::spawn_blocking(move || engine.batch_append_for_topic(&key, &[&data]))
            .await??;
        Ok(())
    }

    pub async fn read_one(&self, wal_key: &str) -> Result<Option<Vec<u8>>> {
        let engine = self.engine.clone();
        let key = wal_key.to_string();
        let entry = tokio::task::spawn_blocking(move || engine.read_next(&key, true)).await??;
        Ok(entry.map(|e| e.data))
    }

    pub async fn update_leases(&self, expected: &HashSet<String>) {
        let mut leases = self.active_leases.write().await;
        leases.retain(|key| expected.contains(key));
        for key in expected.iter() {
            leases.insert(key.clone());
        }
    }

    async fn lock_for_key(&self, key: &str) -> Arc<Mutex<()>> {
        if let Some(existing) = self.write_locks.read().await.get(key).cloned() {
            return existing;
        }
        let mut guard = self.write_locks.write().await;
        guard
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    pub fn get_topic_size_blocking(&self, wal_key: &str) -> u64 {
        self.engine.get_topic_size(wal_key)
    }
}

struct BucketGuard<'a> {
    _lock: tokio::sync::OwnedMutexGuard<()>,
    _storage: &'a Storage,
}

impl<'a> BucketGuard<'a> {
    async fn lock(storage: &'a Storage, wal_key: &str) -> Result<Self> {
        storage.ensure_lease(wal_key).await?;
        let lock = storage.lock_for_key(wal_key).await;
        let guard = lock.lock_owned().await;
        Ok(Self {
            _lock: guard,
            _storage: storage,
        })
    }
}

impl Storage {
    async fn ensure_lease(&self, wal_key: &str) -> Result<()> {
        let leases = self.active_leases.read().await;
        if !leases.contains(wal_key) {
            warn!("write rejected for {} (leases: {:?})", wal_key, leases);
            bail!("NotLeaderForPartition: {}", wal_key);
        }
        Ok(())
    }
}