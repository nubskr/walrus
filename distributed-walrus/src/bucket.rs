//! BucketService: thin Walrus wrapper that enforces leases (who may write), handles per-key
//! mutexes for concurrent writers, and exposes read/append by partition id or raw wal key.

use anyhow::{bail, Result};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn};
use walrus_rust::{Entry, Walrus};

pub const DATA_NAMESPACE: &str = "data_plane";

/// Wraps Walrus with fencing/lease awareness.
pub struct BucketService {
    engine: Arc<Walrus>,
    active_leases: RwLock<HashSet<String>>,
    write_locks: RwLock<HashMap<String, Arc<Mutex<()>>>>,
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
        self.append_raw(wal_key, data).await
    }

    pub async fn read_by_key(&self, wal_key: &str, max_bytes: usize) -> Result<Vec<Entry>> {
        self.read_raw(wal_key, max_bytes, None).await
    }

    pub async fn read_by_key_from_offset(
        &self,
        wal_key: &str,
        start_offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<Entry>> {
        self.read_raw(wal_key, max_bytes, Some(start_offset)).await
    }

    async fn append_raw(&self, wal_key: &str, data: Vec<u8>) -> Result<()> {
        let _guard = BucketGuard::lock(self, wal_key).await?;
        let engine = self.engine.clone();
        let key = wal_key.to_string();
        tokio::task::spawn_blocking(move || engine.batch_append_for_topic(&key, &[&data]))
            .await??;
        Ok(())
    }

    async fn read_raw(
        &self,
        wal_key: &str,
        max_bytes: usize,
        start_offset: Option<u64>,
    ) -> Result<Vec<Entry>> {
        let engine = self.engine.clone();
        let key = wal_key.to_string();

        let entries = tokio::task::spawn_blocking(move || {
            engine.batch_read_for_topic(&key, max_bytes, false, start_offset)
        })
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))??;

        if entries.is_empty() && start_offset.is_some() {
            tracing::warn!(
                "bucket read returned empty for {} (size request {})",
                wal_key,
                max_bytes
            );
        }

        Ok(entries)
    }

    pub async fn sync_leases(&self, expected: &HashSet<String>) {
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

    pub fn get_topic_size_blocking(&self, wal_key: &str) -> u64 {
        self.engine.get_topic_size(wal_key)
    }
}

struct BucketGuard<'a> {
    _lock: tokio::sync::OwnedMutexGuard<()>,
    _bucket: &'a BucketService,
}

impl<'a> BucketGuard<'a> {
    async fn lock(bucket: &'a BucketService, wal_key: &str) -> Result<Self> {
        bucket.ensure_lease(wal_key).await?;
        let lock = bucket.lock_for_key(wal_key).await;
        let guard = lock.lock_owned().await;
        Ok(Self {
            _lock: guard,
            _bucket: bucket,
        })
    }
}

impl BucketService {
    async fn ensure_lease(&self, wal_key: &str) -> Result<()> {
        let leases = self.active_leases.read().await;
        if !leases.contains(wal_key) {
            warn!("write rejected for {} (leases: {:?})", wal_key, leases);
            bail!("NotLeaderForPartition: {}", wal_key);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("walrus-bucket-{name}-{unique}"))
    }

    #[tokio::test]
    async fn leases_gate_io() {
        let path = temp_path("leases");
        let bucket = BucketService::new(path.clone()).await.expect("bucket init");
        let wal_key = format!("t_{}_p_{}_g_{}", 9, 7, 3);

        // Writes without a lease should be rejected.
        let err = bucket
            .append_by_key(&wal_key, b"nope".to_vec())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("NotLeaderForPartition"));

        let mut leases = HashSet::new();
        leases.insert(wal_key.clone());
        bucket.sync_leases(&leases).await;
        bucket
            .append_by_key(&wal_key, b"hello-world".to_vec())
            .await
            .expect("append with lease");
        let entries = bucket
            .read_by_key(&wal_key, 1024)
            .await
            .expect("read with lease");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].data, b"hello-world");

        leases.clear();
        bucket.sync_leases(&leases).await;
        let err = bucket
            .append_by_key(&wal_key, b"after-revoke".to_vec())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("NotLeaderForPartition"));

        let _ = std::fs::remove_dir_all(path);
    }
}
