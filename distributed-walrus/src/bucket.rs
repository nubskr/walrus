use anyhow::{bail, Result};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn};
use walrus_rust::{Entry, Walrus};

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
                warn!("Write rejected for {} (missing lease)", key);
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
                warn!(
                    "append_by_key missing lease for {} (leases: {:?})",
                    wal_key, leases
                );
                bail!("NotLeaderForPartition: {}", wal_key);
            }
        }
        let lock = self.lock_for_key(wal_key).await;
        let _guard = lock.lock().await;
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

    pub async fn read_by_key_from_offset(
        &self,
        wal_key: &str,
        start_offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<Entry>> {
        // Walrus does not expose random access reads today; best-effort by requesting
        // enough bytes and trimming in-memory.
        let offset_as_usize = usize::try_from(start_offset).unwrap_or(usize::MAX);
        let fetch_bytes = max_bytes.saturating_add(offset_as_usize);
        let entries = self.read_by_key(wal_key, fetch_bytes).await?;
        if start_offset == 0 {
            return Ok(entries);
        }

        let mut skipped = 0u64;
        let mut out = Vec::new();
        for mut entry in entries.into_iter() {
            let len = entry.data.len() as u64;
            if skipped + len <= start_offset {
                skipped += len;
                continue;
            }
            let within_entry = (start_offset - skipped) as usize;
            let remaining = &entry.data[within_entry..];
            let capped_len = remaining.len().min(max_bytes);
            entry.data = remaining[..capped_len].to_vec();
            out.push(entry);
            break;
        }
        Ok(out)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("walrus-bucket-{name}-{unique}"))
    }

    #[tokio::test]
    async fn partition_ids_gate_leases_and_io() {
        let path = temp_path("leases");
        let bucket = BucketService::new(path.clone()).await.expect("bucket init");
        let pid = PartitionId {
            topic_id: 9,
            partition_id: 7,
            generation_id: 3,
        };
        assert_eq!(pid.to_wal_key(), "t_9_p_7_g_3");

        // Writes without a lease should be rejected.
        let err = bucket.append(pid, b"nope".to_vec()).await.unwrap_err();
        assert!(err.to_string().contains("NotLeaderForPartition"));

        bucket.grant_lease(pid).await;
        bucket
            .append(pid, b"hello-world".to_vec())
            .await
            .expect("append with lease");
        let entries = bucket.read(pid, 1024).await.expect("read with lease");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].data, b"hello-world");

        bucket.revoke_lease(pid).await;
        let err = bucket.append(pid, b"after-revoke".to_vec()).await.unwrap_err();
        assert!(err.to_string().contains("NotLeaderForPartition"));

        let _ = std::fs::remove_dir_all(path);
    }
}
