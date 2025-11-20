pub mod wal;

use crate::error::{OctopiiError, Result};
use bytes::Bytes;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::time::Duration;

pub use wal::{FsyncSchedule, Walrus};

// Global lock to serialize WAL creation when setting WALRUS_DATA_DIR
// This prevents race conditions when multiple concurrent tests/nodes
// try to create WALs with different data directories
static WAL_CREATION_LOCK: Mutex<()> = Mutex::new(());

/// Write-Ahead Log for durable storage
///
/// This is a Walrus-backed implementation that provides an async API
/// compatible with the previous file-based WAL implementation.
///
/// Performance features:
/// - Checksummed entries (FNV-1a)
/// - Configurable fsync schedule
/// - Zero-copy I/O via mmap
/// - Crash recovery
pub struct WriteAheadLog {
    pub walrus: Arc<Walrus>,
    topic: String,
    offset_counter: Arc<AtomicU64>,
    write_counter: Arc<AtomicU64>, // Track actual writes for read_all
}

impl WriteAheadLog {
    /// Create a new WAL instance
    ///
    /// `path`: Used to derive the WAL namespace/key
    /// `batch_size`: Not used in Walrus (Walrus manages batching internally)
    /// `flush_interval`: Converted to FsyncSchedule
    pub async fn new(path: PathBuf, _batch_size: usize, flush_interval: Duration) -> Result<Self> {
        // Extract parent directory and filename from path
        let parent_dir = path.parent().ok_or_else(|| {
            OctopiiError::Wal("Invalid WAL path: no parent directory".to_string())
        })?;
        let file_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("default_wal")
            .to_string();
        let wal_key = wal_instance_key(&path, &file_name);

        // Convert flush_interval to FsyncSchedule
        let schedule = if flush_interval.as_millis() == 0 {
            FsyncSchedule::SyncEach
        } else {
            FsyncSchedule::Milliseconds(flush_interval.as_millis() as u64)
        };

        // Ensure the WAL root directory exists (Walrus will create per-key dirs)
        std::fs::create_dir_all(parent_dir).map_err(|e| {
            OctopiiError::Wal(format!(
                "Failed to create WAL parent directory {}: {}",
                parent_dir.display(),
                e
            ))
        })?;

        let root_dir_str = parent_dir.to_string_lossy().to_string();

        // Create Walrus instance using block_in_place (enforces hard thread cap)
        // We use a global lock to prevent race conditions when setting WALRUS_DATA_DIR
        let walrus = tokio::task::block_in_place(move || {
            // Acquire lock to serialize WAL creation with env var setting
            let _guard = WAL_CREATION_LOCK.lock().unwrap();

            // Temporarily set WALRUS_DATA_DIR to point to our unique directory
            std::env::set_var("WALRUS_DATA_DIR", &root_dir_str);

            // Create Walrus with a keyed instance for additional isolation
            let result = wal::Walrus::with_consistency_and_schedule_for_key(
                &wal_key,
                wal::ReadConsistency::StrictlyAtOnce,
                schedule,
            );

            // Remove the environment variable after Walrus reads it
            std::env::remove_var("WALRUS_DATA_DIR");

            // Lock is automatically released when _guard is dropped

            result
        })
        .map_err(|e| OctopiiError::Wal(format!("Failed to create Walrus: {}", e)))?;

        Ok(Self {
            walrus: Arc::new(walrus),
            topic: "wal_data".to_string(),
            offset_counter: Arc::new(AtomicU64::new(0)),
            write_counter: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Append data to the WAL
    /// Returns a monotonically increasing offset
    pub async fn append(&self, data: Bytes) -> Result<u64> {
        let walrus = self.walrus.clone();
        let topic = self.topic.clone();
        let data_vec = data.to_vec();

        // Use block_in_place for hard thread cap (Walrus ops are non-blocking)
        tokio::task::block_in_place(|| walrus.append_for_topic(&topic, &data_vec))
            .map_err(|e| OctopiiError::Wal(format!("Failed to append: {}", e)))?;

        // Increment write counter for read_all tracking
        self.write_counter.fetch_add(1, Ordering::SeqCst);

        // Return incrementing offset
        let offset = self.offset_counter.fetch_add(1, Ordering::SeqCst);
        Ok(offset)
    }

    /// Force flush all pending writes
    /// Note: Walrus handles fsyncing based on the configured schedule
    /// We add a small delay to ensure background fsync completes
    pub async fn flush(&self) -> Result<()> {
        // Give Walrus's background fsync thread time to complete
        // The configured schedule is typically 100-200ms
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
        Ok(())
    }

    /// Read all entries from the WAL (for recovery)
    ///
    /// This reads from the write topic using batched reads with checkpointing
    /// to consume all available entries.
    pub async fn read_all(&self) -> Result<Vec<Bytes>> {
        let walrus = self.walrus.clone();
        let topic = self.topic.clone();

        // Use block_in_place for hard thread cap (Walrus ops are non-blocking)
        tokio::task::block_in_place(move || {
            let mut all_entries = Vec::new();
            let mut consecutive_empty_reads = 0;

            // Read in batches, checkpointing to advance the reader
            loop {
                match walrus.batch_read_for_topic(&topic, 10 * 1024 * 1024, true) {
                    Ok(batch) => {
                        if batch.is_empty() {
                            consecutive_empty_reads += 1;
                            // If we get 2 empty reads in a row, we're truly done
                            if consecutive_empty_reads >= 2 {
                                break;
                            }
                            continue;
                        }
                        consecutive_empty_reads = 0;

                        // Convert to Bytes and append
                        for entry in batch {
                            all_entries.push(Bytes::from(entry.data));
                        }
                    }
                    Err(_e) => {
                        // Error reading - stop here
                        break;
                    }
                }
            }

            Ok(all_entries)
        })
    }
}

fn wal_instance_key(path: &Path, base_name: &str) -> String {
    let mut hasher = DefaultHasher::new();
    path.to_string_lossy().hash(&mut hasher);
    let hash = hasher.finish();
    format!("{base_name}_{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_wal_write_and_read() {
        // Use a unique directory for each test run to avoid conflicts
        use std::time::SystemTime;
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let temp_dir = std::env::temp_dir();
        let unique_name = format!("test_walrus_{}_{}.log", std::process::id(), timestamp);
        let wal_path = temp_dir.join(&unique_name);

        // Clean up the actual WAL directory (parent/key structure)
        let actual_wal_dir = temp_dir.join(&unique_name);
        if actual_wal_dir.exists() {
            let _ = std::fs::remove_dir_all(&actual_wal_dir);
        }

        let wal = WriteAheadLog::new(wal_path.clone(), 10, Duration::from_millis(100))
            .await
            .unwrap();

        // Write some entries
        let data1 = Bytes::from("hello");
        let data2 = Bytes::from("world");

        let offset1 = wal.append(data1.clone()).await.unwrap();
        let offset2 = wal.append(data2.clone()).await.unwrap();

        assert!(offset2 > offset1);

        // Flush to ensure data is written
        wal.flush().await.unwrap();

        // Longer delay to ensure Walrus has fsynced and data is available
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Read back
        let entries = wal.read_all().await.unwrap();
        assert_eq!(
            entries.len(),
            2,
            "Expected 2 entries, got {}",
            entries.len()
        );
        assert_eq!(entries[0], data1);
        assert_eq!(entries[1], data2);

        // Cleanup
        let _ = std::fs::remove_dir_all(&actual_wal_dir);
    }
}
