use crate::wal::config::FsyncSchedule;
use crate::wal::paths::WalPathManager;
use std::path::PathBuf;
use std::sync::Arc;

use super::{ReadConsistency, Walrus};

/// A builder for constructing [`Walrus`] instances with explicit configuration.
///
/// Use this when you need to specify the data directory directly, avoiding the
/// process-global `WALRUS_DATA_DIR` environment variable which is subject to
/// race conditions under concurrent access.
///
/// # Example
///
/// ```rust,no_run
/// use walrus_rust::Walrus;
/// use std::path::PathBuf;
///
/// let wal = Walrus::builder()
///     .data_dir(PathBuf::from("/var/lib/myapp/wal"))
///     .key("tenant-123")
///     .build()
///     .unwrap();
/// ```
pub struct WalrusBuilder {
    data_dir: Option<PathBuf>,
    key: Option<String>,
    consistency: Option<ReadConsistency>,
    fsync_schedule: Option<FsyncSchedule>,
}

impl WalrusBuilder {
    /// Create a new builder with all options unset (defaults will apply).
    pub fn new() -> Self {
        Self {
            data_dir: None,
            key: None,
            consistency: None,
            fsync_schedule: None,
        }
    }

    /// Set the root data directory for WAL files.
    ///
    /// When set, this bypasses the `WALRUS_DATA_DIR` environment variable
    /// entirely, eliminating race conditions when multiple threads construct
    /// `Walrus` instances concurrently.
    ///
    /// When not set, falls back to the `WALRUS_DATA_DIR` env var
    /// (or `"wal_files"` if unset).
    pub fn data_dir(mut self, dir: PathBuf) -> Self {
        self.data_dir = Some(dir);
        self
    }

    /// Set the namespace key, creating a subdirectory under the data dir.
    ///
    /// Equivalent to the key in `Walrus::new_for_key()`.
    pub fn key(mut self, key: &str) -> Self {
        self.key = Some(key.to_string());
        self
    }

    /// Set the read consistency model.
    ///
    /// Defaults to [`ReadConsistency::StrictlyAtOnce`].
    pub fn consistency(mut self, mode: ReadConsistency) -> Self {
        self.consistency = Some(mode);
        self
    }

    /// Set the fsync schedule.
    ///
    /// Defaults to [`FsyncSchedule::Milliseconds(200)`].
    pub fn fsync_schedule(mut self, schedule: FsyncSchedule) -> Self {
        self.fsync_schedule = Some(schedule);
        self
    }

    /// Build the [`Walrus`] instance.
    pub fn build(self) -> std::io::Result<Walrus> {
        let consistency = self
            .consistency
            .unwrap_or(ReadConsistency::StrictlyAtOnce);
        let fsync_schedule = self
            .fsync_schedule
            .unwrap_or(FsyncSchedule::Milliseconds(200));

        let paths = match (self.data_dir, self.key.as_deref()) {
            (Some(dir), key) => WalPathManager::with_data_dir(dir, key),
            (None, Some(key)) => WalPathManager::for_key(key),
            (None, None) => WalPathManager::default(),
        };

        Walrus::with_paths(Arc::new(paths), consistency, fsync_schedule)
    }
}

impl Default for WalrusBuilder {
    fn default() -> Self {
        Self::new()
    }
}
