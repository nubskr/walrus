use crate::wal::config::FsyncSchedule;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::IoSlice;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::SystemTime;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(unix)]
use std::os::unix::io::AsRawFd;

#[derive(Debug)]
pub(crate) struct FdBackend {
    file: std::fs::File,
    len: usize,
}

impl FdBackend {
    fn new(path: &str, use_o_sync: bool) -> std::io::Result<Self> {
        let mut opts = OpenOptions::new();
        opts.read(true).write(true);

        #[cfg(unix)]
        if use_o_sync {
            opts.custom_flags(libc::O_SYNC);
        }

        let file = opts.open(path)?;
        let metadata = file.metadata()?;
        let len = metadata.len() as usize;

        Ok(Self { file, len })
    }

    pub(crate) fn write(&self, offset: usize, data: &[u8]) {
        use std::os::unix::fs::FileExt;
        // pwrite doesn't move the file cursor
        let _ = self.file.write_at(data, offset as u64);
    }

    pub(crate) fn write_vectored(&self, offset: usize, bufs: &[IoSlice]) {
        #[cfg(unix)]
        {
            let fd = self.file.as_raw_fd();
            // Convert IoSlice to libc::iovec (layout compatible)
            let iovecs = bufs.as_ptr() as *const libc::iovec;
            let iovcnt = bufs.len() as std::ffi::c_int;
            unsafe {
                libc::pwritev(fd, iovecs, iovcnt, offset as libc::off_t);
            }
        }
        #[cfg(not(unix))]
        {
             // Fallback for non-unix (if any)
             let mut curr = offset;
             for buf in bufs {
                 self.write(curr, buf);
                 curr += buf.len();
             }
        }
    }

    pub(crate) fn read(&self, offset: usize, dest: &mut [u8]) {
        use std::os::unix::fs::FileExt;
        // pread doesn't move the file cursor
        let _ = self.file.read_at(dest, offset as u64);
    }

    pub(crate) fn flush(&self) -> std::io::Result<()> {
        self.file.sync_all()
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn file(&self) -> &std::fs::File {
        &self.file
    }
}

static GLOBAL_FSYNC_SCHEDULE: OnceLock<FsyncSchedule> = OnceLock::new();

fn should_use_o_sync() -> bool {
    GLOBAL_FSYNC_SCHEDULE
        .get()
        .map(|s| matches!(s, FsyncSchedule::SyncEach))
        .unwrap_or(false)
}

#[derive(Debug)]
pub(crate) struct Storage {
    backend: FdBackend,
    last_touched_at: AtomicU64,
}

// SAFETY: `Storage` provides interior mutability only via methods that
// enforce bounds (via backend) and perform atomic timestamp updates; the underlying
// file supports concurrent reads/writes.
unsafe impl Sync for Storage {}
unsafe impl Send for Storage {}

impl Storage {
    pub(crate) fn new(path: &str) -> std::io::Result<Arc<Self>> {
        let use_o_sync = should_use_o_sync();
        let backend = FdBackend::new(path, use_o_sync)?;

        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_millis() as u64;
        Ok(Arc::new(Self {
            backend,
            last_touched_at: AtomicU64::new(now_ms),
        }))
    }

    pub(crate) fn write(&self, offset: usize, data: &[u8]) {
        self.backend.write(offset, data);

        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_millis() as u64;
        self.last_touched_at.store(now_ms, Ordering::Relaxed);
    }

    pub(crate) fn write_vectored(&self, offset: usize, bufs: &[IoSlice]) {
        self.backend.write_vectored(offset, bufs);

        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_millis() as u64;
        self.last_touched_at.store(now_ms, Ordering::Relaxed);
    }

    pub(crate) fn read(&self, offset: usize, dest: &mut [u8]) {
        self.backend.read(offset, dest);
    }

    pub(crate) fn len(&self) -> usize {
        self.backend.len()
    }

    pub(crate) fn flush(&self) -> std::io::Result<()> {
        self.backend.flush()
    }

    pub(crate) fn as_fd(&self) -> Option<&FdBackend> {
        Some(&self.backend)
    }
}

pub(crate) struct StorageKeeper {
    data: HashMap<String, Arc<Storage>>,
}

impl StorageKeeper {
    fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    // Fast path: many readers concurrently
    fn get_storage_arc_read(path: &str) -> Option<Arc<Storage>> {
        static STORAGE_KEEPER: OnceLock<RwLock<StorageKeeper>> = OnceLock::new();
        let keeper_lock = STORAGE_KEEPER.get_or_init(|| RwLock::new(StorageKeeper::new()));
        let keeper = keeper_lock.read().ok()?;
        keeper.data.get(path).cloned()
    }

    // Read-mostly accessor that escalates to write lock only on miss
    pub(crate) fn get_storage_arc(path: &str) -> std::io::Result<Arc<Storage>> {
        if let Some(existing) = Self::get_storage_arc_read(path) {
            return Ok(existing);
        }

        static STORAGE_KEEPER: OnceLock<RwLock<StorageKeeper>> = OnceLock::new();
        let keeper_lock = STORAGE_KEEPER.get_or_init(|| RwLock::new(StorageKeeper::new()));

        // Double-check with a fresh read lock to avoid unnecessary write lock
        {
            let keeper = keeper_lock.read().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "storage keeper read lock poisoned")
            })?;
            if let Some(existing) = keeper.data.get(path) {
                return Ok(existing.clone());
            }
        }

        let mut keeper = keeper_lock.write().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::Other, "storage keeper write lock poisoned")
        })?;
        if let Some(existing) = keeper.data.get(path) {
            return Ok(existing.clone());
        }

        let arc = Storage::new(path)?;
        keeper.data.insert(path.to_string(), arc.clone());
        Ok(arc)
    }
}

pub(crate) fn set_fsync_schedule(schedule: FsyncSchedule) {
    let _ = GLOBAL_FSYNC_SCHEDULE.set(schedule);
}

pub(crate) fn fsync_schedule() -> Option<FsyncSchedule> {
    GLOBAL_FSYNC_SCHEDULE.get().copied()
}

pub(crate) fn open_storage_for_path(path: &str) -> std::io::Result<Arc<Storage>> {
    Storage::new(path)
}