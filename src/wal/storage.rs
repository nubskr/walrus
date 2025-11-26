use crate::wal::config::{FsyncSchedule, USE_FD_BACKEND};
use memmap2::MmapMut;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::SystemTime;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

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

    #[allow(dead_code)]
    pub(crate) fn file(&self) -> &std::fs::File {
        &self.file
    }
}

#[derive(Debug)]
pub(crate) enum StorageImpl {
    Mmap(MmapMut),
    Fd(FdBackend),
}

impl StorageImpl {
    pub(crate) fn write(&self, offset: usize, data: &[u8]) {
        match self {
            StorageImpl::Mmap(mmap) => {
                debug_assert!(offset <= mmap.len());
                debug_assert!(mmap.len() - offset >= data.len());
                unsafe {
                    let ptr = mmap.as_ptr() as *mut u8;
                    std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.add(offset), data.len());
                }
            }
            StorageImpl::Fd(fd) => fd.write(offset, data),
        }
    }

    pub(crate) fn read(&self, offset: usize, dest: &mut [u8]) {
        match self {
            StorageImpl::Mmap(mmap) => {
                debug_assert!(offset + dest.len() <= mmap.len());
                let src = &mmap[offset..offset + dest.len()];
                dest.copy_from_slice(src);
            }
            StorageImpl::Fd(fd) => fd.read(offset, dest),
        }
    }

    pub(crate) fn flush(&self) -> std::io::Result<()> {
        match self {
            StorageImpl::Mmap(mmap) => mmap.flush(),
            StorageImpl::Fd(fd) => fd.flush(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            StorageImpl::Mmap(mmap) => mmap.len(),
            StorageImpl::Fd(fd) => fd.len(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn as_fd(&self) -> Option<&FdBackend> {
        if let StorageImpl::Fd(fd) = self {
            Some(fd)
        } else {
            None
        }
    }
}

static GLOBAL_FSYNC_SCHEDULE: OnceLock<FsyncSchedule> = OnceLock::new();

fn should_use_o_sync() -> bool {
    GLOBAL_FSYNC_SCHEDULE
        .get()
        .map(|s| matches!(s, FsyncSchedule::SyncEach))
        .unwrap_or(false)
}

fn create_storage_impl(path: &str) -> std::io::Result<StorageImpl> {
    if USE_FD_BACKEND.load(Ordering::Relaxed) {
        let use_o_sync = should_use_o_sync();
        Ok(StorageImpl::Fd(FdBackend::new(path, use_o_sync)?))
    } else {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        // SAFETY: `file` is opened read/write and lives for the duration of this
        // mapping; `memmap2` upholds aliasing invariants for `MmapMut`.
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        Ok(StorageImpl::Mmap(mmap))
    }
}

#[derive(Debug)]
pub(crate) struct SharedMmap {
    storage: StorageImpl,
    last_touched_at: AtomicU64,
}

// SAFETY: `SharedMmap` provides interior mutability only via methods that
// enforce bounds and perform atomic timestamp updates; the underlying
// storage supports concurrent reads and explicit flushes.
unsafe impl Sync for SharedMmap {}
// SAFETY: The struct holds storage that is safe to move between threads;
// timestamps are atomics, so sending is sound.
unsafe impl Send for SharedMmap {}

impl SharedMmap {
    pub(crate) fn new(path: &str) -> std::io::Result<Arc<Self>> {
        let storage = create_storage_impl(path)?;

        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_millis() as u64;
        Ok(Arc::new(Self {
            storage,
            last_touched_at: AtomicU64::new(now_ms),
        }))
    }

    pub(crate) fn write(&self, offset: usize, data: &[u8]) {
        // Bounds check before raw copy to maintain memory safety
        debug_assert!(offset <= self.storage.len());
        debug_assert!(self.storage.len() - offset >= data.len());

        self.storage.write(offset, data);

        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_millis() as u64;
        self.last_touched_at.store(now_ms, Ordering::Relaxed);
    }

    pub(crate) fn read(&self, offset: usize, dest: &mut [u8]) {
        debug_assert!(offset + dest.len() <= self.storage.len());
        self.storage.read(offset, dest);
    }

    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.storage.len()
    }

    pub(crate) fn flush(&self) -> std::io::Result<()> {
        self.storage.flush()
    }

    #[allow(dead_code)]
    pub(crate) fn storage(&self) -> &StorageImpl {
        &self.storage
    }
}

pub(crate) struct SharedMmapKeeper {
    data: HashMap<String, Arc<SharedMmap>>,
}

impl SharedMmapKeeper {
    fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    // Fast path: many readers concurrently
    fn get_mmap_arc_read(path: &str) -> Option<Arc<SharedMmap>> {
        static MMAP_KEEPER: OnceLock<RwLock<SharedMmapKeeper>> = OnceLock::new();
        let keeper_lock = MMAP_KEEPER.get_or_init(|| RwLock::new(SharedMmapKeeper::new()));
        let keeper = keeper_lock.read().ok()?;
        keeper.data.get(path).cloned()
    }

    // Read-mostly accessor that escalates to write lock only on miss
    pub(crate) fn get_mmap_arc(path: &str) -> std::io::Result<Arc<SharedMmap>> {
        if let Some(existing) = Self::get_mmap_arc_read(path) {
            return Ok(existing);
        }

        static MMAP_KEEPER: OnceLock<RwLock<SharedMmapKeeper>> = OnceLock::new();
        let keeper_lock = MMAP_KEEPER.get_or_init(|| RwLock::new(SharedMmapKeeper::new()));

        // Double-check with a fresh read lock to avoid unnecessary write lock
        {
            let keeper = keeper_lock.read().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "mmap keeper read lock poisoned")
            })?;
            if let Some(existing) = keeper.data.get(path) {
                return Ok(existing.clone());
            }
        }

        let mut keeper = keeper_lock.write().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::Other, "mmap keeper write lock poisoned")
        })?;
        if let Some(existing) = keeper.data.get(path) {
            return Ok(existing.clone());
        }

        let arc = SharedMmap::new(path)?;
        keeper.data.insert(path.to_string(), arc.clone());
        Ok(arc)
    }
}

pub(crate) fn set_fsync_schedule(schedule: FsyncSchedule) {
    let _ = GLOBAL_FSYNC_SCHEDULE.set(schedule);
}

#[allow(dead_code)]
pub(crate) fn fsync_schedule() -> Option<FsyncSchedule> {
    GLOBAL_FSYNC_SCHEDULE.get().copied()
}

pub(crate) fn open_storage_for_path(path: &str) -> std::io::Result<StorageImpl> {
    create_storage_impl(path)
}
