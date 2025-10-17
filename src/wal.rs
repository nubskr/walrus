use memmap2::MmapMut;
use rkyv::{Archive, Deserialize, Serialize};
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::fs;
use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::thread;
use std::time::Duration;
use std::time::SystemTime;

// Global flag to choose backend
static USE_FD_BACKEND: AtomicBool = AtomicBool::new(true);

// Public function to enable FD backend
pub fn enable_fd_backend() {
    USE_FD_BACKEND.store(true, Ordering::Relaxed);
}

// Macro to conditionally print debug messages
macro_rules! debug_print {
    ($($arg:tt)*) => {
        if std::env::var("WALRUS_QUIET").is_err() {
            println!($($arg)*);
        }
    };
}

const DEFAULT_BLOCK_SIZE: u64 = 10 * 1024 * 1024; // 10mb
const BLOCKS_PER_FILE: u64 = 100;
const MAX_ALLOC: u64 = 1 * 1024 * 1024 * 1024; // 1 GiB cap per block
const PREFIX_META_SIZE: usize = 64;
const MAX_FILE_SIZE: u64 = DEFAULT_BLOCK_SIZE * BLOCKS_PER_FILE;

fn now_millis_str() -> String {
    let ms = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0))
        .as_millis();
    ms.to_string()
}

fn checksum64(data: &[u8]) -> u64 {
    // FNV-1a 64-bit checksum
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x00000100000001B3;
    let mut hash = FNV_OFFSET;
    for &b in data {
        hash ^= b as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

// FD-based backend for storage
#[derive(Debug)]
struct FdBackend {
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

    fn write(&self, offset: usize, data: &[u8]) {
        use std::os::unix::fs::FileExt;
        // pwrite doesn't move the file cursor
        let _ = self.file.write_at(data, offset as u64);
    }

    fn read(&self, offset: usize, dest: &mut [u8]) {
        use std::os::unix::fs::FileExt;
        // pread doesn't move the file cursor
        let _ = self.file.read_at(dest, offset as u64);
    }

    fn flush(&self) -> std::io::Result<()> {
        self.file.sync_all()
    }

    fn len(&self) -> usize {
        self.len
    }
}

// Storage backend abstraction
#[derive(Debug)]
enum StorageImpl {
    Mmap(MmapMut),
    Fd(FdBackend),
}

impl StorageImpl {
    fn write(&self, offset: usize, data: &[u8]) {
        match self {
            StorageImpl::Mmap(mmap) => {
                debug_assert!(offset <= mmap.len());
                debug_assert!(mmap.len() - offset >= data.len());
                unsafe {
                    let ptr = mmap.as_ptr() as *mut u8;
                    std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.add(offset), data.len());
                }
            }
            StorageImpl::Fd(fd) => {
                fd.write(offset, data);
            }
        }
    }

    fn read(&self, offset: usize, dest: &mut [u8]) {
        match self {
            StorageImpl::Mmap(mmap) => {
                debug_assert!(offset + dest.len() <= mmap.len());
                let src = &mmap[offset..offset + dest.len()];
                dest.copy_from_slice(src);
            }
            StorageImpl::Fd(fd) => {
                fd.read(offset, dest);
            }
        }
    }

    fn flush(&self) -> std::io::Result<()> {
        match self {
            StorageImpl::Mmap(mmap) => mmap.flush(),
            StorageImpl::Fd(fd) => fd.flush(),
        }
    }

    fn len(&self) -> usize {
        match self {
            StorageImpl::Mmap(mmap) => mmap.len(),
            StorageImpl::Fd(fd) => fd.len(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Entry {
    pub data: Vec<u8>,
}

#[derive(Archive, Deserialize, Serialize, Debug)]
#[archive(check_bytes)]
struct Metadata {
    read_size: usize,
    owned_by: String,
    next_block_start: u64,
    checksum: u64,
}

#[derive(Clone, Debug)]
pub struct Block {
    id: u64,
    file_path: String,
    offset: u64,
    limit: u64,
    mmap: Arc<SharedMmap>,
    used: u64,
}

impl Block {
    fn write(
        &self,
        in_block_offset: u64,
        data: &[u8],
        owned_by: &str,
        next_block_start: u64,
    ) -> std::io::Result<()> {
        debug_assert!(
            in_block_offset + (data.len() as u64 + PREFIX_META_SIZE as u64) <= self.limit
        );

        let new_meta = Metadata {
            read_size: data.len(),
            owned_by: owned_by.to_string(),
            next_block_start,
            checksum: checksum64(data),
        };

        let meta_bytes = rkyv::to_bytes::<_, 256>(&new_meta).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("serialize metadata failed: {:?}", e),
            )
        })?;
        if meta_bytes.len() > PREFIX_META_SIZE - 2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "metadata too large",
            ));
        }

        let mut meta_buffer = vec![0u8; PREFIX_META_SIZE];
        // Store actual length in first 2 bytes (little endian)
        meta_buffer[0] = (meta_bytes.len() & 0xFF) as u8;
        meta_buffer[1] = ((meta_bytes.len() >> 8) & 0xFF) as u8;
        // Copy actual metadata starting at byte 2
        meta_buffer[2..2 + meta_bytes.len()].copy_from_slice(&meta_bytes);

        // Combine and write
        let mut combined = Vec::with_capacity(PREFIX_META_SIZE + data.len());
        combined.extend_from_slice(&meta_buffer);
        combined.extend_from_slice(data);

        let file_offset = self.offset + in_block_offset;
        self.mmap.write(file_offset as usize, &combined);
        Ok(())
    }

    fn read(&self, in_block_offset: u64) -> std::io::Result<(Entry, usize)> {
        let mut meta_buffer = vec![0; PREFIX_META_SIZE];
        let file_offset = self.offset + in_block_offset;
        self.mmap.read(file_offset as usize, &mut meta_buffer);

        // Read the actual metadata length from first 2 bytes
        let meta_len = (meta_buffer[0] as usize) | ((meta_buffer[1] as usize) << 8);

        if meta_len == 0 || meta_len > PREFIX_META_SIZE - 2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid metadata length: {}", meta_len),
            ));
        }

        // Deserialize only the actual metadata bytes (skip the 2-byte length prefix)
        let mut aligned = rkyv::AlignedVec::with_capacity(meta_len);
        aligned.extend_from_slice(&meta_buffer[2..2 + meta_len]);

        // SAFETY: `aligned` contains bytes we just read from our own file format.
        // We bounded `meta_len` to PREFIX_META_SIZE and copy into an `AlignedVec`,
        // which satisfies alignment requirements of rkyv.
        let archived = unsafe { rkyv::archived_root::<Metadata>(&aligned[..]) };
        let meta: Metadata = archived.deserialize(&mut rkyv::Infallible).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "failed to deserialize metadata",
            )
        })?;
        let actual_entry_size = meta.read_size;

        // Read the actual data
        let new_offset = file_offset + PREFIX_META_SIZE as u64;
        let mut ret_buffer = vec![0; actual_entry_size];
        self.mmap.read(new_offset as usize, &mut ret_buffer);

        // Verify checksum
        let expected = meta.checksum;
        if checksum64(&ret_buffer) != expected {
            debug_print!(
                "[reader] checksum mismatch; skipping corrupted entry at offset={} in file={}, block_id={}",
                in_block_offset,
                self.file_path,
                self.id
            );
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "checksum mismatch, data corruption detected",
            ));
        }

        let consumed = PREFIX_META_SIZE + actual_entry_size;
        Ok((Entry { data: ret_buffer }, consumed))
    }

    fn zero_range(&self, in_block_offset: u64, size: u64) -> std::io::Result<()> {
        // Zero a small region within this block; used to invalidate headers on rollback
        // Caller ensures size is reasonable (typically PREFIX_META_SIZE)
        let len = size as usize;
        if len == 0 {
            return Ok(());
        }
        let zeros = vec![0u8; len];
        let file_offset = self.offset + in_block_offset;
        self.mmap.write(file_offset as usize, &zeros);
        Ok(())
    }
}

fn make_new_file() -> std::io::Result<String> {
    let file_name = now_millis_str();
    let file_path = format!("wal_files/{}", file_name);
    let f = std::fs::File::create(&file_path)?;
    f.set_len(MAX_FILE_SIZE)?;
    Ok(file_path)
}

// has block metas to give out
struct BlockAllocator {
    next_block: UnsafeCell<Block>,
    lock: AtomicBool,
}

impl BlockAllocator {
    pub fn new() -> std::io::Result<Self> {
        std::fs::create_dir_all("wal_files").ok();
        let file1 = make_new_file()?;
        let mmap: Arc<SharedMmap> = SharedMmapKeeper::get_mmap_arc(&file1)?;
        debug_print!(
            "[alloc] init: created file={}, max_file_size={}B, block_size={}B",
            file1,
            MAX_FILE_SIZE,
            DEFAULT_BLOCK_SIZE
        );
        Ok(BlockAllocator {
            next_block: UnsafeCell::new(Block {
                id: 1,
                offset: 0,
                limit: DEFAULT_BLOCK_SIZE,
                file_path: file1,
                mmap,
                used: 0,
            }),
            lock: AtomicBool::new(false),
        })
    }

    // Allocate the next available block with proper locking
    /// SAFETY: Caller must ensure the returned `Block` is treated as uniquely
    /// owned by a single writer until it is sealed. Internally, a spin lock
    /// ensures exclusive mutable access to `next_block` while computing the
    /// next allocation, so the interior `UnsafeCell` is not concurrently
    /// accessed mutably.
    pub unsafe fn get_next_available_block(&self) -> std::io::Result<Block> {
        self.lock();
        // SAFETY: Guarded by `self.lock()` above, providing exclusive access
        // to `next_block` so creating a `&mut` from `UnsafeCell` is sound.
        let data = unsafe { &mut *self.next_block.get() };
        let prev_block_file_path = data.file_path.clone();
        if data.offset >= MAX_FILE_SIZE {
            // mark previous file as fully allocated before switching
            FileStateTracker::set_fully_allocated(prev_block_file_path);
            data.file_path = make_new_file()?;
            data.mmap = SharedMmapKeeper::get_mmap_arc(&data.file_path)?;
            data.offset = 0;
            data.used = 0;
            debug_print!("[alloc] rolled over to new file: {}", data.file_path);
        }

        // set the cur block as locked
        BlockStateTracker::register_block(data.id as usize, &data.file_path);
        FileStateTracker::register_file_if_absent(&data.file_path);
        FileStateTracker::add_block_to_file_state(&data.file_path);
        FileStateTracker::set_block_locked(data.id as usize);
        let ret = data.clone();
        data.offset += DEFAULT_BLOCK_SIZE;
        data.id += 1;
        self.unlock();
        debug_print!(
            "[alloc] handout: block_id={}, file={}, offset={}, limit={}",
            ret.id,
            ret.file_path,
            ret.offset,
            ret.limit
        );
        Ok(ret)
    }

    /// SAFETY: Caller must ensure the resulting `Block` remains uniquely used
    /// by one writer and not read concurrently while being written. The
    /// internal spin lock provides exclusive access to mutate allocator state.
    pub unsafe fn alloc_block(&self, want_bytes: u64) -> std::io::Result<Block> {
        if want_bytes == 0 || want_bytes > MAX_ALLOC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid allocation size, a single entry can't be more than 1gb",
            ));
        }
        let alloc_units = (want_bytes + DEFAULT_BLOCK_SIZE - 1) / DEFAULT_BLOCK_SIZE;
        let alloc_size = alloc_units * DEFAULT_BLOCK_SIZE;
        debug_print!(
            "[alloc] alloc_block: want_bytes={}, units={}, size={}",
            want_bytes,
            alloc_units,
            alloc_size
        );

        self.lock();
        // SAFETY: Guarded by `self.lock()` above, providing exclusive access
        // to `next_block` so creating a `&mut` from `UnsafeCell` is sound.
        let data = unsafe { &mut *self.next_block.get() };
        if data.offset + alloc_size > MAX_FILE_SIZE {
            let prev_block_file_path = data.file_path.clone();
            data.file_path = make_new_file()?;
            data.mmap = SharedMmapKeeper::get_mmap_arc(&data.file_path)?;
            data.offset = 0;
            // mark the previous file fully allocated now
            FileStateTracker::set_fully_allocated(prev_block_file_path);
            debug_print!(
                "[alloc] file rollover for sized alloc -> {}",
                data.file_path
            );
        }
        let ret = Block {
            id: data.id,
            file_path: data.file_path.clone(),
            offset: data.offset,
            limit: alloc_size,
            mmap: data.mmap.clone(),
            used: 0,
        };
        // register the new block before handing it out
        BlockStateTracker::register_block(ret.id as usize, &ret.file_path);
        FileStateTracker::register_file_if_absent(&ret.file_path);
        FileStateTracker::add_block_to_file_state(&ret.file_path);
        FileStateTracker::set_block_locked(ret.id as usize);
        data.offset += alloc_size;
        data.id += 1;
        self.unlock();
        debug_print!(
            "[alloc] handout(sized): block_id={}, file={}, offset={}, limit={}",
            ret.id,
            ret.file_path,
            ret.offset,
            ret.limit
        );
        Ok(ret)
    }

    /*
    the critical section of this call would be absolutely tiny given the exception of when a new file is being created, but it'll be amortized and in the majority of the scenario it would be a handful of microseconds and the overhead of a syscall isnt worth it, a hundred or two cycles are nothing in the grand scheme of things
    */
    fn lock(&self) {
        // Spin lock implementation
        while self
            .lock
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }
    }

    fn unlock(&self) {
        self.lock.store(false, Ordering::Release);
    }
}

// SAFETY: `BlockAllocator` uses an internal spin lock to guard all mutable
// access to `next_block`. It does not expose references to its interior
// without holding that lock, so concurrent access across threads is safe.
unsafe impl Sync for BlockAllocator {}
// SAFETY: The type contains only thread-safe primitives and does not rely on
// thread-affine resources; moving it to another thread is safe.
unsafe impl Send for BlockAllocator {}

struct Writer {
    allocator: Arc<BlockAllocator>,
    current_block: Mutex<Block>,
    reader: Arc<Reader>,
    col: String,
    publisher: Arc<mpsc::Sender<String>>,
    current_offset: Mutex<u64>,
    fsync_schedule: FsyncSchedule,
    is_batch_writing: AtomicBool,
}

impl Writer {
    pub fn new(
        allocator: Arc<BlockAllocator>,
        current_block: Block,
        reader: Arc<Reader>,
        col: String,
        publisher: Arc<mpsc::Sender<String>>,
        fsync_schedule: FsyncSchedule,
    ) -> Self {
        Writer {
            allocator,
            current_block: Mutex::new(current_block),
            reader,
            col: col.clone(),
            publisher,
            current_offset: Mutex::new(0),
            fsync_schedule,
            is_batch_writing: AtomicBool::new(false),
        }
    }

    pub fn write(&self, data: &[u8]) -> std::io::Result<()> {
        // Check if batch write is in progress
        if self.is_batch_writing.load(Ordering::Acquire) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "batch write in progress for this topic",
            ));
        }

        let mut block = self.current_block.lock().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::Other, "current_block lock poisoned")
        })?;
        let mut cur = self.current_offset.lock().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::Other, "current_offset lock poisoned")
        })?;

        let need = (PREFIX_META_SIZE as u64) + (data.len() as u64);
        if *cur + need > block.limit {
            debug_print!(
                "[writer] sealing: col={}, block_id={}, used={}, need={}, limit={}",
                self.col,
                block.id,
                *cur,
                need,
                block.limit
            );
            FileStateTracker::set_block_unlocked(block.id as usize);
            let mut sealed = block.clone();
            sealed.used = *cur;
            sealed.mmap.flush()?;
            let _ = self.reader.append_block_to_chain(&self.col, sealed);
            debug_print!("[writer] appended sealed block to chain: col={}", self.col);
            // switch to new block
            // SAFETY: We hold `current_block` and `current_offset` mutexes, so
            // this writer has exclusive ownership of the active block. The
            // allocator's internal lock ensures unique block handout.
            let new_block = unsafe { self.allocator.alloc_block(need) }?;
            debug_print!(
                "[writer] switched to new block: col={}, new_block_id={}",
                self.col,
                new_block.id
            );
            *block = new_block;
            *cur = 0;
        }
        let next_block_start = block.offset + block.limit; // simplistic for now
        block.write(*cur, data, &self.col, next_block_start)?;
        debug_print!(
            "[writer] wrote: col={}, block_id={}, offset_before={}, bytes={}, offset_after={}",
            self.col,
            block.id,
            *cur,
            need,
            *cur + need
        );
        *cur += need;

        // Handle fsync based on schedule
        match self.fsync_schedule {
            FsyncSchedule::SyncEach => {
                // Immediate mmap flush, skip background flusher
                block.mmap.flush()?;
                debug_print!(
                    "[writer] immediate fsync: col={}, block_id={}",
                    self.col,
                    block.id
                );
            }
            FsyncSchedule::Milliseconds(_) => {
                // Send to background flusher
                let _ = self.publisher.send(block.file_path.clone());
            }
            FsyncSchedule::NoFsync => {
                // No fsyncing at all - maximum throughput, no durability guarantees
                debug_print!("[writer] no fsync: col={}, block_id={}", self.col, block.id);
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
struct SharedMmap {
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

// Store the fsync schedule globally for SharedMmap::new to access
static GLOBAL_FSYNC_SCHEDULE: OnceLock<FsyncSchedule> = OnceLock::new();

impl SharedMmap {
    pub fn new(path: &str) -> std::io::Result<Arc<Self>> {
        let storage = if USE_FD_BACKEND.load(Ordering::Relaxed) {
            // Check if we should use O_SYNC
            let use_o_sync = GLOBAL_FSYNC_SCHEDULE
                .get()
                .map(|s| matches!(s, FsyncSchedule::SyncEach))
                .unwrap_or(false);

            StorageImpl::Fd(FdBackend::new(path, use_o_sync)?)
        } else {
            let file = OpenOptions::new().read(true).write(true).open(path)?;
            // SAFETY: `file` is opened read/write and lives for the duration of this
            // mapping; `memmap2` upholds aliasing invariants for `MmapMut`.
            let mmap = unsafe { MmapMut::map_mut(&file)? };
            StorageImpl::Mmap(mmap)
        };

        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_millis() as u64;
        Ok(Arc::new(Self {
            storage,
            last_touched_at: AtomicU64::new(now_ms),
        }))
    }

    pub fn write(&self, offset: usize, data: &[u8]) {
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

    pub fn read(&self, offset: usize, dest: &mut [u8]) {
        debug_assert!(offset + dest.len() <= self.storage.len());
        self.storage.read(offset, dest);
    }

    pub fn len(&self) -> usize {
        self.storage.len()
    }

    pub fn flush(&self) -> std::io::Result<()> {
        self.storage.flush()
    }
}

struct SharedMmapKeeper {
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
    fn get_mmap_arc(path: &str) -> std::io::Result<Arc<SharedMmap>> {
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
        let mmap_arc = SharedMmap::new(path)?;
        keeper.data.insert(path.to_string(), mmap_arc.clone());
        Ok(mmap_arc)
    }
}

#[derive(Debug)]
struct ColReaderInfo {
    chain: Vec<Block>,
    cur_block_idx: usize,
    cur_block_offset: u64,
    reads_since_persist: u32,
    // In-memory progress for tail (active writer block). This allows AtLeastOnce
    // to advance between reads within a single process without persisting every time.
    tail_block_id: u64,
    tail_offset: u64,
    // Ensure we only hydrate from persisted index once per process per column
    hydrated_from_index: bool,
}

struct Reader {
    data: RwLock<HashMap<String, Arc<RwLock<ColReaderInfo>>>>,
}

impl Reader {
    fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }

    fn get_chain_for_col(&self, col: &str) -> Option<Vec<Block>> {
        let arc_info = {
            let map = self.data.read().ok()?;
            map.get(col)?.clone()
        };
        let info = arc_info.read().ok()?;
        Some(info.chain.clone())
    }

    // internal
    fn append_block_to_chain(&self, col: &str, block: Block) -> std::io::Result<()> {
        // fast path: try read-lock map and use per-column lock
        if let Some(info_arc) = {
            let map = self.data.read().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "reader map read lock poisoned")
            })?;
            map.get(col).cloned()
        } {
            let mut info = info_arc.write().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "col info write lock poisoned")
            })?;
            let before = info.chain.len();
            info.chain.push(block.clone());
            // If we were reading this as the active tail, carry over progress to sealed chain
            let new_idx = info.chain.len().saturating_sub(1);
            if info.tail_block_id == block.id {
                info.cur_block_idx = new_idx;
                info.cur_block_offset = info.tail_offset.min(block.used);
            }
            debug_print!(
                "[reader] chain append(fast): col={}, block_id={}, chain_len {}->{}",
                col,
                block.id,
                before,
                before + 1
            );
            return Ok(());
        }

        // slow path
        let info_arc = {
            let mut map = self.data.write().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "reader map write lock poisoned")
            })?;
            map.entry(col.to_string())
                .or_insert_with(|| {
                    Arc::new(RwLock::new(ColReaderInfo {
                        chain: Vec::new(),
                        cur_block_idx: 0,
                        cur_block_offset: 0,
                        reads_since_persist: 0,
                        tail_block_id: 0,
                        tail_offset: 0,
                        hydrated_from_index: false,
                    }))
                })
                .clone()
        };
        let mut info = info_arc.write().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::Other, "col info write lock poisoned")
        })?;
        info.chain.push(block.clone());
        // If we were reading this as the active tail, carry over progress to sealed chain
        let new_idx = info.chain.len().saturating_sub(1);
        if info.tail_block_id == block.id {
            info.cur_block_idx = new_idx;
            info.cur_block_offset = info.tail_offset.min(block.used);
        }
        debug_print!(
            "[reader] chain append(slow/new): col={}, block_id={}, chain_len {}->{}",
            col,
            block.id,
            0,
            1
        );
        Ok(())
    }
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
pub struct BlockPos {
    pub cur_block_idx: u64,
    pub cur_block_offset: u64,
}

pub struct WalIndex {
    store: HashMap<String, BlockPos>,
    path: String,
}

impl WalIndex {
    pub fn new(file_name: &str) -> std::io::Result<Self> {
        // let tmp_path = format!("{}", );
        fs::create_dir_all("./wal_files").ok();
        let path = format!("./wal_files/{}_index.db", file_name);

        let store = Path::new(&path)
            .exists()
            .then(|| fs::read(&path).ok())
            .flatten()
            .and_then(|bytes| {
                if bytes.is_empty() {
                    return None;
                }
                // SAFETY: `bytes` comes from our persisted index file which we control;
                // we only proceed when the file is non-empty and rkyv can interpret it.
                let archived = unsafe { rkyv::archived_root::<HashMap<String, BlockPos>>(&bytes) };
                archived.deserialize(&mut rkyv::Infallible).ok()
            })
            .unwrap_or_default();

        Ok(Self {
            store,
            path: path.to_string(),
        })
    }

    pub fn set(&mut self, key: String, idx: u64, offset: u64) -> std::io::Result<()> {
        self.store.insert(
            key,
            BlockPos {
                cur_block_idx: idx,
                cur_block_offset: offset,
            },
        );
        self.persist()
    }

    pub fn get(&self, key: &str) -> Option<&BlockPos> {
        self.store.get(key)
    }

    pub fn remove(&mut self, key: &str) -> std::io::Result<Option<BlockPos>> {
        let result = self.store.remove(key);
        if result.is_some() {
            self.persist()?;
        }
        Ok(result)
    }

    fn persist(&self) -> std::io::Result<()> {
        let tmp_path = format!("{}.tmp", self.path);
        let bytes = rkyv::to_bytes::<_, 256>(&self.store).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("index serialize failed: {:?}", e),
            )
        })?;

        fs::write(&tmp_path, &bytes)?;
        fs::File::open(&tmp_path)?.sync_all()?;
        fs::rename(&tmp_path, &self.path)?;
        Ok(())
    }
}

// public APIs
#[derive(Clone, Copy, Debug)]
pub enum ReadConsistency {
    StrictlyAtOnce,
    AtLeastOnce { persist_every: u32 },
}

#[derive(Clone, Copy, Debug)]
pub enum FsyncSchedule {
    Milliseconds(u64),
    SyncEach, // fsync after every single entry
    NoFsync,  // disable fsyncing entirely (maximum throughput, no durability)
}

pub struct Walrus {
    allocator: Arc<BlockAllocator>,
    reader: Arc<Reader>,
    writers: RwLock<HashMap<String, Arc<Writer>>>,
    fsync_tx: Arc<mpsc::Sender<String>>,
    read_offset_index: Arc<RwLock<WalIndex>>,
    read_consistency: ReadConsistency,
    fsync_schedule: FsyncSchedule,
}

impl Walrus {
    pub fn new() -> std::io::Result<Self> {
        Self::with_consistency(ReadConsistency::StrictlyAtOnce)
    }

    pub fn with_consistency(mode: ReadConsistency) -> std::io::Result<Self> {
        Self::with_consistency_and_schedule(mode, FsyncSchedule::Milliseconds(200))
    }

    pub fn with_consistency_and_schedule(
        mode: ReadConsistency,
        fsync_schedule: FsyncSchedule,
    ) -> std::io::Result<Self> {
        debug_print!("[walrus] new");

        // Store the fsync schedule globally for SharedMmap::new to access
        let _ = GLOBAL_FSYNC_SCHEDULE.set(fsync_schedule);

        let allocator = Arc::new(BlockAllocator::new()?);
        let reader = Arc::new(Reader::new());
        let (tx, rx) = mpsc::channel::<String>();
        let tx_arc = Arc::new(tx);
        let (del_tx, del_rx) = mpsc::channel::<String>();
        let del_tx_arc = Arc::new(del_tx);
        let _ = DELETION_TX.set(del_tx_arc.clone());
        let pool: HashMap<String, StorageImpl> = HashMap::new();
        let tick = Arc::new(AtomicU64::new(0));
        let sleep_millis = match fsync_schedule {
            FsyncSchedule::Milliseconds(ms) => ms.max(1),
            FsyncSchedule::SyncEach => 5000, // Still run background thread for cleanup, but less frequently
            FsyncSchedule::NoFsync => 10000, // Even less frequent cleanup when no fsyncing
        };
        // background flusher
        thread::spawn(move || {
            let mut pool = pool;
            let tick = tick;
            let del_rx = del_rx;
            let mut delete_pending = std::collections::HashSet::new();

            // Create io_uring instance for batched fsync (only used with FD backend)
            let mut ring = io_uring::IoUring::new(2048).expect("Failed to create io_uring");

            loop {
                thread::sleep(Duration::from_millis(sleep_millis));

                // Phase 1: Collect unique paths to flush
                let mut unique = std::collections::HashSet::new();
                while let Ok(path) = rx.try_recv() {
                    unique.insert(path);
                }

                if !unique.is_empty() {
                    debug_print!("[flush] scheduling {} paths", unique.len());
                }

                // Phase 2: Open/map files if needed
                for path in unique.iter() {
                    // Skip if file doesn't exist
                    if !std::path::Path::new(&path).exists() {
                        debug_print!("[flush] file does not exist, skipping: {}", path);
                        continue;
                    }

                    if !pool.contains_key(path) {
                        if USE_FD_BACKEND.load(Ordering::Relaxed) {
                            // FD backend path
                            let use_o_sync = GLOBAL_FSYNC_SCHEDULE
                                .get()
                                .map(|s| matches!(s, FsyncSchedule::SyncEach))
                                .unwrap_or(false);

                            match FdBackend::new(&path, use_o_sync) {
                                Ok(fd) => {
                                    pool.insert(path.clone(), StorageImpl::Fd(fd));
                                }
                                Err(e) => {
                                    debug_print!(
                                        "[flush] failed to create FD backend for {}: {}",
                                        path,
                                        e
                                    );
                                }
                            }
                        } else {
                            // Mmap backend path (unchanged)
                            match OpenOptions::new().read(true).write(true).open(&path) {
                                Ok(file) => {
                                    // SAFETY: The file is opened read/write and lives at least until
                                    // the created mapping is inserted into `pool`, which owns it.
                                    match unsafe { MmapMut::map_mut(&file) } {
                                        Ok(mmap) => {
                                            pool.insert(path.clone(), StorageImpl::Mmap(mmap));
                                        }
                                        Err(e) => {
                                            debug_print!(
                                                "[flush] failed to create memory map for {}: {}",
                                                path,
                                                e
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    debug_print!(
                                        "[flush] failed to open file for flushing {}: {}",
                                        path,
                                        e
                                    );
                                }
                            }
                        }
                    }
                }

                // Phase 3: Flush operations
                if USE_FD_BACKEND.load(Ordering::Relaxed) {
                    // FD backend: Use io_uring for batched fsync
                    let mut fsync_batch = Vec::new();

                    for path in unique.iter() {
                        if let Some(StorageImpl::Fd(fd_backend)) = pool.get(path) {
                            let raw_fd = fd_backend.file.as_raw_fd();
                            fsync_batch.push((raw_fd, path.clone()));
                        }
                    }

                    if !fsync_batch.is_empty() {
                        debug_print!("[flush] batching {} fsync operations", fsync_batch.len());

                        // Push all fsync operations to submission queue
                        for (i, (raw_fd, _path)) in fsync_batch.iter().enumerate() {
                            let fd = io_uring::types::Fd(*raw_fd);

                            let fsync_op = io_uring::opcode::Fsync::new(fd).build().user_data(i as u64);

                            unsafe {
                                if ring.submission().push(&fsync_op).is_err() {
                                    // Submission queue full, submit current batch
                                    ring.submit().expect("Failed to submit fsync batch");
                                    ring.submission()
                                        .push(&fsync_op)
                                        .expect("Failed to push fsync op");
                                }
                            }
                        }

                        // Single syscall to submit all fsync operations!
                        match ring.submit_and_wait(fsync_batch.len()) {
                            Ok(submitted) => {
                                debug_print!(
                                    "[flush] submitted {} fsync ops in one syscall",
                                    submitted
                                );
                            }
                            Err(e) => {
                                debug_print!("[flush] failed to submit fsync batch: {}", e);
                            }
                        }

                        // Process completions
                        for _ in 0..fsync_batch.len() {
                            if let Some(cqe) = ring.completion().next() {
                                let idx = cqe.user_data() as usize;
                                let result = cqe.result();

                                if result < 0 {
                                    let (_fd, path) = &fsync_batch[idx];
                                    debug_print!(
                                        "[flush] fsync error for {}: error code {}",
                                        path,
                                        result
                                    );
                                }
                            }
                        }
                    }
                } else {
                    // Mmap backend: Use existing storage.flush() method
                    for path in unique.iter() {
                        if let Some(storage) = pool.get_mut(path) {
                            if let Err(e) = storage.flush() {
                                debug_print!("[flush] flush error for {}: {}", path, e);
                            }
                        }
                    }
                }

                // Phase 4: Handle deletion requests
                while let Ok(path) = del_rx.try_recv() {
                    debug_print!("[reclaim] deletion requested: {}", path);
                    delete_pending.insert(path);
                }

                // Phase 5: Periodic cleanup
                let n = tick.fetch_add(1, Ordering::Relaxed) + 1;
                if n >= 1000 {
                    // WARN: we clean up once every 1000 times the fsync runs
                    if tick
                        .compare_exchange(n, 0, Ordering::AcqRel, Ordering::Relaxed)
                        .is_ok()
                    {
                        let mut empty: HashMap<String, StorageImpl> = HashMap::new();
                        std::mem::swap(&mut pool, &mut empty); // reset map every hour to avoid unconstrained overflow

                        // Perform batched deletions now that mmaps/fds are dropped
                        for path in delete_pending.drain() {
                            match fs::remove_file(&path) {
                                Ok(_) => debug_print!("[reclaim] deleted file {}", path),
                                Err(e) => {
                                    debug_print!("[reclaim] delete failed for {}: {}", path, e)
                                }
                            }
                        }
                    }
                }
            }
        });

        let idx = WalIndex::new("read_offset_idx")?;
        let instance = Walrus {
            allocator,
            reader,
            writers: RwLock::new(HashMap::new()),
            fsync_tx: tx_arc,
            read_offset_index: Arc::new(RwLock::new(idx)),
            read_consistency: mode,
            fsync_schedule,
        };
        instance.startup_chore()?;
        Ok(instance)
    }

    pub fn append_for_topic(&self, col_name: &str, raw_bytes: &[u8]) -> std::io::Result<()> {
        let writer = {
            if let Some(w) = {
                let map = self.writers.read().map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::Other, "writers read lock poisoned")
                })?;
                map.get(col_name).cloned()
            } {
                w
            } else {
                let mut map = self.writers.write().map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::Other, "writers write lock poisoned")
                })?;
                if let Some(w) = map.get(col_name).cloned() {
                    w
                } else {
                    // SAFETY: The returned block will be held by this writer only
                    // and appended/sealed before being exposed to readers.
                    let initial_block = unsafe { self.allocator.get_next_available_block()? };
                    let w = Arc::new(Writer::new(
                        self.allocator.clone(),
                        initial_block,
                        self.reader.clone(),
                        col_name.to_string(),
                        self.fsync_tx.clone(),
                        self.fsync_schedule,
                    ));
                    map.insert(col_name.to_string(), w.clone());
                    w
                }
            }
        };
        writer.write(raw_bytes)
    }

    pub fn batch_append_for_topic(
        &self,
        col_name: &str,
        batch: &[&[u8]],
    ) -> std::io::Result<()> {
        // RAII guard to ensure batch flag is released
        struct BatchGuard<'a> {
            flag: &'a AtomicBool,
        }
        impl<'a> Drop for BatchGuard<'a> {
            fn drop(&mut self) {
                self.flag.store(false, Ordering::Release);
                debug_print!("[batch] released batch_writing flag");
            }
        }

        // Revert info for rollback
        struct BatchRevertInfo {
            original_block_id: u64,
            original_offset: u64,
            allocated_block_ids: Vec<u64>,
        }

        // Phase 0: Validate batch size
        let total_bytes: u64 = batch
            .iter()
            .map(|data| (PREFIX_META_SIZE as u64) + (data.len() as u64))
            .sum();

        const MAX_BATCH_SIZE: u64 = 10 * 1024 * 1024 * 1024; // 10GB
        if total_bytes > MAX_BATCH_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "batch exceeds 10GB limit",
            ));
        }

        if batch.is_empty() {
            return Ok(());
        }

        // Get or create writer
        let writer = {
            if let Some(w) = {
                let map = self.writers.read().map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::Other, "writers read lock poisoned")
                })?;
                map.get(col_name).cloned()
            } {
                w
            } else {
                let mut map = self.writers.write().map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::Other, "writers write lock poisoned")
                })?;
                if let Some(w) = map.get(col_name).cloned() {
                    w
                } else {
                    // SAFETY: The returned block will be held by this writer only
                    let initial_block = unsafe { self.allocator.get_next_available_block()? };
                    let w = Arc::new(Writer::new(
                        self.allocator.clone(),
                        initial_block,
                        self.reader.clone(),
                        col_name.to_string(),
                        self.fsync_tx.clone(),
                        self.fsync_schedule,
                    ));
                    map.insert(col_name.to_string(), w.clone());
                    w
                }
            }
        };

        // Try to acquire batch write flag
        if writer
            .is_batch_writing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "another batch write already in progress",
            ));
        }

        // Ensure we release the flag even if we panic
        let _guard = BatchGuard {
            flag: &writer.is_batch_writing,
        };

        debug_print!(
            "[batch] START: col={}, entries={}, total_bytes={}",
            col_name,
            batch.len(),
            total_bytes
        );

        // Phase 1: Pre-allocation & Planning
        let mut block = writer.current_block.lock().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::Other, "current_block lock poisoned")
        })?;
        let mut cur_offset = writer.current_offset.lock().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::Other, "current_offset lock poisoned")
        })?;

        let mut revert_info = BatchRevertInfo {
            original_block_id: block.id,
            original_offset: *cur_offset,
            allocated_block_ids: Vec::new(),
        };

        // Build write plan: (Block, in_block_offset, batch_index)
        let mut write_plan: Vec<(Block, u64, usize)> = Vec::new();
        let mut batch_idx = 0;

        // Use a LOCAL offset for planning, don't update the writer's offset yet
        let mut planning_offset = *cur_offset;

        while batch_idx < batch.len() {
            let data = batch[batch_idx];
            let need = (PREFIX_META_SIZE as u64) + (data.len() as u64);
            let available = block.limit - planning_offset;

            if available >= need {
                // Fits in current block
                write_plan.push((block.clone(), planning_offset, batch_idx));
                planning_offset += need;
                batch_idx += 1;
            } else {
                // Need to seal and allocate new block
                debug_print!(
                    "[batch] sealing block_id={}, used={}, need={}, limit={}",
                    block.id,
                    planning_offset,
                    need,
                    block.limit
                );
                FileStateTracker::set_block_unlocked(block.id as usize);
                let mut sealed = block.clone();
                sealed.used = planning_offset;
                sealed.mmap.flush()?;
                let _ = self.reader.append_block_to_chain(col_name, sealed);

                // Allocate new block
                // SAFETY: We hold locks, so this writer has exclusive ownership
                let new_block = unsafe { self.allocator.alloc_block(need.max(DEFAULT_BLOCK_SIZE))? };
                debug_print!("[batch] allocated new block_id={}", new_block.id);

                revert_info.allocated_block_ids.push(new_block.id);
                *block = new_block;
                planning_offset = 0;
            }
        }

        debug_print!(
            "[batch] planning complete: {} write operations across {} blocks",
            write_plan.len(),
            revert_info.allocated_block_ids.len() + 1
        );

        // Phase 2 & 3: io_uring preparation and submission (FD backend only)
        let use_io_uring = USE_FD_BACKEND.load(Ordering::Relaxed);

        if use_io_uring {
            // io_uring path
            let ring_size = (write_plan.len() + 64).min(4096) as u32; // Cap at 4096, convert to u32
            let mut ring = io_uring::IoUring::new(ring_size).map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::Other, format!("io_uring init failed: {}", e))
            })?;
            let mut buffers: Vec<Vec<u8>> = Vec::new();

            for (blk, offset, data_idx) in write_plan.iter() {
                let data = batch[*data_idx];
                let next_block_start = blk.offset + blk.limit;

                // Prepare metadata
                let new_meta = Metadata {
                    read_size: data.len(),
                    owned_by: col_name.to_string(),
                    next_block_start,
                    checksum: checksum64(data),
                };

                let meta_bytes = rkyv::to_bytes::<_, 256>(&new_meta).map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("serialize metadata failed: {:?}", e),
                    )
                })?;

                let mut meta_buffer = vec![0u8; PREFIX_META_SIZE];
                meta_buffer[0] = (meta_bytes.len() & 0xFF) as u8;
                meta_buffer[1] = ((meta_bytes.len() >> 8) & 0xFF) as u8;
                meta_buffer[2..2 + meta_bytes.len()].copy_from_slice(&meta_bytes);

                let mut combined = Vec::with_capacity(PREFIX_META_SIZE + data.len());
                combined.extend_from_slice(&meta_buffer);
                combined.extend_from_slice(data);

                let file_offset = blk.offset + offset;

                // Get raw FD
                let fd = if let StorageImpl::Fd(fd_backend) = &blk.mmap.storage {
                    io_uring::types::Fd(fd_backend.file.as_raw_fd())
                } else {
                    // Rollback and fail
                    *cur_offset = revert_info.original_offset;
                    for block_id in revert_info.allocated_block_ids {
                        FileStateTracker::set_block_unlocked(block_id as usize);
                    }
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Unsupported,
                        "batch writes require FD backend",
                    ));
                };

                let write_op = io_uring::opcode::Write::new(
                    fd,
                    combined.as_ptr(),
                    combined.len() as u32,
                )
                .offset(file_offset)
                .build()
                .user_data(*data_idx as u64);

                buffers.push(combined);

                unsafe {
                    ring.submission().push(&write_op).map_err(|e| {
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("io_uring push failed: {}", e),
                        )
                    })?;
                }
            }

            debug_print!(
                "[batch] submitting {} operations via io_uring",
                write_plan.len()
            );

            // Phase 3: Atomic submission
            match ring.submit_and_wait(write_plan.len()) {
                Ok(_) => {
                    let mut all_success = true;
                    for _ in 0..write_plan.len() {
                        if let Some(cqe) = ring.completion().next() {
                            if cqe.result() < 0 {
                                all_success = false;
                                debug_print!(
                                    "[batch] write failed for entry {}: error {}",
                                    cqe.user_data(),
                                    cqe.result()
                                );
                                break;
                            }
                        }
                    }

                    if !all_success {
                        // Clean up garbage before rollback: zero headers for all planned entries
                        for (blk, offset, _idx) in write_plan.iter() {
                            let _ = blk.zero_range(*offset, PREFIX_META_SIZE as u64);
                        }

                        // Ensure zeros are persisted
                        let mut fsynced = std::collections::HashSet::new();
                        for (blk, _, _) in write_plan.iter() {
                            if fsynced.insert(blk.file_path.clone()) {
                                let _ = blk.mmap.flush();
                            }
                        }

                        // Rollback
                        *cur_offset = revert_info.original_offset;
                        for block_id in revert_info.allocated_block_ids {
                            FileStateTracker::set_block_unlocked(block_id as usize);
                        }
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "batch write failed, rolled back",
                        ));
                    }

                    // Success - fsync all touched files
                    let mut fsynced = std::collections::HashSet::new();
                    for (blk, _, _) in write_plan.iter() {
                        if !fsynced.contains(&blk.file_path) {
                            blk.mmap.flush()?;
                            fsynced.insert(blk.file_path.clone());
                        }
                    }

                    // NOW update the writer's offset to make data visible to readers
                    *cur_offset = planning_offset;

                    debug_print!(
                        "[batch] SUCCESS: wrote {} entries, {} bytes to topic={}",
                        batch.len(),
                        total_bytes,
                        col_name
                    );
                    Ok(())
                }
                Err(e) => {
                    // Clean up garbage before rollback: zero headers for all planned entries
                    for (blk, offset, _idx) in write_plan.iter() {
                        let _ = blk.zero_range(*offset, PREFIX_META_SIZE as u64);
                    }

                    // Ensure zeros are persisted
                    let mut fsynced = std::collections::HashSet::new();
                    for (blk, _, _) in write_plan.iter() {
                        if fsynced.insert(blk.file_path.clone()) {
                            let _ = blk.mmap.flush();
                        }
                    }

                    // Rollback
                    *cur_offset = revert_info.original_offset;
                    for block_id in revert_info.allocated_block_ids {
                        FileStateTracker::set_block_unlocked(block_id as usize);
                    }
                    Err(e)
                }
            }
        } else {
            // Fallback: use regular block.write() in a loop (mmap backend)
            for (blk, offset, data_idx) in write_plan.iter() {
                let data = batch[*data_idx];
                let next_block_start = blk.offset + blk.limit;

                if let Err(e) = blk.write(*offset, data, col_name, next_block_start) {
                    // Clean up any partially written headers up to and including the failed index
                    for (w_blk, w_off, _) in write_plan[0..=(*data_idx)].iter() {
                        let _ = w_blk.zero_range(*w_off, PREFIX_META_SIZE as u64);
                    }

                    // Flush zeros and rollback
                    let mut fsynced = std::collections::HashSet::new();
                    for (w_blk, _, _) in write_plan[0..=(*data_idx)].iter() {
                        if fsynced.insert(w_blk.file_path.clone()) {
                            let _ = w_blk.mmap.flush();
                        }
                    }

                    *cur_offset = revert_info.original_offset;
                    for block_id in revert_info.allocated_block_ids {
                        FileStateTracker::set_block_unlocked(block_id as usize);
                    }
                    return Err(e);
                }
            }

            // Success - fsync touched files
            let mut fsynced = std::collections::HashSet::new();
            for (blk, _, _) in write_plan.iter() {
                if !fsynced.contains(&blk.file_path) {
                    blk.mmap.flush()?;
                    fsynced.insert(blk.file_path.clone());
                }
            }

            // NOW update the writer's offset to make data visible to readers
            *cur_offset = planning_offset;

            debug_print!(
                "[batch] SUCCESS (mmap): wrote {} entries, {} bytes to topic={}",
                batch.len(),
                total_bytes,
                col_name
            );
            Ok(())
        }
    }

    pub fn read_next(&self, col_name: &str) -> std::io::Result<Option<Entry>> {
        const TAIL_FLAG: u64 = 1u64 << 63;
        let info_arc = if let Some(arc) = {
            let map = self.reader.data.read().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "reader map read lock poisoned")
            })?;
            map.get(col_name).cloned()
        } {
            arc
        } else {
            let mut map = self.reader.data.write().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "reader map write lock poisoned")
            })?;
            map.entry(col_name.to_string())
                .or_insert_with(|| {
                    Arc::new(RwLock::new(ColReaderInfo {
                        chain: Vec::new(),
                        cur_block_idx: 0,
                        cur_block_offset: 0,
                        reads_since_persist: 0,
                        tail_block_id: 0,
                        tail_offset: 0,
                        hydrated_from_index: false,
                    }))
                })
                .clone()
        };
        let mut info = info_arc.write().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::Other, "col info write lock poisoned")
        })?;
        debug_print!(
            "[reader] read_next start: col={}, chain_len={}, idx={}, offset={}",
            col_name,
            info.chain.len(),
            info.cur_block_idx,
            info.cur_block_offset
        );

        // Load persisted position (supports tail sentinel)
        let mut persisted_tail: Option<(u64 /*block_id*/, u64 /*offset*/)> = None;
        if !info.hydrated_from_index {
            if let Ok(idx_guard) = self.read_offset_index.read() {
                if let Some(pos) = idx_guard.get(col_name) {
                    if (pos.cur_block_idx & TAIL_FLAG) != 0 {
                        let tail_block_id = pos.cur_block_idx & (!TAIL_FLAG);
                        persisted_tail = Some((tail_block_id, pos.cur_block_offset));
                        // sealed state is considered caught up
                        info.cur_block_idx = info.chain.len();
                        info.cur_block_offset = 0;
                    } else {
                        let mut ib = pos.cur_block_idx as usize;
                        if ib > info.chain.len() {
                            ib = info.chain.len();
                        }
                        info.cur_block_idx = ib;
                        if ib < info.chain.len() {
                            let used = info.chain[ib].used;
                            info.cur_block_offset = pos.cur_block_offset.min(used);
                        } else {
                            info.cur_block_offset = 0;
                        }
                    }
                    info.hydrated_from_index = true;
                } else {
                    // No persisted state present; mark hydrated to avoid re-checking every call
                    info.hydrated_from_index = true;
                }
            }
        }

        // If we have a persisted tail and some sealed blocks were recovered, fold into the last block
        if let Some((_, tail_off)) = persisted_tail {
            if !info.chain.is_empty() {
                let ib = info.chain.len() - 1;
                info.cur_block_idx = ib;
                info.cur_block_offset = tail_off.min(info.chain[ib].used);
                if self.should_persist(&mut info, true) {
                    if let Ok(mut idx_guard) = self.read_offset_index.write() {
                        let _ = idx_guard.set(
                            col_name.to_string(),
                            info.cur_block_idx as u64,
                            info.cur_block_offset,
                        );
                    }
                }
                persisted_tail = None;
            }
        }

        // Important: release the per-column lock; we'll reacquire each iteration
        drop(info);

        loop {
            // Reacquire column lock at the start of each iteration
            let mut info = info_arc.write().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "col info write lock poisoned")
            })?;
            // Sealed chain path
            if info.cur_block_idx < info.chain.len() {
                let idx = info.cur_block_idx;
                let off = info.cur_block_offset;
                let block = info.chain[idx].clone();

                if off >= block.used {
                    debug_print!(
                        "[reader] read_next: advance block col={}, block_id={}, offset={}, used={}",
                        col_name,
                        block.id,
                        off,
                        block.used
                    );
                    BlockStateTracker::set_checkpointed_true(block.id as usize);
                    info.cur_block_idx += 1;
                    info.cur_block_offset = 0;
                    continue;
                }

                match block.read(off) {
                    Ok((entry, consumed)) => {
                        // Compute new offset and whether to persist while holding the column lock
                        let new_off = off + consumed as u64;
                        info.cur_block_offset = new_off;
                        let maybe_persist = if self.should_persist(&mut info, false) {
                            Some((info.cur_block_idx as u64, new_off))
                        } else {
                            None
                        };

                        // Drop the column lock before touching the index to avoid lock inversion
                        // Release before returning; we'll not use `info` after this branch
                        drop(info);
                        if let Some((idx_val, off_val)) = maybe_persist {
                            if let Ok(mut idx_guard) = self.read_offset_index.write() {
                                let _ = idx_guard.set(col_name.to_string(), idx_val, off_val);
                            }
                        }

                        debug_print!(
                            "[reader] read_next: OK col={}, block_id={}, consumed={}, new_offset={}",
                            col_name,
                            block.id,
                            consumed,
                            new_off
                        );
                        return Ok(Some(entry));
                    }
                    Err(_) => {
                        debug_print!(
                            "[reader] read_next: read error; skip block col={}, block_id={}, offset={}",
                            col_name,
                            block.id,
                            off
                        );
                        info.cur_block_idx += 1;
                        info.cur_block_offset = 0;
                        continue;
                    }
                }
            }

            // Tail path ---
            // Snapshot in-memory tail state under lock, then drop column lock to avoid lock inversion
            let tail_snapshot = (info.tail_block_id, info.tail_offset);
            drop(info);

            let writer_arc = {
                let map = self.writers.read().map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::Other, "writers read lock poisoned")
                })?;
                match map.get(col_name) {
                    Some(w) => w.clone(),
                    None => return Ok(None),
                }
            };
            let (active_block, written) = {
                let blk = writer_arc.current_block.lock().map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::Other, "current_block lock poisoned")
                })?;
                let off = writer_arc.current_offset.lock().map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::Other, "current_offset lock poisoned")
                })?;
                (blk.clone(), *off)
            };

            // If persisted tail points to a different block and that block is now sealed in chain, fold it
            // Reacquire column lock for folding/rebasing decisions
            let mut info = info_arc.write().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "col info write lock poisoned")
            })?;
            if let Some((tail_block_id, tail_off)) = persisted_tail {
                if tail_block_id != active_block.id {
                    if let Some((idx, _)) = info
                        .chain
                        .iter()
                        .enumerate()
                        .find(|(_, b)| b.id == tail_block_id)
                    {
                        info.cur_block_idx = idx;
                        info.cur_block_offset = tail_off.min(info.chain[idx].used);
                        if self.should_persist(&mut info, true) {
                            if let Ok(mut idx_guard) = self.read_offset_index.write() {
                                let _ = idx_guard.set(
                                    col_name.to_string(),
                                    info.cur_block_idx as u64,
                                    info.cur_block_offset,
                                );
                            }
                        }
                        persisted_tail = None; // sealed now
                        drop(info);
                        continue;
                    } else {
                        // rebase tail to current active block at 0
                        persisted_tail = Some((active_block.id, 0));
                        if self.should_persist(&mut info, true) {
                            if let Ok(mut idx_guard) = self.read_offset_index.write() {
                                let _ = idx_guard.set(
                                    col_name.to_string(),
                                    active_block.id | TAIL_FLAG,
                                    0,
                                );
                            }
                        }
                    }
                }
            } else {
                // No persisted tail; init at current active block start
                persisted_tail = Some((active_block.id, 0));
                if self.should_persist(&mut info, true) {
                    if let Ok(mut idx_guard) = self.read_offset_index.write() {
                        let _ = idx_guard.set(col_name.to_string(), active_block.id | TAIL_FLAG, 0);
                    }
                }
            }
            drop(info);

            // Choose the best known tail offset: prefer in-memory snapshot for current active block
            let (tail_block_id, mut tail_off) = match persisted_tail {
                Some(v) => v,
                None => return Ok(None),
            };
            if tail_block_id == active_block.id {
                let (snap_id, snap_off) = tail_snapshot;
                if snap_id == active_block.id {
                    tail_off = tail_off.max(snap_off);
                }
            } else {
                // If writer rotated and persisted tail points elsewhere, loop above will fold/rebase
            }
            // If writer rotated after we set persisted_tail, loop to fold/rebase
            if tail_block_id != active_block.id {
                // Loop to next iteration; `info` will be reacquired at loop top
                continue;
            }

            if tail_off < written {
                match active_block.read(tail_off) {
                    Ok((entry, consumed)) => {
                        let new_off = tail_off + consumed as u64;
                        // Reacquire column lock to update in-memory progress, then decide persistence
                        let mut info = info_arc.write().map_err(|_| {
                            std::io::Error::new(std::io::ErrorKind::Other, "col info write lock poisoned")
                        })?;
                        info.tail_block_id = active_block.id;
                        info.tail_offset = new_off;
                        let maybe_persist = if self.should_persist(&mut info, false) {
                            Some((tail_block_id | TAIL_FLAG, new_off))
                        } else {
                            None
                        };
                        drop(info);
                        if let Some((idx_val, off_val)) = maybe_persist {
                            if let Ok(mut idx_guard) = self.read_offset_index.write() {
                                let _ = idx_guard.set(col_name.to_string(), idx_val, off_val);
                            }
                        }

                        debug_print!(
                            "[reader] read_next: tail OK col={}, block_id={}, consumed={}, new_tail_off={}",
                            col_name,
                            active_block.id,
                            consumed,
                            new_off
                        );
                        return Ok(Some(entry));
                    }
                    Err(_) => {
                        debug_print!(
                            "[reader] read_next: tail read error col={}, block_id={}, offset={}",
                            col_name,
                            active_block.id,
                            tail_off
                        );
                        return Ok(None);
                    }
                }
            } else {
                debug_print!(
                    "[reader] read_next: tail caught up col={}, block_id={}, off={}, written={}",
                    col_name,
                    active_block.id,
                    tail_off,
                    written
                );
                return Ok(None);
            }
        }
    }

    fn should_persist(&self, info: &mut ColReaderInfo, force: bool) -> bool {
        match self.read_consistency {
            ReadConsistency::StrictlyAtOnce => true,
            ReadConsistency::AtLeastOnce { persist_every } => {
                let every = persist_every.max(1);
                if force {
                    info.reads_since_persist = 0;
                    return true;
                }
                let next = info.reads_since_persist.saturating_add(1);
                if next >= every {
                    info.reads_since_persist = 0;
                    true
                } else {
                    info.reads_since_persist = next;
                    false
                }
            }
        }
    }

    pub fn batch_read_for_topic(&self, col_name: &str, max_bytes: usize) -> std::io::Result<Vec<Entry>> {
        // Helper struct for read planning
        struct ReadPlan {
            blk: Block,
            start: u64,
            end: u64,
            is_tail: bool,
            chain_idx: Option<usize>,
        }

        const TAIL_FLAG: u64 = 1u64 << 63;

        // 1) Get or create reader info
        let info_arc = if let Some(arc) = {
            let map = self.reader.data.read().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "reader map read lock poisoned")
            })?;
            map.get(col_name).cloned()
        } {
            arc
        } else {
            let mut map = self.reader.data.write().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "reader map write lock poisoned")
            })?;
            map.entry(col_name.to_string())
                .or_insert_with(|| {
                    Arc::new(RwLock::new(ColReaderInfo {
                        chain: Vec::new(),
                        cur_block_idx: 0,
                        cur_block_offset: 0,
                        reads_since_persist: 0,
                        tail_block_id: 0,
                        tail_offset: 0,
                        hydrated_from_index: false,
                    }))
                })
                .clone()
        };

        let mut info = info_arc.write().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::Other, "col info write lock poisoned")
        })?;

        // Hydrate from index if needed
        if !info.hydrated_from_index {
            if let Ok(idx_guard) = self.read_offset_index.read() {
                if let Some(pos) = idx_guard.get(col_name) {
                    if (pos.cur_block_idx & TAIL_FLAG) != 0 {
                        let tail_block_id = pos.cur_block_idx & (!TAIL_FLAG);
                        info.tail_block_id = tail_block_id;
                        info.tail_offset = pos.cur_block_offset;
                        info.cur_block_idx = info.chain.len();
                        info.cur_block_offset = 0;
                    } else {
                        let mut ib = pos.cur_block_idx as usize;
                        if ib > info.chain.len() {
                            ib = info.chain.len();
                        }
                        info.cur_block_idx = ib;
                        if ib < info.chain.len() {
                            let used = info.chain[ib].used;
                            info.cur_block_offset = pos.cur_block_offset.min(used);
                        } else {
                            info.cur_block_offset = 0;
                        }
                    }
                }
                info.hydrated_from_index = true;
            }
        }

        // Cache chain snapshot to avoid locking during parse
        let chain_snapshot = info.chain.clone();
        let chain_len_at_plan = chain_snapshot.len();

        // 2) Build read plan from sealed chain, then tail
        // Important: Plan full file ranges; enforce the byte budget only during parse on payload bytes
        let mut plan: Vec<ReadPlan> = Vec::new();
        let mut cur_idx = info.cur_block_idx;
        let mut cur_off = info.cur_block_offset;

        // Plan sealed blocks
        while cur_idx < chain_len_at_plan {
            let blk = chain_snapshot[cur_idx].clone();
            if cur_off < blk.used {
                let end = blk.used; // read rest of sealed block; budget enforced during parse
                plan.push(ReadPlan {
                    blk: blk.clone(),
                    start: cur_off,
                    end,
                    is_tail: false,
                    chain_idx: Some(cur_idx),
                });
            }
            cur_idx += 1;
            cur_off = 0;
        }

        // Plan tail if we're at the end of sealed chain
        if cur_idx >= chain_len_at_plan {
            // Snapshot in-memory tail state then drop column lock before touching writer locks
            let tail_snapshot = (info.tail_block_id, info.tail_offset);
            drop(info);

            // Get active writer block and written offset
            if let Some(writer_arc) = {
                let map = self.writers.read().map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::Other, "writers read lock poisoned")
                })?;
                map.get(col_name).cloned()
            } {
                let (active_block, written) = {
                    let blk = writer_arc.current_block.lock().map_err(|_| {
                        std::io::Error::new(std::io::ErrorKind::Other, "current_block lock poisoned")
                    })?;
                    let off = writer_arc.current_offset.lock().map_err(|_| {
                        std::io::Error::new(std::io::ErrorKind::Other, "current_offset lock poisoned")
                    })?;
                    (blk.clone(), *off)
                };

                // Use in-memory tail progress snapshot if available for this block
                let (snap_id, snap_off) = tail_snapshot;
                let tail_start = if snap_id == active_block.id { snap_off } else { 0 };

                if tail_start < written {
                    let end = written; // read up to current writer offset
                    plan.push(ReadPlan {
                        blk: active_block.clone(),
                        start: tail_start,
                        end,
                        is_tail: true,
                        chain_idx: None,
                    });
                }
            }

            // Reacquire column lock for the rest of the function
            info = info_arc.write().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "col info write lock poisoned")
            })?;
        }

        if plan.is_empty() {
            return Ok(Vec::new());
        }

        // Hold lock across IO/parse for StrictlyAtOnce to avoid duplicate consumption
        let hold_lock_during_io = matches!(self.read_consistency, ReadConsistency::StrictlyAtOnce);
        if !hold_lock_during_io {
            drop(info); // Unlock before IO for AtLeastOnce
        }

        // 3) Read ranges via io_uring (FD backend) or mmap
        let use_io_uring = USE_FD_BACKEND.load(Ordering::Relaxed);
        let buffers: Vec<Vec<u8>>;

        if use_io_uring {
            // io_uring path
            let ring_size = (plan.len() + 64).min(4096) as u32;
            let mut ring = io_uring::IoUring::new(ring_size).map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::Other, format!("io_uring init failed: {}", e))
            })?;

            let mut temp_buffers: Vec<Vec<u8>> = vec![Vec::new(); plan.len()];
            let mut expected_sizes: Vec<usize> = vec![0; plan.len()];

            for (plan_idx, read_plan) in plan.iter().enumerate() {
                let size = (read_plan.end - read_plan.start) as usize;
                expected_sizes[plan_idx] = size;
                let mut buffer = vec![0u8; size];
                let file_offset = read_plan.blk.offset + read_plan.start;

                let fd = if let StorageImpl::Fd(fd_backend) = &read_plan.blk.mmap.storage {
                    io_uring::types::Fd(fd_backend.file.as_raw_fd())
                } else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Unsupported,
                        "batch reads require FD backend when io_uring is enabled",
                    ));
                };

                let read_op = io_uring::opcode::Read::new(fd, buffer.as_mut_ptr(), size as u32)
                    .offset(file_offset)
                    .build()
                    .user_data(plan_idx as u64);

                temp_buffers[plan_idx] = buffer;

                unsafe {
                    ring.submission().push(&read_op).map_err(|e| {
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("io_uring push failed: {}", e),
                        )
                    })?;
                }
            }

            // Submit and wait for all reads
            ring.submit_and_wait(plan.len())?;

            // Process completions and validate read lengths
            for _ in 0..plan.len() {
                if let Some(cqe) = ring.completion().next() {
                    let plan_idx = cqe.user_data() as usize;
                    let got = cqe.result();
                    if got < 0 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("io_uring read failed: {}", got),
                        ));
                    }
                    if (got as usize) != expected_sizes[plan_idx] {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            format!("short read: got {} bytes, expected {}", got, expected_sizes[plan_idx]),
                        ));
                    }
                }
            }

            buffers = temp_buffers;
        } else {
            // mmap fallback: copy ranges
            buffers = plan.iter().map(|read_plan| {
                let size = (read_plan.end - read_plan.start) as usize;
                let mut buffer = vec![0u8; size];
                let file_offset = (read_plan.blk.offset + read_plan.start) as usize;
                read_plan.blk.mmap.read(file_offset, &mut buffer);
                buffer
            }).collect();
        }

        // 4) Parse entries from buffers in plan order
        let mut entries = Vec::new();
        let mut total_data_bytes = 0usize;
        let mut final_block_idx = 0usize;
        let mut final_block_offset = 0u64;
        let mut final_tail_block_id = 0u64;
        let mut final_tail_offset = 0u64;
        let mut entries_parsed = 0u32;
        let mut saw_tail = false;

        for (plan_idx, read_plan) in plan.iter().enumerate() {
            let buffer = &buffers[plan_idx];
            let mut buf_offset = 0usize;

            while buf_offset < buffer.len() {
                // Try to read metadata header
                if buf_offset + PREFIX_META_SIZE > buffer.len() {
                    break; // Not enough data for header
                }

                let meta_len = (buffer[buf_offset] as usize) | ((buffer[buf_offset + 1] as usize) << 8);

                if meta_len == 0 || meta_len > PREFIX_META_SIZE - 2 {
                    // Invalid or zeroed header - stop parsing this block
                    break;
                }

                // Deserialize metadata
                let mut aligned = rkyv::AlignedVec::with_capacity(meta_len);
                aligned.extend_from_slice(&buffer[buf_offset + 2..buf_offset + 2 + meta_len]);

                let archived = unsafe { rkyv::archived_root::<Metadata>(&aligned[..]) };
                let meta: Metadata = match archived.deserialize(&mut rkyv::Infallible) {
                    Ok(m) => m,
                    Err(_) => break, // Parse error - stop
                };

                let data_size = meta.read_size;
                let entry_consumed = PREFIX_META_SIZE + data_size;

                // Check if we have enough buffer space for the data
                if buf_offset + entry_consumed > buffer.len() {
                    break; // Incomplete entry
                }

                // Check budget
                if total_data_bytes + data_size > max_bytes {
                    // Would exceed budget - stop here
                    break;
                }

                // Extract and verify data
                let data_start = buf_offset + PREFIX_META_SIZE;
                let data_end = data_start + data_size;
                let data_slice = &buffer[data_start..data_end];

                // Verify checksum
                if checksum64(data_slice) != meta.checksum {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "checksum mismatch in batch read",
                    ));
                }

                // Add to results
                entries.push(Entry { data: data_slice.to_vec() });
                total_data_bytes += data_size;
                entries_parsed += 1;

                // Update position tracking
                let in_block_offset = read_plan.start + buf_offset as u64 + entry_consumed as u64;

                if read_plan.is_tail {
                    saw_tail = true;
                    final_tail_block_id = read_plan.blk.id;
                    final_tail_offset = in_block_offset;
                } else if let Some(idx) = read_plan.chain_idx {
                    final_block_idx = idx;
                    final_block_offset = in_block_offset;
                }

                buf_offset += entry_consumed;
            }
        }

        // 5) Commit progress
        if entries_parsed > 0 {
            // Prepare persistence target while holding the appropriate lock
            enum PersistTarget { Tail { blk_id: u64, off: u64 }, Sealed { idx: u64, off: u64 }, None }
            let mut target = PersistTarget::None;

            if hold_lock_during_io {
                // We still hold the original write guard `info` here
                // Update position
                if saw_tail {
                    info.cur_block_idx = chain_len_at_plan;
                    info.cur_block_offset = 0;
                    info.tail_block_id = final_tail_block_id;
                    info.tail_offset = final_tail_offset;
                    match self.read_consistency {
                        ReadConsistency::StrictlyAtOnce => {
                            target = PersistTarget::Tail { blk_id: final_tail_block_id, off: final_tail_offset };
                        }
                        ReadConsistency::AtLeastOnce { persist_every: _ } => {}
                    }
                } else {
                    info.cur_block_idx = final_block_idx;
                    info.cur_block_offset = final_block_offset;
                    match self.read_consistency {
                        ReadConsistency::StrictlyAtOnce => {
                            target = PersistTarget::Sealed { idx: final_block_idx as u64, off: final_block_offset };
                        }
                        ReadConsistency::AtLeastOnce { persist_every: _ } => {}
                    }
                }
                // Release before persisting index
                drop(info);
            } else {
                // Reacquire to update
                let mut info2 = info_arc.write().map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::Other, "col info write lock poisoned")
                })?;
                if saw_tail {
                    info2.cur_block_idx = chain_len_at_plan;
                    info2.cur_block_offset = 0;
                    info2.tail_block_id = final_tail_block_id;
                    info2.tail_offset = final_tail_offset;
                    if let ReadConsistency::AtLeastOnce { persist_every } = self.read_consistency {
                        info2.reads_since_persist = info2.reads_since_persist.saturating_add(entries_parsed);
                        if info2.reads_since_persist >= persist_every {
                            info2.reads_since_persist = 0;
                            target = PersistTarget::Tail { blk_id: final_tail_block_id, off: final_tail_offset };
                        }
                    }
                } else {
                    info2.cur_block_idx = final_block_idx;
                    info2.cur_block_offset = final_block_offset;
                    if let ReadConsistency::AtLeastOnce { persist_every } = self.read_consistency {
                        info2.reads_since_persist = info2.reads_since_persist.saturating_add(entries_parsed);
                        if info2.reads_since_persist >= persist_every {
                            info2.reads_since_persist = 0;
                            target = PersistTarget::Sealed { idx: final_block_idx as u64, off: final_block_offset };
                        }
                    }
                }
                drop(info2);
            }

            // Persist if needed
            match target {
                PersistTarget::Tail { blk_id, off } => {
                    if let Ok(mut idx_guard) = self.read_offset_index.write() {
                        let _ = idx_guard.set(col_name.to_string(), blk_id | TAIL_FLAG, off);
                    }
                }
                PersistTarget::Sealed { idx, off } => {
                    if let Ok(mut idx_guard) = self.read_offset_index.write() {
                        let _ = idx_guard.set(col_name.to_string(), idx, off);
                    }
                }
                PersistTarget::None => {}
            }
        }

        Ok(entries)
    }

    fn startup_chore(&self) -> std::io::Result<()> {
        // Minimal recovery: scan wal_files, build reader chains, and rebuild trackers
        let dir = match fs::read_dir("./wal_files") {
            Ok(d) => d,
            Err(_) => return Ok(()),
        };
        let mut files: Vec<String> = Vec::new();
        for entry in dir.flatten() {
            let path = entry.path();
            if let Some(s) = path.to_str() {
                // skip index files
                if s.ends_with("_index.db") {
                    continue;
                }
                files.push(s.to_string());
            }
        }
        files.sort();
        if !files.is_empty() {
            debug_print!("[recovery] scanning files: {}", files.len());
        }

        // synthetic block ids btw
        let mut next_block_id: usize = 1;
        let mut seen_files = std::collections::HashSet::new();

        for file_path in files.iter() {
            let mmap = match SharedMmapKeeper::get_mmap_arc(file_path) {
                Ok(m) => m,
                Err(e) => {
                    debug_print!("[recovery] mmap open failed for {}: {}", file_path, e);
                    continue;
                }
            };
            seen_files.insert(file_path.clone());
            FileStateTracker::register_file_if_absent(file_path);
            debug_print!("[recovery] file {}", file_path);

            let mut block_offset: u64 = 0;
            while block_offset + DEFAULT_BLOCK_SIZE <= MAX_FILE_SIZE {
                // heuristic: if first bytes are zero, assume no more blocks
                let mut probe = [0u8; 8];
                mmap.read(block_offset as usize, &mut probe);
                if probe.iter().all(|&b| b == 0) {
                    break;
                }

                let mut used: u64 = 0;
                let mut col_name: Option<String> = None;

                // try to read first metadata to get column name (with 2-byte length prefix)
                let mut meta_buf = vec![0u8; PREFIX_META_SIZE];
                mmap.read(block_offset as usize, &mut meta_buf);
                let meta_len = (meta_buf[0] as usize) | ((meta_buf[1] as usize) << 8);
                if meta_len == 0 || meta_len > PREFIX_META_SIZE - 2 {
                    break;
                }
                let mut aligned = rkyv::AlignedVec::with_capacity(meta_len);
                aligned.extend_from_slice(&meta_buf[2..2 + meta_len]);
                // SAFETY: `aligned` was constructed from a bounded metadata slice
                // read from our file; alignment is ensured by `AlignedVec`.
                // SAFETY: `aligned` is built from bounded bytes inside the block,
                // copied into `AlignedVec` ensuring alignment for rkyv.
                let archived = unsafe { rkyv::archived_root::<Metadata>(&aligned[..]) };
                let md: Metadata = match archived.deserialize(&mut rkyv::Infallible) {
                    Ok(m) => m,
                    Err(_) => {
                        break;
                    }
                };
                col_name = Some(md.owned_by);

                // scan entries to compute used
                let block_stub = Block {
                    id: next_block_id as u64,
                    file_path: file_path.clone(),
                    offset: block_offset,
                    limit: DEFAULT_BLOCK_SIZE,
                    mmap: mmap.clone(),
                    used: 0,
                };
                let mut in_block_off: u64 = 0;
                loop {
                    match block_stub.read(in_block_off) {
                        Ok((_entry, consumed)) => {
                            used += consumed as u64;
                            in_block_off += consumed as u64;
                            if in_block_off >= DEFAULT_BLOCK_SIZE {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                if used == 0 {
                    break;
                }

                let block = Block {
                    id: next_block_id as u64,
                    file_path: file_path.clone(),
                    offset: block_offset,
                    limit: DEFAULT_BLOCK_SIZE,
                    mmap: mmap.clone(),
                    used,
                };
                // register and append
                BlockStateTracker::register_block(next_block_id, file_path);
                FileStateTracker::add_block_to_file_state(file_path);
                if let Some(col) = col_name {
                    let _ = self.reader.append_block_to_chain(&col, block.clone());
                    debug_print!(
                        "[recovery] appended block: file={}, block_id={}, used={}, col={}",
                        file_path,
                        block.id,
                        block.used,
                        col
                    );
                }
                next_block_id += 1;
                block_offset += DEFAULT_BLOCK_SIZE;
            }
        }

        // restore read positions and checkpoint state
        if let Ok(idx_guard) = self.read_offset_index.read() {
            let map = self.reader.data.read().ok();
            if let Some(map) = map {
                for (col, info_arc) in map.iter() {
                    if let Some(pos) = idx_guard.get(col) {
                        let mut info = match info_arc.write() {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        let mut ib = pos.cur_block_idx as usize;
                        if ib > info.chain.len() {
                            ib = info.chain.len();
                        }
                        info.cur_block_idx = ib;
                        if ib < info.chain.len() {
                            let used = info.chain[ib].used;
                            info.cur_block_offset = pos.cur_block_offset.min(used);
                        } else {
                            info.cur_block_offset = 0;
                        }
                        for i in 0..ib {
                            BlockStateTracker::set_checkpointed_true(info.chain[i].id as usize);
                        }
                        if ib < info.chain.len() && info.cur_block_offset >= info.chain[ib].used {
                            BlockStateTracker::set_checkpointed_true(info.chain[ib].id as usize);
                        }
                    }
                }
            }
        }

        // enqueue deletion checks
        for f in seen_files.into_iter() {
            flush_check(f);
        }
        Ok(())
    }
}
static DELETION_TX: OnceLock<Arc<mpsc::Sender<String>>> = OnceLock::new();

fn flush_check(file_path: String) {
    // readiness check fast path; hook actual reclamation later
    if let Some((locked, checkpointed, total, fully_allocated)) =
        FileStateTracker::get_state_snapshot(&file_path)
    {
        let ready_to_delete = fully_allocated && locked == 0 && total > 0 && checkpointed >= total;
        if ready_to_delete {
            if let Some(tx) = DELETION_TX.get() {
                let _ = tx.send(file_path);
            }
        }
    }
}

struct BlockState {
    is_checkpointed: AtomicBool,
    file_path: String,
}

struct BlockStateTracker {}

impl BlockStateTracker {
    fn map() -> &'static RwLock<HashMap<usize, BlockState>> {
        static MAP: OnceLock<RwLock<HashMap<usize, BlockState>>> = OnceLock::new();
        MAP.get_or_init(|| RwLock::new(HashMap::new()))
    }

    fn new() {
        let _ = Self::map();
    }

    fn register_block(block_id: usize, file_path: &str) {
        let map = Self::map();
        if let Ok(mut w) = map.write() {
            w.entry(block_id).or_insert_with(|| BlockState {
                is_checkpointed: AtomicBool::new(false),
                file_path: file_path.to_string(),
            });
        }
    }

    fn get_file_path_for_block(block_id: usize) -> Option<String> {
        let map = Self::map();
        let r = map.read().ok()?;
        r.get(&block_id).map(|b| b.file_path.clone())
    }

    fn set_checkpointed_true(block_id: usize) {
        let path_opt = {
            let map = Self::map();
            if let Ok(r) = map.read() {
                if let Some(b) = r.get(&block_id) {
                    b.is_checkpointed.store(true, Ordering::Release);
                    Some(b.file_path.clone())
                } else {
                    None
                }
            } else {
                None
            }
        };

        if let Some(path) = path_opt {
            FileStateTracker::inc_checkpoint_for_file(&path);
            flush_check(path);
        }
    }
}

struct FileState {
    locked_block_ctr: AtomicU16,
    checkpoint_block_ctr: AtomicU16,
    total_blocks: AtomicU16,
    is_fully_allocated: AtomicBool,
}

struct FileStateTracker {}

impl FileStateTracker {
    fn map() -> &'static RwLock<HashMap<String, FileState>> {
        static MAP: OnceLock<RwLock<HashMap<String, FileState>>> = OnceLock::new();
        MAP.get_or_init(|| RwLock::new(HashMap::new()))
    }

    fn new() {
        let _ = Self::map();
    }

    fn register_file_if_absent(file_path: &str) {
        let map = Self::map();
        let mut w = map.write().expect("file state map write lock poisoned");
        w.entry(file_path.to_string()).or_insert_with(|| FileState {
            locked_block_ctr: AtomicU16::new(0),
            checkpoint_block_ctr: AtomicU16::new(0),
            total_blocks: AtomicU16::new(0),
            is_fully_allocated: AtomicBool::new(false),
        });
    }

    fn add_block_to_file_state(file_path: &str) {
        Self::register_file_if_absent(file_path);
        let map = Self::map();
        if let Ok(r) = map.read() {
            if let Some(st) = r.get(file_path) {
                st.total_blocks.fetch_add(1, Ordering::AcqRel);
            }
        }
    }

    fn set_fully_allocated(file_path: String) {
        Self::register_file_if_absent(&file_path);
        let map = Self::map();
        if let Ok(r) = map.read() {
            if let Some(st) = r.get(&file_path) {
                st.is_fully_allocated.store(true, Ordering::Release);
            }
        }
        flush_check(file_path);
    }

    fn set_block_locked(block_id: usize) {
        if let Some(path) = BlockStateTracker::get_file_path_for_block(block_id) {
            let map = Self::map();
            if let Ok(r) = map.read() {
                if let Some(st) = r.get(&path) {
                    st.locked_block_ctr.fetch_add(1, Ordering::AcqRel);
                }
            }
        }
    }

    fn set_block_unlocked(block_id: usize) {
        if let Some(path) = BlockStateTracker::get_file_path_for_block(block_id) {
            let map = Self::map();
            if let Ok(r) = map.read() {
                if let Some(st) = r.get(&path) {
                    st.locked_block_ctr.fetch_sub(1, Ordering::AcqRel);
                }
            }
            flush_check(path);
        }
    }

    fn inc_checkpoint_for_file(file_path: &str) {
        let map = Self::map();
        if let Ok(r) = map.read() {
            if let Some(st) = r.get(file_path) {
                st.checkpoint_block_ctr.fetch_add(1, Ordering::AcqRel);
            }
        }
    }

    fn get_state_snapshot(file_path: &str) -> Option<(u16, u16, u16, bool)> {
        let map = Self::map();
        let r = map.read().ok()?;
        let st = r.get(file_path)?;
        let locked = st.locked_block_ctr.load(Ordering::Acquire);
        let checkpointed = st.checkpoint_block_ctr.load(Ordering::Acquire);
        let total = st.total_blocks.load(Ordering::Acquire);
        let fully = st.is_fully_allocated.load(Ordering::Acquire);
        Some((locked, checkpointed, total, fully))
    }
}
