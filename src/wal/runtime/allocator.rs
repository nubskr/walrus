use crate::wal::block::Block;
use crate::wal::config::{DEFAULT_BLOCK_SIZE, MAX_ALLOC, MAX_FILE_SIZE, debug_print};
use crate::wal::paths::WalPathManager;
use crate::wal::storage::{SharedMmap, SharedMmapKeeper};
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::{Arc, OnceLock, RwLock};

use super::DELETION_TX;

pub(super) struct BlockAllocator {
    next_block: UnsafeCell<Block>,
    lock: AtomicBool,
    paths: Arc<WalPathManager>,
}

impl BlockAllocator {
    pub(super) fn new(paths: Arc<WalPathManager>) -> std::io::Result<Self> {
        let file1 = paths.create_new_file()?;
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
            paths,
        })
    }

    /// SAFETY: Caller must ensure the returned `Block` is treated as uniquely
    /// owned by a single writer until it is sealed. Internally, a spin lock
    /// ensures exclusive mutable access to `next_block` while computing the
    /// next allocation, so the interior `UnsafeCell` is not concurrently
    /// accessed mutably.
    pub(super) unsafe fn get_next_available_block(&self) -> std::io::Result<Block> {
        self.lock();
        // SAFETY: Guarded by `self.lock()` above, providing exclusive access
        // to `next_block` so creating a `&mut` from `UnsafeCell` is sound.
        let data = unsafe { &mut *self.next_block.get() };
        let prev_block_file_path = data.file_path.clone();
        if data.offset >= MAX_FILE_SIZE {
            // mark previous file as fully allocated before switching
            FileStateTracker::set_fully_allocated(prev_block_file_path);
            data.file_path = self.paths.create_new_file()?;
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
    pub(super) unsafe fn alloc_block(&self, want_bytes: u64) -> std::io::Result<Block> {
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
            data.file_path = self.paths.create_new_file()?;
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

    pub(super) unsafe fn fast_forward(&self, next_id: u64) {
        self.lock();
        let data = unsafe { &mut *self.next_block.get() };
        if next_id > data.id {
            let _diff = next_id - data.id;
            data.id = next_id;
            // Assume sequential blocks in current file
            // This might be wrong if we spanned multiple files, but startup_chore
            // should have handled file rollover by calling create_new_file?
            // Actually startup_chore just scans.
            // We need to ensure allocator points to a fresh location.
            // If we just increment ID, offset remains 0 (in new file).
            // This is fine! We want NEW blocks.
            // We just want to avoid ID collision.
        }
        self.unlock();
    }
}

// SAFETY: `BlockAllocator` uses an internal spin lock to guard all mutable
// access to `next_block`. It does not expose references to its interior
// without holding that lock, so concurrent access across threads is safe.
unsafe impl Sync for BlockAllocator {}
// SAFETY: The type contains only thread-safe primitives and does not rely on
// thread-affine resources; moving it to another thread is safe.
unsafe impl Send for BlockAllocator {}

pub(super) fn flush_check(file_path: String) {
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

pub(super) struct BlockStateTracker {}

impl BlockStateTracker {
    fn map() -> &'static RwLock<HashMap<usize, BlockState>> {
        static MAP: OnceLock<RwLock<HashMap<usize, BlockState>>> = OnceLock::new();
        MAP.get_or_init(|| RwLock::new(HashMap::new()))
    }

    pub(super) fn register_block(block_id: usize, file_path: &str) {
        let map = Self::map();
        if let Ok(mut w) = map.write() {
            w.entry(block_id).or_insert_with(|| BlockState {
                is_checkpointed: AtomicBool::new(false),
                file_path: file_path.to_string(),
            });
        }
    }

    pub(super) fn get_file_path_for_block(block_id: usize) -> Option<String> {
        let map = Self::map();
        let r = map.read().ok()?;
        r.get(&block_id).map(|b| b.file_path.clone())
    }

    pub(super) fn set_checkpointed_true(block_id: usize) {
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

pub(super) struct FileStateTracker {}

impl FileStateTracker {
    fn map() -> &'static RwLock<HashMap<String, FileState>> {
        static MAP: OnceLock<RwLock<HashMap<String, FileState>>> = OnceLock::new();
        MAP.get_or_init(|| RwLock::new(HashMap::new()))
    }

    pub(super) fn register_file_if_absent(file_path: &str) {
        let map = Self::map();
        let mut w = map.write().expect("file state map write lock poisoned");
        w.entry(file_path.to_string()).or_insert_with(|| FileState {
            locked_block_ctr: AtomicU16::new(0),
            checkpoint_block_ctr: AtomicU16::new(0),
            total_blocks: AtomicU16::new(0),
            is_fully_allocated: AtomicBool::new(false),
        });
    }

    pub(super) fn add_block_to_file_state(file_path: &str) {
        Self::register_file_if_absent(file_path);
        let map = Self::map();
        if let Ok(r) = map.read() {
            if let Some(st) = r.get(file_path) {
                st.total_blocks.fetch_add(1, Ordering::AcqRel);
            }
        }
    }

    pub(super) fn set_fully_allocated(file_path: String) {
        Self::register_file_if_absent(&file_path);
        let map = Self::map();
        if let Ok(r) = map.read() {
            if let Some(st) = r.get(&file_path) {
                st.is_fully_allocated.store(true, Ordering::Release);
            }
        }
        flush_check(file_path);
    }

    pub(super) fn set_block_locked(block_id: usize) {
        if let Some(path) = BlockStateTracker::get_file_path_for_block(block_id) {
            let map = Self::map();
            if let Ok(r) = map.read() {
                if let Some(st) = r.get(&path) {
                    st.locked_block_ctr.fetch_add(1, Ordering::AcqRel);
                }
            }
        }
    }

    pub(super) fn set_block_unlocked(block_id: usize) {
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

    pub(super) fn inc_checkpoint_for_file(file_path: &str) {
        let map = Self::map();
        if let Ok(r) = map.read() {
            if let Some(st) = r.get(file_path) {
                st.checkpoint_block_ctr.fetch_add(1, Ordering::AcqRel);
            }
        }
    }

    pub(super) fn get_state_snapshot(file_path: &str) -> Option<(u16, u16, u16, bool)> {
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
