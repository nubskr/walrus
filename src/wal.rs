/*
something like:
just do the dumbest thing now, first thing which comes to your mind, that's it
we will do it all in one file
*/
use std::collections::HashMap;
use std::time::SystemTime;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::cell::UnsafeCell;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use rkyv::{Archive, Deserialize, Serialize};
// removed unused rkyv::ser imports
use std::fs;
use std::path::Path;

// Macro to conditionally print debug messages
macro_rules! debug_print {
    ($($arg:tt)*) => {
        if std::env::var("WALRUS_QUIET").is_err() {
            println!($($arg)*);
        }
    };
}

const DEFAULT_BLOCK_SIZE:u64 = 10 * 1024 * 1024; // 10mb
const BLOCKS_PER_FILE:u64 = 100;
const MAX_ALLOC:u64 = 1 * 1024 * 1024 * 1024; // 1 GiB cap per block
const FSYNC_SCHEDULE:u64 = 1; // seconds btw
const PREFIX_META_SIZE: usize = 64; 
const MAX_FILE_SIZE:u64 = DEFAULT_BLOCK_SIZE * BLOCKS_PER_FILE;

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

#[derive(Clone, Debug)]

pub struct Entry {
    pub data: Vec<u8> // raw bytes
}

#[derive(Archive, Deserialize, Serialize, Debug)]
#[archive(check_bytes)]
struct Metadata {
    read_size: usize,
    owned_by: String,
    next_block_start: u64,
    checksum: u64,
}

// todo: add checksum and mmap object here too
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
    fn write(&self, in_block_offset: u64, data: &[u8], owned_by: &str, next_block_start: u64) -> std::io::Result<()> {
        debug_assert!(in_block_offset + (data.len() as u64 + PREFIX_META_SIZE as u64) <= self.limit);
        
        let new_meta = Metadata { 
            read_size: data.len(),
            owned_by: owned_by.to_string(),
            next_block_start,
            checksum: checksum64(data),
        };
        
        let meta_bytes = rkyv::to_bytes::<_, 256>(&new_meta)
            .map_err(|e| std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("serialize metadata failed: {:?}", e),
            ))?;
        if meta_bytes.len() > PREFIX_META_SIZE - 2 {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "metadata too large"));
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
                format!("invalid metadata length: {}", meta_len)
            ));
        }
        
        // Deserialize only the actual metadata bytes (skip the 2-byte length prefix)
        let mut aligned = rkyv::AlignedVec::with_capacity(meta_len);
        aligned.extend_from_slice(&meta_buffer[2..2 + meta_len]);
        
        let archived = unsafe { rkyv::archived_root::<Metadata>(&aligned[..]) };
        let meta: Metadata = archived
            .deserialize(&mut rkyv::Infallible)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "failed to deserialize metadata"))?;
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
                in_block_offset, self.file_path, self.id
            );
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData, 
                "checksum mismatch, data corruption detected"
            ));
        }
        
        let consumed = PREFIX_META_SIZE + actual_entry_size;
        Ok((Entry { data: ret_buffer }, consumed))
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
    lock: AtomicBool
}

impl BlockAllocator {
    pub fn new() -> std::io::Result<Self> {
        std::fs::create_dir_all("wal_files").ok();
        let file1 = make_new_file()?;
        let mmap: Arc<SharedMmap> = SharedMmapKeeper::get_mmap_arc(&file1)?;
        debug_print!("[alloc] init: created file={}, max_file_size={}B, block_size={}B", file1, MAX_FILE_SIZE, DEFAULT_BLOCK_SIZE);
        Ok(BlockAllocator {
            next_block: UnsafeCell::new(Block { id: 1, offset: 0, limit: DEFAULT_BLOCK_SIZE, file_path: file1, mmap, used: 0 }),
            lock: AtomicBool::new(false),
        })
    }
    
    // I NEED TO LOCK THE FUCKING BLOCK WE ARE ALLOCATING, WHERE DO I PUT IT HERE AAAAAAAAAAAAAAA
    pub unsafe fn get_next_available_block(&self) -> std::io::Result<Block> {
        self.lock();
        let data = unsafe { &mut *self.next_block.get() };
        let mut prev_block_file_path = data.file_path.clone();
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
            ret.id, ret.file_path, ret.offset, ret.limit
        );
        Ok(ret)
    }

    pub unsafe fn alloc_block(&self, want_bytes: u64) -> std::io::Result<Block> {
        if want_bytes == 0 || want_bytes > MAX_ALLOC {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid allocation size, a single entry can't be more than 1gb"));
        }
        let alloc_units = (want_bytes + DEFAULT_BLOCK_SIZE - 1) / DEFAULT_BLOCK_SIZE;
        let alloc_size = alloc_units * DEFAULT_BLOCK_SIZE;
        debug_print!("[alloc] alloc_block: want_bytes={}, units={}, size={}", want_bytes, alloc_units, alloc_size);

        self.lock();
        let data = unsafe { &mut *self.next_block.get() };
        if data.offset + alloc_size > MAX_FILE_SIZE {
            let prev_block_file_path = data.file_path.clone();
            data.file_path = make_new_file()?;
            data.mmap = SharedMmapKeeper::get_mmap_arc(&data.file_path)?;
            data.offset = 0;
            // mark the previous file fully allocated now
            FileStateTracker::set_fully_allocated(prev_block_file_path);
            debug_print!("[alloc] file rollover for sized alloc -> {}", data.file_path);
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
            ret.id, ret.file_path, ret.offset, ret.limit
        );
        Ok(ret)
    }

    fn lock(&self) {
        // just keep spinnnnnnnnning
        while self.lock.compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed).is_err() {
            std::hint::spin_loop();  // atleast let the poor sucker know to not go BRRRRR in full speed here
        }
    }

    fn unlock(&self) {
        self.lock.store(false, Ordering::Release);
    }

}

unsafe impl Sync for BlockAllocator {}
unsafe impl Send for BlockAllocator {}

struct Writer{
    allocator: Arc<BlockAllocator>,
    current_block: Mutex<Block>,
    reader: Arc<Reader>,
    col: String,
    publisher: Arc<mpsc::Sender<String>>,
    current_offset: Mutex<u64>,
}

impl Writer {
    pub fn new(allocator: Arc<BlockAllocator>, current_block: Block, reader: Arc<Reader>, col: String, publisher: Arc<mpsc::Sender<String>>) -> Self {
        Writer { allocator, current_block: Mutex::new(current_block), reader, col: col.clone(), publisher, current_offset: Mutex::new(0) }
    }

    pub fn write(&self, data: &[u8]) -> std::io::Result<()> {
        let mut block = self.current_block.lock().map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "current_block lock poisoned"))?;
        let mut cur = self.current_offset.lock().map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "current_offset lock poisoned"))?;

        let need = (PREFIX_META_SIZE as u64) + (data.len() as u64);
        if *cur + need > block.limit {
            debug_print!(
                "[writer] sealing: col={}, block_id={}, used={}, need={}, limit={}",
                self.col, block.id, *cur, need, block.limit
            );
            FileStateTracker::set_block_unlocked(block.id as usize);
            let mut sealed = block.clone();
            sealed.used = *cur;
            sealed.mmap.flush()?;
            let _ = self.reader.append_block_to_chain(&self.col, sealed);
            debug_print!("[writer] appended sealed block to chain: col={}", self.col);
            // switch to new block
            let new_block = unsafe { self.allocator.alloc_block(need) }?;
            debug_print!("[writer] switched to new block: col={}, new_block_id={}", self.col, new_block.id);
            *block = new_block;
            *cur = 0;
        }
        let next_block_start = block.offset + block.limit; // simplistic for now
        block.write(*cur, data, &self.col, next_block_start)?;
        debug_print!(
            "[writer] wrote: col={}, block_id={}, offset_before={}, bytes={}, offset_after={}",
            self.col, block.id, *cur, need, *cur + need
        );
        *cur += need;
        let _ = self.publisher.send(block.file_path.clone());
        Ok(())
    }
}


// this is gonna slappp, f1 level stuff

#[derive(Debug)]
struct SharedMmap {
    mmap: MmapMut,
    last_touched_at: AtomicU64,
}

unsafe impl Sync for SharedMmap {}
unsafe impl Send for SharedMmap {}

impl SharedMmap {
    pub fn new(path: &str) -> std::io::Result<Arc<Self>> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_millis() as u64;
        Ok(Arc::new(Self { mmap, last_touched_at: AtomicU64::new(now_ms) }))
    }
    
    pub fn write(&self, offset: usize, data: &[u8]) {
        unsafe  {
            let ptr = self.mmap.as_ptr() as *mut u8; // Get pointer when needed
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                ptr.add(offset),
                data.len()
            );
        }
        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_millis() as u64;
        self.last_touched_at.store(now_ms, Ordering::Relaxed);
    }
    
    pub fn read(&self, offset: usize, dest: &mut [u8]) {
        debug_assert!(offset + dest.len() <= self.mmap.len());
        let src = &self.mmap[offset..offset + dest.len()];
        dest.copy_from_slice(src);
    }
    
    pub fn len(&self) -> usize {
        self.mmap.len()
    }

    pub fn flush(&self) -> std::io::Result<()> { // DONT USE THIS
        self.mmap.flush()
    }
}

struct SharedMmapKeeper {
    data: HashMap<String,Arc<SharedMmap>>
}

impl SharedMmapKeeper {
    fn new() -> Self {
        Self {
            data: HashMap::new()
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
            let keeper = keeper_lock.read().map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "mmap keeper read lock poisoned"))?;
            if let Some(existing) = keeper.data.get(path) {
                return Ok(existing.clone());
            }
        }

        let mut keeper = keeper_lock.write().map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "mmap keeper write lock poisoned"))?;
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
    cur_block_offset: u64
}

struct Reader {
    data: RwLock<HashMap<String,Arc<RwLock<ColReaderInfo>>>> // M[col] -> {chain,last_read_offset}
}

impl Reader {
    fn new() -> Self {
        Self { data: RwLock::new(HashMap::new()) }
    }

    fn get_chain_for_col(&self, col: &str) -> Option<Vec<Block>>{
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
            let map = self.data.read().map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "reader map read lock poisoned"))?;
            map.get(col).cloned()
        } {
            let mut info = info_arc.write().map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "col info write lock poisoned"))?;
            let before = info.chain.len();
            info.chain.push(block.clone());
            debug_print!(
                "[reader] chain append(fast): col={}, block_id={}, chain_len {}->{}",
                col, block.id, before, before + 1
            );
            return Ok(());
        }

        // slow path
        let info_arc = {
            let mut map = self.data.write().map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "reader map write lock poisoned"))?;
            map.entry(col.to_string())
                .or_insert_with(|| Arc::new(RwLock::new(ColReaderInfo { chain: Vec::new(), cur_block_idx: 0, cur_block_offset: 0 })))
                .clone()
        };
        let mut info = info_arc.write().map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "col info write lock poisoned"))?;
        info.chain.push(block.clone());
        debug_print!(
            "[reader] chain append(slow/new): col={}, block_id={}, chain_len {}->{}",
            col, block.id, 0, 1
        );
        Ok(())
    }
}


// ------------------


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
        let path = format!("./wal_files/{}_index.db",file_name) ;

        let store = Path::new(&path).exists()
            .then(|| fs::read(&path).ok())
            .flatten()
            .and_then(|bytes| {
                if bytes.is_empty() { return None; }
                let archived = unsafe { rkyv::archived_root::<HashMap<String, BlockPos>>(&bytes) };
                archived.deserialize(&mut rkyv::Infallible).ok()
            })
            .unwrap_or_default();
        
        Ok(Self { 
            store, 
            path: path.to_string() 
        })
    }
    
    pub fn set(&mut self, key: String, idx: u64, offset: u64) -> std::io::Result<()> {
        self.store.insert(key, BlockPos { 
            cur_block_idx: idx, 
            cur_block_offset: offset 
        });
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
        let bytes = rkyv::to_bytes::<_, 256>(&self.store)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("index serialize failed: {:?}", e)))?;
        
        fs::write(&tmp_path, &bytes)?;
        fs::File::open(&tmp_path)?.sync_all()?;
        fs::rename(&tmp_path, &self.path)?;
        Ok(())
    }
}

// -------------------

// public APIs
pub struct Walrus {
    allocator: Arc<BlockAllocator>,
    reader: Arc<Reader>,
    writers: RwLock<HashMap<String, Arc<Writer>>>,
    fsync_tx: Arc<mpsc::Sender<String>>,
    read_offset_index: Arc<RwLock<WalIndex>>,
}

impl Walrus {
    pub fn new() -> std::io::Result<Self> {
        debug_print!("[walrus] new");
        let allocator = Arc::new(BlockAllocator::new()?);
        let reader = Arc::new(Reader::new());
        let (tx, rx) = mpsc::channel::<String>();
        let tx_arc = Arc::new(tx);
        let (del_tx, del_rx) = mpsc::channel::<String>();
        let del_tx_arc = Arc::new(del_tx);
        let _ = DELETION_TX.set(del_tx_arc.clone());
        let pool: HashMap<String, MmapMut> = HashMap::new();
        let tick = Arc::new(AtomicU64::new(0));
        // background flusher
        thread::spawn(move || {
            let mut pool = pool;
            let tick = tick;
            let del_rx = del_rx;
            let mut delete_pending = std::collections::HashSet::new();
            loop {
                thread::sleep(Duration::from_secs(FSYNC_SCHEDULE));
                let mut unique = std::collections::HashSet::new();
                while let Ok(path) = rx.try_recv() {
                    unique.insert(path);
                }
                if !unique.is_empty() { debug_print!("[flush] scheduling {} paths", unique.len()); }
                for path in unique.into_iter() {
                    // Skip if file doesn't exist
                    if !std::path::Path::new(&path).exists() {
                        debug_print!("[flush] file does not exist, skipping: {}", path);
                        continue;
                    }
                    
                    if !pool.contains_key(&path) {
                        match OpenOptions::new().read(true).write(true).open(&path) {
                            Ok(file) => match unsafe { MmapMut::map_mut(&file) } {
                                Ok(mmap) => { pool.insert(path.clone(), mmap); }
                                Err(e) => { debug_print!("[flush] failed to create memory map for {}: {}", path, e); continue; }
                            },
                            Err(e) => { debug_print!("[flush] failed to open file for flushing {}: {}", path, e); continue; }
                        }
                    }
                    if let Some(mmap) = pool.get_mut(&path) {
                        if let Err(e) = mmap.flush() { debug_print!("[flush] flush error for {}: {}", path, e); }
                    }
                }
                // collect deletion requests
                while let Ok(path) = del_rx.try_recv() {
                    debug_print!("[reclaim] deletion requested: {}", path);
                    delete_pending.insert(path);
                }
                let n = tick.fetch_add(1, Ordering::Relaxed) + 1;
                if n >= 3600 { // WARN: this currently assumes the FSYNC runs every second
                    if tick.compare_exchange(n, 0, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                        let mut empty: HashMap<String, MmapMut> = HashMap::new();
                        std::mem::swap(&mut pool, &mut empty); // reset map every hour to avoid unconstrained overflow
                        // perform batched deletions now that mmaps are dropped
                        for path in delete_pending.drain() {
                            match fs::remove_file(&path) {
                                Ok(_) => debug_print!("[reclaim] deleted file {}", path),
                                Err(e) => debug_print!("[reclaim] delete failed for {}: {}", path, e),
                            }
                        }
                    }
                }
            }
        });
        let idx = WalIndex::new("read_offset_idx")?;
        let instance = Walrus { allocator, reader, writers: RwLock::new(HashMap::new()), fsync_tx: tx_arc, read_offset_index: Arc::new(RwLock::new(idx)) };
        instance.startup_chore()?;
        Ok(instance)
    }

    pub fn append_for_topic(&self, col_name: &str, raw_bytes: &[u8]) -> std::io::Result<()> {
        let writer = {
            if let Some(w) = {
                let map = self.writers.read().map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "writers read lock poisoned"))?;
                map.get(col_name).cloned()
            } {
                w
            } else {
                let mut map = self.writers.write().map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "writers write lock poisoned"))?;
                if let Some(w) = map.get(col_name).cloned() { w } else {
                    let initial_block = unsafe { self.allocator.get_next_available_block()? };
                    let w = Arc::new(Writer::new(self.allocator.clone(), initial_block, self.reader.clone(), col_name.to_string(), self.fsync_tx.clone()));
                    map.insert(col_name.to_string(), w.clone());
                    w
                }
            }
        };
        writer.write(raw_bytes)
    }

    pub fn read_next(&self, col_name: &str) -> std::io::Result<Option<Entry>> {
        const TAIL_FLAG: u64 = 1u64 << 63;
        let info_arc = if let Some(arc) = {
            let map = self.reader.data.read().map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "reader map read lock poisoned"))?;
            map.get(col_name).cloned()
        } { arc } else {
            let mut map = self.reader.data.write().map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "reader map write lock poisoned"))?;
            map.entry(col_name.to_string())
                .or_insert_with(|| Arc::new(RwLock::new(ColReaderInfo { chain: Vec::new(), cur_block_idx: 0, cur_block_offset: 0 })))
                .clone()
        };
        let mut info = info_arc.write().map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "col info write lock poisoned"))?;
        debug_print!(
            "[reader] read_next start: col={}, chain_len={}, idx={}, offset={}",
            col_name, info.chain.len(), info.cur_block_idx, info.cur_block_offset
        );
 
        // Load persisted position (supports tail sentinel)
        let mut persisted_tail: Option<(u64 /*block_id*/, u64 /*offset*/)> = None;
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
                    if ib > info.chain.len() { ib = info.chain.len(); }
                    info.cur_block_idx = ib;
                    if ib < info.chain.len() {
                        let used = info.chain[ib].used;
                        info.cur_block_offset = pos.cur_block_offset.min(used);
                    } else {
                        info.cur_block_offset = 0;
                    }
                }
            }
        }

        // If we have a persisted tail and some sealed blocks were recovered, fold into the last block
        if let Some((_, tail_off)) = persisted_tail {
            if !info.chain.is_empty() {
                let ib = info.chain.len() - 1;
                info.cur_block_idx = ib;
                info.cur_block_offset = tail_off.min(info.chain[ib].used);
                if let Ok(mut idx_guard) = self.read_offset_index.write() {
                    let _ = idx_guard.set(col_name.to_string(), info.cur_block_idx as u64, info.cur_block_offset);
                }
                persisted_tail = None;
            }
        }

        loop {
            // Sealed chain path
            if info.cur_block_idx < info.chain.len() {
                let idx = info.cur_block_idx;
                let off = info.cur_block_offset;
                let block = info.chain[idx].clone();

                if off >= block.used {
                    debug_print!("[reader] read_next: advance block col={}, block_id={}, offset={}, used={}", col_name, block.id, off, block.used);
                    BlockStateTracker::set_checkpointed_true(block.id as usize);
                    info.cur_block_idx += 1;
                    info.cur_block_offset = 0;
                    continue;
                }

                match block.read(off) {
                    Ok((entry, consumed)) => {
                        info.cur_block_offset = off + consumed as u64;
                        if let Ok(mut idx_guard) = self.read_offset_index.write() {
                            let _ = idx_guard.set(col_name.to_string(), info.cur_block_idx as u64, info.cur_block_offset);
                        }
                        debug_print!("[reader] read_next: OK col={}, block_id={}, consumed={}, new_offset={}", col_name, block.id, consumed, info.cur_block_offset);
                        return Ok(Some(entry));
                    }
                    Err(_) => {
                        debug_print!("[reader] read_next: read error; skip block col={}, block_id={}, offset={}", col_name, block.id, off);
                        info.cur_block_idx += 1;
                        info.cur_block_offset = 0;
                        continue;
                    }
                }
            }

            // Tail path
            let writer_arc = {
                let map = self.writers.read().map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "writers read lock poisoned"))?;
                match map.get(col_name) { Some(w) => w.clone(), None => return Ok(None) }
            };
            let (active_block, written) = {
                let blk = writer_arc.current_block.lock().map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "current_block lock poisoned"))?;
                let off = writer_arc.current_offset.lock().map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "current_offset lock poisoned"))?;
                (blk.clone(), *off)
            };

            // If persisted tail points to a different block and that block is now sealed in chain, fold it
            if let Some((tail_block_id, tail_off)) = persisted_tail {
                if tail_block_id != active_block.id {
                    if let Some((idx, _)) = info.chain.iter().enumerate().find(|(_, b)| b.id == tail_block_id) {
                        info.cur_block_idx = idx;
                        info.cur_block_offset = tail_off.min(info.chain[idx].used);
                        if let Ok(mut idx_guard) = self.read_offset_index.write() {
                            let _ = idx_guard.set(col_name.to_string(), info.cur_block_idx as u64, info.cur_block_offset);
                        }
                        persisted_tail = None; // sealed now
                        continue;
                    } else {
                        // rebase tail to current active block at 0
                        persisted_tail = Some((active_block.id, 0));
                        if let Ok(mut idx_guard) = self.read_offset_index.write() {
                            let _ = idx_guard.set(col_name.to_string(), (active_block.id | TAIL_FLAG), 0);
                        }
                    }
                }
            } else {
                // No persisted tail; initialize at current active block start
                persisted_tail = Some((active_block.id, 0));
                if let Ok(mut idx_guard) = self.read_offset_index.write() {
                    let _ = idx_guard.set(col_name.to_string(), (active_block.id | TAIL_FLAG), 0);
                }
            }

            let (tail_block_id, tail_off) = match persisted_tail {
                Some(v) => v,
                None => return Ok(None),
            };
            // If writer rotated after we set persisted_tail, loop to fold/rebase
            if tail_block_id != active_block.id { continue; }

            if tail_off < written {
                match active_block.read(tail_off) {
                    Ok((entry, consumed)) => {
                        let new_off = tail_off + consumed as u64;
                        persisted_tail = Some((tail_block_id, new_off));
                        if let Ok(mut idx_guard) = self.read_offset_index.write() {
                            let _ = idx_guard.set(col_name.to_string(), (tail_block_id | TAIL_FLAG), new_off);
                        }
                        debug_print!("[reader] read_next: tail OK col={}, block_id={}, consumed={}, new_tail_off={}", col_name, active_block.id, consumed, new_off);
                        return Ok(Some(entry));
                    }
                    Err(_) => {
                        debug_print!("[reader] read_next: tail read error col={}, block_id={}, offset={}", col_name, active_block.id, tail_off);
                        return Ok(None);
                    }
                }
            } else {
                debug_print!("[reader] read_next: tail caught up col={}, block_id={}, off={}, written={}", col_name, active_block.id, tail_off, written);
                return Ok(None);
            }
        }
    }

    // reclaims disk space
    pub fn truncate_from_offset() {
        // how ?
        // so the thing is, for each col... FUCK, 

    }

    fn startup_chore(&self) -> std::io::Result<()> {
        // Minimal recovery: scan wal_files, build reader chains, and rebuild trackers
        let dir = match fs::read_dir("./wal_files") { Ok(d) => d, Err(_) => return Ok(()) };
        let mut files: Vec<String> = Vec::new();
        for entry in dir.flatten() {
            let path = entry.path();
            if let Some(s) = path.to_str() {
                // skip index files
                if s.ends_with("_index.db") { continue; }
                files.push(s.to_string());
            }
        }
        files.sort();
        if !files.is_empty() { debug_print!("[recovery] scanning files: {}", files.len()); }

        // assign synthetic block ids
        let mut next_block_id: usize = 1;
        let mut seen_files = std::collections::HashSet::new();

        for file_path in files.iter() {
            let mmap = match SharedMmapKeeper::get_mmap_arc(file_path) {
                Ok(m) => m,
                Err(e) => { debug_print!("[recovery] mmap open failed for {}: {}", file_path, e); continue; }
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
                let archived = unsafe { rkyv::archived_root::<Metadata>(&aligned[..]) };
                let md: Metadata = match archived.deserialize(&mut rkyv::Infallible) { Ok(m) => m, Err(_) => { break; } };
                col_name = Some(md.owned_by);

                // scan entries to compute used
                let block_stub = Block { id: next_block_id as u64, file_path: file_path.clone(), offset: block_offset, limit: DEFAULT_BLOCK_SIZE, mmap: mmap.clone(), used: 0 };
                let mut in_block_off: u64 = 0;
                loop {
                    match block_stub.read(in_block_off) {
                        Ok((_entry, consumed)) => {
                            used += consumed as u64;
                            in_block_off += consumed as u64;
                            if in_block_off >= DEFAULT_BLOCK_SIZE { break; }
                        }
                        Err(_) => break,
                    }
                }
                if used == 0 { break; }

                let block = Block { id: next_block_id as u64, file_path: file_path.clone(), offset: block_offset, limit: DEFAULT_BLOCK_SIZE, mmap: mmap.clone(), used };
                // register and append
                BlockStateTracker::register_block(next_block_id, file_path);
                FileStateTracker::add_block_to_file_state(file_path);
                if let Some(col) = col_name {
                    let _ = self.reader.append_block_to_chain(&col, block.clone());
                    debug_print!("[recovery] appended block: file={}, block_id={}, used={}, col={}", file_path, block.id, block.used, col);
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
                        let mut info = match info_arc.write() { Ok(v) => v, Err(_) => continue };
                        let mut ib = pos.cur_block_idx as usize;
                        if ib > info.chain.len() { ib = info.chain.len(); }
                        info.cur_block_idx = ib;
                        if ib < info.chain.len() {
                            let used = info.chain[ib].used;
                            info.cur_block_offset = pos.cur_block_offset.min(used);
                        } else {
                            info.cur_block_offset = 0;
                        }
                        // checkpoint all blocks strictly before ib
                        for i in 0..ib { BlockStateTracker::set_checkpointed_true(info.chain[i].id as usize); }
                        // and checkpoint current if at end
                        if ib < info.chain.len() && info.cur_block_offset >= info.chain[ib].used { BlockStateTracker::set_checkpointed_true(info.chain[ib].id as usize); }
                    }
                }
            }
        }

        // enqueue deletion checks
        for f in seen_files.into_iter() { flush_check(f); }
        Ok(())
    }

    // TODO: we need to add checksums in the entry metadata btw
    // TODO: Crash recovery ?? we need to make some things durable...
    // truncation(dealing with this right now)
    // failure recovery!!
}

// it would all make sense one day :)) , not today though, today we grind, amist all the chaos, we grind, for the beauty that is to come is unbounded





/*
okay, so we wish to make some changes here:

1. we wish to remove the concept of `offset` completely for reading a file here, it's all bull, we are getting rid of this abstraction
, instead, what we'll do is, for the per column last_read_offset, we'll have... a more dedicated tracking thingy like: {current_block_idx,current_block_offset}


and we remove the concept of reading at will and just give the option to read the next `Entry` for some column, so when we call it, initially it's at the first block in the reader block chain and the block offset would be 0,
we return the first entry from there and increase the block offset by the size of that entry, we keep doing this for each request untill we reach the last entry of the block and then just jump to the next block in the chain and reset the offset and current block idx thingy

got it ? 
*/

/*
okay, so we got a persistent state store now, what we'll do is:
- whenever we do read_next() we update state store too!!
*/


/*
so we want three persistent states (WALIdx):

- M[file_path] -> how much space is allocated in this // future nubskr: "not needed"
---
- M[col_name] -> what block and where in it are we at, i.e. block checkpoints
- M[file_path] -> what blocks in this shit are locked by writers (and their counts as well)

okay, okay, so... whenever a... block allocator is... changing to a new file, it does some of these checks

like.... it already knows that this file is full huh... yeah, so I think we can skip the first persistent state.... yayyy
nice, what else...

- any writer still on this ??? okay.. this is tricky to do in sync, because.... it's blocking... or not ? oh wait, we have it all in mem huh, yeah, so just check for locks, ehehehh, nice
okay, no locks, what after that
all blocks acknowledged.... hmmm, tricky one, how to do this ? 
so... for this, we would need to have.... block list per file in memory ?? yeah, seems like it or we can do it lazily(should we ? it's on the blockAllocator critical path sir)
is there anythign we can do here ?? 

blockallocator, what if we look at it..

blockallocator knows when a file is fully allocated,
blockallocator can easily... put it such that what block belongs to what file, can also add a... flag there, smth smth `is_checkpointed` for some block in that list for some file
hmm, so... when a... block gets fully done... we make read_next() to update it in that state , oh wow, I see it now, this is beautiful
what about the writer thingy... how to know if no... oh... okay

so.... whenever we get an update in that file block chain thingy.... we would just check... "is this all ?" maybe have a darn.. counter there, 
we would also need to only let it happen when a file is fully allocated...

this is nice, I must draw it before it flies away...

but what after it ? what after we realize that it's fully allocated ? 
what/how to take it `into consideration` ?? 

checks remaining after that:
- no block in here locked by any writer (hmm, doable with the counter thing though..)
- each individual block acknowledged, easy one with the checkpointed_block_ctr thingy 

but the thing is... how the hell do we check... like, we check it after full allocation lock goes away, it has some writer block or some pending checkpoint acknowledgements, then what ?
hmmm, fucking polling is annoying, idek from where to do it... and I don't want to do it... the thing is.. we just have to check two atomic counters each time huh.., yeah...

`unlocked_block_ctr` and `checkpointed_block_ctr` , that's it
but how/when to check ?? hmm, whenever update any of those two I guess ? :)))) eheheh
holy fuck, woah, waow UwU , so just attempt the check of all done whenever:
1. some writer unlocks some block and does atomic +1 on unlocked_block_ctr
2. some read_next() call completes checkpointing a whole block

and once we're good, we just.. do a mpsc producer event to delete a file and the consumer in our occasional job thread would pick it up, dedup and delete the file(s) :))))
lets go mog everyone

so the second one is literally free O(1) , the first one... can we do something similar to that cnt thingy ?
smth like locked_block_ctr, I hope there are no race conditions here man.. like... I mean... we can just put that there before letting the darn fully_allocated lock go huh, LMAO, no race conditions then, eeheheheheheh
ughh, 

where to even start...
let's add some sort of shitty first, the carcass

I will destroy every single one of them
own everything, destroy it all, burn it down
*/

static DELETION_TX: OnceLock<Arc<mpsc::Sender<String>>> = OnceLock::new();

fn flush_check(file_path: String) {
    // Quick readiness check; hook actual reclamation later
    if let Some((locked, checkpointed, total, fully_allocated)) = FileStateTracker::get_state_snapshot(&file_path) {
        let ready_to_delete = fully_allocated && locked == 0 && total > 0 && checkpointed >= total;
        if ready_to_delete {
            if let Some(tx) = DELETION_TX.get() {
                let _ = tx.send(file_path);
            }
        }
    }
}

struct BlockState {
    is_checkpointed: AtomicBool, // is this thingy fully done or not, that's it, good thing that block IDs are unique globally
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
                } else { None }
            } else { None }
        };

        if let Some(path) = path_opt {
            FileStateTracker::inc_checkpoint_for_file(&path);
            flush_check(path);
        }
    }
}

struct FileState {
    // need to fucking keep track of what blocks this shit has too 
    locked_block_ctr: AtomicU16, // no. of block locked by writers
    checkpoint_block_ctr: AtomicU16, // no. of blocks already checkpointed
    total_blocks: AtomicU16, // total blocks in this file
    is_fully_allocated: AtomicBool // all blocks in the file allocated or not
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


// make sure to use AcqRelease while updating Atomics ffs or you'll be fucked by entropy when you least expect it AHAHAHAH