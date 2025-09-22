/*
something like:
just do the dumbest thing now, first thing which comes to your mind, that's it
we will do it all in one file
*/
use std::collections::HashMap;
use std::time::SystemTime;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::cell::UnsafeCell;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use rkyv::{Archive, Deserialize, Serialize};
use rkyv::ser::{Serializer, serializers::AllocSerializer};
use std::fs;
use std::path::Path;

const DEFAULT_BLOCK_SIZE:u64 = 10 * 1024 * 1024; // 10mb
const BLOCKS_PER_FILE:u64 = 100;
const MAX_ALLOC:u64 = 1 * 1024 * 1024 * 1024; // 1 GiB cap per block
const FSYNC_SCHEDULE:u64 = 1; // seconds btw
const PREFIX_META_SIZE: usize = 64; 
const MAX_FILE_SIZE:u64 = DEFAULT_BLOCK_SIZE * BLOCKS_PER_FILE;

fn now_millis_str() -> String {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .to_string()
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

#[derive(Clone)]

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
#[derive(Clone)]
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
        // write via the shared mmap at this block's file offset
        // Create metadata
        let new_meta = Metadata { 
            read_size: data.len(),
            owned_by: owned_by.to_string(),
            next_block_start,
            checksum: checksum64(data),
        };
        
        // Serialize metadata with rkyv - MUCH faster than JSON
        let mut serializer = AllocSerializer::<256>::default();
        serializer.serialize_value(&new_meta).unwrap();
        let meta_bytes = serializer.into_serializer().into_inner();
        if meta_bytes.len() > PREFIX_META_SIZE {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "metadata too large for prefix"));
        }
        
        // Pad to exact size
        let mut meta_buffer = vec![0u8; PREFIX_META_SIZE];
        meta_buffer[..meta_bytes.len()].copy_from_slice(&meta_bytes);
        
        // Combine and write in one syscall
        let mut combined = Vec::with_capacity(PREFIX_META_SIZE + data.len());
        combined.extend_from_slice(&meta_buffer);
        combined.extend_from_slice(&data);
        
        let file_offset = self.offset + in_block_offset;
        self.mmap.write(file_offset as usize, &combined); // btw, we can also do write asynq here, 
        Ok(())
    }

    fn read(&self, in_block_offset: u64) -> std::io::Result<(Entry, usize)>{
        let mut meta_buffer = vec![0; PREFIX_META_SIZE];
        let file_offset = self.offset + in_block_offset;
        self.mmap.read(file_offset as usize, &mut meta_buffer);


        // Zero-copy access to metadata - NO DESERIALIZATION
        let archived = unsafe {
            // This is safe because we know the buffer is valid
            rkyv::archived_root::<Metadata>(&meta_buffer[..])
        };
        let actual_entry_size = archived.read_size;

        // Now read the actual data
        let new_offset = file_offset + PREFIX_META_SIZE as u64;
        let mut ret_buffer = vec![0; actual_entry_size as usize];
        self.mmap.read(new_offset as usize, &mut ret_buffer);

        // Verify checksum; error on mismatch
        let expected = archived.checksum;
        if checksum64(&ret_buffer) != expected {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "checksum mismatch, data corruption detected"));
        }

        let consumed = (PREFIX_META_SIZE as usize) + (actual_entry_size as usize);
        Ok((Entry { data: ret_buffer }, consumed))
    }

}


fn make_new_file() -> String {
    let file_name = now_millis_str();
    let file_path = format!("wal_files/{}", file_name);
    let f = std::fs::File::create(&file_path).unwrap();
    f.set_len(MAX_FILE_SIZE).unwrap();
    file_path
}

// has block metas to give out
struct BlockAllocator {
    next_block: UnsafeCell<Block>,
    lock: AtomicBool
}

impl BlockAllocator {
    pub fn new() -> std::io::Result<Self> {
        std::fs::create_dir_all("wal_files").ok();
        let file1 = make_new_file();
        let mmap: Arc<SharedMmap> = SharedMmapKeeper::get_mmap_arc(&file1);
        Ok(BlockAllocator {
            next_block: UnsafeCell::new(Block { id: 1, offset: 0, limit: DEFAULT_BLOCK_SIZE, file_path: file1, mmap, used: 0 }),
            lock: AtomicBool::new(false),
        })
    }
    
    pub unsafe fn get_next_available_block(&self) -> Block {
        self.lock();
        let data = unsafe { &mut *self.next_block.get() };
        if data.offset >= MAX_FILE_SIZE {
            data.file_path = make_new_file();
            data.mmap = SharedMmapKeeper::get_mmap_arc(&data.file_path);
            data.offset = 0;
            data.used = 0;
        }
        let ret = data.clone();
        data.offset += DEFAULT_BLOCK_SIZE;
        data.id += 1;
        self.unlock();
        ret
    }

    pub unsafe fn alloc_block(&self, want_bytes: u64) -> std::io::Result<Block> {
        if want_bytes == 0 || want_bytes > MAX_ALLOC {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid allocation size, a single entry can't be more than 1gb"));
        }
        let alloc_units = (want_bytes + DEFAULT_BLOCK_SIZE - 1) / DEFAULT_BLOCK_SIZE;
        let alloc_size = alloc_units * DEFAULT_BLOCK_SIZE;

        self.lock();
        let data = unsafe { &mut *self.next_block.get() };
        if data.offset + alloc_size > MAX_FILE_SIZE {
            data.file_path = make_new_file();
            data.mmap = SharedMmapKeeper::get_mmap_arc(&data.file_path);
            data.offset = 0;
        }
        let ret = Block {
            id: data.id,
            file_path: data.file_path.clone(),
            offset: data.offset,
            limit: alloc_size,
            mmap: data.mmap.clone(),
            used: 0,
        };
        data.offset += alloc_size;
        data.id += 1;
        self.unlock();
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
        let mut block = self.current_block.lock().unwrap();
        let mut cur = self.current_offset.lock().unwrap();

        let need = (PREFIX_META_SIZE as u64) + (data.len() as u64);
        if *cur + need > block.limit {
            let mut sealed = block.clone();
            sealed.used = *cur;
            self.reader.append_block_to_chain(&self.col, sealed);
            let new_block = unsafe { self.allocator.alloc_block(data.len() as u64) }?;
            *block = new_block;
            *cur = 0;
        }
        let next_block_start = block.offset + block.limit; // simplistic for now
        block.write(*cur, data, &self.col, next_block_start)?;
        *cur += need;
        let _ = self.publisher.send(block.file_path.clone());
        Ok(())
    }
}


// this is gonna slappp, f1 level stuff

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
            .unwrap()
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
            .unwrap()
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
        let keeper = keeper_lock.read().unwrap();
        keeper.data.get(path).cloned()
    }

    // Read-mostly accessor that escalates to write lock only on miss
    fn get_mmap_arc(path: &str) -> Arc<SharedMmap> {
        if let Some(existing) = Self::get_mmap_arc_read(path) {
            return existing;
        }

        static MMAP_KEEPER: OnceLock<RwLock<SharedMmapKeeper>> = OnceLock::new();
        let keeper_lock = MMAP_KEEPER.get_or_init(|| RwLock::new(SharedMmapKeeper::new()));

        // Double-check with a fresh read lock to avoid unnecessary write lock
        {
            let keeper = keeper_lock.read().unwrap();
            if let Some(existing) = keeper.data.get(path) {
                return existing.clone();
            }
        }

        let mut keeper = keeper_lock.write().unwrap();
        if let Some(existing) = keeper.data.get(path) {
            return existing.clone();
        }
        let mmap_arc = SharedMmap::new(path).unwrap();
        keeper.data.insert(path.to_string(), mmap_arc.clone());
        mmap_arc
    }
}

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
            let map = self.data.read().unwrap();
            map.get(col)?.clone()
        };
        let info = arc_info.read().unwrap();
        Some(info.chain.clone())
    }

    // internal 
    fn append_block_to_chain(&self, col: &str, block: Block) {
        // fast path: try read-lock map and use per-column lock
        if let Some(info_arc) = {
            let map = self.data.read().unwrap();
            map.get(col).cloned()
        } {
            let mut info = info_arc.write().unwrap();
            info.chain.push(block);
            return;
        }

        // slow path
        let info_arc = {
            let mut map = self.data.write().unwrap();
            map.entry(col.to_string())
                .or_insert_with(|| Arc::new(RwLock::new(ColReaderInfo { chain: Vec::new(), cur_block_idx: 0, cur_block_offset: 0 })))
                .clone()
        };
        let mut info = info_arc.write().unwrap();
        info.chain.push(block);
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
    pub fn new() -> Self {
        fs::create_dir_all("./wal_files").ok();
        let path = "./wal_files/index.db";
        
        let store = Path::new(path).exists()
            .then(|| fs::read(path).ok())
            .flatten()
            .and_then(|bytes| (!bytes.is_empty()).then(|| unsafe {
                rkyv::archived_root::<HashMap<String, BlockPos>>(&bytes)
                    .deserialize(&mut rkyv::Infallible)
                    .unwrap()
            }))
            .unwrap_or_default();
        
        Self { 
            store, 
            path: path.to_string() 
        }
    }
    
    pub fn set(&mut self, key: String, idx: u64, offset: u64) {
        self.store.insert(key, BlockPos { 
            cur_block_idx: idx, 
            cur_block_offset: offset 
        });
        self.persist();
    }
    
    pub fn get(&self, key: &str) -> Option<&BlockPos> {
        self.store.get(key)
    }
    
    pub fn remove(&mut self, key: &str) -> Option<BlockPos> {
        let result = self.store.remove(key);
        if result.is_some() {
            self.persist();
        }
        result
    }
    
    fn persist(&self) {
        let tmp_path = format!("{}.tmp", self.path);
        let bytes = rkyv::to_bytes::<_, 256>(&self.store).unwrap();
        
        fs::write(&tmp_path, &bytes).unwrap();
        fs::File::open(&tmp_path).unwrap().sync_all().unwrap();
        fs::rename(&tmp_path, &self.path).unwrap();
    }
}

// -------------------

// public APIs
pub struct Walrus {
    allocator: Arc<BlockAllocator>,
    reader: Arc<Reader>,
    writers: RwLock<HashMap<String, Arc<Writer>>>,
    fsync_tx: Arc<mpsc::Sender<String>>,
    index: Arc<RwLock<WalIndex>>,
}

impl Walrus {
    pub fn new() -> Self {
        let allocator = Arc::new(BlockAllocator::new().unwrap());
        let reader = Arc::new(Reader::new());
        let (tx, rx) = mpsc::channel::<String>();
        let tx_arc = Arc::new(tx);
        let pool: HashMap<String, MmapMut> = HashMap::new();
        let tick = Arc::new(AtomicU64::new(0));
        // background flusher
        thread::spawn(move || {
            let mut pool = pool;
            let tick = tick;
            loop {
                thread::sleep(Duration::from_secs(FSYNC_SCHEDULE));
                let mut unique = std::collections::HashSet::new();
                while let Ok(path) = rx.try_recv() {
                    unique.insert(path);
                }
                for path in unique.into_iter() {
                    let mmap = pool.entry(path.clone()).or_insert_with(|| {
                        let file = OpenOptions::new().read(true).write(true).open(&path).unwrap();
                        unsafe { MmapMut::map_mut(&file).unwrap() }
                    });
                    let _ = mmap.flush();
                }
                let n = tick.fetch_add(1, Ordering::Relaxed) + 1;
                if n >= 3600 { // WARN: this currently assumes the FSYNC runs every second
                    if tick.compare_exchange(n, 0, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                        let mut empty: HashMap<String, MmapMut> = HashMap::new();
                        std::mem::swap(&mut pool, &mut empty); // reset map every hour to avoid unconstrained overflow
                    }
                }
            }
        });
        Walrus { allocator, reader, writers: RwLock::new(HashMap::new()), fsync_tx: tx_arc, index: Arc::new(RwLock::new(WalIndex::new())) }
    }

    pub fn append_for_topic(&self, col_name: &str, raw_bytes: &[u8]) -> std::io::Result<()> {
        let writer = {
            if let Some(w) = {
                let map = self.writers.read().unwrap();
                map.get(col_name).cloned()
            } {
                w
            } else {
                let mut map = self.writers.write().unwrap();
                map.entry(col_name.to_string()).or_insert_with(|| {
                    let initial_block = unsafe { self.allocator.get_next_available_block() };
                    Arc::new(Writer::new(self.allocator.clone(), initial_block, self.reader.clone(), col_name.to_string(), self.fsync_tx.clone()))
                }).clone()
            }
        };
        writer.write(raw_bytes)
    }

    pub fn read_next(&self, col_name: &str) -> Option<Entry> {
        let info_arc = {
            let map = self.reader.data.read().ok()?;
            map.get(col_name)?.clone()
        };
        let mut info = info_arc.write().ok()?;

        // Load current persisted position for this column and clamp to bounds
        if let Ok(idx_guard) = self.index.read() {
            if let Some(pos) = idx_guard.get(col_name) {
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

        loop {
            if info.cur_block_idx >= info.chain.len() {
                return None;
            }
            let block = &info.chain[info.cur_block_idx];

            if info.cur_block_offset >= block.used {
                info.cur_block_idx += 1;
                info.cur_block_offset = 0;
                continue;
            }

            match block.read(info.cur_block_offset) {
                Ok((entry, consumed)) => {
                    info.cur_block_offset += consumed as u64;
                    // Persist updated position
                    if let Ok(mut idx_guard) = self.index.write() {
                        idx_guard.set(col_name.to_string(), info.cur_block_idx as u64, info.cur_block_offset);
                    }
                    return Some(entry);
                }
                Err(_) => {
                    info.cur_block_idx += 1;
                    info.cur_block_offset = 0;
                }
            }
        }
    }

    // reclaims disk space
    pub fn truncate_from_offset() {
        // how ?
        // so the thing is, for each col... FUCK, 

    }

    fn startup_chore() {
        // what this is does is:
        // - go to ./wal_files and checks all the files in there
        // let's take care of the darn truncate from offset stuff
        // okay, so for that to work, I need to ? /
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