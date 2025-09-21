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
use rkyv::de::deserializers::SharedDeserializeMap;
use rkyv::validation::validators::DefaultValidator;
use rkyv::{Archive, Deserialize, Serialize};
use rkyv::ser::{Serializer, serializers::AllocSerializer};

const DEFAULT_BLOCK_SIZE:u64 = 10 * 1024 * 1024; // 10mb
const BLOCKS_PER_FILE:u64 = 100;
const MAX_ALLOC:u64 = 1 * 1024 * 1024 * 1024; // 1 GiB cap per block
const FSYNC_SCHEDULE:u64 = 1; // seconds btw
const PREFIX_META_SIZE: usize = 64; 
const MAX_FILE_SIZE:u64 = DEFAULT_BLOCK_SIZE * BLOCKS_PER_FILE;

fn rand_str(n: usize) -> String {
    let mut seed = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    const ALPHANUM: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut out = String::with_capacity(n);
    for _ in 0..n {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        out.push(ALPHANUM[(seed % 62) as usize] as char);
    }
    out
}

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

struct Entry {
    data: Vec<u8> // raw bytes
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
}

impl Block {
    fn write(&self, data: &[u8], owned_by: &str, next_block_start: u64) -> std::io::Result<()> {
        debug_assert!((data.len() as u64 + PREFIX_META_SIZE as u64) <= self.limit);
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
        
        self.mmap.write(self.offset as usize, &combined); // btw, we can also do write asynq here, 
        Ok(())
    }

    fn read(&self, in_block_offset: u64) -> std::io::Result<Entry>{
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

        Ok(Entry { data: ret_buffer })
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
            next_block: UnsafeCell::new(Block { id: 1, offset: 0, limit: DEFAULT_BLOCK_SIZE, file_path: file1, mmap }),
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
}

impl Writer {
    pub fn new(allocator: Arc<BlockAllocator>, current_block: Block, reader: Arc<Reader>, col: String, publisher: Arc<mpsc::Sender<String>>) -> Self {
        Writer { allocator, current_block: Mutex::new(current_block), reader, col: col.clone(), publisher }
    }

    pub fn write(&self, data: &[u8]) -> std::io::Result<()> {
        let mut block = self.current_block.lock().unwrap();
        if (data.len() as u64) + PREFIX_META_SIZE as u64 > block.limit {
            let old_block = block.clone();
            self.reader.append_block_to_chain(&self.col, old_block);
            let new_block = unsafe { self.allocator.alloc_block(data.len() as u64) }?;
            *block = new_block;
        }
        let next_block_start = block.offset + block.limit; // simplistic for now
        block.write(data, &self.col, next_block_start)?;
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
    last_read_offset: u64
}

struct Reader {
    data: RwLock<HashMap<String,Arc<RwLock<ColReaderInfo>>>>
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
                .or_insert_with(|| Arc::new(RwLock::new(ColReaderInfo { chain: Vec::new(), last_read_offset: 0 })))
                .clone()
        };
        let mut info = info_arc.write().unwrap();
        info.chain.push(block);
    }
}

// public APIs
pub struct Walrus {
    allocator: Arc<BlockAllocator>,
    reader: Arc<Reader>,
    writers: RwLock<HashMap<String, Arc<Writer>>>,
    fsync_tx: Arc<mpsc::Sender<String>>,
}

impl Walrus {
    pub fn new() -> Self {
        let allocator = Arc::new(BlockAllocator::new().unwrap());
        let reader = Arc::new(Reader::new());
        let (tx, rx) = mpsc::channel::<String>();
        let tx_arc = Arc::new(tx);
        let mut pool: HashMap<String, MmapMut> = HashMap::new();
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
        Walrus { allocator, reader, writers: RwLock::new(HashMap::new()), fsync_tx: tx_arc }
    }

    pub fn append_for_col(&self, col_name: &str, raw_bytes: &[u8]) -> std::io::Result<()> {
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

    pub fn read_from_col_offset(&self, col_name: &str, offset: u64) -> Option<Block> {
        // just doing a linear psum search for now
        let chain = self.reader.get_chain_for_col(col_name)?;
        let mut p_sum: u64 = 0;
        for block in chain.into_iter() {
            let block_size = block.limit - block.offset;
            if offset < p_sum + block_size {
                return Some(block);
            }
            p_sum += block_size;
        }
        None
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
    }

    // TODO: we need to add checksums in the entry metadata btw
    // TODO: Crash recovery ?? we need to make some things durable...
    // truncation(dealing with this right now)
    // failure recovery!!
}

// it would all make sense one day :)) , not today though, today we grind, amist all the chaos, we grind, for the beauty that is to come is unbounded