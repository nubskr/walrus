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
// use std::io::SeekFrom; // not needed with mmap reads

const BLOCK_SIZE:u64 = 10 * 1024 * 1024; // 10mb
const BLOCKS_PER_FILE:u64 = 10;
const MAX_FILE_SIZE:u64 = BLOCK_SIZE * BLOCKS_PER_FILE;
const MAX_ALLOC:u64 = 1 * 1024 * 1024 * 1024; // 1 GiB cap per block
const FSYNC_SCHEDULE:u64 = 1;

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

#[derive(Clone)]

struct Entry {
    data: Vec<u8> // raw bytes
}

const PREFIX_META_SIZE: usize = 64; 

#[derive(Archive, Deserialize, Serialize, Debug)]
#[archive(check_bytes)]
struct Metadata {
    read_size: usize
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
    fn write(&self, data: &[u8]) {
        debug_assert!((data.len() as u64 + PREFIX_META_SIZE as u64) <= self.limit);
        // write via the shared mmap at this block's file offset

        // Create metadata
        let new_meta = Metadata { 
            read_size: data.len() 
        };
        
        // Serialize metadata with rkyv - MUCH faster than JSON
        let mut serializer = AllocSerializer::<256>::default();
        serializer.serialize_value(&new_meta).unwrap();
        let meta_bytes = serializer.into_serializer().into_inner();
        
        // Pad to exact size
        let mut meta_buffer = vec![0u8; PREFIX_META_SIZE];
        meta_buffer[..meta_bytes.len()].copy_from_slice(&meta_bytes);
        
        // Combine and write in one syscall
        let mut combined = Vec::with_capacity(PREFIX_META_SIZE + data.len());
        combined.extend_from_slice(&meta_buffer);
        combined.extend_from_slice(&data);
        
        self.mmap.write(self.offset as usize, &combined); // btw, we can also do write asynq here, 
    }

    fn read(&self, in_block_offset: u64) -> Option<Entry>{
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

        Some(Entry { data: ret_buffer })
    }
}

// has block metas to give out
struct BlockAllocator {
    next_block: UnsafeCell<Block>,
    lock: AtomicBool
}

fn make_new_file() -> String {
    let file_name = rand_str(5);
    let file_path = format!("wal_files/{}", file_name);
    let f = std::fs::File::create(&file_path).unwrap();
    f.set_len(MAX_FILE_SIZE).unwrap();
    file_path
}

impl BlockAllocator {
    pub fn new() -> std::io::Result<Self> {
        std::fs::create_dir_all("wal_files").ok();
        let file1 = make_new_file();
        let mmap: Arc<SharedMmap> = SharedMmapKeeper::get_mmap_arc(&file1);
        Ok(BlockAllocator {
            next_block: UnsafeCell::new(Block { id: 1, offset: 0, limit: BLOCK_SIZE, file_path: file1, mmap }),
            lock: AtomicBool::new(false),
        })
    }
    
    pub unsafe fn get_next_available_block(&self) -> Block {
        // if we are out of blocks in the current file, just switch to a new file
        self.lock();
        let data = unsafe { &mut *self.next_block.get() };
        if data.offset >= MAX_FILE_SIZE {
            data.file_path = make_new_file();
            data.mmap = SharedMmapKeeper::get_mmap_arc(&data.file_path);
            data.offset = 0;
        }
        let ret = data.clone();
        data.offset += BLOCK_SIZE;
        data.id += 1;
        self.unlock();
        ret
    }

    pub unsafe fn alloc_block(&self, want_bytes: u64) -> std::io::Result<Block> {
        if want_bytes == 0 || want_bytes > MAX_ALLOC {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid allocation size, a single entry can't be more than 1gb"));
        }
        let alloc_units = (want_bytes + BLOCK_SIZE - 1) / BLOCK_SIZE;
        let alloc_size = alloc_units * BLOCK_SIZE;

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


// -------
// okay, so block allocator part done, now... keeper... 

/*
should I also abuse that Arc<RWLock<Map<col,BlockChain>>> pattern here ? lol, I mean, okay, so how decoupled I want stuff to be ?

so, our BlockAllocator is different, we would need to wire stuff to it, more like, we just do Arc<BlockAllocator> and pass it around everywhere..


so.. we can kinda... make a 

okay, I dont fucking care, I am using a map:

what do we need for a chain ??
- the mmap'ed file or an Arc<> of it ?? I think we can... oh wait, do we need writing ability here ?? 
this is a darn keeper man
I think we also need to add the writer thingy here somewhere
- 

what we need per chain:
- mmap of file or ARC of it (if just for reading)
- the offset position of the block in that file 

okay, you can do it per chain what, I get it

HOW THE HELL DO WE GUARANTEE LINEARIZABLE WRITES PER COLUMN ?? 
two threads trying to update a column, must be non fucking blocking as fast as fuck

thread1 -> append_to_wal(data1) {}
thread2 -> append_to_wal(data2) {}

wwyd ?? 
like, the thing is, writing a darn wal entry is cheap, cheap as hell, and we can kinda guarantee that.. ok wait, 
okay, so just CAS and fall back to regular OS mutex if needed, this would be fast enough

oooo, so.. should I just, the thing is that oh fuck, how the hell do we serialize, :((((

append_to_wal(self,stuff) {
    // we have the access to the block here btw
    mutex.lock()
    // check if offset + stuff is greater than the MAX_FILE_SIZE, if yes, then we need to just put the current block in the chain and get a new one, that would be the slowest critical section branch here

    // serialize and just append
    block.write(offset,stuff)
    mutex.unlock()

}
okay, how the hell do we.. enforce fsyncing at some level, so the thing is, we can kind of just.. do that here, after mutex, in a way,
sort of something lazy like: keep the last fsync time in object, and just spam it and update time, maybe make it atomic, idk, lol
yeah, good enough, not the end of the world at all

or, we just... dont handle it there, but just handle it at the darn mmap level per file

I still havent decided how to separate readers from writers, there or if we even need to separate them at all
fuck this shit man, idk

okay, so when would we NEED a dedicated reader which is kinda non blocked by any writer ?? 
umm, I guess, when some high consistency stuff requires it ?? nice, so we dont need it RIGHT now huh, right ? yeah
okay, so no dedicated reader for now, we just store file mmaps uniquely somewhere and get it from there and just store an Arc<RW<mmap>> reference to it I guess ? the thing with this is, it would literally destroy per file parallelism, only one shit would be able to write in it, there is literally no point of it then man, let's just... have multiple mmaps for a same file
tell me something, can you guarantee(for now) that for a certain column, you'll only be actively writing to ONE of its blocks ?? can you do that ?? 
I think I can, yeah
okay, so for each file

M[col_name] -> 
*/

// we need to make this shit more sophisticated
// 1. if the data is more than... hmm, okay,
// 1gb, that's the hard limit, we scale up from defaut block size if need arises, yayyy, good stuff
// okay, so the thing is, whenever we have to get a new block, we would tell them the size of the data that we want to insert in there,
// and the blockallocator would give you a block of size BLOCK_SIZE*X where X is the smallest possible number such that BLOCK_SIZE*X >= data size , note. that if the data is bigger than 1gb, we can't do shit, so we just return an error, so what the block allocator would do is, determine the size of block needed, check if it's doable in the current WAL file, if yes, do it, if not, go to the next file and allocated there, so I think for this we would need to also... add a new field to the Block object, along with offset, we would need the... limit as well, so we could handle such dynamic Block sizes, and ofc the current stuff would need to be updated in place to reflect such changes as well, good stuff
// 
struct Writer{
    allocator: Arc<BlockAllocator>,
    current_block: Mutex<Block>,
    reader: Arc<Reader>,
    col: String,
    publisher: Arc<mpsc::Sender<String>>,
}

impl Writer {
    pub fn new(allocator: Arc<BlockAllocator>, current_block: Block, reader: Arc<Reader>, col: String, publisher: Arc<mpsc::Sender<String>>) -> Self {
        Writer { allocator, current_block: Mutex::new(current_block), reader, col, publisher }
    }

    pub fn write(&self, data: &[u8]) -> std::io::Result<()> {
        let mut block = self.current_block.lock().unwrap();
        if (data.len() as u64) + PREFIX_META_SIZE as u64 > block.limit {
            let old_block = block.clone();
            self.reader.append_block_to_chain(&self.col, old_block);
            let new_block = unsafe { self.allocator.alloc_block(data.len() as u64) }?;
            *block = new_block;
        }
        block.write(data);
        // publish file path for fsync dedupe
        let _ = self.publisher.send(block.file_path.clone());
        Ok(())
    }
}


// -------------------------
// this bad boi is gonna slapppp man, fucking f1 car
// but we still need a Map store it lel

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


// ---------------------
// I am so drained and fucked man... 

// okay, back at it now, where were again ??
// okay, so we need a way to , okay, put this shit in a map for now whatever
// so we only need to give it someone when a new block is made huh, yeah, right, okay

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

// what now huh, what's left ??


// okay, yeah, reader stuff, how the hell do I do that one now, it's pretty clear actually
// when the writer needs to switch blocks, it just yeets the block to reader chain
// so we need per column level block chains

// M[col] -> []~[]~[]~...
// okay, okay...

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
        // if col not exists, yeet the guy 
        let arc_info = {
            let map = self.data.read().unwrap();
            map.get(col)?.clone()
        };
        // assuming column exists from here on
        let info = arc_info.read().unwrap();
        // just return the chain clone
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

        // slow path: column missing; lock map for insertion only
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



// okay, another TODO:

/*
so we need a way to reliably flush the dirty mmaped files on a configured basis (FSYNC_SCHEDULE for now)

so it would be like a CRON thingy, but on a low level, something like: "fsync all dirty files every X seconds"

so for that I have added the last_touched_at timestamp on sharedmmap object, what I plan to do is:

- Have a Priority Queue,we stuff SHharedMmap objects in it, we would sort stuff by last_touched_at key, and every FSYNC_SCHEDULE'th second, we
trigger a untracked thread which runs, checks the dirty files since the last time it ran, flush them and update the last_flushed_at for them ( we need to add this flag too btw )

so ofc we cant use the same unsafe raw mmap we use for everything as something might be running on it parallely while the CRON runs and we dont want half flushed states, 

so for this, we would have our own separate raw mmap pool so that things are atomic at a level, okay

TODOOOOO
*/

/*
new day, let's get this done today,
so... okay, the thing is, the `active` blocks (i.e. the ones being written to writers by different threads)

okay, so.. it has something to do with active Writers only huh, hmmmmm

oh, wait, this can be easier than I thought, the Writer, it can mutate our PriorityQueue stuff atomically, but how ? so the thing is...
no, no NO NO, this can become a bottleneck, a bad one, multiple WRITING threads mutating PriorityQueue in a syncronous way, NO..

can we do something event based instead ?? MPSC ?? but that thing guarnatees linearizability on a producer level, 

fuck me man, this is so annoying, holy fuck, I cant stop the world to fsync, NO

this is so tricky, oh, wait, oh my my my, okay


so what I want is, to have an MPSC, and each Writer has a publisher in it, and whenever it writes something, it just produces an file_path thingy and every FSYNC_SCHEDULE seconds, we spawn a thread and just flush the dirty stuff that we got uniquely(so that we dont repeatedly flush the same file and that's it), no need to use `last_touched_at` thingy
*/

// ==================================================================================================================> Public stuff

// wait, wait, so what sort of external API do I need to provide ??
// "just let me put this wal to a fucking column you clown", okay, so logrus.append_for_col(col_name,raw_bytes) ?? yeah, oh wow, we dont need to use our own serialization, yayyy
// and this is embedded btw ofc
// "just tell me the fucking next WAL entry that I havent read you moron", okay, so logrus.read_from_col() , we'll handle the internal pointer for them
// let's also give them the option to read from a certain offset if they want to(certainly wont recommend it though, pwease dont do it senpai, bad BAD *bonks on head*)

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
        // MPSC for fsync scheduling
        let (tx, rx) = mpsc::channel::<String>();
        let tx_arc = Arc::new(tx);
        // Dedicated mmap pool for flusher: initialized once here
        let mut pool: HashMap<String, MmapMut> = HashMap::new();
        // Hourly tick counter
        let tick = Arc::new(AtomicU64::new(0));
        // Spawn background flusher
        thread::spawn(move || {
            // move the pool into the thread and only update it on misses
            let mut pool = pool; // btw, we need to find some way to reinitialize it every once in a while so that this shit doesnt just keeps growing AHAH
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
                // tick and hourly pool reset (~3600 ticks if FSYNC_SCHEDULE is 1s)
                let n = tick.fetch_add(1, Ordering::Relaxed) + 1;
                if n >= 3600 {
                    if tick.compare_exchange(n, 0, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                        let mut empty: HashMap<String, MmapMut> = HashMap::new();
                        std::mem::swap(&mut pool, &mut empty);
                        // old mmaps dropped here when `empty` goes out of scope so we dont keep growing linearly
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
        // okay, so for now, we just do a linear search over the chain while adding up block sizes and just returning the block which contains that offset inside it, for now, just do that
        let chain = self.reader.get_chain_for_col(col_name)?;
        let mut p_sum: u64 = 0;
        for block in chain.into_iter() {
            let block_size = block.limit - block.offset; // capacity of this block
            if offset < p_sum + block_size {
                return Some(block);
            }
            p_sum += block_size;
        }
        None
    }
}

// it would all make sense one day :)) , not today though, today we grind, amist all the chaos, we grind, for the beauty that is to come is unbounded