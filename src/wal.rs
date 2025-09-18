/*
something like:


just do the dumbest thing now, first thing which comes to your mind, that's it

we will do it all in one file
*/
use std::time::SystemTime;
use std::sync::atomic::{AtomicBool, Ordering};
use std::cell::UnsafeCell;
use std::sync::{Arc, Mutex};
use memmap2::MmapMut;
use std::fs::OpenOptions;


const BLOCK_SIZE:u64 = 10 * 1024 * 1024; // 10mb
const BLOCKS_PER_FILE:u64 = 10;
const MAX_FILE_SIZE:u64 = BLOCK_SIZE * BLOCKS_PER_FILE;
const FSYNC_SCHEDULE:u64 = 5;

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

// todo: add checksum and mmap object here too
#[derive(Clone)]
struct Block {
    id: u64,
    file_path: String,
    offset: u64,
    mmap: Arc<MmapMut>,
}

impl Block {
    pub fn write(&self, data: &[u8]) {
        use std::io::{Seek, SeekFrom, Write};
        let mut file = std::fs::OpenOptions::new().read(true).write(true).open(&self.file_path).unwrap();
        file.seek(SeekFrom::Start(self.offset)).unwrap();
        file.write_all(data).unwrap();
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
    std::fs::File::create(&file_path).unwrap();
    file_path
}

impl BlockAllocator {
    pub fn new() -> Self {
        std::fs::create_dir_all("wal_files").ok();
        let file1 = make_new_file();
        BlockAllocator { next_block: UnsafeCell::new(Block {id: 1 , offset: 0, file_path: file1}), lock: AtomicBool::new(false)}
    }
    
    pub unsafe fn get_next_available_block(&self) -> Block {
        // if we are out of blocks in the current file, just switch to a new file
        self.lock();
        let data = &mut *self.next_block.get();
        if data.offset >= MAX_FILE_SIZE {
            data.file_path = make_new_file();
            data.offset = 0;
        }
        let ret = data.clone();
        data.offset += BLOCK_SIZE;
        data.id += 1;
        self.unlock();
        ret
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

struct Writer{
    current_block: Block,
    mutex: Mutex<()>,
}

impl Writer {
    pub fn new(current_block: Block) -> Self {
        Writer { current_block, mutex: Mutex::new(()) }
    }

    pub fn write(&self, data: &[u8]) {
        let _guard = self.mutex.lock();
        self.current_block.write(data);
    }
}


// -------------------------
// this bad boi is gonna slapppp man, fucking f1 car
// but we still need a Map store it lel

pub struct SharedMmap {
    mmap: MmapMut,
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
        
        Ok(Arc::new(Self { mmap }))
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
    }
    
    pub fn len(&self) -> usize {
        self.mmap.len()
    }
}


// ---------------------
// I am so drained and fucked man... 