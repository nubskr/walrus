/*
something like:


just do the dumbest thing now, first thing which comes to your mind, that's it

we will do it all in one file
*/
use std::time::SystemTime;
use std::sync::atomic::{AtomicBool, Ordering};
use std::cell::UnsafeCell;

const BLOCK_SIZE:u64 = 10 * 1024 * 1024; // 10mb
const BLOCKS_PER_FILE:u64 = 10;
const MAX_FILE_SIZE:u64 = BLOCK_SIZE * BLOCKS_PER_FILE;


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
struct Block {
    id: u64,
    file_path: String,
    offset: u64
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

M[col_name] -> 
*/

