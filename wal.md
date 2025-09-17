reqs:
* be highly available
* somehow guarantee 'reads your writes' (in consistent mode)

the below stuff is a bunch of bullshit btw, I have better stuff

-----

# Lock-Free Columnar WAL Using Memory-Mapped Block Chains

The core insight: **Give each column its own independent append log**, implemented as a chain of memory-mapped blocks that appear virtually contiguous through careful pointer management.

```
Column A: [Block 1] -> [Block 7] -> [Block 15] -> ...
Column B: [Block 2] -> [Block 8] -> [Block 16] -> ...
Column C: [Block 3] -> [Block 9] -> [Block 17] -> ...
```

Each column writes to its own blocks without any coordination with other columns.

## Core Components

### 1. Block Structure

Each block is a fixed-size chunk (e.g., 10MB) of memory-mapped file space:

```rust
const BLOCK_SIZE: usize = 10 * 1024 * 1024; // 10MB blocks

struct Block {
    header: BlockHeader,
    data: [u8; BLOCK_SIZE - sizeof(BlockHeader)],
}

struct BlockHeader {
    magic: u32,                    // Validity check
    column_id: u32,                 // Owner column
    sequence_num: u64,              // Block sequence in column's chain
    write_offset: AtomicUsize,      // Current write position
    next_block: Option<BlockLocation>, // Next block in chain
    checksum: u32,                  // Data integrity
}

struct BlockLocation {
    file_id: u32,
    offset: u64,  // Multiple of BLOCK_SIZE
}
```

### 2. Block Allocator

A minimally-locking component that hands out fresh blocks to columns:

```rust
struct BlockAllocator {
    current_file: File,
    current_file_id: AtomicU32,
    next_block_offset: AtomicU64,
    lock: Mutex<()>, // Only for new file creation
}

impl BlockAllocator {
    fn allocate_block(&self) -> Result<BlockLocation> {
        let offset = self.next_block_offset.fetch_add(BLOCK_SIZE, Ordering::Relaxed);

        if offset + BLOCK_SIZE > MAX_FILE_SIZE {
            // Rare path: need new file (takes lock)
            let _guard = self.lock.lock().unwrap();
            self.create_new_file()?;
            return self.allocate_block(); // Retry
        }

        Ok(BlockLocation {
            file_id: self.current_file_id.load(Ordering::Relaxed),
            offset,
        })
    }

    fn create_new_file(&self) -> Result<()> {
        let new_id = self.current_file_id.load(Ordering::Relaxed) + 1;
        let file = File::create(format!("wal_{}.dat", new_id))?;

        // Pre-allocate as sparse file
        file.set_len(MAX_FILE_SIZE)?;

        self.current_file_id.store(new_id, Ordering::Relaxed);
        self.next_block_offset.store(0, Ordering::Relaxed);
        Ok(())
    }
}
```

### 3. Column WAL

Each column maintains its own chain of blocks:

```rust
struct ColumnWAL {
    column_id: u32,
    current_block: RwLock<CurrentBlock>,
    block_chain: RwLock<Vec<BlockLocation>>,
    allocator: Arc<BlockAllocator>,
}

struct CurrentBlock {
    location: BlockLocation,
    mmap: MmapMut,
    write_position: usize,
}

impl ColumnWAL {
    fn append(&self, entry: &WALEntry) -> Result<()> {
        let entry_size = entry.serialized_size();

        // Fast path: try to write to current block
        {
            let block = self.current_block.read().unwrap();
            if block.write_position + entry_size <= BLOCK_SIZE {
                // Atomic increment to reserve space
                let offset = block.header().write_offset
                    .fetch_add(entry_size, Ordering::AcqRel);

                if offset + entry_size <= BLOCK_SIZE {
                    // We got space! Write without any locks
                    block.mmap[offset..offset + entry_size]
                        .copy_from_slice(&entry.serialize());

                    // Optional: flush for durability
                    block.mmap.flush_range(offset, entry_size)?;

                    return Ok(());
                }
            }
        }

        // Slow path: need new block (rare)
        self.allocate_new_block()?;
        self.append(entry) // Retry
    }

    fn allocate_new_block(&self) -> Result<()> {
        let mut current = self.current_block.write().unwrap();

        // Double-check after acquiring write lock
        if current.write_position >= BLOCK_SIZE - MIN_ENTRY_SIZE {
            let new_location = self.allocator.allocate_block()?;
            let new_mmap = unsafe {
                MmapOptions::new()
                    .offset(new_location.offset)
                    .len(BLOCK_SIZE)
                    .map_mut(&self.get_file(new_location.file_id))?
            };

            // Link the old block to new one
            current.set_next_block(new_location);

            // Update current block
            *current = CurrentBlock {
                location: new_location,
                mmap: new_mmap,
                write_position: size_of::<BlockHeader>(),
            };

            // Add to chain
            self.block_chain.write().unwrap().push(new_location);
        }

        Ok(())
    }
}
```

### 4. Virtual Continuous Reader

Makes scattered blocks appear as one continuous stream:

```rust
struct WALReader<'a> {
    column_wal: &'a ColumnWAL,
    block_chain: Vec<BlockLocation>,
    current_block_idx: usize,
    current_block_mmap: Option<Mmap>,
    position_in_block: usize,
}

impl<'a> WALReader<'a> {
    fn new(column_wal: &'a ColumnWAL) -> Self {
        let chain = column_wal.block_chain.read().unwrap().clone();
        WALReader {
            column_wal,
            block_chain: chain,
            current_block_idx: 0,
            current_block_mmap: None,
            position_in_block: size_of::<BlockHeader>(),
        }
    }

    fn read_next(&mut self) -> Option<WALEntry> {
        loop {
            // Ensure current block is mapped
            if self.current_block_mmap.is_none() {
                if self.current_block_idx >= self.block_chain.len() {
                    return None; // End of chain
                }

                let location = &self.block_chain[self.current_block_idx];
                let mmap = unsafe {
                    MmapOptions::new()
                        .offset(location.offset)
                        .len(BLOCK_SIZE)
                        .map(&self.column_wal.get_file(location.file_id))
                        .ok()?
                };
                self.current_block_mmap = Some(mmap);
            }

            let mmap = self.current_block_mmap.as_ref()?;

            // Try to read entry from current position
            if let Some(entry) = WALEntry::deserialize(&mmap[self.position_in_block..]) {
                self.position_in_block += entry.serialized_size();
                return Some(entry);
            } else {
                // Move to next block
                self.current_block_idx += 1;
                self.current_block_mmap = None;
                self.position_in_block = size_of::<BlockHeader>();
            }
        }
    }
}

impl<'a> Iterator for WALReader<'a> {
    type Item = WALEntry;

    fn next(&mut self) -> Option<Self::Item> {
        self.read_next()
    }
}
```

## Key Optimizations

### 1. Lock-Free Appends Within Blocks

Using atomic operations for the write offset within each block enables multiple threads to append to the same column without locks:

```rust
fn append_lockfree(&self, entry: &WALEntry) -> Result<()> {
    let block = self.current_block.read().unwrap();
    let header = unsafe {
        &*(block.mmap.as_ptr() as *const BlockHeader)
    };

    // Atomically reserve space
    let offset = header.write_offset
        .fetch_add(entry.size(), Ordering::AcqRel);

    if offset + entry.size() <= BLOCK_SIZE {
        // We own this range, write without locks
        block.mmap[offset..offset + entry.size()]
            .copy_from_slice(&entry.serialize());
        Ok(())
    } else {
        // Block full, need new one (rare path)
        drop(block);
        self.allocate_new_block()
    }
}
```

### 2. Sparse File Pre-allocation

Pre-allocating WAL files as sparse files provides instant block allocation without disk space overhead:

```rust
fn initialize_wal_file(path: &Path, size: u64) -> Result<File> {
    let file = File::create(path)?;
    file.set_len(size)?; // Sparse file - no actual disk usage
    Ok(file)
}
```

### 3. Per-Column Flush Control

Each column can have different durability guarantees:

```rust
enum DurabilityMode {
    Immediate,      // Flush after every write
    Batched(Duration), // Flush periodically
    Lazy,          // Let OS handle it
}

impl ColumnWAL {
    fn set_durability(&mut self, mode: DurabilityMode) {
        self.durability_mode = mode;
    }
}
```

### 4. Parallel Recovery

Since each column has independent WAL chains, recovery can be parallelized:

```rust
fn recover_database(wal_dir: &Path) -> Result<Database> {
    let columns = discover_columns(wal_dir)?;

    // Parallel recovery - one thread per column
    let handles: Vec<_> = columns
        .into_iter()
        .map(|col_id| {
            thread::spawn(move || {
                recover_column(col_id, wal_dir)
            })
        })
        .collect();

    let recovered_columns: Vec<_> = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect()?;

    Ok(Database::from_columns(recovered_columns))
}
```

### Memory Usage
- **Virtual memory**: `num_columns × blocks_per_column × block_size`
- **Physical memory**: Only actively accessed pages (kernel managed)
- **Overhead**: ~2% for page tables and metadata

- Sufficient virtual address space needed (64-bit systems)

### Configuration Parameters
```rust
struct WALConfig {
    block_size: usize,           // 10MB default
    max_file_size: u64,          // 1GB default
    flush_mode: DurabilityMode,  // Batched(100ms) default
    compression: Option<CompressionType>,
    checksum: bool,              // true default
}
```

---

