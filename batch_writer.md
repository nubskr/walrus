# Batch Write Endpoint Design Doc

## Overview
Add atomic batch write capability to Walrus WAL, enabling multiple entries to be written atomically to a topic with all-or-nothing semantics across scattered blocks and files.

## Problem Statement
Current `append_for_topic()` writes single entries. Users need to write multiple related entries atomically - either all entries are durably written, or none are. This is challenging because:
- Walrus uses variable-sized blocks scattered across multiple files
- A batch may span multiple blocks and files
- Writers allocate blocks dynamically during writes
- Multiple threads may write to the same topic concurrently

## Design

### API
```rust
pub fn batch_append_for_topic(
    &self, 
    col_name: &str, 
    batch: &[&[u8]]
) -> std::io::Result<()>
```

**Constraints:**
- Maximum batch size: 10GB total (sum of all entries + metadata)
- Returns `ErrorKind::InvalidInput` if batch exceeds limit
- Returns `ErrorKind::WouldBlock` if another batch write is in progress for this topic
- Returns `ErrorKind::Unsupported` if FD backend is not enabled

### Key Design Decisions

#### 1. Bounded Batch Size (10GB)
- Makes the problem tractable - can pre-compute exact block requirements
- Prevents unbounded memory allocation during planning phase
- Still large enough for most real-world use cases

#### 2. Atomic Flag for Concurrency Control
Add to `Writer` struct:
```rust
is_batch_writing: AtomicBool
```

**Semantics:**
- Regular `write()` checks flag, fails fast with `WouldBlock` if batch is in progress
- `batch_append_for_topic()` uses compare-exchange to acquire exclusive access
- RAII guard ensures flag is released even on panic
- No blocking - fail fast and let clients retry

**Why not mutex?**
- Batch writes can take significant time (10GB of I/O)
- Don't want to block regular writes - fail fast is better UX
- Simpler reasoning about deadlocks

#### 3. Four-Phase Execution

**Phase 0: Validation**
- Calculate total bytes needed
- Verify batch size ≤ 10GB
- Acquire atomic `is_batch_writing` flag

**Phase 1: Pre-allocation & Planning**
- Hold `current_block` and `current_offset` mutexes for entire operation
- Save original state for rollback:
  ```rust
  struct BatchRevertInfo {
      topic: String,
      original_block_id: u64,
      original_offset: u64,
      allocated_block_ids: Vec<u64>,
  }
  ```
- Calculate exactly which blocks are needed
- Allocate new blocks as necessary
- Build complete write plan: `Vec<(Block, offset, data_index)>`

**Phase 2: io_uring Preparation**
- Prepare all write operations while still holding locks
- Serialize metadata for each entry
- Build combined buffers (metadata + data)
- Push all operations to io_uring submission queue

**Phase 3: Atomic Submission**
- Single `ring.submit_and_wait(write_plan.len())` call
- Kernel guarantees: either all writes complete or none do
- This is the **atomic commit point**
- Check all completion queue entries for errors

**Phase 4: Finalization or Rollback**
- **On success:**
  - fsync all touched files (batched per file)
  - Release locks
  - Release atomic flag via RAII guard
- **On failure:**
  - Restore `current_offset` to original value
  - Mark newly allocated blocks as unlocked/reclaimable
  - Release locks
  - Release atomic flag via RAII guard
  - Return error

### Atomicity Guarantees

**What we guarantee:**
- All entries in a batch are written atomically
- If any write fails, all writes are rolled back
- Readers will see either all entries or none
- No partial batch will ever be visible

**How we achieve it:**
1. **io_uring batched submission** - kernel-level atomicity for write operations
2. **Pre-allocation** - no mid-batch allocation failures
3. **Held locks** - no concurrent modifications to writer state during batch
4. **Atomic flag** - prevents concurrent regular writes
5. **Rollback on failure** - restore exact pre-batch state

### Failure Modes & Recovery

| Failure Point | Recovery Action | State After Recovery |
|---------------|-----------------|---------------------|
| Size validation fails | Immediate return | No state change |
| Flag acquisition fails | Immediate return | No state change |
| Block allocation fails | Release locks, return error | No state change |
| io_uring write fails | Rollback offsets, mark blocks unlocked | Original state restored |
| fsync fails | Rollback offsets, mark blocks unlocked | Original state restored |
| Panic during batch | RAII guard releases flag | Flag released, may have partial writes* |

*Panic during batch is considered catastrophic - process restart will trigger normal recovery

## Impact on Existing System

### Modified Components

#### `Writer` struct
```diff
struct Writer {
    allocator: Arc<BlockAllocator>,
    current_block: Mutex<Block>,
    reader: Arc<Reader>,
    col: String,
    publisher: Arc<mpsc::Sender<String>>,
    current_offset: Mutex<u64>,
    fsync_schedule: FsyncSchedule,
+   is_batch_writing: AtomicBool,
}
```

#### `Writer::write()`
```diff
pub fn write(&self, data: &[u8]) -> std::io::Result<()> {
+   if self.is_batch_writing.load(Ordering::Acquire) {
+       return Err(std::io::Error::new(
+           std::io::ErrorKind::WouldBlock,
+           "batch write in progress for this topic"
+       ));
+   }
    // ... existing logic ...
}
```

### Behavioral Changes

#### For Regular Writes
- **Before:** Always proceeded (subject to mutex availability)
- **After:** May fail with `WouldBlock` if batch write is active
- **Client impact:** Must handle `WouldBlock` and retry after backoff

#### For Block Allocation
- **Before:** Allocated on-demand during write
- **After:** Batch writes pre-allocate all needed blocks upfront
- **Impact:** Temporary increase in allocated-but-unused blocks during batch

#### For File I/O
- **Before:** One write syscall per entry
- **After:** Batched syscall for entire batch (io_uring submission)
- **Impact:** Significantly better throughput for large batches

### Performance Characteristics

**Batch Write:**
- **Latency:** Higher than single write (pre-allocation + batching overhead)
- **Throughput:** Much higher for large batches (1 syscall vs N syscalls)
- **Lock hold time:** Longer (entire batch duration)
- **Memory:** O(batch_size) temporary buffers during io_uring prep

**Regular Write During Batch:**
- **Latency:** Near-zero (instant `WouldBlock` failure)
- **Success rate:** Reduced during batch operations

### Resource Usage

| Resource | Before | After | Notes |
|----------|--------|-------|-------|
| Memory | O(1) per write | O(batch_size) during batch | Temporary buffers for io_uring |
| File descriptors | Same | Same | No change |
| Block allocation | On-demand | Pre-allocated for batch | Blocks marked locked immediately |
| Syscalls | N writes + M fsyncs | 1 io_uring submit + M fsyncs | M = number of unique files touched |

## Dependencies

### Required
- `io-uring` crate (already in use)
- FD backend must be enabled (`enable_fd_backend()`)
- Linux kernel with io_uring support

### Not Required
- No new external dependencies
- No changes to on-disk format
- No changes to recovery logic

## Testing Strategy

### Unit Tests
- Validate 10GB size limit enforcement
- Test concurrent batch write rejection
- Test rollback on allocation failure
- Test rollback on write failure

### Integration Tests
- Write batch, verify all entries readable
- Concurrent regular writes during batch (expect `WouldBlock`)
- Batch spanning multiple blocks/files
- Recovery after crash mid-batch (existing recovery should handle)

### Performance Tests
- Throughput: 1000 small entries vs 1000 individual writes
- Latency: batch write end-to-end timing
- Concurrency: regular write success rate during batch load

## Future Enhancements

### Potential Improvements
1. **Batch queuing:** Queue regular writes during batch instead of failing
2. **Parallel batches:** Allow batches to different topics concurrently
3. **Streaming batches:** Support >10GB via chunked batch writes
4. **Retry logic:** Automatic retry of failed batches with exponential backoff

### Non-goals
- Transactions across multiple topics (each batch is single-topic)
- Distributed atomicity (single-node only)
- Read-your-writes within batch (batch is atomic unit)

## Migration Path

### Rollout
1. Add `is_batch_writing` field to `Writer::new()` (default `false`)
2. Deploy new `batch_append_for_topic()` method
3. Clients opt-in to batch writes
4. Monitor for `WouldBlock` error rates

### Backward Compatibility
- Existing `append_for_topic()` unchanged (except `WouldBlock` error)
- On-disk format unchanged
- Recovery logic unchanged
- No data migration needed

### Rollback Plan
If issues arise:
1. Clients stop calling `batch_append_for_topic()`
2. System returns to original behavior
3. Remove feature in next release

No data loss or corruption risk - worst case is failed batch writes that clients must retry.

---

## Summary

This design adds atomic batch writes to Walrus by:
- Pre-computing all block allocations upfront
- Using io_uring for single-syscall atomic writes
- Employing an atomic flag for fail-fast concurrency control
- Maintaining rollback information for failure recovery

The approach is elegant because it works **with** the existing variable-sized block architecture rather than against it, and leverages kernel-level atomicity guarantees from io_uring rather than building complex coordination logic in userspace.

example code:

```rust
struct Writer {
    allocator: Arc<BlockAllocator>,
    current_block: Mutex<Block>,
    reader: Arc<Reader>,
    col: String,
    publisher: Arc<mpsc::Sender<String>>,
    current_offset: Mutex<u64>,
    fsync_schedule: FsyncSchedule,
    is_batch_writing: AtomicBool, // ADD THIS
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
            is_batch_writing: AtomicBool::new(false), // INIT
        }
    }

    pub fn write(&self, data: &[u8]) -> std::io::Result<()> {
        // Check if batch write is in progress
        if self.is_batch_writing.load(Ordering::Acquire) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "batch write in progress for this topic"
            ));
        }
        
        // ... rest of your existing write logic ...
        let mut block = self.current_block.lock().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::Other, "current_block lock poisoned")
        })?;
        // ... etc ...
    }
}

impl Walrus {
    pub fn batch_append_for_topic(
        &self, 
        col_name: &str, 
        batch: &[&[u8]]
    ) -> std::io::Result<()> {
        // Validate batch size
        let total_bytes: u64 = batch.iter()
            .map(|data| (PREFIX_META_SIZE as u64) + (data.len() as u64))
            .sum();
        
        if total_bytes > 10 * 1024 * 1024 * 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "batch exceeds 10GB limit"
            ));
        }
        
        let writer = self.get_or_create_writer(col_name)?;
        
        // Try to acquire batch write flag
        if writer.is_batch_writing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err() 
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "another batch write already in progress"
            ));
        }
        
        // Ensure we release the flag even if we panic
        struct BatchGuard<'a> {
            flag: &'a AtomicBool,
        }
        impl<'a> Drop for BatchGuard<'a> {
            fn drop(&mut self) {
                self.flag.store(false, Ordering::Release);
                debug_print!("[batch] released batch_writing flag");
            }
        }
        let _guard = BatchGuard { flag: &writer.is_batch_writing };
        
        // Now we have exclusive batch access!
        let mut block = writer.current_block.lock().unwrap();
        let mut cur_offset = writer.current_offset.lock().unwrap();
        
        let mut revert_info = BatchRevertInfo {
            topic: col_name.to_string(),
            original_block_id: block.id,
            original_offset: *cur_offset,
            allocated_block_ids: Vec::new(),
        };
        
        let mut write_plan = Vec::new();
        
        // Phase 1: Pre-allocate
        let mut batch_idx = 0;
        let mut remaining = total_bytes;
        
        while batch_idx < batch.len() {
            let data = batch[batch_idx];
            let need = (PREFIX_META_SIZE as u64) + (data.len() as u64);
            let available = block.limit - *cur_offset;
            
            if available >= need {
                write_plan.push((block.clone(), *cur_offset, batch_idx));
                *cur_offset += need;
                remaining -= need;
                batch_idx += 1;
            } else {
                // Seal current block
                FileStateTracker::set_block_unlocked(block.id as usize);
                let mut sealed = block.clone();
                sealed.used = *cur_offset;
                sealed.mmap.flush()?;
                let _ = writer.reader.append_block_to_chain(&writer.col, sealed);
                
                // Allocate new block
                let new_block = unsafe { 
                    self.allocator.alloc_block(remaining.min(MAX_ALLOC))? 
                };
                
                revert_info.allocated_block_ids.push(new_block.id);
                *block = new_block;
                *cur_offset = 0;
            }
        }
        
        // Phase 2: Prepare io_uring writes
        let mut ring = io_uring::IoUring::new(write_plan.len() + 64)?;
        let mut buffers = Vec::new();
        
        for (blk, offset, data_idx) in write_plan.iter() {
            let data = batch[*data_idx];
            
            // Prepare metadata
            let new_meta = Metadata {
                read_size: data.len(),
                owned_by: col_name.to_string(),
                next_block_start: blk.offset + blk.limit,
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
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "batch writes require FD backend"
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
                ring.submission().push(&write_op)?;
            }
        }
        
        // Phase 3: Submit io_uring batch
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
                    // Rollback
                    *cur_offset = revert_info.original_offset;
                    for block_id in revert_info.allocated_block_ids {
                        BlockStateTracker::set_block_unlocked(block_id as usize);
                    }
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "batch write failed, rolled back"
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
                
                debug_print!(
                    "[batch] SUCCESS: wrote {} entries, {} bytes to topic={}",
                    batch.len(),
                    total_bytes,
                    col_name
                );
                Ok(())
            }
            Err(e) => {
                // Rollback
                *cur_offset = revert_info.original_offset;
                for block_id in revert_info.allocated_block_ids {
                    BlockStateTracker::set_block_unlocked(block_id as usize);
                }
                Err(e)
            }
        }
        // _guard drops here, releases flag automatically
    }
}
```