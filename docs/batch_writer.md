# Batch Write Endpoint Design Doc

## Overview
Add atomic batch write capability to Walrus WAL, enabling multiple entries to be written atomically to a topic with all-or-nothing semantics across scattered blocks and files.

## Problem Statement
Current `append_for_topic()` writes single entries. Users need to write multiple related entries atomically, either all entries are durably written, or none are. This is challenging because:
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
- Maximum entries per batch: 2,000 (hard cap shared with batch reads due to the default size limitations of io_uring submission ring)
- Maximum batch size: 10GB total (sum of all entries + metadata)
- Returns `ErrorKind::InvalidInput` if batch exceeds limit
- Returns `ErrorKind::WouldBlock` if another batch write is in progress for this topic
- Falls back to sequential writes when the mmap backend is active; the io_uring
  path requires the FD backend and will return `ErrorKind::Unsupported` if the
  storage handle is not fd-backed.

### Key Design Decisions

#### 1. Bounded Batch Size (10GB)
- Makes the problem tractable, can pre-compute exact block requirements
- Prevents unbounded memory allocation during planning phase
- Still large enough for most real world use cases

#### 2. Atomic Flag for Concurrency Control
Add to `Writer` struct:
```rust
is_batch_writing: AtomicBool
```

**Semantics:**
- Regular `write()` checks flag, fails fast with `WouldBlock` if batch is in progress
- `batch_append_for_topic()` uses compare-exchange to acquire exclusive access
- RAII guard ensures flag is released even on panic
- No blocking, fail fast and let clients retry

**Why not mutex?**
- Batch writes can take significant time (10GB of I/O)
- Don't want to block regular writes, fail fast is better UX
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
1. **io_uring batched submission**, kernel-level atomicity for write operations
2. **Pre-allocation**, no mid-batch allocation failures
3. **Held locks**, no concurrent modifications to writer state during batch
4. **Atomic flag**, prevents concurrent regular writes
5. **Rollback on failure**, restore exact pre-batch state

### Failure Modes & Recovery

| Failure Point | Recovery Action | State After Recovery |
|---------------|-----------------|---------------------|
| Size validation fails | Immediate return | No state change |
| Flag acquisition fails | Immediate return | No state change |
| Block allocation fails | Release locks, return error | No state change |
| io_uring write fails | Rollback offsets, mark blocks unlocked | Original state restored |
| fsync fails | Rollback offsets, mark blocks unlocked | Original state restored |
| Panic during batch | RAII guard releases flag | Flag released, may have partial writes* |

*Panic during batch is considered catastrophic, process restart will trigger normal recovery

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

No data loss or corruption risk, worst case is failed batch writes that clients must retry.

---

## Summary

This design adds atomic batch writes to Walrus by:
- Pre-computing all block allocations upfront
- Using io_uring for single-syscall atomic writes
- Employing an atomic flag for fail-fast concurrency control
- Maintaining rollback information for failure recovery

The approach is elegant because it works **with** the existing variable-sized block architecture rather than against it, and leverages kernel-level atomicity guarantees from io_uring rather than building complex coordination logic in userspace.
