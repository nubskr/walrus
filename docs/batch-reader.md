# Batch Read Endpoint Design Notes

## Overview
Walrus exposes a `batch_read_for_topic` API that lets clients page through
entries for a topic while respecting both byte and entry limits. The reader
walks the sealed block chain first and then the active writer tail, issuing
batched I/O via `io_uring` (when the FD backend is enabled) or mmap reads
otherwise.

```rust
pub fn batch_read_for_topic(
    &self,
    col_name: &str,
    max_bytes: usize,
    checkpoint: bool,
) -> std::io::Result<Vec<Entry>>;
```

## Behavior
- Entries are returned in commit order and each call advances the persisted
  read offset (StrictlyAtOnce) or the in-memory cursor (AtLeastOnce) **only when**
  `checkpoint` is `true`. Passing `false` leaves the cursor untouched so callers
  can peek.
- `max_bytes` is applied to the payload size only. We always return at least
  one entry per call even if it exceeds the byte budget.
- For Linux FD builds, we submit one `io_uring::opcode::Read` per contiguous
  range, then verify every completion before parsing metadata and payload
  bytes.
- Checksums are verified for every entry prior to emitting them to the caller.

## Entry Cap
- Batch reads now share the same `MAX_BATCH_ENTRIES` constant (2,000) that
  batch writes use. Even if `max_bytes` is very large, the reader stops parsing
  after 2,000 entries to stay well below the `io_uring` submission queue size
  limit (2,047 entries). Remaining entries are surfaced on subsequent calls.
- Calls that exceed the cap still update the reader cursor so the next call
  continues where the previous one left off.

## Error Handling
- If any read completion reports an error or a short read we return
  `UnexpectedEof`.
- Metadata parsing failures or checksum mismatches produce `InvalidData`.
- When the FD backend is disabled on Linux builds we fall back to mmap reads,
  otherwise we error out if the configuration is inconsistent.
