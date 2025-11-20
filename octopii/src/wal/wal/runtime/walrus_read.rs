use super::allocator::BlockStateTracker;
use super::reader::ColReaderInfo;
use super::{ReadConsistency, Walrus};
use crate::wal::wal::block::{Block, Entry, Metadata};
use crate::wal::wal::config::{checksum64, debug_print, MAX_BATCH_ENTRIES, PREFIX_META_SIZE};
use std::io;
use std::sync::{Arc, RwLock};

use rkyv::{AlignedVec, Deserialize};

#[cfg(target_os = "linux")]
use crate::wal::wal::config::USE_FD_BACKEND;
#[cfg(target_os = "linux")]
use std::sync::atomic::Ordering;

#[cfg(target_os = "linux")]
use io_uring;

#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

impl Walrus {
    pub fn read_next(&self, col_name: &str, checkpoint: bool) -> io::Result<Option<Entry>> {
        const TAIL_FLAG: u64 = 1u64 << 63;
        let info_arc = if let Some(arc) = {
            let map = self.reader.data.read().map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "reader map read lock poisoned")
            })?;
            map.get(col_name).cloned()
        } {
            arc
        } else {
            let mut map = self.reader.data.write().map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "reader map write lock poisoned")
            })?;
            map.entry(col_name.to_string())
                .or_insert_with(|| {
                    Arc::new(RwLock::new(ColReaderInfo {
                        chain: Vec::new(),
                        cur_block_idx: 0,
                        cur_block_offset: 0,
                        reads_since_persist: 0,
                        tail_block_id: 0,
                        tail_offset: 0,
                        hydrated_from_index: false,
                    }))
                })
                .clone()
        };
        let mut info = info_arc
            .write()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "col info write lock poisoned"))?;
        debug_print!(
            "[reader] read_next start: col={}, chain_len={}, idx={}, offset={}",
            col_name,
            info.chain.len(),
            info.cur_block_idx,
            info.cur_block_offset
        );

        // Load persisted position (supports tail sentinel)
        let mut persisted_tail: Option<(u64 /*block_id*/, u64 /*offset*/)> = None;
        if !info.hydrated_from_index {
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
                        if ib > info.chain.len() {
                            ib = info.chain.len();
                        }
                        info.cur_block_idx = ib;
                        if ib < info.chain.len() {
                            let used = info.chain[ib].used;
                            info.cur_block_offset = pos.cur_block_offset.min(used);
                        } else {
                            info.cur_block_offset = 0;
                        }
                    }
                    info.hydrated_from_index = true;
                } else {
                    // No persisted state present; mark hydrated to avoid re-checking every call
                    info.hydrated_from_index = true;
                }
            }
        }

        // If we have a persisted tail and some sealed blocks were recovered, fold into the last block
        if let Some((tail_block_id, tail_off)) = persisted_tail {
            if !info.chain.is_empty() {
                if let Some((idx, block)) = info
                    .chain
                    .iter()
                    .enumerate()
                    .find(|(_, b)| b.id == tail_block_id)
                {
                    let used = block.used;
                    info.cur_block_idx = idx;
                    info.cur_block_offset = tail_off.min(used);
                } else {
                    info.cur_block_idx = 0;
                    info.cur_block_offset = 0;
                }
            }
            persisted_tail = None;
        }

        // Important: release the per-column lock; we'll reacquire each iteration
        drop(info);

        loop {
            // Reacquire column lock at the start of each iteration
            let mut info = info_arc.write().map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "col info write lock poisoned")
            })?;
            // Sealed chain path
            if info.cur_block_idx < info.chain.len() {
                let idx = info.cur_block_idx;
                let off = info.cur_block_offset;
                let block = info.chain[idx].clone();

                if off >= block.used {
                    debug_print!(
                        "[reader] read_next: advance block col={}, block_id={}, offset={}, used={}",
                        col_name,
                        block.id,
                        off,
                        block.used
                    );
                    BlockStateTracker::set_checkpointed_true(block.id as usize);
                    info.cur_block_idx += 1;
                    info.cur_block_offset = 0;
                    continue;
                }

                match block.read(off) {
                    Ok((entry, consumed)) => {
                        // Compute new offset and decide whether to commit progress
                        let new_off = off + consumed as u64;
                        let mut maybe_persist = None;
                        if checkpoint {
                            info.cur_block_offset = new_off;
                            maybe_persist = if self.should_persist(&mut info, false) {
                                Some((info.cur_block_idx as u64, new_off))
                            } else {
                                None
                            };
                        }

                        // Drop the column lock before touching the index to avoid lock inversion
                        drop(info);
                        if checkpoint {
                            if let Some((idx_val, off_val)) = maybe_persist {
                                if let Ok(mut idx_guard) = self.read_offset_index.write() {
                                    let _ = idx_guard.set(col_name.to_string(), idx_val, off_val);
                                }
                            }
                        }

                        debug_print!(
                            "[reader] read_next: OK col={}, block_id={}, consumed={}, new_offset={}",
                            col_name,
                            block.id,
                            consumed,
                            new_off
                        );
                        return Ok(Some(entry));
                    }
                    Err(_) => {
                        debug_print!(
                            "[reader] read_next: read error col={}, block_id={}, offset={}",
                            col_name,
                            block.id,
                            off
                        );
                        return Ok(None);
                    }
                }
            }

            // Tail path
            let tail_snapshot = (info.tail_block_id, info.tail_offset);
            drop(info);

            let writer_arc = {
                let map = self.writers.read().map_err(|_| {
                    io::Error::new(io::ErrorKind::Other, "writers read lock poisoned")
                })?;
                match map.get(col_name) {
                    Some(w) => w.clone(),
                    None => return Ok(None),
                }
            };
            let (active_block, written) = writer_arc.snapshot_block()?;

            // If persisted tail points to a different block and that block is now sealed in chain, fold it
            // Reacquire column lock for folding/rebasing decisions
            let mut info = info_arc.write().map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "col info write lock poisoned")
            })?;
            if let Some((tail_block_id, tail_off)) = persisted_tail {
                if tail_block_id != active_block.id {
                    if let Some((idx, _)) = info
                        .chain
                        .iter()
                        .enumerate()
                        .find(|(_, b)| b.id == tail_block_id)
                    {
                        info.cur_block_idx = idx;
                        info.cur_block_offset = tail_off.min(info.chain[idx].used);
                        if checkpoint {
                            if self.should_persist(&mut info, true) {
                                if let Ok(mut idx_guard) = self.read_offset_index.write() {
                                    let _ = idx_guard.set(
                                        col_name.to_string(),
                                        info.cur_block_idx as u64,
                                        info.cur_block_offset,
                                    );
                                }
                            }
                        }
                        persisted_tail = None; // sealed now
                        drop(info);
                        continue;
                    } else {
                        // rebase tail to current active block at 0
                        persisted_tail = Some((active_block.id, 0));
                        if checkpoint {
                            if self.should_persist(&mut info, true) {
                                if let Ok(mut idx_guard) = self.read_offset_index.write() {
                                    let _ = idx_guard.set(
                                        col_name.to_string(),
                                        active_block.id | TAIL_FLAG,
                                        0,
                                    );
                                }
                            }
                        }
                    }
                }
            } else {
                // No persisted tail; init at current active block start
                persisted_tail = Some((active_block.id, 0));
                if checkpoint {
                    if self.should_persist(&mut info, true) {
                        if let Ok(mut idx_guard) = self.read_offset_index.write() {
                            let _ =
                                idx_guard.set(col_name.to_string(), active_block.id | TAIL_FLAG, 0);
                        }
                    }
                }
            }
            drop(info);

            // Choose the best known tail offset: prefer in-memory snapshot for current active block
            let (tail_block_id, mut tail_off) = match persisted_tail {
                Some(v) => v,
                None => return Ok(None),
            };
            if tail_block_id == active_block.id {
                let (snap_id, snap_off) = tail_snapshot;
                if snap_id == active_block.id {
                    tail_off = tail_off.max(snap_off);
                }
            } else {
                // If writer rotated and persisted tail points elsewhere, loop above will fold/rebase
            }
            // If writer rotated after we set persisted_tail, loop to fold/rebase
            if tail_block_id != active_block.id {
                // Loop to next iteration; `info` will be reacquired at loop top
                continue;
            }

            if tail_off < written {
                match active_block.read(tail_off) {
                    Ok((entry, consumed)) => {
                        let new_off = tail_off + consumed as u64;
                        // Reacquire column lock to update in-memory progress, then decide persistence
                        let mut info = info_arc.write().map_err(|_| {
                            io::Error::new(io::ErrorKind::Other, "col info write lock poisoned")
                        })?;
                        let mut maybe_persist = None;
                        if checkpoint {
                            info.tail_block_id = active_block.id;
                            info.tail_offset = new_off;
                            maybe_persist = if self.should_persist(&mut info, false) {
                                Some((tail_block_id | TAIL_FLAG, new_off))
                            } else {
                                None
                            };
                        }
                        drop(info);
                        if checkpoint {
                            if let Some((idx_val, off_val)) = maybe_persist {
                                if let Ok(mut idx_guard) = self.read_offset_index.write() {
                                    let _ = idx_guard.set(col_name.to_string(), idx_val, off_val);
                                }
                            }
                        }

                        debug_print!(
                            "[reader] read_next: tail OK col={}, block_id={}, consumed={}, new_tail_off={}",
                            col_name,
                            active_block.id,
                            consumed,
                            new_off
                        );
                        return Ok(Some(entry));
                    }
                    Err(_) => {
                        debug_print!(
                            "[reader] read_next: tail read error col={}, block_id={}, offset={}",
                            col_name,
                            active_block.id,
                            tail_off
                        );
                        return Ok(None);
                    }
                }
            } else {
                debug_print!(
                    "[reader] read_next: tail caught up col={}, block_id={}, off={}, written={}",
                    col_name,
                    active_block.id,
                    tail_off,
                    written
                );
                return Ok(None);
            }
        }
    }

    fn should_persist(&self, info: &mut ColReaderInfo, force: bool) -> bool {
        match self.read_consistency {
            ReadConsistency::StrictlyAtOnce => true,
            ReadConsistency::AtLeastOnce { persist_every } => {
                let every = persist_every.max(1);
                if force {
                    info.reads_since_persist = 0;
                    return true;
                }
                let next = info.reads_since_persist.saturating_add(1);
                if next >= every {
                    info.reads_since_persist = 0;
                    true
                } else {
                    info.reads_since_persist = next;
                    false
                }
            }
        }
    }

    pub fn batch_read_for_topic(
        &self,
        col_name: &str,
        max_bytes: usize,
        checkpoint: bool,
    ) -> io::Result<Vec<Entry>> {
        // Helper struct for read planning
        struct ReadPlan {
            blk: Block,
            start: u64,
            end: u64,
            is_tail: bool,
            chain_idx: Option<usize>,
        }

        const TAIL_FLAG: u64 = 1u64 << 63;

        // Pre-snapshot active writer state to avoid lock-order inversion later
        let writer_snapshot: Option<(Block, u64)> = {
            let map = self
                .writers
                .read()
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "writers read lock poisoned"))?;
            match map.get(col_name).cloned() {
                Some(w) => match w.snapshot_block() {
                    Ok(snapshot) => Some(snapshot),
                    Err(_) => None,
                },
                None => None,
            }
        };

        // 1) Get or create reader info
        let info_arc = if let Some(arc) = {
            let map = self.reader.data.read().map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "reader map read lock poisoned")
            })?;
            map.get(col_name).cloned()
        } {
            arc
        } else {
            let mut map = self.reader.data.write().map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "reader map write lock poisoned")
            })?;
            map.entry(col_name.to_string())
                .or_insert_with(|| {
                    Arc::new(RwLock::new(ColReaderInfo {
                        chain: Vec::new(),
                        cur_block_idx: 0,
                        cur_block_offset: 0,
                        reads_since_persist: 0,
                        tail_block_id: 0,
                        tail_offset: 0,
                        hydrated_from_index: false,
                    }))
                })
                .clone()
        };

        let mut info = info_arc
            .write()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "col info write lock poisoned"))?;

        // Hydrate from index if needed
        let mut persisted_tail_for_fold: Option<(u64 /*block_id*/, u64 /*offset*/)> = None;
        if !info.hydrated_from_index {
            if let Ok(idx_guard) = self.read_offset_index.read() {
                if let Some(pos) = idx_guard.get(col_name) {
                    if (pos.cur_block_idx & TAIL_FLAG) != 0 {
                        let tail_block_id = pos.cur_block_idx & (!TAIL_FLAG);
                        info.tail_block_id = tail_block_id;
                        info.tail_offset = pos.cur_block_offset;
                        info.cur_block_idx = info.chain.len();
                        info.cur_block_offset = 0;
                        persisted_tail_for_fold = Some((tail_block_id, pos.cur_block_offset));
                    } else {
                        let mut ib = pos.cur_block_idx as usize;
                        if ib > info.chain.len() {
                            ib = info.chain.len();
                        }
                        info.cur_block_idx = ib;
                        if ib < info.chain.len() {
                            let used = info.chain[ib].used;
                            info.cur_block_offset = pos.cur_block_offset.min(used);
                        } else {
                            info.cur_block_offset = 0;
                        }
                    }

                    info.hydrated_from_index = true;
                } else {
                    info.hydrated_from_index = true;
                }
            }
        }

        // Fold persisted tail into sealed blocks if possible
        if let Some((tail_block_id, tail_off)) = persisted_tail_for_fold {
            if let Some(idx) = info
                .chain
                .iter()
                .enumerate()
                .find(|(_, b)| b.id == tail_block_id)
                .map(|(idx, _)| idx)
            {
                let used = info.chain[idx].used;
                info.cur_block_idx = idx;
                info.cur_block_offset = tail_off.min(used);
            }
        }

        // 2) Build read plan up to byte and entry limits
        let mut plan: Vec<ReadPlan> = Vec::new();
        let mut planned_bytes: usize = 0;
        let chain_len_at_plan = info.chain.len();
        let mut cur_idx = info.cur_block_idx;
        let mut cur_off = info.cur_block_offset;

        while cur_idx < info.chain.len() && planned_bytes < max_bytes {
            let block = info.chain[cur_idx].clone();
            if cur_off >= block.used {
                BlockStateTracker::set_checkpointed_true(block.id as usize);
                cur_idx += 1;
                cur_off = 0;
                continue;
            }

            let end = block.used.min(cur_off + (max_bytes - planned_bytes) as u64);
            if end > cur_off {
                plan.push(ReadPlan {
                    blk: block.clone(),
                    start: cur_off,
                    end,
                    is_tail: false,
                    chain_idx: Some(cur_idx),
                });
                planned_bytes += (end - cur_off) as usize;
            }
            cur_idx += 1;
            cur_off = 0;
        }

        // Plan tail if we're at the end of sealed chain
        if cur_idx >= chain_len_at_plan {
            if let Some((active_block, written)) = writer_snapshot.clone() {
                // Use in-memory tail progress if available for this block
                let tail_start = if info.tail_block_id == active_block.id {
                    info.tail_offset
                } else {
                    0
                };
                if tail_start < written {
                    let end = written; // read up to current writer offset
                    plan.push(ReadPlan {
                        blk: active_block.clone(),
                        start: tail_start,
                        end,
                        is_tail: true,
                        chain_idx: None,
                    });
                }
            }
        }

        if plan.is_empty() {
            return Ok(Vec::new());
        }

        // Hold lock across IO/parse for StrictlyAtOnce to avoid duplicate consumption
        let hold_lock_during_io = matches!(self.read_consistency, ReadConsistency::StrictlyAtOnce);
        // Manage the guard explicitly to satisfy the borrow checker
        let mut info_opt = Some(info);
        if !hold_lock_during_io {
            // Release lock for AtLeastOnce before IO
            drop(info_opt.take().unwrap());
        }

        // 3) Read ranges via io_uring (FD backend) or mmap
        #[cfg(target_os = "linux")]
        let buffers = if USE_FD_BACKEND.load(Ordering::Relaxed) {
            // io_uring path - try to initialize, fall back to mmap if not supported
            let ring_size = (plan.len() + 64).min(4096) as u32;
            let ring = match io_uring::IoUring::new(ring_size) {
                Ok(r) => Some(r),
                Err(_) => {
                    // io_uring not supported, will fall back to mmap path below
                    None
                }
            };

            if let Some(mut ring) = ring {
                // io_uring is available, use it
                let mut temp_buffers: Vec<Vec<u8>> = vec![Vec::new(); plan.len()];
                let mut expected_sizes: Vec<usize> = vec![0; plan.len()];

                for (plan_idx, read_plan) in plan.iter().enumerate() {
                    let size = (read_plan.end - read_plan.start) as usize;
                    expected_sizes[plan_idx] = size;
                    let mut buffer = vec![0u8; size];
                    let file_offset = read_plan.blk.offset + read_plan.start;

                    let fd = if let Some(fd_backend) = read_plan.blk.mmap.storage().as_fd() {
                        io_uring::types::Fd(fd_backend.file().as_raw_fd())
                    } else {
                        return Err(io::Error::new(
                            io::ErrorKind::Unsupported,
                            "batch reads require FD backend when io_uring is enabled",
                        ));
                    };

                    let read_op = io_uring::opcode::Read::new(fd, buffer.as_mut_ptr(), size as u32)
                        .offset(file_offset)
                        .build()
                        .user_data(plan_idx as u64);

                    temp_buffers[plan_idx] = buffer;

                    unsafe {
                        ring.submission().push(&read_op).map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::Other,
                                format!("io_uring push failed: {}", e),
                            )
                        })?;
                    }
                }

                // Submit and wait for all reads
                ring.submit_and_wait(plan.len())?;

                // Process completions and validate read lengths
                for _ in 0..plan.len() {
                    if let Some(cqe) = ring.completion().next() {
                        let plan_idx = cqe.user_data() as usize;
                        let got = cqe.result();
                        if got < 0 {
                            return Err(io::Error::new(
                                io::ErrorKind::Other,
                                format!("io_uring read failed: {}", got),
                            ));
                        }
                        if (got as usize) != expected_sizes[plan_idx] {
                            return Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                format!(
                                    "short read: got {} bytes, expected {}",
                                    got, expected_sizes[plan_idx]
                                ),
                            ));
                        }
                    }
                }

                temp_buffers
            } else {
                // io_uring not available, fall back to mmap reads
                plan.iter()
                    .map(|read_plan| {
                        let size = (read_plan.end - read_plan.start) as usize;
                        let mut buffer = vec![0u8; size];
                        let file_offset = (read_plan.blk.offset + read_plan.start) as usize;
                        read_plan.blk.mmap.read(file_offset, &mut buffer);
                        buffer
                    })
                    .collect()
            }
        } else {
            plan.iter()
                .map(|read_plan| {
                    let size = (read_plan.end - read_plan.start) as usize;
                    let mut buffer = vec![0u8; size];
                    let file_offset = (read_plan.blk.offset + read_plan.start) as usize;
                    read_plan.blk.mmap.read(file_offset, &mut buffer);
                    buffer
                })
                .collect()
        };

        #[cfg(not(target_os = "linux"))]
        let buffers: Vec<Vec<u8>> = plan
            .iter()
            .map(|read_plan| {
                let size = (read_plan.end - read_plan.start) as usize;
                let mut buffer = vec![0u8; size];
                let file_offset = (read_plan.blk.offset + read_plan.start) as usize;
                read_plan.blk.mmap.read(file_offset, &mut buffer);
                buffer
            })
            .collect();

        // 4) Parse entries from buffers in plan order
        let mut entries = Vec::new();
        let mut total_data_bytes = 0usize;
        let mut final_block_idx = 0usize;
        let mut final_block_offset = 0u64;
        let mut final_tail_block_id = 0u64;
        let mut final_tail_offset = 0u64;
        let mut entries_parsed = 0u32;
        let mut saw_tail = false;

        for (plan_idx, read_plan) in plan.iter().enumerate() {
            if entries.len() >= MAX_BATCH_ENTRIES {
                break;
            }
            let buffer = &buffers[plan_idx];
            let mut buf_offset = 0usize;

            while buf_offset < buffer.len() {
                if entries.len() >= MAX_BATCH_ENTRIES {
                    break;
                }
                // Try to read metadata header
                if buf_offset + PREFIX_META_SIZE > buffer.len() {
                    break; // Not enough data for header
                }

                let meta_len =
                    (buffer[buf_offset] as usize) | ((buffer[buf_offset + 1] as usize) << 8);

                if meta_len == 0 || meta_len > PREFIX_META_SIZE - 2 {
                    // Invalid or zeroed header - stop parsing this block
                    break;
                }

                // Deserialize metadata
                let mut aligned = AlignedVec::with_capacity(meta_len);
                aligned.extend_from_slice(&buffer[buf_offset + 2..buf_offset + 2 + meta_len]);

                let archived = unsafe { rkyv::archived_root::<Metadata>(&aligned[..]) };
                let meta: Metadata = match archived.deserialize(&mut rkyv::Infallible) {
                    Ok(m) => m,
                    Err(_) => break, // Parse error - stop
                };

                let data_size = meta.read_size;
                let entry_consumed = PREFIX_META_SIZE + data_size;

                // Check if we have enough buffer space for the data
                if buf_offset + entry_consumed > buffer.len() {
                    break; // Incomplete entry
                }

                // Enforce byte budget on payload bytes, but always allow at least one entry.
                let next_total = total_data_bytes
                    .checked_add(data_size)
                    .unwrap_or(usize::MAX);
                if next_total > max_bytes && !entries.is_empty() {
                    break;
                }

                // Extract and verify data
                let data_start = buf_offset + PREFIX_META_SIZE;
                let data_end = data_start + data_size;
                let data_slice = &buffer[data_start..data_end];

                // Verify checksum
                if checksum64(data_slice) != meta.checksum {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "checksum mismatch in batch read",
                    ));
                }

                // Add to results
                entries.push(Entry {
                    data: data_slice.to_vec(),
                });
                total_data_bytes = next_total;
                entries_parsed += 1;

                // Update position tracking
                let in_block_offset = read_plan.start + buf_offset as u64 + entry_consumed as u64;

                if read_plan.is_tail {
                    saw_tail = true;
                    final_tail_block_id = read_plan.blk.id;
                    final_tail_offset = in_block_offset;
                } else if let Some(idx) = read_plan.chain_idx {
                    final_block_idx = idx;
                    final_block_offset = in_block_offset;
                }

                buf_offset += entry_consumed;
            }
        }

        // 5) Commit progress (optional)
        if entries_parsed > 0 {
            enum PersistTarget {
                Tail { blk_id: u64, off: u64 },
                Sealed { idx: u64, off: u64 },
                None,
            }
            let mut target = PersistTarget::None;

            if hold_lock_during_io {
                // We still hold the original write guard here
                let mut info = info_opt.take().expect("column lock should be held");
                if checkpoint {
                    if saw_tail {
                        info.cur_block_idx = chain_len_at_plan;
                        info.cur_block_offset = 0;
                        info.tail_block_id = final_tail_block_id;
                        info.tail_offset = final_tail_offset;
                        target = PersistTarget::Tail {
                            blk_id: final_tail_block_id,
                            off: final_tail_offset,
                        };
                    } else {
                        info.cur_block_idx = final_block_idx;
                        info.cur_block_offset = final_block_offset;
                        target = PersistTarget::Sealed {
                            idx: final_block_idx as u64,
                            off: final_block_offset,
                        };
                    }
                }
                drop(info);
            } else {
                // Reacquire to update
                let mut info2 = info_arc.write().map_err(|_| {
                    io::Error::new(io::ErrorKind::Other, "col info write lock poisoned")
                })?;
                if checkpoint {
                    if saw_tail {
                        info2.cur_block_idx = chain_len_at_plan;
                        info2.cur_block_offset = 0;
                        info2.tail_block_id = final_tail_block_id;
                        info2.tail_offset = final_tail_offset;
                        if let ReadConsistency::AtLeastOnce { persist_every } =
                            self.read_consistency
                        {
                            // Clamp contribution so a single call can't reach the threshold
                            let room = persist_every
                                .saturating_sub(1)
                                .saturating_sub(info2.reads_since_persist);
                            let add = entries_parsed.min(room);
                            info2.reads_since_persist =
                                info2.reads_since_persist.saturating_add(add);
                            // target remains None here to avoid persisting to end in one batch
                        }
                    } else {
                        info2.cur_block_idx = final_block_idx;
                        info2.cur_block_offset = final_block_offset;
                        if let ReadConsistency::AtLeastOnce { persist_every } =
                            self.read_consistency
                        {
                            let room = persist_every
                                .saturating_sub(1)
                                .saturating_sub(info2.reads_since_persist);
                            let add = entries_parsed.min(room);
                            info2.reads_since_persist =
                                info2.reads_since_persist.saturating_add(add);
                        }
                    }
                }
                drop(info2);
            }

            if checkpoint {
                match target {
                    PersistTarget::Tail { blk_id, off } => {
                        if let Ok(mut idx_guard) = self.read_offset_index.write() {
                            let _ = idx_guard.set(col_name.to_string(), blk_id | TAIL_FLAG, off);
                        }
                    }
                    PersistTarget::Sealed { idx, off } => {
                        if let Ok(mut idx_guard) = self.read_offset_index.write() {
                            let _ = idx_guard.set(col_name.to_string(), idx, off);
                        }
                    }
                    PersistTarget::None => {}
                }
            }
        }

        Ok(entries)
    }
}
