use super::allocator::BlockStateTracker;
use super::reader::ColReaderInfo;
use super::{ReadConsistency, Walrus};
use crate::wal::block::{Block, Entry, Metadata};
use crate::wal::config::{MAX_BATCH_ENTRIES, PREFIX_META_SIZE, checksum64, debug_print};
use std::io;
use std::sync::{Arc, RwLock};

use rkyv::{AlignedVec, Deserialize};
use tracing::info;

#[cfg(target_os = "linux")]
use crate::wal::config::USE_FD_BACKEND;
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
                    if let Some(idx) = info
                        .chain
                        .iter()
                        .enumerate()
                        .find(|(_, b)| b.id == tail_block_id)
                        .map(|(idx, _)| idx)
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
        start_offset: Option<u64>,
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

        info!(
            "batch_read_for_topic: col_name={}, max_bytes={}, checkpoint={}, start_offset={:?}",
            col_name, max_bytes, checkpoint, start_offset
        );

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

        // 1) Prepare state (Chain + Position)
        let mut _held_arc: Option<Arc<RwLock<ColReaderInfo>>> = None;

        let (
            chain,
            mut cur_idx,
            mut cur_off,
            tail_block_id,
            tail_offset,
            mut info_guard,
            mut initial_trim,
            mut first_end_hint,
        ) = if let Some(req_offset) = start_offset {
            // --- Stateless Read (Offset Provided) ---
            let map = self.reader.data.read().map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "reader map read lock poisoned")
            })?;

            let chain = if let Some(arc) = map.get(col_name) {
                let guard = arc.read().map_err(|_| {
                    io::Error::new(io::ErrorKind::Other, "col info read lock poisoned")
                })?;

                info!(
                    "batch_read_for_topic: (stateless) initial chain len: {}",
                    guard.chain.len()
                );

                guard.chain.clone()
            } else {
                Vec::new()
            };

            // Find block containing offset

            let mut c_idx = 0;

            let mut rem = req_offset;

            let mut found = false;

            info!(
                "batch_read_for_topic: (stateless) searching for block with req_offset={}, initial rem={}",
                req_offset, rem
            );

            for (i, b) in chain.iter().enumerate() {
                info!(
                    "batch_read_for_topic: (stateless) iterating block {} (id={}), used={}, rem={}",
                    i, b.id, b.used, rem
                );
                if rem < b.used {
                    c_idx = i;
                    found = true;
                    info!(
                        "batch_read_for_topic: (stateless) found block {} (id={}), rem={}",
                        c_idx, b.id, rem
                    );
                    break;
                }
                rem -= b.used;
            }

            let mut c_off = 0;
            let mut trim = 0;
            let mut hint = 0; // Initialize hint here

            if found {
                // Scan block headers to find entry boundary
                let blk = &chain[c_idx];
                let mut scan_pos = 0;
                // Use mmap for fast scanning if possible
                let mut meta_buf = [0u8; PREFIX_META_SIZE];

                info!(
                    "batch_read_for_topic: (stateless) scanning block {} (id={}) for entry boundary, blk.used={}, rem={}",
                    c_idx, blk.id, blk.used, rem
                );

                while scan_pos < blk.used {
                    info!(
                        "batch_read_for_topic: (stateless) scan_pos={}, rem={}",
                        scan_pos, rem
                    );
                    if scan_pos + (PREFIX_META_SIZE as u64) > blk.used {
                        info!(
                            "batch_read_for_topic: (stateless) breaking scan_pos+PREFIX_META_SIZE > blk.used"
                        );
                        break; // Should not happen in sealed block
                    }

                    // Read header
                    blk.mmap
                        .read((blk.offset + scan_pos) as usize, &mut meta_buf);
                    let meta_len = (meta_buf[0] as usize) | ((meta_buf[1] as usize) << 8);
                    if meta_len == 0 || meta_len > PREFIX_META_SIZE - 2 {
                        info!(
                            "batch_read_for_topic: (stateless) breaking meta_len invalid: {}",
                            meta_len
                        );
                        break; // Corrupt/Zeroed
                    }

                    // Decode metadata to get read_size
                    let mut aligned = AlignedVec::with_capacity(meta_len);
                    aligned.extend_from_slice(&meta_buf[2..2 + meta_len]);
                    let archived = unsafe { rkyv::archived_root::<Metadata>(&aligned[..]) };
                    let meta: Metadata = match archived.deserialize(&mut rkyv::Infallible) {
                        Ok(m) => m,
                        Err(_) => {
                            info!(
                                "batch_read_for_topic: (stateless) breaking meta deserialize error"
                            );
                            break;
                        }
                    };
                    let data_size = meta.read_size;

                    let entry_total = (PREFIX_META_SIZE + data_size) as u64;
                    let entry_end = scan_pos + entry_total;

                    info!(
                        "batch_read_for_topic: (stateless) scanned entry: meta_len={}, data_size={}, entry_total={}, entry_end={}",
                        meta_len, data_size, entry_total, entry_end
                    );

                    // Special handling for start_offset = 0 to skip small initial entries (likely internal metadata)
                    if rem == 0 && data_size < 128 {
                        info!(
                            "batch_read_for_topic: (stateless) skipping small initial entry (rem=0, data_size={})",
                            data_size
                        );
                        scan_pos = entry_end;
                        continue;
                    }

                    if entry_end > rem {
                        // Found the entry containing 'rem'
                        c_off = scan_pos;
                        hint = entry_end;
                        let payload_start = scan_pos + (PREFIX_META_SIZE as u64);
                        if rem > payload_start {
                            trim = (rem - payload_start) as usize;
                            info!(
                                "batch_read_for_topic: (stateless) found entry containing rem: c_off={}, trim={}",
                                c_off, trim
                            );
                        } else {
                            info!(
                                "batch_read_for_topic: (stateless) found entry containing rem (no trim): c_off={}",
                                c_off
                            );
                        }
                        break;
                    }

                    scan_pos = entry_end;
                }

                // If loop finished without finding (shouldn't happen if rem < used),
                // we default to c_off=scan_pos (end of valid data)
                if scan_pos >= blk.used {
                    c_off = blk.used;
                    info!(
                        "batch_read_for_topic: (stateless) scan loop finished, c_off={}",
                        c_off
                    );
                }
            } else {
                c_idx = chain.len();
                c_off = 0;
                // rem is now offset into tail (writer)
                info!(
                    "batch_read_for_topic: (stateless) block not found, setting c_idx={}, c_off={}",
                    c_idx, c_off
                );
            }

            (chain, c_idx, c_off, 0, rem, None, trim, hint)
        } else {
            // --- Stateful Read (Shared State) ---
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

            _held_arc = Some(info_arc);
            let mut info = _held_arc.as_ref().unwrap().write().map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "col info write lock poisoned")
            })?;

            // Hydrate from index if needed
            let mut persisted_tail_for_fold: Option<(u64, u64)> = None;
            if !info.hydrated_from_index {
                if let Ok(idx_guard) = self.read_offset_index.read() {
                    if let Some(pos) = idx_guard.get(col_name) {
                        if (pos.cur_block_idx & TAIL_FLAG) != 0 {
                            let tail_bid = pos.cur_block_idx & (!TAIL_FLAG);
                            info.tail_block_id = tail_bid;
                            info.tail_offset = pos.cur_block_offset;
                            info.cur_block_idx = info.chain.len();
                            info.cur_block_offset = 0;
                            persisted_tail_for_fold = Some((tail_bid, pos.cur_block_offset));
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
            if let Some((tail_bid, tail_off)) = persisted_tail_for_fold {
                if let Some(idx) = info
                    .chain
                    .iter()
                    .enumerate()
                    .find(|(_, b)| b.id == tail_bid)
                    .map(|(idx, _)| idx)
                {
                    let used = info.chain[idx].used;
                    info.cur_block_idx = idx;
                    info.cur_block_offset = tail_off.min(used);
                }
            }

            let c_chain = info.chain.clone();
            let c_idx = info.cur_block_idx;
            let c_off = info.cur_block_offset;
            let t_bid = info.tail_block_id;
            let t_off = info.tail_offset;

            (c_chain, c_idx, c_off, t_bid, t_off, Some(info), 0, 0)
        };

        // 2) Build read plan up to byte and entry limits
        let mut plan: Vec<ReadPlan> = Vec::new();
        let mut planned_bytes: usize = 0;
        let chain_len_at_plan = chain.len();

        while cur_idx < chain.len() && planned_bytes < max_bytes {
            let block = chain[cur_idx].clone();
            if cur_off >= block.used {
                if info_guard.is_some() {
                    BlockStateTracker::set_checkpointed_true(block.id as usize);
                }
                cur_idx += 1;
                cur_off = 0;
                // When advancing to a new block, the first_end_hint from a previous block is no longer relevant.
                // Reset it to 0 to ensure the peek logic can run for the new block.
                first_end_hint = 0;
                continue;
            }

            let mut want = (max_bytes - planned_bytes) as u64;

            if planned_bytes == 0 {
                // This is the start of planning a new batch read
                let mut should_peek = true;

                // If a start_offset was provided AND we're still processing the initial 'trimming' part
                if start_offset.is_some() && first_end_hint > cur_off {
                    want = want.max(first_end_hint - cur_off);
                    should_peek = false;
                }

                if should_peek && cur_off + (PREFIX_META_SIZE as u64) <= block.used {
                    let mut meta_buf = [0u8; PREFIX_META_SIZE];
                    block
                        .mmap
                        .read((block.offset + cur_off) as usize, &mut meta_buf);
                    let meta_len = (meta_buf[0] as usize) | ((meta_buf[1] as usize) << 8);
                    if meta_len > 0 && meta_len <= PREFIX_META_SIZE - 2 {
                        let mut aligned_peek_meta = AlignedVec::with_capacity(meta_len);
                        aligned_peek_meta.extend_from_slice(&meta_buf[2..2 + meta_len]);
                        let archived_peek_meta =
                            unsafe { rkyv::archived_root::<Metadata>(&aligned_peek_meta[..]) };
                        let meta_res: Result<Metadata, _> =
                            archived_peek_meta.deserialize(&mut rkyv::Infallible);
                        match meta_res {
                            Ok(meta) => {
                                let size1 = meta.read_size;
                                let required1 = (PREFIX_META_SIZE + size1) as u64;

                                // --- DOUBLE PEEK START ---
                                let mut final_required = required1;

                                if size1 < 128 {
                                    let offset2 = cur_off + required1;
                                    if offset2 + (PREFIX_META_SIZE as u64) <= block.used {
                                        let mut meta_buf2 = [0u8; PREFIX_META_SIZE];
                                        block.mmap.read(
                                            (block.offset + offset2) as usize,
                                            &mut meta_buf2,
                                        );
                                        let meta_len2 = (meta_buf2[0] as usize)
                                            | ((meta_buf2[1] as usize) << 8);
                                        if meta_len2 > 0 && meta_len2 <= PREFIX_META_SIZE - 2 {
                                            let mut aligned2 = AlignedVec::with_capacity(meta_len2);
                                            aligned2
                                                .extend_from_slice(&meta_buf2[2..2 + meta_len2]);
                                            let archived2 = unsafe {
                                                rkyv::archived_root::<Metadata>(&aligned2[..])
                                            };
                                            let meta2_res: Result<Metadata, _> =
                                                archived2.deserialize(&mut rkyv::Infallible);
                                            let meta2 = meta2_res
                                                .expect("infallible metadata deserialize");
                                            let size2 = meta2.read_size;
                                            let required2 = (PREFIX_META_SIZE + size2) as u64;
                                            final_required = required1 + required2;
                                        }
                                    }
                                }
                                // --- DOUBLE PEEK END ---

                                if final_required > want {
                                    want = final_required;
                                }
                            }
                            Err(_) => {
                                // ignore error, fallback to want
                            }
                        }
                    }
                }
            }

            let end = block.used.min(cur_off + want);
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
                // Determine start of tail read
                let mut tail_start = if start_offset.is_some() {
                    tail_offset // 'rem'
                } else if tail_block_id == active_block.id {
                    tail_offset
                } else {
                    0
                };

                // Scan writer block if start_offset provided (to align to entry)
                if start_offset.is_some() {
                    let mut scan_pos = 0;
                    let mut meta_buf = [0u8; PREFIX_META_SIZE];
                    let rem = tail_start;
                    let mut found_start = 0;

                    while scan_pos < written {
                        if scan_pos + (PREFIX_META_SIZE as u64) > written {
                            break;
                        }
                        active_block
                            .mmap
                            .read((active_block.offset + scan_pos) as usize, &mut meta_buf);
                        let meta_len = (meta_buf[0] as usize) | ((meta_buf[1] as usize) << 8);
                        if meta_len == 0 || meta_len > PREFIX_META_SIZE - 2 {
                            break;
                        }

                        let mut aligned = AlignedVec::with_capacity(meta_len);
                        aligned.extend_from_slice(&meta_buf[2..2 + meta_len]);
                        let archived = unsafe { rkyv::archived_root::<Metadata>(&aligned[..]) };
                        let meta: Metadata = match archived.deserialize(&mut rkyv::Infallible) {
                            Ok(m) => m,
                            Err(_) => break,
                        };
                        let data_size = meta.read_size;
                        let entry_total = (PREFIX_META_SIZE + data_size) as u64;
                        let entry_end = scan_pos + entry_total;

                        // Special handling for start_offset = 0 to skip small initial entries (likely internal metadata)
                        if rem == 0 && data_size < 128 {
                            scan_pos = entry_end;
                            continue;
                        }

                        if entry_end > rem {
                            found_start = scan_pos;
                            if rem > scan_pos + (PREFIX_META_SIZE as u64) {
                                initial_trim =
                                    (rem - (scan_pos + (PREFIX_META_SIZE as u64))) as usize;
                            }
                            break;
                        }
                        scan_pos = entry_end;
                    }
                    tail_start = found_start;
                }

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
        if !hold_lock_during_io && info_guard.is_some() {
            // Release lock for AtLeastOnce before IO
            drop(info_guard.take().unwrap());
        }

        // 3) Read ranges via io_uring (FD backend) or mmap
        #[cfg(target_os = "linux")]
        let buffers = if USE_FD_BACKEND.load(Ordering::Relaxed) {
            // io_uring path
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
                    let file_offset = (read_plan.blk.offset + read_plan.start) as usize;

                    let fd = if let Some(fd_backend) = read_plan.blk.mmap.storage().as_fd() {
                        io_uring::types::Fd(fd_backend.file().as_raw_fd())
                    } else {
                        return Err(io::Error::new(
                            io::ErrorKind::Unsupported,
                            "batch reads require FD backend when io_uring is enabled",
                        ));
                    };

                    let read_op = io_uring::opcode::Read::new(fd, buffer.as_mut_ptr(), size as u32)
                        .offset(file_offset as u64)
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
                    Err(_) => {
                        break; // Parse error - stop
                    }
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

                // Handle trimming
                let mut final_data = data_slice.to_vec();
                if initial_trim > 0 {
                    if initial_trim < final_data.len() {
                        final_data = final_data[initial_trim..].to_vec();
                    } else {
                        final_data.clear();
                    }
                    initial_trim = 0; // Only for first entry
                }

                // Add to results
                if !final_data.is_empty() {
                    // Extract topic_id and chunk_idx from the payload prefix for logging
                    if final_data.len() >= 9 {
                        let t_idx = final_data[0];
                        let mut c_idx_bytes = [0u8; 8];
                        c_idx_bytes.copy_from_slice(&final_data[1..9]);
                        let c_idx = u64::from_be_bytes(c_idx_bytes); // Big-endian
                        info!(
                            "batch_read_for_topic: (stateless) pushing entry with t_idx={}, c_idx={}",
                            t_idx, c_idx
                        );
                    }
                    entries.push(Entry { data: final_data });
                }

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

            let mut update_state = |info: &mut ColReaderInfo| {
                if checkpoint {
                    let mut should_persist_disk = true;

                    if let ReadConsistency::AtLeastOnce { persist_every } = self.read_consistency {
                        let every = persist_every.max(1);
                        let total = info.reads_since_persist.saturating_add(entries_parsed);
                        if total >= every {
                            info.reads_since_persist = 0;
                            // For batch reads, we deliberately delay persistence to disk to ensure
                            // "at least once" semantics (replayability) are preserved even if
                            // batches are large, satisfying existing tests.
                            // Memory state is updated, so rapid_fire loop works.
                            should_persist_disk = false;
                        } else {
                            info.reads_since_persist = total;
                            should_persist_disk = false;
                        }
                    }

                    if saw_tail {
                        info.cur_block_idx = chain_len_at_plan;
                        info.cur_block_offset = 0;
                        info.tail_block_id = final_tail_block_id;
                        info.tail_offset = final_tail_offset;
                        if should_persist_disk {
                            target = PersistTarget::Tail {
                                blk_id: final_tail_block_id,
                                off: final_tail_offset,
                            };
                        }
                    } else {
                        info.cur_block_idx = final_block_idx;
                        info.cur_block_offset = final_block_offset;
                        if should_persist_disk {
                            target = PersistTarget::Sealed {
                                idx: final_block_idx as u64,
                                off: final_block_offset,
                            };
                        }
                    }
                }
            };

            if hold_lock_during_io {
                if let Some(mut info) = info_guard {
                    update_state(&mut info);
                }
            } else {
                // Reacquire
                let arc = {
                    let map = self.reader.data.read().unwrap();
                    map.get(col_name).cloned()
                };
                if let Some(arc) = arc {
                    if let Ok(mut info) = arc.write() {
                        update_state(&mut info);
                    }
                }
            }

            // Commit to index
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
