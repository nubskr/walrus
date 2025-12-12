use crate::wal::block::{Block, Metadata};
use crate::wal::config::{
    DEFAULT_BLOCK_SIZE, FsyncSchedule, MAX_FILE_SIZE, PREFIX_META_SIZE, debug_print,
};
use crate::wal::paths::WalPathManager;
use crate::wal::storage::{SharedMmapKeeper, set_fsync_schedule};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::sync::mpsc;
use std::sync::{Arc, RwLock};

use super::WalIndex;
use super::allocator::{BlockAllocator, BlockStateTracker, FileStateTracker, flush_check};
use super::background::start_background_workers;
use super::reader::Reader;
use super::topic_clean::{CleanMarkerStore, TopicCleanTracker};
use super::writer::Writer;
use rkyv::Deserialize;

#[derive(Clone, Copy, Debug)]
pub enum ReadConsistency {
    StrictlyAtOnce,
    AtLeastOnce { persist_every: u32 },
}

pub struct Walrus {
    pub(super) allocator: Arc<BlockAllocator>,
    pub(super) reader: Arc<Reader>,
    pub(super) writers: RwLock<HashMap<String, Arc<Writer>>>,
    pub(super) fsync_tx: Arc<mpsc::Sender<String>>,
    pub(super) read_offset_index: Arc<RwLock<WalIndex>>,
    pub(super) read_consistency: ReadConsistency,
    pub(super) fsync_schedule: FsyncSchedule,
    pub(super) paths: Arc<WalPathManager>,
    topic_clean_tracker: Arc<TopicCleanTracker>,
    topic_entry_counts: RwLock<HashMap<String, u64>>,
}

impl Walrus {
    pub fn new() -> std::io::Result<Self> {
        Self::with_consistency(ReadConsistency::StrictlyAtOnce)
    }

    pub fn with_consistency(mode: ReadConsistency) -> std::io::Result<Self> {
        Self::with_consistency_and_schedule(mode, FsyncSchedule::Milliseconds(200))
    }

    pub fn with_consistency_and_schedule(
        mode: ReadConsistency,
        fsync_schedule: FsyncSchedule,
    ) -> std::io::Result<Self> {
        let paths = Arc::new(WalPathManager::default());
        Self::with_paths(paths, mode, fsync_schedule)
    }

    pub fn new_for_key(key: &str) -> std::io::Result<Self> {
        Self::with_consistency_for_key(key, ReadConsistency::StrictlyAtOnce)
    }

    pub fn with_consistency_for_key(key: &str, mode: ReadConsistency) -> std::io::Result<Self> {
        Self::with_consistency_and_schedule_for_key(key, mode, FsyncSchedule::Milliseconds(200))
    }

    pub fn with_consistency_and_schedule_for_key(
        key: &str,
        mode: ReadConsistency,
        fsync_schedule: FsyncSchedule,
    ) -> std::io::Result<Self> {
        let paths = WalPathManager::for_key(key);
        Self::with_paths(Arc::new(paths), mode, fsync_schedule)
    }

    fn with_paths(
        paths: Arc<WalPathManager>,
        mode: ReadConsistency,
        fsync_schedule: FsyncSchedule,
    ) -> std::io::Result<Self> {
        debug_print!("[walrus] new");

        // Store the fsync schedule globally for SharedMmap::new to access
        set_fsync_schedule(fsync_schedule);

        let allocator = Arc::new(BlockAllocator::new(paths.clone())?);
        let reader = Arc::new(Reader::new());
        let tx_arc = start_background_workers(fsync_schedule);
        let clean_store = Arc::new(CleanMarkerStore::new_in(&paths, "topic_clean")?);
        let topic_clean_tracker = TopicCleanTracker::new(clean_store.clone());
        topic_clean_tracker.hydrate(clean_store.snapshot());

        let idx = WalIndex::new_in(&paths, "read_offset_idx")?;
        let instance = Walrus {
            allocator,
            reader,
            writers: RwLock::new(HashMap::new()),
            fsync_tx: tx_arc,
            read_offset_index: Arc::new(RwLock::new(idx)),
            read_consistency: mode,
            fsync_schedule,
            paths,
            topic_clean_tracker,
            topic_entry_counts: RwLock::new(HashMap::new()),
        };
        instance.startup_chore()?;
        Ok(instance)
    }

    pub fn mark_topic_dirty(&self, topic: &str) {
        self.topic_clean_tracker.mark_dirty(topic);
    }

    pub fn mark_topic_clean(&self, topic: &str) {
        self.topic_clean_tracker.mark_clean(topic);
    }

    pub fn topic_is_clean(&self, topic: &str) -> bool {
        self.topic_clean_tracker.topic_is_clean(topic)
    }

    pub fn get_topic_entry_count(&self, topic: &str) -> u64 {
        self.topic_entry_counts
            .read()
            .ok()
            .and_then(|m| m.get(topic).copied())
            .unwrap_or(0)
    }

    pub fn get_topic_entry_counts(&self) -> HashMap<String, u64> {
        self.topic_entry_counts
            .read()
            .map(|m| m.clone())
            .unwrap_or_default()
    }

    pub fn get_topic_size(&self, topic: &str) -> u64 {
        // 1. Get sealed size from reader
        let sealed_size: u64 = if let Some(info_arc) = self
            .reader
            .data
            .read()
            .ok()
            .and_then(|m| m.get(topic).cloned())
        {
            if let Ok(info) = info_arc.read() {
                info.chain.iter().map(|b| b.used).sum()
            } else {
                0
            }
        } else {
            0
        };

        // 2. Get active size from writer
        let active_size: u64 =
            if let Some(writer) = self.writers.read().ok().and_then(|m| m.get(topic).cloned()) {
                if let Ok((_, offset)) = writer.snapshot_block() {
                    offset
                } else {
                    debug_print!("[size_debug] writer snapshot failed for {}", topic);
                    0
                }
            } else {
                debug_print!("[size_debug] writer not found for {}", topic);
                0
            };

        // debug_print!("[size_debug] {} sealed={} active={}", topic, sealed_size, active_size);
        sealed_size + active_size
    }

    #[cfg(test)]
    pub(crate) fn force_flush_clean_markers_for_test(&self) -> std::io::Result<()> {
        self.topic_clean_tracker.force_flush_for_test()
    }

    pub(super) fn increment_topic_entry_count(&self, topic: &str, delta: u64) {
        if delta == 0 {
            return;
        }
        if let Ok(mut guard) = self.topic_entry_counts.write() {
            let entry = guard.entry(topic.to_string()).or_insert(0);
            *entry = entry.saturating_add(delta);
        }
    }

    pub(super) fn decrement_topic_entry_count(&self, topic: &str, delta: u64) {
        if delta == 0 {
            return;
        }
        if let Ok(mut guard) = self.topic_entry_counts.write() {
            let entry = guard.entry(topic.to_string()).or_insert(0);
            *entry = entry.saturating_sub(delta);
        }
    }

    pub(super) fn get_or_create_writer(&self, col_name: &str) -> std::io::Result<Arc<Writer>> {
        if let Some(writer) = {
            let map = self.writers.read().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "writers read lock poisoned")
            })?;
            map.get(col_name).cloned()
        } {
            return Ok(writer);
        }

        debug_print!("[writer_debug] creating new writer for {}", col_name);

        let mut map = self.writers.write().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::Other, "writers write lock poisoned")
        })?;

        if let Some(writer) = map.get(col_name).cloned() {
            return Ok(writer);
        }

        // SAFETY: The returned block will be held by this writer only
        // and appended/sealed before being exposed to readers.
        let initial_block = unsafe { self.allocator.get_next_available_block()? };
        let writer = Arc::new(Writer::new(
            self.allocator.clone(),
            initial_block,
            self.reader.clone(),
            col_name.to_string(),
            self.fsync_tx.clone(),
            self.fsync_schedule,
        ));
        map.insert(col_name.to_string(), writer.clone());
        Ok(writer)
    }

    pub(super) fn startup_chore(&self) -> std::io::Result<()> {
        // Minimal recovery: scan wal data dir, build reader chains, and rebuild trackers
        let dir = match fs::read_dir(self.paths.root()) {
            Ok(d) => d,
            Err(_) => return Ok(()),
        };
        let mut files: Vec<String> = Vec::new();
        for entry in dir {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };
            let path = entry.path();
            if let Ok(ft) = entry.file_type() {
                if ft.is_dir() {
                    continue;
                }
            }
            if let Some(s) = path.to_str() {
                // skip index files
                if s.ends_with("_index.db") {
                    continue;
                }
                files.push(s.to_string());
            }
        }
        files.sort();
        if !files.is_empty() {
            debug_print!("[recovery] scanning files: {}", files.len());
        }

        // synthetic block ids btw
        let mut next_block_id: usize = 1;
        let mut seen_files = HashSet::new();
        let mut topic_block_entry_counts: HashMap<String, Vec<u64>> = HashMap::new();

        for file_path in files.iter() {
            let mmap = match SharedMmapKeeper::get_mmap_arc(file_path) {
                Ok(m) => m,
                Err(e) => {
                    debug_print!("[recovery] mmap open failed for {}: {}", file_path, e);
                    continue;
                }
            };
            seen_files.insert(file_path.clone());
            FileStateTracker::register_file_if_absent(file_path);
            debug_print!("[recovery] file {}", file_path);

            let mut block_offset: u64 = 0;
            while block_offset + DEFAULT_BLOCK_SIZE <= MAX_FILE_SIZE {
                // heuristic: if first bytes are zero, assume no more blocks
                let mut probe = [0u8; 8];
                mmap.read(block_offset as usize, &mut probe);
                if probe.iter().all(|&b| b == 0) {
                    break;
                }

                let mut used: u64 = 0;
                let mut entries_in_block: u64 = 0;

                // try to read first metadata to get column name (with 2-byte length prefix)
                let mut meta_buf = vec![0u8; PREFIX_META_SIZE];
                mmap.read(block_offset as usize, &mut meta_buf);
                let meta_len = (meta_buf[0] as usize) | ((meta_buf[1] as usize) << 8);
                if meta_len == 0 || meta_len > PREFIX_META_SIZE - 2 {
                    block_offset += DEFAULT_BLOCK_SIZE;
                    next_block_id += 1;
                    continue;
                }
                let mut aligned = rkyv::AlignedVec::with_capacity(meta_len);
                aligned.extend_from_slice(&meta_buf[2..2 + meta_len]);
                // SAFETY: `aligned` was constructed from a bounded metadata slice
                // read from our file; alignment is ensured by `AlignedVec`.
                // SAFETY: `aligned` is built from bounded bytes inside the block,
                // copied into `AlignedVec` ensuring alignment for rkyv.
                let archived = unsafe { rkyv::archived_root::<Metadata>(&aligned[..]) };
                let md: Metadata = match archived.deserialize(&mut rkyv::Infallible) {
                    Ok(m) => m,
                    Err(_) => {
                        break;
                    }
                };
                let col_name = md.owned_by;

                // scan entries to compute used
                let block_stub = Block {
                    id: next_block_id as u64,
                    file_path: file_path.clone(),
                    offset: block_offset,
                    limit: DEFAULT_BLOCK_SIZE,
                    mmap: mmap.clone(),
                    used: 0,
                };
                let mut in_block_off: u64 = 0;
                loop {
                    match block_stub.read(in_block_off) {
                        Ok((_entry, consumed)) => {
                            used += consumed as u64;
                            in_block_off += consumed as u64;
                            entries_in_block = entries_in_block.saturating_add(1);
                            if in_block_off >= DEFAULT_BLOCK_SIZE {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                if used == 0 {
                    break;
                }

                let block = Block {
                    id: next_block_id as u64,
                    file_path: file_path.clone(),
                    offset: block_offset,
                    limit: DEFAULT_BLOCK_SIZE,
                    mmap: mmap.clone(),
                    used,
                };
                // register and append
                BlockStateTracker::register_block(next_block_id, file_path);
                FileStateTracker::add_block_to_file_state(file_path);
                if !col_name.is_empty() {
                    let _ = self.reader.append_block_to_chain(&col_name, block.clone());
                    topic_block_entry_counts
                        .entry(col_name.clone())
                        .or_default()
                        .push(entries_in_block);
                    debug_print!(
                        "[recovery] appended block: file={}, block_id={}, used={}, col={}",
                        file_path,
                        block.id,
                        block.used,
                        col_name
                    );
                }
                next_block_id += 1;
                block_offset += DEFAULT_BLOCK_SIZE;
            }
        }

        self.rebuild_topic_entry_counts_after_recovery(&topic_block_entry_counts)?;

        // hydrate index into memory and mark checkpointed blocks
        if let Ok(idx_guard) = self.read_offset_index.read() {
            let map = self.reader.data.read().ok();
            if let Some(map) = map {
                for (col, info_arc) in map.iter() {
                    if let Some(pos) = idx_guard.get(col) {
                        let mut info = match info_arc.write() {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
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
                        for i in 0..ib {
                            BlockStateTracker::set_checkpointed_true(info.chain[i].id as usize);
                        }
                        if ib < info.chain.len() && info.cur_block_offset >= info.chain[ib].used {
                            BlockStateTracker::set_checkpointed_true(info.chain[ib].id as usize);
                        }
                    }
                }
            }
        }

        // enqueue deletion checks
        for f in seen_files.into_iter() {
            flush_check(f);
        }

        unsafe {
            self.allocator.fast_forward(next_block_id as u64);
        }

        Ok(())
    }
}

impl Walrus {
    fn rebuild_topic_entry_counts_after_recovery(
        &self,
        topic_block_entry_counts: &HashMap<String, Vec<u64>>,
    ) -> std::io::Result<()> {
        const TAIL_FLAG: u64 = 1u64 << 63;

        fn count_entries_in_block_up_to(block: &Block, limit: u64) -> u64 {
            let limit = limit.min(block.used);
            let mut off: u64 = 0;
            let mut count: u64 = 0;
            while off < limit {
                match block.read(off) {
                    Ok((_entry, consumed)) => {
                        let next = off.saturating_add(consumed as u64);
                        if next > limit {
                            break;
                        }
                        count = count.saturating_add(1);
                        off = next;
                    }
                    Err(_) => break,
                }
            }
            count
        }

        let mut counts: HashMap<String, u64> = HashMap::new();

        let idx_guard = self.read_offset_index.read().ok();
        let reader_guard = self.reader.data.read().ok();

        if let Some(reader_guard) = reader_guard {
            for (topic, info_arc) in reader_guard.iter() {
                let Ok(info) = info_arc.read() else {
                    continue;
                };

                let total_entries: u64 = topic_block_entry_counts
                    .get(topic)
                    .map(|v| v.iter().copied().sum())
                    .unwrap_or(0);

                let mut consumed_entries: u64 = 0;

                if let Some(idx_guard) = idx_guard.as_ref() {
                    if let Some(pos) = idx_guard.get(topic) {
                        if (pos.cur_block_idx & TAIL_FLAG) != 0 {
                            let tail_block_id = pos.cur_block_idx & (!TAIL_FLAG);
                            if let Some(tail_idx) = info
                                .chain
                                .iter()
                                .position(|b| b.id == tail_block_id)
                            {
                                // full blocks before tail_idx
                                if let Some(per_block) = topic_block_entry_counts.get(topic) {
                                    consumed_entries = consumed_entries.saturating_add(
                                        per_block
                                            .iter()
                                            .take(tail_idx)
                                            .copied()
                                            .sum::<u64>(),
                                    );
                                }
                                // partial within tail block
                                if let Some(block) = info.chain.get(tail_idx) {
                                    if pos.cur_block_offset >= block.used {
                                        if let Some(per_block) = topic_block_entry_counts.get(topic)
                                        {
                                            consumed_entries = consumed_entries.saturating_add(
                                                per_block.get(tail_idx).copied().unwrap_or(0),
                                            );
                                        }
                                    } else {
                                        consumed_entries = consumed_entries.saturating_add(
                                            count_entries_in_block_up_to(
                                                block,
                                                pos.cur_block_offset,
                                            ),
                                        );
                                    }
                                }
                            }
                        } else {
                            let block_idx = (pos.cur_block_idx as usize).min(info.chain.len());

                            if let Some(per_block) = topic_block_entry_counts.get(topic) {
                                consumed_entries = consumed_entries.saturating_add(
                                    per_block
                                        .iter()
                                        .take(block_idx)
                                        .copied()
                                        .sum::<u64>(),
                                );
                            }

                            if let Some(block) = info.chain.get(block_idx) {
                                if pos.cur_block_offset >= block.used {
                                    if let Some(per_block) = topic_block_entry_counts.get(topic) {
                                        consumed_entries = consumed_entries.saturating_add(
                                            per_block.get(block_idx).copied().unwrap_or(0),
                                        );
                                    }
                                } else {
                                    consumed_entries = consumed_entries.saturating_add(
                                        count_entries_in_block_up_to(block, pos.cur_block_offset),
                                    );
                                }
                            }
                        }
                    }
                }

                counts.insert(topic.to_string(), total_entries.saturating_sub(consumed_entries));
            }
        }

        if let Ok(mut guard) = self.topic_entry_counts.write() {
            *guard = counts;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::random;
    use std::fs;
    use std::path::Path;

    fn unique_key() -> String {
        format!("topic_clean_test_{}", random::<u64>())
    }

    fn cleanup_key(key: &str) {
        let path = Path::new("wal_files").join(key);
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn append_marks_topic_dirty() {
        let key = unique_key();
        let wal = Walrus::with_consistency_and_schedule_for_key(
            &key,
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::Milliseconds(5),
        )
        .unwrap();

        assert!(wal.topic_is_clean("alpha"));
        wal.append_for_topic("alpha", b"hello world").unwrap();
        assert!(!wal.topic_is_clean("alpha"));

        wal.mark_topic_clean("alpha");
        wal.force_flush_clean_markers_for_test().unwrap();
        assert!(wal.topic_is_clean("alpha"));

        drop(wal);
        cleanup_key(&key);
    }

    #[test]
    fn cleanliness_survives_restart() {
        let key = unique_key();

        {
            let wal = Walrus::with_consistency_and_schedule_for_key(
                &key,
                ReadConsistency::StrictlyAtOnce,
                FsyncSchedule::Milliseconds(5),
            )
            .unwrap();
            wal.append_for_topic("beta", b"bytes").unwrap();
            wal.force_flush_clean_markers_for_test().unwrap();
            assert!(!wal.topic_is_clean("beta"));
        }

        {
            let wal = Walrus::with_consistency_and_schedule_for_key(
                &key,
                ReadConsistency::StrictlyAtOnce,
                FsyncSchedule::Milliseconds(5),
            )
            .unwrap();
            assert!(!wal.topic_is_clean("beta"));
            wal.mark_topic_clean("beta");
            wal.force_flush_clean_markers_for_test().unwrap();
            assert!(wal.topic_is_clean("beta"));
        }

        {
            let wal = Walrus::with_consistency_and_schedule_for_key(
                &key,
                ReadConsistency::StrictlyAtOnce,
                FsyncSchedule::Milliseconds(5),
            )
            .unwrap();
            assert!(wal.topic_is_clean("beta"));
        }

        cleanup_key(&key);
    }

    #[test]
    fn test_batch_read_scanning() {
        let key = unique_key();
        crate::wal::config::enable_fd_backend();

        let wal = Walrus::with_consistency_for_key(&key, ReadConsistency::StrictlyAtOnce).unwrap();

        // 1. Write a sequence of entries
        let count = 100;
        for i in 0..count {
            let payload = format!("entry-{}", i);
            wal.append_for_topic("scan_col", payload.as_bytes())
                .unwrap();
        }

        // 2. Read them back sequentially without checkpointing (cursor stays at 0)
        // This simulates the `bucket.rs` behavior of reading from offset X by
        // reading everything from 0 and skipping X bytes.
        let mut offset = 0;
        let mut total_read_entries = 0;

        loop {
            // Read a batch from the *current cursor* (which is 0 because checkpoint=false)
            // We ask for enough bytes to cover our offset + some data
            let entries = wal
                .batch_read_for_topic("scan_col", offset + 1024, false, None)
                .unwrap();
            if entries.is_empty() {
                break;
            }

            // Simulate "skipping" bytes to find the entry at `offset`
            let mut current_pos = 0;
            let mut found_new = false;

            for entry in entries {
                let len = entry.data.len();
                if current_pos >= offset {
                    // This is a new entry we haven't "processed" yet
                    // Verify content
                    let payload = String::from_utf8(entry.data.clone()).unwrap();
                    let expected = format!("entry-{}", total_read_entries);
                    assert_eq!(
                        payload, expected,
                        "Data mismatch at entry index {}",
                        total_read_entries
                    );

                    total_read_entries += 1;
                    offset += len; // Advance our logical offset
                    found_new = true;
                    // In a real scenario we might stop here or consume more,
                    // but for this test let's read one-by-one to stress the loop
                    break;
                }
                current_pos += len;
            }

            if !found_new && total_read_entries < count {
                // If we didn't find new data but expect more, it means batch size wasn't large enough
                // or we are stuck. For this test, we increased batch size above so we should find it.
                // If we reach here, it might be an infinite loop.
                if total_read_entries == count {
                    break;
                }
                panic!(
                    "Stuck at offset {} with {} entries read",
                    offset, total_read_entries
                );
            }

            if total_read_entries == count {
                break;
            }
        }

        assert_eq!(total_read_entries, count);
        cleanup_key(&key);
    }

    #[test]
    fn entry_counts_decrement_on_checkpoint_and_survive_restart_strict() {
        let key = unique_key();
        crate::wal::config::enable_fd_backend();

        {
            let wal = Walrus::with_consistency_for_key(&key, ReadConsistency::StrictlyAtOnce)
                .unwrap();
            wal.append_for_topic("counts", b"one").unwrap();
            wal.append_for_topic("counts", b"two").unwrap();
            wal.batch_append_for_topic("counts", &[b"three", b"four"])
                .unwrap();
            assert_eq!(wal.get_topic_entry_count("counts"), 4);

            // checkpoint consumes
            let _ = wal.read_next("counts", true).unwrap().unwrap();
            assert_eq!(wal.get_topic_entry_count("counts"), 3);

            // peek does not consume
            let _ = wal.read_next("counts", false).unwrap().unwrap();
            assert_eq!(wal.get_topic_entry_count("counts"), 3);
        }

        {
            // Restart should rebuild counts from WAL + persisted read offsets.
            let wal = Walrus::with_consistency_for_key(&key, ReadConsistency::StrictlyAtOnce)
                .unwrap();
            assert_eq!(wal.get_topic_entry_count("counts"), 3);

            let entries = wal
                .batch_read_for_topic("counts", 16 * 1024, true, None)
                .unwrap();
            assert_eq!(entries.len(), 3);
            assert_eq!(wal.get_topic_entry_count("counts"), 0);
        }

        cleanup_key(&key);
    }

    #[test]
    fn entry_counts_increment_immediately_single_and_batch() {
        let key = unique_key();
        crate::wal::config::enable_fd_backend();

        let wal = Walrus::with_consistency_for_key(&key, ReadConsistency::StrictlyAtOnce).unwrap();

        assert_eq!(wal.get_topic_entry_count("a"), 0);
        wal.append_for_topic("a", b"1").unwrap();
        assert_eq!(wal.get_topic_entry_count("a"), 1);
        wal.append_for_topic("a", b"2").unwrap();
        assert_eq!(wal.get_topic_entry_count("a"), 2);

        wal.batch_append_for_topic("a", &[b"3", b"4", b"5"]).unwrap();
        assert_eq!(wal.get_topic_entry_count("a"), 5);

        cleanup_key(&key);
    }

    #[test]
    fn entry_counts_are_per_topic() {
        let key = unique_key();
        crate::wal::config::enable_fd_backend();

        let wal = Walrus::with_consistency_for_key(&key, ReadConsistency::StrictlyAtOnce).unwrap();
        wal.append_for_topic("t1", b"a").unwrap();
        wal.batch_append_for_topic("t2", &[b"a", b"b"]).unwrap();
        wal.append_for_topic("t1", b"b").unwrap();

        assert_eq!(wal.get_topic_entry_count("t1"), 2);
        assert_eq!(wal.get_topic_entry_count("t2"), 2);
        assert_eq!(wal.get_topic_entry_count("missing"), 0);

        cleanup_key(&key);
    }

    #[test]
    fn entry_counts_decrement_on_read_next_checkpoint_only() {
        let key = unique_key();
        crate::wal::config::enable_fd_backend();

        let wal = Walrus::with_consistency_for_key(&key, ReadConsistency::StrictlyAtOnce).unwrap();
        wal.batch_append_for_topic("t", &[b"a", b"b", b"c"]).unwrap();
        assert_eq!(wal.get_topic_entry_count("t"), 3);

        let _ = wal.read_next("t", false).unwrap().unwrap();
        assert_eq!(wal.get_topic_entry_count("t"), 3);

        let _ = wal.read_next("t", true).unwrap().unwrap();
        assert_eq!(wal.get_topic_entry_count("t"), 2);

        let _ = wal.read_next("t", true).unwrap().unwrap();
        assert_eq!(wal.get_topic_entry_count("t"), 1);

        let _ = wal.read_next("t", true).unwrap().unwrap();
        assert_eq!(wal.get_topic_entry_count("t"), 0);

        // No entry => no decrement below zero
        assert!(wal.read_next("t", true).unwrap().is_none());
        assert_eq!(wal.get_topic_entry_count("t"), 0);

        cleanup_key(&key);
    }

    #[test]
    fn entry_counts_decrement_on_batch_read_checkpoint_only() {
        let key = unique_key();
        crate::wal::config::enable_fd_backend();

        let wal = Walrus::with_consistency_for_key(&key, ReadConsistency::StrictlyAtOnce).unwrap();
        wal.batch_append_for_topic("t", &[b"a", b"b", b"c", b"d", b"e"])
            .unwrap();
        assert_eq!(wal.get_topic_entry_count("t"), 5);

        // checkpoint=false does not decrement
        let entries = wal.batch_read_for_topic("t", 16 * 1024, false, None).unwrap();
        assert_eq!(entries.len(), 5);
        assert_eq!(wal.get_topic_entry_count("t"), 5);

        // checkpoint=true decrements
        let entries = wal.batch_read_for_topic("t", 16 * 1024, true, None).unwrap();
        assert_eq!(entries.len(), 5);
        assert_eq!(wal.get_topic_entry_count("t"), 0);

        // empty read => no decrement below zero
        let entries = wal.batch_read_for_topic("t", 16 * 1024, true, None).unwrap();
        assert!(entries.is_empty());
        assert_eq!(wal.get_topic_entry_count("t"), 0);

        cleanup_key(&key);
    }

    #[test]
    fn entry_counts_survive_restart_after_partial_read_next_strict() {
        let key = unique_key();
        crate::wal::config::enable_fd_backend();

        {
            let wal =
                Walrus::with_consistency_for_key(&key, ReadConsistency::StrictlyAtOnce).unwrap();
            wal.batch_append_for_topic("t", &[b"a", b"b", b"c", b"d"])
                .unwrap();
            assert_eq!(wal.get_topic_entry_count("t"), 4);

            let _ = wal.read_next("t", true).unwrap().unwrap();
            let _ = wal.read_next("t", true).unwrap().unwrap();
            assert_eq!(wal.get_topic_entry_count("t"), 2);
        }

        {
            let wal =
                Walrus::with_consistency_for_key(&key, ReadConsistency::StrictlyAtOnce).unwrap();
            assert_eq!(wal.get_topic_entry_count("t"), 2);
        }

        cleanup_key(&key);
    }

    #[test]
    fn entry_counts_survive_restart_after_batch_read_strict() {
        let key = unique_key();
        crate::wal::config::enable_fd_backend();

        {
            let wal =
                Walrus::with_consistency_for_key(&key, ReadConsistency::StrictlyAtOnce).unwrap();
            wal.batch_append_for_topic("t", &[b"a", b"b", b"c"]).unwrap();
            assert_eq!(wal.get_topic_entry_count("t"), 3);

            let entries = wal.batch_read_for_topic("t", 16 * 1024, true, None).unwrap();
            assert_eq!(entries.len(), 3);
            assert_eq!(wal.get_topic_entry_count("t"), 0);
        }

        {
            let wal =
                Walrus::with_consistency_for_key(&key, ReadConsistency::StrictlyAtOnce).unwrap();
            assert_eq!(wal.get_topic_entry_count("t"), 0);
        }

        cleanup_key(&key);
    }

    #[test]
    fn entry_counts_survive_restart_with_multiple_topics_strict() {
        let key = unique_key();
        crate::wal::config::enable_fd_backend();

        {
            let wal =
                Walrus::with_consistency_for_key(&key, ReadConsistency::StrictlyAtOnce).unwrap();
            wal.batch_append_for_topic("t1", &[b"a", b"b"]).unwrap();
            wal.batch_append_for_topic("t2", &[b"a", b"b", b"c"]).unwrap();
            assert_eq!(wal.get_topic_entry_count("t1"), 2);
            assert_eq!(wal.get_topic_entry_count("t2"), 3);

            let _ = wal.read_next("t2", true).unwrap().unwrap();
            assert_eq!(wal.get_topic_entry_count("t2"), 2);
        }

        {
            let wal =
                Walrus::with_consistency_for_key(&key, ReadConsistency::StrictlyAtOnce).unwrap();
            assert_eq!(wal.get_topic_entry_count("t1"), 2);
            assert_eq!(wal.get_topic_entry_count("t2"), 2);
        }

        cleanup_key(&key);
    }

    #[test]
    fn batch_append_over_limit_does_not_change_count() {
        let key = unique_key();
        crate::wal::config::enable_fd_backend();

        let wal = Walrus::with_consistency_for_key(&key, ReadConsistency::StrictlyAtOnce).unwrap();
        wal.append_for_topic("t", b"seed").unwrap();
        assert_eq!(wal.get_topic_entry_count("t"), 1);

        let too_big: Vec<&[u8]> = std::iter::repeat(b"x".as_slice()).take(2001).collect();
        let err = wal.batch_append_for_topic("t", &too_big).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(wal.get_topic_entry_count("t"), 1);

        cleanup_key(&key);
    }

    #[test]
    fn stateless_batch_read_checkpoint_does_not_change_count() {
        let key = unique_key();
        crate::wal::config::enable_fd_backend();

        let wal = Walrus::with_consistency_for_key(&key, ReadConsistency::StrictlyAtOnce).unwrap();
        wal.batch_append_for_topic("t", &[b"a", b"b", b"c"]).unwrap();
        assert_eq!(wal.get_topic_entry_count("t"), 3);

        // Stateless read (start_offset=Some) should not advance the shared cursor,
        // so "checkpoint" must not affect the global count.
        let entries = wal
            .batch_read_for_topic("t", 16 * 1024, true, Some(0))
            .unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(wal.get_topic_entry_count("t"), 3);

        cleanup_key(&key);
    }

    #[test]
    fn entry_counts_are_accurate_in_process_at_least_once_mode() {
        let key = unique_key();
        crate::wal::config::enable_fd_backend();

        let wal = Walrus::with_consistency_for_key(
            &key,
            ReadConsistency::AtLeastOnce { persist_every: 10_000 },
        )
        .unwrap();

        wal.append_for_topic("t", b"a").unwrap();
        wal.batch_append_for_topic("t", &[b"b", b"c"]).unwrap();
        assert_eq!(wal.get_topic_entry_count("t"), 3);

        let _ = wal.read_next("t", true).unwrap().unwrap();
        assert_eq!(wal.get_topic_entry_count("t"), 2);

        let entries = wal.batch_read_for_topic("t", 16 * 1024, true, None).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(wal.get_topic_entry_count("t"), 0);

        cleanup_key(&key);
    }
}
