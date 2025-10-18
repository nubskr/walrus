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
        };
        instance.startup_chore()?;
        Ok(instance)
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

                // try to read first metadata to get column name (with 2-byte length prefix)
                let mut meta_buf = vec![0u8; PREFIX_META_SIZE];
                mmap.read(block_offset as usize, &mut meta_buf);
                let meta_len = (meta_buf[0] as usize) | ((meta_buf[1] as usize) << 8);
                if meta_len == 0 || meta_len > PREFIX_META_SIZE - 2 {
                    break;
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
        Ok(())
    }
}
