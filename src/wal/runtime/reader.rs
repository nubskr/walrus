use crate::wal::block::Block;
use crate::wal::config::debug_print;
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, RwLock};

#[derive(Debug)]
pub(super) struct ColReaderInfo {
    pub(super) chain: Vec<Block>,
    pub(super) cur_block_idx: usize,
    pub(super) cur_block_offset: u64,
    pub(super) reads_since_persist: u32,
    // In-memory progress for tail (active writer block). This allows AtLeastOnce
    // to advance between reads within a single process without persisting every time.
    pub(super) tail_block_id: u64,
    pub(super) tail_offset: u64,
    // Ensure we only hydrate from persisted index once per process per column
    pub(super) hydrated_from_index: bool,
}

pub(super) struct Reader {
    pub(super) data: RwLock<HashMap<String, Arc<RwLock<ColReaderInfo>>>>,
}

impl Reader {
    pub(super) fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }

    pub(super) fn append_block_to_chain(&self, col: &str, block: Block) -> io::Result<()> {
        // fast path: try read-lock map and use per-column lock
        if let Some(info_arc) = {
            let map = self.data.read().map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "reader map read lock poisoned")
            })?;
            map.get(col).cloned()
        } {
            let mut info = info_arc.write().map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "col info write lock poisoned")
            })?;
            let before = info.chain.len();
            info.chain.push(block.clone());
            // If we were reading this as the active tail, carry over progress to sealed chain
            let new_idx = info.chain.len().saturating_sub(1);
            if info.tail_block_id == block.id {
                info.cur_block_idx = new_idx;
                info.cur_block_offset = info.tail_offset.min(block.used);
            }
            debug_print!(
                "[reader] chain append(fast): col={}, block_id={}, chain_len {}->{}",
                col,
                block.id,
                before,
                before + 1
            );
            return Ok(());
        }

        // slow path
        let info_arc = {
            let mut map = self.data.write().map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "reader map write lock poisoned")
            })?;
            map.entry(col.to_string())
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
        info.chain.push(block.clone());
        // If we were reading this as the active tail, carry over progress to sealed chain
        let new_idx = info.chain.len().saturating_sub(1);
        if info.tail_block_id == block.id {
            info.cur_block_idx = new_idx;
            info.cur_block_offset = info.tail_offset.min(block.used);
        }
        debug_print!(
            "[reader] chain append(slow/new): col={}, block_id={}, chain_len {}->{}",
            col,
            block.id,
            0,
            1
        );
        Ok(())
    }
}
