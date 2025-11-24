use crate::wal::config::{PREFIX_META_SIZE, checksum64, debug_print};
use crate::wal::storage::SharedMmap;
use rkyv::Deserialize as _;
use rkyv_derive::{Archive, Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Entry {
    pub data: Vec<u8>,
}

#[derive(Archive, Deserialize, Serialize, Debug)]
#[archive(check_bytes)]
pub(crate) struct Metadata {
    pub(crate) read_size: usize,
    pub(crate) owned_by: String,
    pub(crate) next_block_start: u64,
    pub(crate) checksum: u64,
}

#[derive(Clone, Debug)]
pub struct Block {
    pub(crate) id: u64,
    pub(crate) file_path: String,
    pub(crate) offset: u64,
    pub(crate) limit: u64,
    pub(crate) mmap: Arc<SharedMmap>,
    pub(crate) used: u64,
}

impl Block {
    pub(crate) fn write(
        &self,
        in_block_offset: u64,
        data: &[u8],
        owned_by: &str,
        next_block_start: u64,
    ) -> std::io::Result<()> {
        debug_assert!(
            in_block_offset + (data.len() as u64 + PREFIX_META_SIZE as u64) <= self.limit
        );

        let new_meta = Metadata {
            read_size: data.len(),
            owned_by: owned_by.to_string(),
            next_block_start,
            checksum: checksum64(data),
        };

        let meta_bytes = rkyv::to_bytes::<_, 256>(&new_meta).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("serialize metadata failed: {:?}", e),
            )
        })?;
        if meta_bytes.len() > PREFIX_META_SIZE - 2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "metadata too large",
            ));
        }

        let mut meta_buffer = vec![0u8; PREFIX_META_SIZE];
        // Store actual length in first 2 bytes (little endian)
        meta_buffer[0] = (meta_bytes.len() & 0xFF) as u8;
        meta_buffer[1] = ((meta_bytes.len() >> 8) & 0xFF) as u8;
        // Copy actual metadata starting at byte 2
        meta_buffer[2..2 + meta_bytes.len()].copy_from_slice(&meta_bytes);

        // Combine and write
        let mut combined = Vec::with_capacity(PREFIX_META_SIZE + data.len());
        combined.extend_from_slice(&meta_buffer);
        combined.extend_from_slice(data);

        let file_offset = self.offset + in_block_offset;
        self.mmap.write(file_offset as usize, &combined);
        Ok(())
    }

    pub(crate) fn read(&self, in_block_offset: u64) -> std::io::Result<(Entry, usize)> {
        let mut meta_buffer = vec![0; PREFIX_META_SIZE];
        let file_offset = self.offset + in_block_offset;
        self.mmap.read(file_offset as usize, &mut meta_buffer);

        // Read the actual metadata length from first 2 bytes
        let meta_len = (meta_buffer[0] as usize) | ((meta_buffer[1] as usize) << 8);

        if meta_len == 0 || meta_len > PREFIX_META_SIZE - 2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid metadata length: {}", meta_len),
            ));
        }

        // Deserialize only the actual metadata bytes (skip the 2-byte length prefix)
        let mut aligned = rkyv::AlignedVec::with_capacity(meta_len);
        aligned.extend_from_slice(&meta_buffer[2..2 + meta_len]);

        // SAFETY: `aligned` contains bytes we just read from our own file format.
        // We bounded `meta_len` to PREFIX_META_SIZE and copy into an `AlignedVec`,
        // which satisfies alignment requirements of rkyv.
        let archived = unsafe { rkyv::archived_root::<Metadata>(&aligned[..]) };
        let meta: Metadata = archived.deserialize(&mut rkyv::Infallible).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "failed to deserialize metadata",
            )
        })?;
        let actual_entry_size = meta.read_size;

        // Read the actual data
        let new_offset = file_offset + PREFIX_META_SIZE as u64;
        let mut ret_buffer = vec![0; actual_entry_size];
        self.mmap.read(new_offset as usize, &mut ret_buffer);

        // Verify checksum
        let expected = meta.checksum;
        if checksum64(&ret_buffer) != expected {
            debug_print!(
                "[reader] checksum mismatch; skipping corrupted entry at offset={} in file={}, block_id={}",
                in_block_offset,
                self.file_path,
                self.id
            );
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "checksum mismatch, data corruption detected",
            ));
        }

        let consumed = PREFIX_META_SIZE + actual_entry_size;
        Ok((Entry { data: ret_buffer }, consumed))
    }

    pub(crate) fn zero_range(&self, in_block_offset: u64, size: u64) -> std::io::Result<()> {
        // Zero a small region within this block; used to invalidate headers on rollback
        // Caller ensures size is reasonable (typically PREFIX_META_SIZE)
        let len = size as usize;
        if len == 0 {
            return Ok(());
        }
        let zeros = vec![0u8; len];
        let file_offset = self.offset + in_block_offset;
        self.mmap.write(file_offset as usize, &zeros);
        Ok(())
    }
}
