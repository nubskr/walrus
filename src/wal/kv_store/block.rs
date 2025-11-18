use crate::wal::block::Block;
use crate::wal::config::{checksum64, checksum64_update};
use crate::wal::kv_store::config::*;
use std::io::{Error, ErrorKind, Result};

/// A wrapper around the generic WAL Block to enforce KV-specific serialization.
#[derive(Clone, Debug)]
pub struct KvBlock(pub(crate) Block);

impl KvBlock {
    /// Appends a Key-Value pair to the block.
    /// Returns the new offset (current offset + written bytes).
    pub fn write_kv(&self, offset: u64, key: &[u8], value: &[u8], is_tombstone: bool) -> Result<u64> {
        if key.len() > MAX_KEY_SIZE {
            return Err(Error::new(ErrorKind::InvalidInput, "Key too large"));
        }
        if value.len() > MAX_VALUE_SIZE {
            return Err(Error::new(ErrorKind::InvalidInput, "Value too large"));
        }

        let total_len = KV_HEADER_SIZE + key.len() + value.len() + KV_CHECKSUM_SIZE;
        if offset + total_len as u64 > self.0.limit {
            return Err(Error::new(ErrorKind::FileTooLarge, "Block full"));
        }

        let mut flags = 0u8;
        if is_tombstone {
            flags |= FLAG_TOMBSTONE;
        }

        // Header on stack
        let mut header = [0u8; KV_HEADER_SIZE];
        header[0] = flags;
        header[1..3].copy_from_slice(&(key.len() as u16).to_le_bytes());
        header[3..7].copy_from_slice(&(value.len() as u32).to_le_bytes());

        // Checksum incrementally
        let mut csum = checksum64(&header);
        csum = checksum64_update(key, csum);
        csum = checksum64_update(value, csum);
        let csum32 = csum as u32;
        
        // Write parts directly
        let mut current_file_offset = self.0.offset + offset;
        
        self.0.mmap.write(current_file_offset as usize, &header);
        current_file_offset += KV_HEADER_SIZE as u64;
        
        self.0.mmap.write(current_file_offset as usize, key);
        current_file_offset += key.len() as u64;
        
        self.0.mmap.write(current_file_offset as usize, value);
        current_file_offset += value.len() as u64;
        
        self.0.mmap.write(current_file_offset as usize, &csum32.to_le_bytes());

        Ok(offset + total_len as u64)
    }

    /// Reads a Key-Value pair at the given offset.
    /// Optimistically reads the entire entry if `total_len` is known (from Index).
    pub fn read_kv(&self, offset: u64, total_len: u32) -> Result<(Vec<u8>, Vec<u8>)> {
        let len = total_len as usize;
        if len < KV_OVERHEAD {
             return Err(Error::new(ErrorKind::InvalidData, "Invalid length"));
        }

        let mut buffer = vec![0u8; len];
        let file_offset = self.0.offset + offset;
        self.0.mmap.read(file_offset as usize, &mut buffer);

        // Verify Checksum
        let stored_csum_bytes = &buffer[len - KV_CHECKSUM_SIZE..];
        let stored_csum = u32::from_le_bytes(stored_csum_bytes.try_into().unwrap());
        
        let data_to_check = &buffer[..len - KV_CHECKSUM_SIZE];
        let calculated = checksum64(data_to_check) as u32;

        if calculated != stored_csum {
             return Err(Error::new(ErrorKind::InvalidData, "Checksum mismatch"));
        }

        // Parse
        let flags = buffer[0];
        if flags & FLAG_TOMBSTONE != 0 {
            // It's a valid read, but it represents a deletion. 
            // We return generic NotFound to indicate it's gone.
            return Err(Error::new(ErrorKind::NotFound, "Entry is a tombstone"));
        }

        let k_len = u16::from_le_bytes(buffer[1..3].try_into().unwrap()) as usize;
        // v_len is technically redundant if we trust total_len, but good for sanity check
        let v_len = u32::from_le_bytes(buffer[3..7].try_into().unwrap()) as usize;

        if KV_HEADER_SIZE + k_len + v_len + KV_CHECKSUM_SIZE != len {
             return Err(Error::new(ErrorKind::InvalidData, "Length mismatch inside entry"));
        }

        let key = buffer[KV_HEADER_SIZE..KV_HEADER_SIZE + k_len].to_vec();
        let val = buffer[KV_HEADER_SIZE + k_len..KV_HEADER_SIZE + k_len + v_len].to_vec();

        Ok((key, val))
    }

    pub fn id(&self) -> u64 {
        self.0.id
    }
}
