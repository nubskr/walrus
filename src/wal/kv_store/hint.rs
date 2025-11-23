use crate::wal::config::checksum64;
use std::io::{self, Read, Write};
use std::path::Path;

/// Hint file entry structure
/// [KeyLen: u16][ValLen: u32][Offset: u64][Key: KeyLen][Checksum: u32]
/// Total Overhead: 2 + 4 + 8 + 4 = 18 bytes + Key
pub struct HintEntry {
    pub key_len: u16,
    pub val_len: u32,
    pub offset: u64,
    pub key: Vec<u8>,
}

impl HintEntry {
    pub fn total_len(&self) -> usize {
        18 + self.key.len()
    }

    pub fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let mut buf = Vec::with_capacity(self.total_len());
        buf.extend_from_slice(&self.key_len.to_le_bytes());
        buf.extend_from_slice(&self.val_len.to_le_bytes());
        buf.extend_from_slice(&self.offset.to_le_bytes());
        buf.extend_from_slice(&self.key);

        let csum = checksum64(&buf) as u32;
        buf.extend_from_slice(&csum.to_le_bytes());

        writer.write_all(&buf)
    }

    pub fn read<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut header = [0u8; 14]; // 2 + 4 + 8
        reader.read_exact(&mut header)?;

        let key_len = u16::from_le_bytes(header[0..2].try_into().unwrap());
        let val_len = u32::from_le_bytes(header[2..6].try_into().unwrap());
        let offset = u64::from_le_bytes(header[6..14].try_into().unwrap());

        let mut key = vec![0u8; key_len as usize];
        reader.read_exact(&mut key)?;

        let mut csum_buf = [0u8; 4];
        reader.read_exact(&mut csum_buf)?;
        let expected_csum = u32::from_le_bytes(csum_buf);

        // Verify checksum
        let mut check_buf = Vec::with_capacity(14 + key_len as usize);
        check_buf.extend_from_slice(&header);
        check_buf.extend_from_slice(&key);
        let calculated = checksum64(&check_buf) as u32;

        if calculated != expected_csum {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Hint checksum mismatch"));
        }

        Ok(Self {
            key_len,
            val_len,
            offset,
            key,
        })
    }
}

pub fn hint_file_path(kv_root: &Path, file_id: u64) -> std::path::PathBuf {
    kv_root.join(format!("{}.hint", file_id))
}
