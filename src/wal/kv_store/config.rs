/// Header size: Flags(1) + KeyLen(2) + ValLen(4) = 7 bytes
pub const KV_HEADER_SIZE: usize = 7;

/// Checksum size: 4 bytes (CRC32)
pub const KV_CHECKSUM_SIZE: usize = 4;

/// Total overhead per entry
pub const KV_OVERHEAD: usize = KV_HEADER_SIZE + KV_CHECKSUM_SIZE;

/// Flag: Tombstone (deleted)
pub const FLAG_TOMBSTONE: u8 = 1 << 0;

/// Flag: Compressed (reserved for future)
pub const FLAG_COMPRESSED: u8 = 1 << 1;

/// Max Key Size (u16::MAX)
pub const MAX_KEY_SIZE: usize = 65535;

/// Max Value Size (u32::MAX)
pub const MAX_VALUE_SIZE: usize = u32::MAX as usize;

/// Default KV directory
pub const DEFAULT_KV_DIR: &str = "wal_kv";
