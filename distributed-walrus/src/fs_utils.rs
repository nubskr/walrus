use anyhow::Result;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use tokio::fs;

const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x00000100000001B3;

/// Recursively compute the size of a Walrus segment directory.
pub async fn dir_size(path: impl AsRef<Path>) -> Result<u64> {
    let mut total = 0;
    let mut stack = vec![path.as_ref().to_path_buf()];

    while let Some(current) = stack.pop() {
        let meta = match fs::metadata(&current).await {
            Ok(meta) => meta,
            Err(err) if err.kind() == ErrorKind::NotFound => continue,
            Err(err) => return Err(err.into()),
        };

        if meta.is_file() {
            total += meta.len();
            continue;
        }

        if meta.is_dir() {
            let mut entries = fs::read_dir(&current).await?;
            while let Some(entry) = entries.next_entry().await? {
                stack.push(entry.path());
            }
        }
    }

    Ok(total)
}

/// Delete the provided Walrus segment directory if it exists.
pub async fn remove_segment_dir(path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();
    if matches!(fs::metadata(path).await, Ok(meta) if meta.is_dir()) {
        tracing::info!("Garbage collecting Walrus segment {:?}", path);
        fs::remove_dir_all(path).await?;
    }
    Ok(())
}

/// Replica of Walrus' namespace sanitization to find on-disk paths for topics.
pub fn walrus_path_for_key(root: &Path, key: &str) -> PathBuf {
    let mut sanitized: String = key
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();

    if sanitized.trim_matches('_').is_empty() {
        sanitized = format!("ns_{:x}", checksum64(key.as_bytes()));
    }

    root.join(sanitized)
}

fn checksum64(data: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET;
    for &b in data {
        hash ^= b as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}
