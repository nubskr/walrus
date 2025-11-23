use std::fs;
use std::io;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use walrus_rust::Walrus;

fn temp_dir(suffix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("walrus-local-{suffix}-{nanos}"))
}

fn new_wal(base: &PathBuf, key_suffix: &str) -> io::Result<Walrus> {
    std::env::set_var("WALRUS_DISABLE_IO_URING", "1");
    std::env::set_var("WALRUS_DATA_DIR", base);
    fs::create_dir_all(base)?;
    Walrus::new_for_key(&format!("local-{key_suffix}"))
}

fn payload(chunk_idx: u64, size: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(size);
    buf.extend_from_slice(&chunk_idx.to_be_bytes());
    buf.resize(size, b'X');
    buf
}

fn write_chunks(wal: &Walrus, topic: &str, total_bytes: usize, chunk_size: usize) -> io::Result<()> {
    let mut sent = 0usize;
    let mut idx = 0u64;
    while sent < total_bytes {
        let data = payload(idx, chunk_size);
        wal.append_for_topic(topic, &data)?;
        sent += data.len();
        idx += 1;
    }
    Ok(())
}

fn read_and_verify(wal: &Walrus, topic: &str, total_bytes: usize, chunk_size: usize) -> io::Result<()> {
    let mut seen: usize = 0;
    let mut expected_idx: u64 = 0;

    while seen < total_bytes {
        let entry = wal
            .read_next(topic, true)?
            .expect("expected more entries but hit end");
        assert_eq!(entry.data.len(), chunk_size, "entry length mismatch");
        let idx = u64::from_be_bytes(entry.data[..8].try_into().unwrap());
        assert_eq!(idx, expected_idx, "out of order chunk");
        assert!(entry.data[8..].iter().all(|b| *b == b'X'));

        seen += entry.data.len();
        expected_idx += 1;
    }

    assert_eq!(seen, total_bytes, "did not read expected payload");
    Ok(())
}

/// Heavy local throughput test mirroring the Python cluster test but without Docker.
/// Uses a single WAL instance, writes 500MB in 1MB chunks, then reads it back via
/// offset-based batch reads. Marked ignored to avoid running on every `cargo test`.
#[test]
#[ignore]
fn throughput_single_topic_500mb() -> io::Result<()> {
    let base = temp_dir("throughput-500mb");
    let wal = new_wal(&base, "throughput-500mb")?;
    let topic = "throughput-local-500mb";
    let total_bytes = 500 * 1024 * 1024;
    let chunk_size = 1 * 1024 * 1024;

    write_chunks(&wal, topic, total_bytes, chunk_size)?;
    read_and_verify(&wal, topic, total_bytes, chunk_size)?;

    let _ = fs::remove_dir_all(base);
    Ok(())
}

/// Smaller smoke test that still exercises large sequential payloads locally.
#[test]
fn throughput_single_topic_64mb_smoke() -> io::Result<()> {
    let base = temp_dir("throughput-64mb");
    let wal = new_wal(&base, "throughput-64mb")?;
    let topic = "throughput-local-64mb";
    let total_bytes = 64 * 1024 * 1024;
    let chunk_size = 1 * 1024 * 1024;

    write_chunks(&wal, topic, total_bytes, chunk_size)?;
    read_and_verify(&wal, topic, total_bytes, chunk_size)?;

    let _ = fs::remove_dir_all(base);
    Ok(())
}
