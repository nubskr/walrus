use std::fs;
use std::sync::Arc;
use std::time::Instant;
use walrus_rust::Walrus;

#[test]
fn test_throughput_3gb_write_read_local() {
    let mut dir = std::env::temp_dir();
    dir.push(format!("walrus_repro_3gb_{}", std::process::id()));
    if dir.exists() {
        fs::remove_dir_all(&dir).unwrap();
    }
    fs::create_dir_all(&dir).unwrap();

    unsafe {
        std::env::set_var("WALRUS_DATA_DIR", dir.to_str().unwrap());
    }

    let wal = Arc::new(Walrus::new().unwrap());
    let col = "throughput_topic";

    let target_bytes: usize = 3 * 1024 * 1024 * 1024; // 3 GB
    let chunk_size: usize = 20 * 1024 * 1024; // 20 MB
    let chunk_data = vec![b'X'; chunk_size]; // Static data to save generation time

    println!("Starting 3GB write...");
    let start = Instant::now();
    let mut written = 0;
    let mut chunks = 0;

    while written < target_bytes {
        wal.append_for_topic(col, &chunk_data).unwrap();
        written += chunk_size;
        chunks += 1;
        if chunks % 10 == 0 {
            println!("Written {} MB...", written / 1024 / 1024);
        }
    }
    println!("Write complete: {} bytes in {:?}", written, start.elapsed());

    println!("Starting sequential read verification...");
    let mut read_offset = 0;
    let read_chunk_size = 1024 * 1024; // 1MB read buffer (simulating client)

    // We just need to ensure we can make progress past the first few blocks
    // checking the first 500MB is sufficient to prove the fix
    let check_limit = 500 * 1024 * 1024;

    while read_offset < check_limit {
        let result = wal
            .batch_read_for_topic(col, read_chunk_size, false, Some(read_offset as u64))
            .unwrap();

        if result.is_empty() {
            panic!("Stuck! Read returned 0 entries at offset {}", read_offset);
        }

        for entry in result {
            read_offset += entry.data.len();
        }

        if read_offset % (100 * 1024 * 1024) == 0 {
            println!("Read {} MB...", read_offset / 1024 / 1024);
        }
    }

    println!(
        "Success! Read passed {} MB mark without getting stuck.",
        check_limit / 1024 / 1024
    );
    let _ = fs::remove_dir_all(&dir);
}
