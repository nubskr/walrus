use std::fs;
use std::sync::Arc;
use walrus_rust::Walrus;

#[test]
fn test_repro_stuck_read_with_small_initial_entry() {
    let mut dir = std::env::temp_dir();
    dir.push(format!("walrus_repro_{}", std::process::id()));
    if dir.exists() {
        fs::remove_dir_all(&dir).unwrap();
    }
    fs::create_dir_all(&dir).unwrap();

    unsafe {
        std::env::set_var("WALRUS_DATA_DIR", dir.to_str().unwrap());
    }

    let wal = Arc::new(Walrus::new().unwrap());
    let col = "repro_topic";

    // 1. Write a small entry (mimicking the 18-byte internal metadata)
    // This will go into block 101.
    let small_data = vec![b'X'; 18];
    wal.append_for_topic(col, &small_data).unwrap();

    // 2. Write a large entry (20MB).
    // This won't fit in block 101 (10MB limit).
    // Block 101 will be sealed.
    // This writes to block 102.
    let large_size = 20 * 1024 * 1024;
    let large_data = vec![b'Y'; large_size];
    wal.append_for_topic(col, &large_data).unwrap();

    // 3. Write another small entry.
    // This won't fit in block 102 (limit expanded to ~30MB? No, standard blocks are 10MB, but large blocks are sized).
    // Wait, large blocks are "handout(sized)".
    // If I write another entry, will it go into 102 or force 102 to seal?
    // Let's write enough to force a seal if necessary, or just write something.
    wal.append_for_topic(col, &small_data).unwrap();

    // Force a new writer/block just to be sure 102 is sealed and in the chain.
    // Writing a very large entry usually forces the previous one to seal if it was sized.
    let large_data_2 = vec![b'Z'; 10 * 1024 * 1024];
    wal.append_for_topic(col, &large_data_2).unwrap();

    println!("--- Starting Read Request ---");
    // 3. Request 1MB.
    let max_bytes = 1024 * 1024;
    let result = wal
        .batch_read_for_topic(col, max_bytes, false, Some(0))
        .unwrap();

    println!("Read {} entries", result.len());
    for (i, entry) in result.iter().enumerate() {
        println!("Entry {}: {} bytes", i, entry.data.len());
    }

    let _ = fs::remove_dir_all(&dir);

    // We expect to see the large entry (20MB).
    // The first small entry might be skipped.
    assert!(
        result.len() >= 1,
        "Should have read at least the large entry"
    );
    // Find the large entry
    let found_large = result.iter().any(|e| e.data.len() == large_size);
    assert!(found_large, "Failed to read the 20MB entry! Stuck?");
}
