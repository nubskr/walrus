use walrus_rust::{enable_fd_backend, FsyncSchedule, ReadConsistency, Walrus};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;

// Global lock to serialize tests
static TEST_LOCK: std::sync::LazyLock<Mutex<()>> = std::sync::LazyLock::new(|| Mutex::new(()));

fn setup_test_env() -> std::sync::MutexGuard<'static, ()> {
    let guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    unsafe { std::env::set_var("WALRUS_QUIET", "1"); }
    let _ = std::fs::remove_dir_all("wal_files");
    std::fs::create_dir_all("wal_files").unwrap();
    guard
}

fn cleanup_test_env() {
    let _ = std::fs::remove_dir_all("wal_files");
}

// ============================================================================
// BLOCK BOUNDARY TESTS
// ============================================================================

#[test]
fn test_batch_read_spans_multiple_blocks() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Write large entries to force block sealing (10MB block size)
    // Write 3 x 8MB entries -> should span 3 blocks
    for i in 0..3 {
        let data = vec![i as u8; 8 * 1024 * 1024];
        wal.append_for_topic("span_blocks", &data).unwrap();
    }

    // Batch read all entries in one call
    let entries = wal.batch_read_for_topic("span_blocks", 30 * 1024 * 1024).unwrap();
    assert_eq!(entries.len(), 3, "Should read all 3 entries spanning multiple blocks");

    for (i, entry) in entries.iter().enumerate() {
        assert_eq!(entry.data.len(), 8 * 1024 * 1024);
        assert_eq!(entry.data[0], i as u8, "Entry {} has wrong pattern", i);
    }

    // Verify nothing left
    let remaining = wal.batch_read_for_topic("span_blocks", 1000).unwrap();
    assert!(remaining.is_empty(), "Should have no remaining entries");

    cleanup_test_env();
}

#[test]
fn test_batch_read_stops_mid_block() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Write 100 small entries within one block
    for i in 0..100 {
        let data = format!("entry_{:04}", i);
        wal.append_for_topic("mid_block", data.as_bytes()).unwrap();
    }

    // Read in chunks, ensuring we stop mid-block each time
    let mut total_read = 0;
    for chunk_num in 0..10 {
        let chunk = wal.batch_read_for_topic("mid_block", 100).unwrap();
        assert!(!chunk.is_empty(), "Chunk {} should not be empty", chunk_num);

        for (i, entry) in chunk.iter().enumerate() {
            let expected = format!("entry_{:04}", total_read + i);
            assert_eq!(entry.data, expected.as_bytes(), "Entry mismatch at position {}", total_read + i);
        }

        total_read += chunk.len();
    }

    assert_eq!(total_read, 100, "Should read all 100 entries across chunks");

    cleanup_test_env();
}

// ============================================================================
// TAIL/SEALED BOUNDARY TESTS
// ============================================================================

#[test]
fn test_batch_read_crosses_sealed_to_tail() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Write large entry to seal a block
    let large = vec![0xAA; 9 * 1024 * 1024];
    wal.append_for_topic("tail_boundary", &large).unwrap();

    // Write small entries in new (tail) block
    for i in 0..10 {
        let data = format!("tail_entry_{}", i);
        wal.append_for_topic("tail_boundary", data.as_bytes()).unwrap();
    }

    // Read nothing yet
    let all = wal.batch_read_for_topic("tail_boundary", 20 * 1024 * 1024).unwrap();
    assert_eq!(all.len(), 11, "Should read sealed block + tail entries");
    assert_eq!(all[0].data.len(), 9 * 1024 * 1024);
    assert_eq!(all[0].data[0], 0xAA);

    for i in 1..11 {
        let expected = format!("tail_entry_{}", i - 1);
        assert_eq!(all[i].data, expected.as_bytes());
    }

    cleanup_test_env();
}

#[test]
fn test_batch_read_tail_only() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Write only to tail (no sealing)
    for i in 0..20 {
        let data = format!("tail_only_{}", i);
        wal.append_for_topic("tail_only", data.as_bytes()).unwrap();
    }

    // Batch read from tail
    let batch1 = wal.batch_read_for_topic("tail_only", 200).unwrap();
    assert!(!batch1.is_empty(), "Should read from tail");

    let batch2 = wal.batch_read_for_topic("tail_only", 200).unwrap();
    assert!(!batch2.is_empty(), "Should continue reading from tail");

    // Verify no overlap
    for entry in &batch2 {
        for prev_entry in &batch1 {
            assert_ne!(entry.data, prev_entry.data, "Should not have duplicate reads");
        }
    }

    cleanup_test_env();
}

// ============================================================================
// CONCURRENT WRITE/READ CHAOS
// ============================================================================

#[test]
fn test_batch_read_during_concurrent_writes() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Arc::new(Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap());

    let barrier = Arc::new(Barrier::new(3));

    // Writer thread 1: continuous small writes
    let wal1 = wal.clone();
    let barrier1 = barrier.clone();
    let writer1 = thread::spawn(move || {
        barrier1.wait();
        for i in 0..100 {
            let data = format!("writer1_{:04}", i);
            let _ = wal1.append_for_topic("chaos", data.as_bytes());
            thread::sleep(std::time::Duration::from_micros(100));
        }
    });

    // Writer thread 2: occasional large writes (force sealing)
    let wal2 = wal.clone();
    let barrier2 = barrier.clone();
    let writer2 = thread::spawn(move || {
        barrier2.wait();
        for i in 0..5 {
            let data = vec![(0x10 + i) as u8; 6 * 1024 * 1024];
            let _ = wal2.append_for_topic("chaos", &data);
            thread::sleep(std::time::Duration::from_millis(10));
        }
    });

    // Reader thread: continuous batch reads
    let wal3 = wal.clone();
    let barrier3 = barrier.clone();
    let reader = thread::spawn(move || {
        barrier3.wait();
        thread::sleep(std::time::Duration::from_millis(5)); // Let writers get ahead

        let mut total_read = 0;
        let mut seen = std::collections::HashSet::new();

        for _ in 0..50 {
            if let Ok(batch) = wal3.batch_read_for_topic("chaos", 1024 * 1024) {
                for entry in batch {
                    // Verify we haven't seen this exact data before
                    assert!(seen.insert(entry.data.clone()), "Duplicate read detected!");
                    total_read += 1;
                }
            }
            thread::sleep(std::time::Duration::from_millis(2));
        }

        total_read
    });

    writer1.join().unwrap();
    writer2.join().unwrap();
    let read_count = reader.join().unwrap();

    // Should have read at least some entries (exact count varies due to timing)
    assert!(read_count > 0, "Reader should have read some entries during concurrent writes");

    cleanup_test_env();
}

#[test]
fn test_concurrent_batch_reads_same_topic() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Arc::new(Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap());

    // Pre-populate with 1000 entries
    for i in 0..1000 {
        let data = format!("entry_{:05}", i);
        wal.append_for_topic("concurrent_reads", data.as_bytes()).unwrap();
    }

    // Multiple readers competing for same topic
    let barrier = Arc::new(Barrier::new(5));
    let mut handles = vec![];

    for reader_id in 0..5 {
        let wal_clone = wal.clone();
        let barrier_clone = barrier.clone();

        let handle = thread::spawn(move || {
            barrier_clone.wait();

            let mut total_read = 0;
            loop {
                match wal_clone.batch_read_for_topic("concurrent_reads", 500) {
                    Ok(batch) if batch.is_empty() => break,
                    Ok(batch) => total_read += batch.len(),
                    Err(_) => break,
                }
            }

            (reader_id, total_read)
        });

        handles.push(handle);
    }

    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let total: usize = results.iter().map(|(_, count)| count).sum();

    // All entries should be read exactly once (due to StrictlyAtOnce)
    assert_eq!(total, 1000, "Concurrent readers should read all entries exactly once");

    cleanup_test_env();
}

// ============================================================================
// MIXED ENTRY SIZES - STRESS THE BUDGET CALCULATION
// ============================================================================

#[test]
fn test_batch_read_mixed_entry_sizes() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Write entries with wildly varying sizes
    let sizes = vec![
        10, 1000, 50, 10000, 100, 500000, 20, 2000000, 30, 100000,
        5, 50000, 15, 1000000, 25, 300000, 40, 150000, 8, 75000
    ];

    for (i, &size) in sizes.iter().enumerate() {
        let data = vec![i as u8; size];
        wal.append_for_topic("mixed_sizes", &data).unwrap();
    }

    // Read in small batches (budget will cut off at different entry boundaries)
    let mut total_entries = 0;
    let mut total_bytes = 0;

    loop {
        let batch = wal.batch_read_for_topic("mixed_sizes", 600000).unwrap();
        if batch.is_empty() {
            break;
        }

        for (local_idx, entry) in batch.iter().enumerate() {
            let global_idx = total_entries + local_idx;
            assert_eq!(entry.data.len(), sizes[global_idx],
                "Entry {} size mismatch: expected {}, got {}",
                global_idx, sizes[global_idx], entry.data.len());
            assert_eq!(entry.data[0], global_idx as u8,
                "Entry {} pattern mismatch", global_idx);
            total_bytes += entry.data.len();
        }

        total_entries += batch.len();
    }

    assert_eq!(total_entries, sizes.len(), "Should read all entries");

    cleanup_test_env();
}

// ============================================================================
// RECOVERY AND PERSISTENCE TESTS
// ============================================================================

#[test]
fn test_batch_read_recovery_mid_read() {
    let _guard = setup_test_env();
    enable_fd_backend();

    // Phase 1: Write data and partially read
    {
        let wal = Walrus::with_consistency_and_schedule(
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::NoFsync,
        )
        .unwrap();

        for i in 0..100 {
            let data = format!("recovery_{:04}", i);
            wal.append_for_topic("recovery", data.as_bytes()).unwrap();
        }

        // Read first 30 entries
        let mut read_so_far = 0;
        while read_so_far < 30 {
            let batch = wal.batch_read_for_topic("recovery", 300).unwrap();
            read_so_far += batch.len();
        }

        // Drop (simulates crash)
    }

    // Phase 2: Recover and continue reading
    {
        let wal = Walrus::with_consistency_and_schedule(
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::NoFsync,
        )
        .unwrap();

        // Should continue from entry 30
        let remaining = wal.batch_read_for_topic("recovery", 10000).unwrap();
        assert_eq!(remaining.len(), 70, "Should read remaining 70 entries after recovery");

        // Verify we got entries 30-99
        for (i, entry) in remaining.iter().enumerate() {
            let expected = format!("recovery_{:04}", 30 + i);
            assert_eq!(entry.data, expected.as_bytes(), "Entry mismatch at position {}", 30 + i);
        }
    }

    cleanup_test_env();
}

#[test]
fn test_batch_read_at_least_once_duplicates() {
    let _guard = setup_test_env();
    enable_fd_backend();

    // Phase 1: Write and read with AtLeastOnce
    {
        let wal = Walrus::with_consistency_and_schedule(
            ReadConsistency::AtLeastOnce { persist_every: 10 },
            FsyncSchedule::NoFsync,
        )
        .unwrap();

        for i in 0..50 {
            let data = format!("alo_{:04}", i);
            wal.append_for_topic("at_least_once", data.as_bytes()).unwrap();
        }

        // Read 15 entries (only persists after 10th)
        let mut count = 0;
        while count < 15 {
            let batch = wal.batch_read_for_topic("at_least_once", 200).unwrap();
            count += batch.len();
        }

        // Crash without reading more
    }

    // Phase 2: Recover - should see duplicates due to AtLeastOnce
    {
        let wal = Walrus::with_consistency_and_schedule(
            ReadConsistency::AtLeastOnce { persist_every: 10 },
            FsyncSchedule::NoFsync,
        )
        .unwrap();

        let mut all_entries = Vec::new();
        loop {
            let batch = wal.batch_read_for_topic("at_least_once", 1000).unwrap();
            if batch.is_empty() {
                break;
            }
            all_entries.extend(batch);
        }

        // Should have read at least 50 (may have duplicates of entries 10-15)
        assert!(all_entries.len() >= 50, "Should read at least all original entries");

        // Last entry should definitely be entry 49
        let last = &all_entries[all_entries.len() - 1];
        assert_eq!(last.data, b"alo_0049");
    }

    cleanup_test_env();
}

// ============================================================================
// ROLLBACK INTERACTION TESTS
// ============================================================================

#[test]
fn test_batch_read_with_zeroed_headers() {
    let _guard = setup_test_env();
    enable_fd_backend();

    // Phase 1: Write entries and manually zero some headers (simulates rollback)
    {
        let wal = Walrus::with_consistency_and_schedule(
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::NoFsync,
        )
        .unwrap();

        for i in 0..20 {
            let data = format!("zeroed_{:04}", i);
            wal.append_for_topic("zeroed", data.as_bytes()).unwrap();
        }

        drop(wal);
    }

    // Phase 2: Manually zero headers of entries 10-15 (simulates partial rollback)
    {
        use std::os::unix::fs::FileExt;

        let wal_files: Vec<_> = std::fs::read_dir("wal_files")
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| !e.path().to_str().unwrap().ends_with("_index.db"))
            .collect();

        if !wal_files.is_empty() {
            let file_path = wal_files[0].path();
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&file_path)
                .unwrap();

            // Calculate offset of entry 10 (each entry is ~64 + data_len bytes)
            // This is approximate - zero a range in the middle
            let approx_offset = 10 * (64 + 12); // 12 is ~size of "zeroed_XXXX"
            let zeros = vec![0u8; 64 * 6]; // Zero 6 headers
            file.write_at(&zeros, approx_offset as u64).unwrap();
            file.sync_all().unwrap();
        }
    }

    // Phase 3: Batch read should stop at zeroed header
    {
        let wal = Walrus::with_consistency_and_schedule(
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::NoFsync,
        )
        .unwrap();

        let mut all_entries = Vec::new();
        loop {
            let batch = wal.batch_read_for_topic("zeroed", 10000).unwrap();
            if batch.is_empty() {
                break;
            }
            all_entries.extend(batch);
        }

        // Should have read fewer than 20 entries (stopped at zeroed header)
        assert!(all_entries.len() < 20,
            "Should stop reading at zeroed header, got {} entries", all_entries.len());
        assert!(all_entries.len() >= 5,
            "Should have read at least some entries before zeroed header");
    }

    cleanup_test_env();
}

// ============================================================================
// INTERLEAVED SINGLE/BATCH READS
// ============================================================================

#[test]
fn test_interleaved_single_and_batch_reads() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Write 100 entries
    for i in 0..100 {
        let data = format!("interleaved_{:04}", i);
        wal.append_for_topic("interleaved", data.as_bytes()).unwrap();
    }

    let mut next_expected = 0;

    // Alternate between single and batch reads
    for round in 0..10 {
        if round % 2 == 0 {
            // Batch read
            let batch = wal.batch_read_for_topic("interleaved", 150).unwrap();
            for entry in batch {
                let expected = format!("interleaved_{:04}", next_expected);
                assert_eq!(entry.data, expected.as_bytes(),
                    "Batch read mismatch at position {}", next_expected);
                next_expected += 1;
            }
        } else {
            // Single reads
            for _ in 0..5 {
                if let Some(entry) = wal.read_next("interleaved").unwrap() {
                    let expected = format!("interleaved_{:04}", next_expected);
                    assert_eq!(entry.data, expected.as_bytes(),
                        "Single read mismatch at position {}", next_expected);
                    next_expected += 1;
                } else {
                    break;
                }
            }
        }
    }

    assert_eq!(next_expected, 100, "Should have read all entries via interleaved reads");

    cleanup_test_env();
}

// ============================================================================
// CONCURRENT BATCH WRITES VS BATCH READS
// ============================================================================

#[test]
fn test_batch_read_during_batch_writes() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Arc::new(Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap());

    let barrier = Arc::new(Barrier::new(4));

    // 3 concurrent batch writers
    let mut writers = vec![];
    for writer_id in 0..3 {
        let wal_clone = wal.clone();
        let barrier_clone = barrier.clone();

        let handle = thread::spawn(move || {
            barrier_clone.wait();

            for batch_num in 0..10 {
                let entries: Vec<Vec<u8>> = (0..20)
                    .map(|i| format!("w{}_b{}_e{}", writer_id, batch_num, i).into_bytes())
                    .collect();
                let refs: Vec<&[u8]> = entries.iter().map(|e| e.as_slice()).collect();

                let _ = wal_clone.batch_append_for_topic("batch_chaos", &refs);
                thread::sleep(std::time::Duration::from_millis(5));
            }
        });

        writers.push(handle);
    }

    // 1 concurrent batch reader
    let wal_clone = wal.clone();
    let barrier_clone = barrier.clone();
    let reader = thread::spawn(move || {
        barrier_clone.wait();
        thread::sleep(std::time::Duration::from_millis(10)); // Let some writes happen

        let mut total_read = 0;
        let mut seen = std::collections::HashSet::new();

        for _ in 0..100 {
            if let Ok(batch) = wal_clone.batch_read_for_topic("batch_chaos", 2048) {
                for entry in batch {
                    assert!(seen.insert(entry.data.clone()), "Duplicate batch read!");
                    total_read += 1;
                }
            }
            thread::sleep(std::time::Duration::from_millis(3));
        }

        total_read
    });

    for w in writers {
        w.join().unwrap();
    }
    let read_count = reader.join().unwrap();

    // Should have read at least some entries (exact count varies)
    assert!(read_count > 0, "Should have read some entries during concurrent batch writes");

    cleanup_test_env();
}

// ============================================================================
// EDGE CASE: EXACT BUDGET BOUNDARIES
// ============================================================================

#[test]
fn test_batch_read_exact_budget_boundary() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Write entries of exactly 100 bytes each
    for i in 0..20 {
        let data = vec![i as u8; 100];
        wal.append_for_topic("exact_budget", &data).unwrap();
    }

    // Budget for exactly 3 entries (300 bytes)
    let batch1 = wal.batch_read_for_topic("exact_budget", 300).unwrap();
    assert_eq!(batch1.len(), 3, "Should read exactly 3 entries with 300-byte budget");

    // Budget for exactly 5 more entries
    let batch2 = wal.batch_read_for_topic("exact_budget", 500).unwrap();
    assert_eq!(batch2.len(), 5, "Should read exactly 5 entries with 500-byte budget");

    // Budget of 1 byte - should read 0 entries (can't fit any)
    let batch3 = wal.batch_read_for_topic("exact_budget", 1).unwrap();
    assert_eq!(batch3.len(), 0, "Should read 0 entries with 1-byte budget");

    // Budget that would include partial entry - should stop before it
    let batch4 = wal.batch_read_for_topic("exact_budget", 350).unwrap();
    assert_eq!(batch4.len(), 3, "Should read 3 full entries and stop (not 3.5)");

    cleanup_test_env();
}

// ============================================================================
// STRESS: RAPID FIRE BATCH READS
// ============================================================================

#[test]
fn test_rapid_fire_batch_reads() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::AtLeastOnce { persist_every: 50 },
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Pre-populate with many entries
    for i in 0..10000 {
        let data = format!("{:06}", i);
        wal.append_for_topic("rapid_fire", data.as_bytes()).unwrap();
    }

    // Rapid fire batch reads with tiny budgets
    let mut total_read = 0;
    let mut iterations = 0;

    loop {
        let batch = wal.batch_read_for_topic("rapid_fire", 64).unwrap();
        if batch.is_empty() {
            break;
        }
        total_read += batch.len();
        iterations += 1;
    }

    assert_eq!(total_read, 10000, "Should read all entries via rapid-fire batch reads");
    assert!(iterations > 100, "Should have taken many iterations with tiny budgets");

    cleanup_test_env();
}

// ============================================================================
// CHAOS: EVERYTHING AT ONCE
// ============================================================================

#[test]
fn test_full_chaos_all_operations() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Arc::new(Walrus::with_consistency_and_schedule(
        ReadConsistency::AtLeastOnce { persist_every: 10 },
        FsyncSchedule::NoFsync,
    )
    .unwrap());

    let barrier = Arc::new(Barrier::new(8));
    let mut handles = vec![];

    // 2 single-entry writers
    for writer_id in 0..2 {
        let wal_clone = wal.clone();
        let barrier_clone = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier_clone.wait();
            for i in 0..200 {
                let data = format!("single_w{}_e{}", writer_id, i);
                let _ = wal_clone.append_for_topic("chaos_all", data.as_bytes());
                if i % 10 == 0 {
                    thread::sleep(std::time::Duration::from_micros(500));
                }
            }
        }));
    }

    // 2 batch writers
    for writer_id in 2..4 {
        let wal_clone = wal.clone();
        let barrier_clone = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier_clone.wait();
            for batch_num in 0..20 {
                let entries: Vec<Vec<u8>> = (0..15)
                    .map(|i| format!("batch_w{}_b{}_e{}", writer_id, batch_num, i).into_bytes())
                    .collect();
                let refs: Vec<&[u8]> = entries.iter().map(|e| e.as_slice()).collect();
                let _ = wal_clone.batch_append_for_topic("chaos_all", &refs);
                thread::sleep(std::time::Duration::from_millis(5));
            }
        }));
    }

    // 2 single readers
    for reader_id in 4..6 {
        let wal_clone = wal.clone();
        let barrier_clone = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier_clone.wait();
            thread::sleep(std::time::Duration::from_millis(20));
            let mut count = 0;
            for _ in 0..500 {
                if let Ok(Some(_entry)) = wal_clone.read_next("chaos_all") {
                    count += 1;
                } else {
                    thread::sleep(std::time::Duration::from_micros(100));
                }
            }
            (reader_id, count)
        }));
    }

    // 2 batch readers
    for reader_id in 6..8 {
        let wal_clone = wal.clone();
        let barrier_clone = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier_clone.wait();
            thread::sleep(std::time::Duration::from_millis(30));
            let mut count = 0;
            for _ in 0..100 {
                if let Ok(batch) = wal_clone.batch_read_for_topic("chaos_all", 1024) {
                    count += batch.len();
                } else {
                    thread::sleep(std::time::Duration::from_micros(100));
                }
            }
            (reader_id, count)
        }));
    }

    // Wait for all threads
    let mut total_written = 0;
    let mut total_read = 0;

    for (i, handle) in handles.into_iter().enumerate() {
        let result = handle.join().unwrap();
        if i < 4 {
            // Writers
            if i < 2 {
                total_written += 200; // single writers
            } else {
                total_written += 20 * 15; // batch writers
            }
        } else {
            // Readers
            let (_, count) = result;
            total_read += count;
        }
    }

    println!("Chaos test: wrote {}, read {}", total_written, total_read);

    // Due to AtLeastOnce and concurrent access, we can't guarantee exact counts,
    // but we should have read something
    assert!(total_read > 0, "Readers should have read some entries");

    cleanup_test_env();
}
