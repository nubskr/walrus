use walrus_rust::{enable_fd_backend, FsyncSchedule, ReadConsistency, Walrus};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::Duration;

// Global lock to serialize tests (prevents parallel test interference)
static TEST_LOCK: std::sync::LazyLock<Mutex<()>> = std::sync::LazyLock::new(|| Mutex::new(()));

fn setup_test_env() -> std::sync::MutexGuard<'static, ()> {
    let guard = TEST_LOCK.lock().unwrap();

    unsafe { std::env::set_var("WALRUS_QUIET", "1"); }

    // Clean up old test directories
    let _ = std::fs::remove_dir_all("wal_files");
    std::fs::create_dir_all("wal_files").unwrap();

    guard
}

fn cleanup_test_env() {
    let _ = std::fs::remove_dir_all("wal_files");
}

// ============================================================================
// BASIC FUNCTIONALITY TESTS
// ============================================================================

#[test]
fn test_batch_write_basic() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    let entries: Vec<&[u8]> = vec![b"entry1", b"entry2", b"entry3"];

    // Write batch
    wal.batch_append_for_topic("test_topic", &entries).unwrap();

    // Read back and verify
    let e1 = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(e1.data, b"entry1");

    let e2 = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(e2.data, b"entry2");

    let e3 = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(e3.data, b"entry3");

    // Should be no more entries
    assert!(wal.read_next("test_topic").unwrap().is_none());

    cleanup_test_env();
}

#[test]
fn test_batch_write_atomicity_with_reader() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Write some initial data
    wal.append_for_topic("test_topic", b"before").unwrap();

    // Reader consumes it
    let e = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(e.data, b"before");

    // Now do batch write
    let entries: Vec<&[u8]> = vec![b"batch1", b"batch2", b"batch3"];
    wal.batch_append_for_topic("test_topic", &entries).unwrap();

    // Reader should see all batch entries atomically
    let e1 = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(e1.data, b"batch1");

    let e2 = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(e2.data, b"batch2");

    let e3 = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(e3.data, b"batch3");

    cleanup_test_env();
}

// ============================================================================
// FAILURE SCENARIO TESTS
// ============================================================================

#[test]
fn test_batch_size_limit_enforcement() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Try to write more than 10GB
    // Each entry is 1GB, so 11 entries = 11GB (plus metadata)
    let one_gb = vec![0u8; 1024 * 1024 * 1024];
    let entries: Vec<&[u8]> = vec![
        &one_gb, &one_gb, &one_gb, &one_gb, &one_gb,
        &one_gb, &one_gb, &one_gb, &one_gb, &one_gb,
        &one_gb, // 11th entry
    ];

    let result = wal.batch_append_for_topic("test_topic", &entries);

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    assert!(err.to_string().contains("10GB limit"));

    // Verify nothing was written
    assert!(wal.read_next("test_topic").unwrap().is_none());

    cleanup_test_env();
}

#[test]
fn test_concurrent_batch_writes_rejected() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Arc::new(Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap());

    let barrier = Arc::new(Barrier::new(2));
    let success_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let would_block_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut handles = vec![];

    for i in 0..2 {
        let wal_clone = wal.clone();
        let barrier_clone = barrier.clone();
        let success = success_count.clone();
        let blocked = would_block_count.clone();

        let handle = thread::spawn(move || {
            // Prepare large batch to ensure overlap
            let large_entry = vec![0u8; 10 * 1024 * 1024]; // 10MB per entry
            let entries: Vec<&[u8]> = (0..100).map(|_| large_entry.as_slice()).collect();

            // Synchronize start
            barrier_clone.wait();

            let result = wal_clone.batch_append_for_topic("test_topic", &entries);

            match result {
                Ok(_) => {
                    success.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    blocked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                Err(e) => panic!("Unexpected error: {}", e),
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let successes = success_count.load(std::sync::atomic::Ordering::SeqCst);
    let blocks = would_block_count.load(std::sync::atomic::Ordering::SeqCst);

    // Exactly one should succeed, one should be blocked
    assert_eq!(successes, 1, "Expected exactly 1 successful batch write");
    assert_eq!(blocks, 1, "Expected exactly 1 blocked batch write");

    cleanup_test_env();
}

#[test]
fn test_regular_write_blocked_during_batch() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Arc::new(Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap());

    let barrier = Arc::new(Barrier::new(2));
    let batch_started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let write_blocked = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Thread 1: Batch write (slow)
    let wal_clone = wal.clone();
    let barrier_clone = barrier.clone();
    let batch_flag = batch_started.clone();
    let batch_handle = thread::spawn(move || {
        let large_entry = vec![0u8; 50 * 1024 * 1024]; // 50MB per entry
        let entries: Vec<&[u8]> = (0..50).map(|_| large_entry.as_slice()).collect();

        barrier_clone.wait();
        batch_flag.store(true, std::sync::atomic::Ordering::SeqCst);

        wal_clone.batch_append_for_topic("test_topic", &entries).unwrap();
    });

    // Thread 2: Regular write (should be blocked)
    let wal_clone = wal.clone();
    let barrier_clone = barrier.clone();
    let batch_flag = batch_started.clone();
    let blocked_flag = write_blocked.clone();
    let write_handle = thread::spawn(move || {
        barrier_clone.wait();

        // Wait a bit to ensure batch has started
        while !batch_flag.load(std::sync::atomic::Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(1));
        }
        thread::sleep(Duration::from_millis(10));

        // Try regular write
        let result = wal_clone.append_for_topic("test_topic", b"regular_entry");

        if let Err(e) = result {
            if e.kind() == std::io::ErrorKind::WouldBlock {
                blocked_flag.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
    });

    batch_handle.join().unwrap();
    write_handle.join().unwrap();

    assert!(
        write_blocked.load(std::sync::atomic::Ordering::SeqCst),
        "Regular write should have been blocked during batch"
    );

    cleanup_test_env();
}

#[test]
fn test_empty_batch() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    let entries: Vec<&[u8]> = vec![];

    // Empty batch should succeed (no-op)
    wal.batch_append_for_topic("test_topic", &entries).unwrap();

    // Nothing to read
    assert!(wal.read_next("test_topic").unwrap().is_none());

    cleanup_test_env();
}

// ============================================================================
// MULTI-BLOCK SPANNING TESTS
// ============================================================================

#[test]
fn test_batch_spans_multiple_blocks() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Each entry is 5MB, 100 entries = 500MB, will span multiple 10MB blocks
    let large_entry = vec![0xAB; 5 * 1024 * 1024];
    let entries: Vec<&[u8]> = (0..100).map(|_| large_entry.as_slice()).collect();

    wal.batch_append_for_topic("test_topic", &entries).unwrap();

    // Read back and verify all entries
    for i in 0..100 {
        let entry = wal.read_next("test_topic").unwrap().unwrap();
        assert_eq!(entry.data.len(), 5 * 1024 * 1024);
        assert_eq!(entry.data[0], 0xAB);
    }

    // No more entries
    assert!(wal.read_next("test_topic").unwrap().is_none());

    cleanup_test_env();
}

#[test]
fn test_batch_with_varying_entry_sizes() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Create entries of varying sizes
    let e1 = vec![1u8; 100];
    let e2 = vec![2u8; 1024 * 1024]; // 1MB
    let e3 = vec![3u8; 10 * 1024 * 1024]; // 10MB
    let e4 = vec![4u8; 500];
    let e5 = vec![5u8; 50 * 1024 * 1024]; // 50MB

    let entries: Vec<&[u8]> = vec![&e1, &e2, &e3, &e4, &e5];

    wal.batch_append_for_topic("test_topic", &entries).unwrap();

    // Verify all entries
    let r1 = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(r1.data.len(), 100);
    assert_eq!(r1.data[0], 1);

    let r2 = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(r2.data.len(), 1024 * 1024);
    assert_eq!(r2.data[0], 2);

    let r3 = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(r3.data.len(), 10 * 1024 * 1024);
    assert_eq!(r3.data[0], 3);

    let r4 = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(r4.data.len(), 500);
    assert_eq!(r4.data[0], 4);

    let r5 = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(r5.data.len(), 50 * 1024 * 1024);
    assert_eq!(r5.data[0], 5);

    cleanup_test_env();
}

// ============================================================================
// CHAOS TESTS - CONCURRENT OPERATIONS
// ============================================================================

#[test]
fn test_chaos_interleaved_batch_and_regular_writes() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Arc::new(Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap());

    let num_threads = 10;
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let wal_clone = wal.clone();

        let handle = thread::spawn(move || {
            for i in 0..20 {
                if i % 3 == 0 {
                    // Try batch write
                    let entries: Vec<&[u8]> = vec![b"batch1", b"batch2", b"batch3"];
                    let _ = wal_clone.batch_append_for_topic("chaos_topic", &entries);
                } else {
                    // Regular write
                    let data = format!("regular_t{}_i{}", thread_id, i);
                    let _ = wal_clone.append_for_topic("chaos_topic", data.as_bytes());
                }

                // Random small delay
                thread::sleep(Duration::from_micros(100));
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify we can read all written data without corruption
    let mut count = 0;
    while let Some(entry) = wal.read_next("chaos_topic").unwrap() {
        // Just verify we can read without panic/corruption
        assert!(!entry.data.is_empty());
        count += 1;
    }

    // Should have written some data
    assert!(count > 0, "Expected some entries to be written");

    cleanup_test_env();
}

#[test]
fn test_chaos_multiple_topics_concurrent_batches() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Arc::new(Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap());

    let num_topics = 5;
    let mut handles = vec![];

    for topic_id in 0..num_topics {
        let wal_clone = wal.clone();

        let handle = thread::spawn(move || {
            let topic_name = format!("topic_{}", topic_id);

            for batch_num in 0..10 {
                let entry_data = format!("t{}_b{}_e", topic_id, batch_num);
                let e1 = entry_data.clone() + "1";
                let e2 = entry_data.clone() + "2";
                let e3 = entry_data.clone() + "3";

                let entries: Vec<&[u8]> = vec![e1.as_bytes(), e2.as_bytes(), e3.as_bytes()];

                wal_clone.batch_append_for_topic(&topic_name, &entries).unwrap();

                thread::sleep(Duration::from_millis(5));
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify each topic has correct data
    for topic_id in 0..num_topics {
        let topic_name = format!("topic_{}", topic_id);
        let mut count = 0;

        while let Some(entry) = wal.read_next(&topic_name).unwrap() {
            let data_str = String::from_utf8_lossy(&entry.data);
            assert!(data_str.starts_with(&format!("t{}_", topic_id)));
            count += 1;
        }

        // Each topic should have 10 batches * 3 entries = 30 entries
        assert_eq!(count, 30, "Topic {} should have 30 entries", topic_id);
    }

    cleanup_test_env();
}

#[test]
fn test_chaos_batch_write_with_concurrent_readers() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Arc::new(Walrus::with_consistency_and_schedule(
        ReadConsistency::AtLeastOnce { persist_every: 10 },
        FsyncSchedule::NoFsync,
    )
    .unwrap());

    let stop_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut handles = vec![];

    // Writer threads
    for writer_id in 0..3 {
        let wal_clone = wal.clone();
        let stop = stop_flag.clone();

        let handle = thread::spawn(move || {
            let mut batch_count = 0;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let entry_data = format!("writer_{}_batch_{}", writer_id, batch_count);
                let entries: Vec<&[u8]> = vec![
                    entry_data.as_bytes(),
                    entry_data.as_bytes(),
                    entry_data.as_bytes(),
                ];

                let _ = wal_clone.batch_append_for_topic("chaos_rw_topic", &entries);
                batch_count += 1;

                thread::sleep(Duration::from_millis(10));
            }
        });

        handles.push(handle);
    }

    // Reader threads
    for _ in 0..5 {
        let wal_clone = wal.clone();
        let stop = stop_flag.clone();

        let handle = thread::spawn(move || {
            let mut read_count = 0;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                if let Ok(Some(entry)) = wal_clone.read_next("chaos_rw_topic") {
                    // Verify data is not corrupted
                    assert!(!entry.data.is_empty());
                    read_count += 1;
                } else {
                    thread::sleep(Duration::from_millis(5));
                }
            }
            // Return nothing to match writer thread type
        });

        handles.push(handle);
    }

    // Let it run for a bit
    thread::sleep(Duration::from_secs(3));
    stop_flag.store(true, std::sync::atomic::Ordering::Relaxed);

    for handle in handles {
        handle.join().unwrap();
    }

    cleanup_test_env();
}

// ============================================================================
// EDGE CASE TESTS
// ============================================================================

#[test]
fn test_batch_single_entry() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    let entries: Vec<&[u8]> = vec![b"single_entry"];

    wal.batch_append_for_topic("test_topic", &entries).unwrap();

    let entry = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(entry.data, b"single_entry");

    cleanup_test_env();
}

#[test]
fn test_batch_exactly_at_block_boundary() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Default block size is 10MB
    // Create entries that exactly fill one block (10MB - metadata overhead)
    let metadata_overhead = 64; // PREFIX_META_SIZE
    let entry_size = (10 * 1024 * 1024 - metadata_overhead * 2) / 2;

    let e1 = vec![0xAA; entry_size];
    let e2 = vec![0xBB; entry_size];

    let entries: Vec<&[u8]> = vec![&e1, &e2];

    wal.batch_append_for_topic("test_topic", &entries).unwrap();

    let r1 = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(r1.data[0], 0xAA);

    let r2 = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(r2.data[0], 0xBB);

    cleanup_test_env();
}

#[test]
fn test_batch_then_regular_write() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Batch write
    let entries: Vec<&[u8]> = vec![b"batch1", b"batch2"];
    wal.batch_append_for_topic("test_topic", &entries).unwrap();

    // Regular write after batch
    wal.append_for_topic("test_topic", b"regular").unwrap();

    // Verify order
    assert_eq!(wal.read_next("test_topic").unwrap().unwrap().data, b"batch1");
    assert_eq!(wal.read_next("test_topic").unwrap().unwrap().data, b"batch2");
    assert_eq!(wal.read_next("test_topic").unwrap().unwrap().data, b"regular");

    cleanup_test_env();
}

#[test]
fn test_multiple_sequential_batches() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Write multiple batches sequentially
    for batch_num in 0..5 {
        let e1 = format!("batch_{}_entry_1", batch_num);
        let e2 = format!("batch_{}_entry_2", batch_num);
        let e3 = format!("batch_{}_entry_3", batch_num);

        let entries: Vec<&[u8]> = vec![e1.as_bytes(), e2.as_bytes(), e3.as_bytes()];
        wal.batch_append_for_topic("test_topic", &entries).unwrap();
    }

    // Verify all entries in order
    for batch_num in 0..5 {
        for entry_num in 1..=3 {
            let entry = wal.read_next("test_topic").unwrap().unwrap();
            let expected = format!("batch_{}_entry_{}", batch_num, entry_num);
            assert_eq!(entry.data, expected.as_bytes());
        }
    }

    cleanup_test_env();
}

// ============================================================================
// STRESS TESTS
// ============================================================================

#[test]
#[ignore] // Run with --ignored flag for long-running tests
fn test_stress_large_batch_1000_entries() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    println!("[stress] Creating 1000 entries of 1MB each...");
    // Create 1000 entries of 1MB each
    let entry = vec![0xCD; 1024 * 1024];
    let entries: Vec<&[u8]> = (0..1000).map(|_| entry.as_slice()).collect();

    println!("[stress] Writing batch of 1GB...");
    let start = std::time::Instant::now();
    wal.batch_append_for_topic("stress_topic", &entries).unwrap();
    let duration = start.elapsed();

    println!("[stress] Batch write of 1000x1MB entries took: {:?}", duration);

    println!("[stress] Verifying all 1000 entries...");
    // Verify all entries
    for i in 0..1000 {
        if i % 100 == 0 {
            println!("[stress] Verified {} entries...", i);
        }
        let e = wal.read_next("stress_topic").unwrap().unwrap();
        assert_eq!(e.data.len(), 1024 * 1024);
        assert_eq!(e.data[0], 0xCD);
    }

    println!("[stress] All 1000 entries verified successfully!");
    cleanup_test_env();
}

#[test]
#[ignore]
fn test_stress_many_small_batches() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    println!("[stress] Writing 10000 batches of 10 entries each...");
    let start = std::time::Instant::now();

    // Write 10000 batches of 10 small entries each
    for i in 0..10000 {
        if i % 1000 == 0 {
            println!("[stress] Written {} batches...", i);
        }
        let data = format!("batch_{}", i);
        let entries: Vec<&[u8]> = (0..10).map(|_| data.as_bytes()).collect();
        wal.batch_append_for_topic("stress_topic", &entries).unwrap();
    }

    let duration = start.elapsed();
    println!("[stress] 10000 batches of 10 entries took: {:?}", duration);

    println!("[stress] Reading and verifying 100000 entries...");
    // Count entries
    let mut count = 0;
    while wal.read_next("stress_topic").unwrap().is_some() {
        count += 1;
        if count % 10000 == 0 {
            println!("[stress] Read {} entries...", count);
        }
    }

    assert_eq!(count, 100000);
    println!("[stress] All 100000 entries verified successfully!");

    cleanup_test_env();
}
