mod common;

use common::{TestEnv, current_wal_dir};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use walrus_rust::{FsyncSchedule, ReadConsistency, Walrus, enable_fd_backend};

fn setup_test_env() -> TestEnv {
    TestEnv::new()
}

fn cleanup_test_env() {
    let _ = std::fs::remove_dir_all(current_wal_dir());
}





#[test]
fn test_batch_read_spans_multiple_blocks() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();



    for i in 0..3 {
        let data = vec![i as u8; 8 * 1024 * 1024];
        wal.append_for_topic("span_blocks", &data).unwrap();
    }


    let entries = wal
        .batch_read_for_topic("span_blocks", 30 * 1024 * 1024, true)
        .unwrap();
    assert_eq!(
        entries.len(),
        3,
        "Should read all 3 entries spanning multiple blocks"
    );

    for (i, entry) in entries.iter().enumerate() {
        assert_eq!(entry.data.len(), 8 * 1024 * 1024);
        assert_eq!(entry.data[0], i as u8, "Entry {} has wrong pattern", i);
    }


    let remaining = wal.batch_read_for_topic("span_blocks", 1000, true).unwrap();
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


    for i in 0..100 {
        let data = format!("entry_{:04}", i);
        wal.append_for_topic("mid_block", data.as_bytes()).unwrap();
    }


    let mut total_read = 0;
    for chunk_num in 0..10 {
        let chunk = wal.batch_read_for_topic("mid_block", 100, true).unwrap();
        assert!(!chunk.is_empty(), "Chunk {} should not be empty", chunk_num);

        for (i, entry) in chunk.iter().enumerate() {
            let expected = format!("entry_{:04}", total_read + i);
            assert_eq!(
                entry.data,
                expected.as_bytes(),
                "Entry mismatch at position {}",
                total_read + i
            );
        }

        total_read += chunk.len();
    }

    assert_eq!(total_read, 100, "Should read all 100 entries across chunks");

    cleanup_test_env();
}





#[test]
fn test_batch_read_crosses_sealed_to_tail() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();


    let large = vec![0xAA; 9 * 1024 * 1024];
    wal.append_for_topic("tail_boundary", &large).unwrap();


    for i in 0..10 {
        let data = format!("tail_entry_{}", i);
        wal.append_for_topic("tail_boundary", data.as_bytes())
            .unwrap();
    }


    let all = wal
        .batch_read_for_topic("tail_boundary", 20 * 1024 * 1024, true)
        .unwrap();
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


    for i in 0..20 {
        let data = format!("tail_only_{}", i);
        wal.append_for_topic("tail_only", data.as_bytes()).unwrap();
    }


    let batch1 = wal.batch_read_for_topic("tail_only", 200, true).unwrap();
    assert!(!batch1.is_empty(), "Should read from tail");

    let batch2 = wal.batch_read_for_topic("tail_only", 200, true).unwrap();
    assert!(!batch2.is_empty(), "Should continue reading from tail");


    for entry in &batch2 {
        for prev_entry in &batch1 {
            assert_ne!(
                entry.data, prev_entry.data,
                "Should not have duplicate reads"
            );
        }
    }

    cleanup_test_env();
}





#[test]
fn test_batch_read_respects_entry_cap() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    const LIMIT: usize = 2000;


    let batch_one_storage: Vec<Vec<u8>> = (0..LIMIT)
        .map(|i| format!("entry_{:04}", i).into_bytes())
        .collect();
    let batch_two_storage: Vec<Vec<u8>> = (LIMIT..(LIMIT * 2))
        .map(|i| format!("entry_{:04}", i).into_bytes())
        .collect();

    let batch_one: Vec<&[u8]> = batch_one_storage.iter().map(|v| v.as_slice()).collect();
    let batch_two: Vec<&[u8]> = batch_two_storage.iter().map(|v| v.as_slice()).collect();

    wal.batch_append_for_topic("entry_cap", &batch_one)
        .expect("batch append 1 should succeed");
    wal.batch_append_for_topic("entry_cap", &batch_two)
        .expect("batch append 2 should succeed");


    let first_read = wal
        .batch_read_for_topic("entry_cap", usize::MAX, true)
        .expect("batch read should succeed");
    assert_eq!(
        first_read.len(),
        LIMIT,
        "batch read should stop at entry cap"
    );
    assert_eq!(
        first_read.first().unwrap().data,
        b"entry_0000",
        "first batch entry mismatch"
    );
    assert_eq!(
        first_read.last().unwrap().data,
        format!("entry_{:04}", LIMIT - 1).as_bytes(),
        "last batch entry mismatch"
    );


    let second_read = wal
        .batch_read_for_topic("entry_cap", usize::MAX, true)
        .expect("second batch read should succeed");
    assert_eq!(
        second_read.len(),
        LIMIT,
        "second batch read should return the remaining entries"
    );
    assert_eq!(
        second_read.first().unwrap().data,
        format!("entry_{:04}", LIMIT).as_bytes(),
        "first entry of second batch mismatch"
    );
    assert_eq!(
        second_read.last().unwrap().data,
        format!("entry_{:04}", LIMIT * 2 - 1).as_bytes(),
        "last entry of second batch mismatch"
    );


    let third_read = wal
        .batch_read_for_topic("entry_cap", usize::MAX, true)
        .expect("third batch read should succeed");
    assert!(
        third_read.is_empty(),
        "no entries should remain after consuming two batches"
    );

    cleanup_test_env();
}

#[test]
fn test_batch_read_without_checkpoint() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    let entries: Vec<Vec<u8>> = (0..3).map(|i| format!("item_{i}").into_bytes()).collect();
    let refs: Vec<&[u8]> = entries.iter().map(|v| v.as_slice()).collect();
    wal.batch_append_for_topic("peek_batch", &refs).unwrap();


    let first = wal
        .batch_read_for_topic("peek_batch", usize::MAX, false)
        .unwrap();
    assert_eq!(first.len(), 3);
    assert_eq!(first[0].data, b"item_0");

    let again = wal
        .batch_read_for_topic("peek_batch", usize::MAX, false)
        .unwrap();
    assert_eq!(again.len(), 3);
    assert_eq!(again[0].data, b"item_0");


    let committed = wal
        .batch_read_for_topic("peek_batch", usize::MAX, true)
        .unwrap();
    assert_eq!(committed.len(), 3);


    let empty = wal
        .batch_read_for_topic("peek_batch", usize::MAX, true)
        .unwrap();
    assert!(empty.is_empty());

    cleanup_test_env();
}





#[test]
fn test_batch_read_during_concurrent_writes() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Arc::new(
        Walrus::with_consistency_and_schedule(
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::NoFsync,
        )
        .unwrap(),
    );

    let barrier = Arc::new(Barrier::new(3));


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


    let wal3 = wal.clone();
    let barrier3 = barrier.clone();
    let reader = thread::spawn(move || {
        barrier3.wait();
        thread::sleep(std::time::Duration::from_millis(5));

        let mut total_read = 0;
        let mut seen = std::collections::HashSet::new();

        for _ in 0..50 {
            if let Ok(batch) = wal3.batch_read_for_topic("chaos", 1024 * 1024, true) {
                for entry in batch {

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


    assert!(
        read_count > 0,
        "Reader should have read some entries during concurrent writes"
    );

    cleanup_test_env();
}

#[test]
fn test_concurrent_batch_reads_same_topic() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Arc::new(
        Walrus::with_consistency_and_schedule(
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::NoFsync,
        )
        .unwrap(),
    );


    test_println!("Writing 500 entries for concurrent reads test...");
    for i in 0..500 {
        let data = format!("entry_{:05}", i);
        wal.append_for_topic("concurrent_reads", data.as_bytes())
            .unwrap();
    }
    test_println!("Finished writing entries");


    let barrier = Arc::new(Barrier::new(5));
    let mut handles = vec![];

    for reader_id in 0..5 {
        let wal_clone = wal.clone();
        let barrier_clone = barrier.clone();

        let handle = thread::spawn(move || {
            barrier_clone.wait();
            test_println!("Concurrent reader {} starting", reader_id);

            let mut total_read = 0;
            let mut batch_count = 0;
            loop {
                match wal_clone.batch_read_for_topic("concurrent_reads", 500, true) {
                    Ok(batch) if batch.is_empty() => {
                        test_println!("Reader {} got empty batch, stopping", reader_id);
                        break;
                    }
                    Ok(batch) => {
                        total_read += batch.len();
                        batch_count += 1;
                        if batch_count % 10 == 0 {
                            test_println!(
                                "Reader {} batch {}: read {} entries, total: {}",
                                reader_id,
                                batch_count,
                                batch.len(),
                                total_read
                            );
                        }
                    }
                    Err(e) => {
                        test_println!("Reader {} got error: {:?}, stopping", reader_id, e);
                        break;
                    }
                }
            }

            test_println!(
                "Concurrent reader {} finished with {} entries",
                reader_id,
                total_read
            );
            (reader_id, total_read)
        });

        handles.push(handle);
    }

    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let total: usize = results.iter().map(|(_, count)| count).sum();

    test_println!("Concurrent reads results: {:?}", results);
    test_println!("Total entries read: {}", total);


    assert_eq!(
        total, 500,
        "Concurrent readers should read all entries exactly once"
    );

    cleanup_test_env();
}





#[test]
fn test_batch_read_mixed_entry_sizes() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();


    let sizes = vec![
        10, 1000, 50, 10000, 100, 500000, 20, 2000000, 30, 100000, 5, 50000, 15, 1000000, 25,
        300000, 40, 150000, 8, 75000,
    ];

    for (i, &size) in sizes.iter().enumerate() {
        let data = vec![i as u8; size];
        wal.append_for_topic("mixed_sizes", &data).unwrap();
    }


    let mut total_entries = 0;
    let mut _total_bytes = 0;

    loop {
        let batch = wal
            .batch_read_for_topic("mixed_sizes", 600000, true)
            .unwrap();
        if batch.is_empty() {
            break;
        }

        for (local_idx, entry) in batch.iter().enumerate() {
            let global_idx = total_entries + local_idx;
            assert_eq!(
                entry.data.len(),
                sizes[global_idx],
                "Entry {} size mismatch: expected {}, got {}",
                global_idx,
                sizes[global_idx],
                entry.data.len()
            );
            assert_eq!(
                entry.data[0], global_idx as u8,
                "Entry {} pattern mismatch",
                global_idx
            );
            _total_bytes += entry.data.len();
        }

        total_entries += batch.len();
    }

    assert_eq!(total_entries, sizes.len(), "Should read all entries");

    cleanup_test_env();
}





#[test]
fn test_batch_read_recovery_mid_read() {
    let _guard = setup_test_env();
    enable_fd_backend();

    test_println!("Starting recovery test...");


    let read_before_crash = {
        test_println!("Phase 1: Writing and partially reading data");
        let wal = Walrus::with_consistency_and_schedule(
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::NoFsync,
        )
        .unwrap();


        for i in 0..50 {
            let data = format!("recovery_{:04}", i);
            wal.append_for_topic("recovery", data.as_bytes()).unwrap();
        }
        test_println!("Written 50 entries");


        let mut read_so_far = 0;
        let mut batch_count = 0;
        while read_so_far < 20 {
            let batch = wal.batch_read_for_topic("recovery", 300, true).unwrap();
            test_println!(
                "Batch {}: read {} entries, total so far: {}",
                batch_count,
                batch.len(),
                read_so_far + batch.len()
            );

            if batch.is_empty() {
                test_println!("WARNING: Got empty batch, breaking early");
                break;
            }
            read_so_far += batch.len();
            batch_count += 1;
        }
        test_println!("Phase 1 complete: read {} entries", read_so_far);


        read_so_far
    };



    thread::sleep(Duration::from_millis(50));


    {
        test_println!("Phase 2: Recovering and continuing read");
        let wal = Walrus::with_consistency_and_schedule(
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::NoFsync,
        )
        .unwrap();


        let remaining = wal.batch_read_for_topic("recovery", 10000, true).unwrap();
        test_println!("Recovery read: got {} entries", remaining.len());


        let expected_remaining = 50 - read_before_crash;
        assert_eq!(
            remaining.len(),
            expected_remaining,
            "Should read remaining {} entries after recovery, got {}",
            expected_remaining,
            remaining.len()
        );


        for (i, entry) in remaining.iter().enumerate() {
            let expected = format!("recovery_{:04}", read_before_crash + i);
            let actual = String::from_utf8_lossy(&entry.data);
            if actual != expected {
                test_println!(
                    "Mismatch at index {}: expected '{}', got '{}'",
                    i,
                    expected,
                    actual
                );
            }
            assert_eq!(
                entry.data,
                expected.as_bytes(),
                "Entry mismatch at position {}",
                read_before_crash + i
            );
        }
        test_println!("All remaining entries verified correctly");
    }

    cleanup_test_env();
    test_println!("Recovery test completed successfully");
}

#[test]
fn test_batch_read_at_least_once_duplicates() {
    let _guard = setup_test_env();
    enable_fd_backend();

    test_println!("Starting AtLeastOnce duplicates test...");


    {
        test_println!("Phase 1: Writing and reading with AtLeastOnce");
        let wal = Walrus::with_consistency_and_schedule(
            ReadConsistency::AtLeastOnce { persist_every: 5 },
            FsyncSchedule::NoFsync,
        )
        .unwrap();


        for i in 0..25 {
            let data = format!("alo_{:04}", i);
            wal.append_for_topic("at_least_once", data.as_bytes())
                .unwrap();
        }
        test_println!("Written 25 entries");


        let mut count = 0;
        let mut batch_num = 0;
        while count < 8 {
            let batch = wal
                .batch_read_for_topic("at_least_once", 200, true)
                .unwrap();
            test_println!(
                "Phase 1 Batch {}: read {} entries, total: {}",
                batch_num,
                batch.len(),
                count + batch.len()
            );
            count += batch.len();
            batch_num += 1;

            if batch.is_empty() {
                test_println!("WARNING: Got empty batch in phase 1, breaking early");
                break;
            }
        }
        test_println!("Phase 1 complete: read {} entries", count);


    }



    thread::sleep(Duration::from_millis(50));


    {
        test_println!("Phase 2: Recovering with AtLeastOnce (expecting duplicates)");
        let wal = Walrus::with_consistency_and_schedule(
            ReadConsistency::AtLeastOnce { persist_every: 5 },
            FsyncSchedule::NoFsync,
        )
        .unwrap();

        let mut all_entries = Vec::new();
        let mut batch_num = 0;
        loop {
            let batch = wal
                .batch_read_for_topic("at_least_once", 1000, true)
                .unwrap();
            if batch.is_empty() {
                test_println!("Phase 2: Got empty batch, stopping");
                break;
            }
            test_println!("Phase 2 Batch {}: read {} entries", batch_num, batch.len());
            all_entries.extend(batch);
            batch_num += 1;


            if batch_num > 50 {
                test_println!("WARNING: Too many batches, breaking to prevent infinite loop");
                break;
            }
        }

        test_println!("Phase 2 complete: read {} total entries", all_entries.len());


        assert!(
            all_entries.len() >= 25,
            "Should read at least all original entries, got {}",
            all_entries.len()
        );


        test_println!("First 5 entries:");
        for (i, entry) in all_entries.iter().take(5).enumerate() {
            test_println!("  {}: {}", i, String::from_utf8_lossy(&entry.data));
        }

        test_println!("Last 5 entries:");
        let start = all_entries.len().saturating_sub(5);
        for (i, entry) in all_entries.iter().skip(start).enumerate() {
            test_println!("  {}: {}", start + i, String::from_utf8_lossy(&entry.data));
        }


        let last = &all_entries[all_entries.len() - 1];
        let expected_last = b"alo_0024";
        test_println!(
            "Checking last entry: expected '{}', got '{}'",
            String::from_utf8_lossy(expected_last),
            String::from_utf8_lossy(&last.data)
        );
        assert_eq!(last.data, expected_last, "Last entry should be alo_0024");
    }

    cleanup_test_env();
    test_println!("AtLeastOnce duplicates test completed successfully");
}





#[test]
fn test_batch_read_with_zeroed_headers() {
    let _guard = setup_test_env();
    enable_fd_backend();


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



        thread::sleep(Duration::from_millis(50));
    }


    {
        use std::os::unix::fs::FileExt;

        let wal_files: Vec<_> = std::fs::read_dir(current_wal_dir())
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



            let approx_offset = 10 * (64 + 12);
            let zeros = vec![0u8; 64 * 6];
            file.write_at(&zeros, approx_offset as u64).unwrap();
            file.sync_all().unwrap();
        }
    }


    {
        let wal = Walrus::with_consistency_and_schedule(
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::NoFsync,
        )
        .unwrap();

        let mut all_entries = Vec::new();
        loop {
            let batch = wal.batch_read_for_topic("zeroed", 10000, true).unwrap();
            if batch.is_empty() {
                break;
            }
            all_entries.extend(batch);
        }


        assert!(
            all_entries.len() < 20,
            "Should stop reading at zeroed header, got {} entries",
            all_entries.len()
        );
        assert!(
            all_entries.len() >= 5,
            "Should have read at least some entries before zeroed header"
        );
    }

    cleanup_test_env();
}





#[test]
fn test_interleaved_single_and_batch_reads() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();


    for i in 0..100 {
        let data = format!("interleaved_{:04}", i);
        wal.append_for_topic("interleaved", data.as_bytes())
            .unwrap();
    }

    let mut next_expected = 0;


    for round in 0..10 {
        if round % 2 == 0 {

            let batch = wal.batch_read_for_topic("interleaved", 150, true).unwrap();
            for entry in batch {
                let expected = format!("interleaved_{:04}", next_expected);
                assert_eq!(
                    entry.data,
                    expected.as_bytes(),
                    "Batch read mismatch at position {}",
                    next_expected
                );
                next_expected += 1;
            }
        } else {

            for _ in 0..5 {
                if let Some(entry) = wal.read_next("interleaved", true).unwrap() {
                    let expected = format!("interleaved_{:04}", next_expected);
                    assert_eq!(
                        entry.data,
                        expected.as_bytes(),
                        "Single read mismatch at position {}",
                        next_expected
                    );
                    next_expected += 1;
                } else {
                    break;
                }
            }
        }
    }


    while next_expected < 100 {
        let batch = wal.batch_read_for_topic("interleaved", 150, true).unwrap();
        if batch.is_empty() {
            if let Some(entry) = wal.read_next("interleaved", true).unwrap() {
                let expected = format!("interleaved_{:04}", next_expected);
                assert_eq!(
                    entry.data,
                    expected.as_bytes(),
                    "Final drain (single) mismatch at position {}",
                    next_expected
                );
                next_expected += 1;
            } else {
                break;
            }
        } else {
            for entry in batch {
                let expected = format!("interleaved_{:04}", next_expected);
                assert_eq!(
                    entry.data,
                    expected.as_bytes(),
                    "Final drain (batch) mismatch at position {}",
                    next_expected
                );
                next_expected += 1;
            }
        }
    }

    assert_eq!(
        next_expected, 100,
        "Should have read all entries via interleaved reads"
    );

    cleanup_test_env();
}





#[test]
fn test_batch_read_during_batch_writes() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Arc::new(
        Walrus::with_consistency_and_schedule(
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::NoFsync,
        )
        .unwrap(),
    );

    let barrier = Arc::new(Barrier::new(4));


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


    let wal_clone = wal.clone();
    let barrier_clone = barrier.clone();
    let reader = thread::spawn(move || {
        barrier_clone.wait();
        thread::sleep(std::time::Duration::from_millis(10));

        let mut total_read = 0;
        let mut seen = std::collections::HashSet::new();

        for _ in 0..100 {
            if let Ok(batch) = wal_clone.batch_read_for_topic("batch_chaos", 2048, true) {
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


    assert!(
        read_count > 0,
        "Should have read some entries during concurrent batch writes"
    );

    cleanup_test_env();
}





#[test]
fn test_batch_read_exact_budget_boundary() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();


    for i in 0..20 {
        let data = vec![i as u8; 100];
        wal.append_for_topic("exact_budget", &data).unwrap();
    }


    let batch1 = wal.batch_read_for_topic("exact_budget", 300, true).unwrap();
    assert_eq!(
        batch1.len(),
        3,
        "Should read exactly 3 entries with 300-byte budget"
    );


    let batch2 = wal.batch_read_for_topic("exact_budget", 500, true).unwrap();
    assert_eq!(
        batch2.len(),
        5,
        "Should read exactly 5 entries with 500-byte budget"
    );


    let batch3 = wal.batch_read_for_topic("exact_budget", 1, true).unwrap();
    assert_eq!(
        batch3.len(),
        1,
        "Should return a single entry even if it exceeds the budget"
    );


    let batch4 = wal.batch_read_for_topic("exact_budget", 350, true).unwrap();
    assert_eq!(
        batch4.len(),
        3,
        "Should read 3 full entries and stop (not 3.5)"
    );

    cleanup_test_env();
}





#[test]
fn test_rapid_fire_batch_reads() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::AtLeastOnce { persist_every: 50 },
        FsyncSchedule::NoFsync,
    )
    .unwrap();


    test_println!("Writing 1000 entries for rapid fire test...");
    for i in 0..1000 {
        let data = format!("{:06}", i);
        wal.append_for_topic("rapid_fire", data.as_bytes()).unwrap();
    }
    test_println!("Finished writing entries");


    let mut total_read = 0;
    let mut iterations = 0;

    loop {
        let batch = wal.batch_read_for_topic("rapid_fire", 64, true).unwrap();
        if batch.is_empty() {
            break;
        }
        total_read += batch.len();
        iterations += 1;

        if iterations % 50 == 0 {
            test_println!(
                "Rapid fire: iteration {}, read {} entries so far",
                iterations,
                total_read
            );
        }
    }

    test_println!(
        "Rapid fire complete: {} iterations, {} entries read",
        iterations,
        total_read
    );
    assert_eq!(
        total_read, 1000,
        "Should read all entries via rapid-fire batch reads"
    );
    assert!(
        iterations > 10,
        "Should have taken many iterations with tiny budgets"
    );

    cleanup_test_env();
}





#[test]
fn test_simple_deadlock_repro() {
    let _guard = setup_test_env();
    enable_fd_backend();

    test_println!("Starting simple deadlock reproduction test...");

    let wal = Arc::new(
        Walrus::with_consistency_and_schedule(
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::NoFsync,
        )
        .unwrap(),
    );

    let barrier = Arc::new(Barrier::new(3));


    let wal1 = wal.clone();
    let barrier1 = barrier.clone();
    let writer = thread::spawn(move || {
        barrier1.wait();
        test_println!("Writer starting...");
        for i in 0..10 {

            let data = vec![i as u8; 1024 * 1024];
            match wal1.append_for_topic("deadlock_test", &data) {
                Ok(_) => test_println!("Writer: wrote entry {}", i),
                Err(e) => test_println!("Writer: error on entry {}: {:?}", i, e),
            }
        }
        test_println!("Writer finished");
    });


    let wal2 = wal.clone();
    let barrier2 = barrier.clone();
    let reader1 = thread::spawn(move || {
        barrier2.wait();
        test_println!("Reader 1 starting...");
        for i in 0..20 {
            match wal2.batch_read_for_topic("deadlock_test", 512 * 1024, true) {
                Ok(batch) => test_println!("Reader 1: batch {} read {} entries", i, batch.len()),
                Err(e) => test_println!("Reader 1: batch {} error: {:?}", i, e),
            }
            thread::sleep(std::time::Duration::from_millis(10));
        }
        test_println!("Reader 1 finished");
    });


    let wal3 = wal.clone();
    let barrier3 = barrier.clone();
    let reader2 = thread::spawn(move || {
        barrier3.wait();
        test_println!("Reader 2 starting...");
        for i in 0..50 {
            match wal3.read_next("deadlock_test", true) {
                Ok(Some(_)) => test_println!("Reader 2: read entry {}", i),
                Ok(None) => test_println!("Reader 2: no entry at {}", i),
                Err(e) => test_println!("Reader 2: error at {}: {:?}", i, e),
            }
            thread::sleep(std::time::Duration::from_millis(5));
        }
        test_println!("Reader 2 finished");
    });


    let timeout = std::time::Duration::from_secs(30);

    match writer.join() {
        Ok(_) => test_println!("Writer joined successfully"),
        Err(_) => test_println!("Writer panicked"),
    }

    match reader1.join() {
        Ok(_) => test_println!("Reader 1 joined successfully"),
        Err(_) => test_println!("Reader 1 panicked"),
    }

    match reader2.join() {
        Ok(_) => test_println!("Reader 2 joined successfully"),
        Err(_) => test_println!("Reader 2 panicked"),
    }

    cleanup_test_env();
    test_println!("Simple deadlock test completed");
}

#[test]
fn test_full_chaos_all_operations() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Arc::new(
        Walrus::with_consistency_and_schedule(
            ReadConsistency::AtLeastOnce { persist_every: 10 },
            FsyncSchedule::NoFsync,
        )
        .unwrap(),
    );

    let barrier = Arc::new(Barrier::new(8));
    let mut writer_handles = vec![];
    let mut reader_handles = vec![];

    test_println!("Starting chaos test with 8 threads...");


    for writer_id in 0..2 {
        let wal_clone = wal.clone();
        let barrier_clone = barrier.clone();
        writer_handles.push(thread::spawn(move || {
            barrier_clone.wait();
            test_println!("Single writer {} starting", writer_id);
            for i in 0..50 {
                let data = format!("single_w{}_e{}", writer_id, i);
                let _ = wal_clone.append_for_topic("chaos_all", data.as_bytes());
                if i % 10 == 0 {
                    thread::sleep(std::time::Duration::from_micros(500));
                }
            }
            test_println!("Single writer {} finished", writer_id);
        }));
    }


    for writer_id in 2..4 {
        let wal_clone = wal.clone();
        let barrier_clone = barrier.clone();
        writer_handles.push(thread::spawn(move || {
            barrier_clone.wait();
            test_println!("Batch writer {} starting", writer_id);
            for batch_num in 0..10 {
                let entries: Vec<Vec<u8>> = (0..10)
                    .map(|i| format!("batch_w{}_b{}_e{}", writer_id, batch_num, i).into_bytes())
                    .collect();
                let refs: Vec<&[u8]> = entries.iter().map(|e| e.as_slice()).collect();
                let _ = wal_clone.batch_append_for_topic("chaos_all", &refs);
                thread::sleep(std::time::Duration::from_millis(5));
            }
            test_println!("Batch writer {} finished", writer_id);
        }));
    }


    for reader_id in 4..6 {
        let wal_clone = wal.clone();
        let barrier_clone = barrier.clone();
        reader_handles.push(thread::spawn(move || {
            barrier_clone.wait();
            thread::sleep(std::time::Duration::from_millis(20));
            test_println!("Single reader {} starting", reader_id);
            let mut count = 0;
            for _ in 0..100 {
                if let Ok(Some(_entry)) = wal_clone.read_next("chaos_all", true) {
                    count += 1;
                } else {
                    thread::sleep(std::time::Duration::from_micros(100));
                }
            }
            test_println!(
                "Single reader {} finished with {} entries",
                reader_id,
                count
            );
            (reader_id, count)
        }));
    }


    for reader_id in 6..8 {
        let wal_clone = wal.clone();
        let barrier_clone = barrier.clone();
        reader_handles.push(thread::spawn(move || {
            barrier_clone.wait();
            thread::sleep(std::time::Duration::from_millis(30));
            test_println!("Batch reader {} starting", reader_id);
            let mut count = 0;
            for _ in 0..50 {
                if let Ok(batch) = wal_clone.batch_read_for_topic("chaos_all", 1024, true) {
                    count += batch.len();
                } else {
                    thread::sleep(std::time::Duration::from_micros(100));
                }
            }
            test_println!("Batch reader {} finished with {} entries", reader_id, count);
            (reader_id, count)
        }));
    }


    let mut total_written = 0;
    let mut total_read = 0;


    for handle in writer_handles {
        handle.join().unwrap();
    }
    total_written += 50 * 2;
    total_written += 10 * 10 * 2;


    for handle in reader_handles {
        let (_, count) = handle.join().unwrap();
        total_read += count;
    }

    test_println!("Chaos test: wrote {}, read {}", total_written, total_read);



    assert!(total_read > 0, "Readers should have read some entries");

    cleanup_test_env();
}
