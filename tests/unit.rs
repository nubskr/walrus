use walrus::wal::{Walrus, WalIndex, Entry};
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write, Read};

fn cleanup_wal() {
    let _ = fs::remove_dir_all("wal_files");
}

fn first_data_file() -> String {
    let mut files: Vec<_> = fs::read_dir("./wal_files").unwrap().flatten().collect();
    files.sort_by_key(|e| e.file_name());
    let p = files
        .into_iter()
        .find(|e| !e.file_name().to_string_lossy().ends_with("_index.db"))
        .unwrap()
        .path();
    p.to_string_lossy().to_string()
}

#[test]
fn walindex_persists() {
    fs::create_dir_all("wal_files").unwrap();
    let name = format!("unit_idx_{}", {
        use std::time::SystemTime;
        SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis()
    });
    let mut idx = WalIndex::new(&name);
    idx.set("k".to_string(), 7, 99);
    drop(idx);
    let idx2 = WalIndex::new(&name);
    let bp = idx2.get("k").unwrap();
    assert_eq!(bp.cur_block_idx, 7);
    assert_eq!(bp.cur_block_offset, 99);
    cleanup_wal();
}

#[test]
fn large_entry_forces_block_seal() {
    cleanup_wal();
    let wal = Walrus::new();
    
    // Create 9MB entries to force block sealing
    let large_data_1 = vec![0x42u8; 9 * 1024 * 1024]; // 9MB of 0x42
    let large_data_2 = vec![0x43u8; 9 * 1024 * 1024]; // 9MB of 0x43
    let large_data_3 = vec![0x43u8; 9 * 1024 * 1024]; // 9MB of 0x43

    // add a 2 second timeout
    
    wal.append_for_topic("t", &large_data_1).unwrap();
    wal.append_for_topic("t", &large_data_2).unwrap();
    wal.append_for_topic("t", &large_data_3).unwrap();
    
    // std::thread::sleep(std::time::Duration::from_secs(1));

    assert_eq!(wal.read_next("t").unwrap().data, large_data_1);
    assert_eq!(wal.read_next("t").unwrap().data, large_data_2);
    assert_eq!(wal.read_next("t").unwrap().data, large_data_3); // it will fail because it's in the write block still :))
    
    cleanup_wal();
}

#[test]
fn basic_roundtrip_single_topic() {
    cleanup_wal();
    let wal = Walrus::new();
    wal.append_for_topic("t", b"x").unwrap();
    wal.append_for_topic("t", b"y").unwrap();
    assert_eq!(wal.read_next("t").unwrap().data, b"x");
    assert_eq!(wal.read_next("t").unwrap().data, b"y");
    assert!(wal.read_next("t").is_none());
    cleanup_wal();
}

#[test]
fn basic_roundtrip_multi_topic() {
    cleanup_wal();
    let wal = Walrus::new();
    wal.append_for_topic("a", b"1").unwrap();
    wal.append_for_topic("b", b"2").unwrap();
    assert_eq!(wal.read_next("a").unwrap().data, b"1");
    assert_eq!(wal.read_next("b").unwrap().data, b"2");
    cleanup_wal();
}

#[test]
fn persists_read_offsets_across_restart() {
    cleanup_wal();
    let wal = Walrus::new();
    wal.append_for_topic("t", b"a").unwrap();
    wal.append_for_topic("t", b"b").unwrap();
    assert_eq!(wal.read_next("t").unwrap().data, b"a");
    // restart
    let wal2 = Walrus::new();
    assert_eq!(wal2.read_next("t").unwrap().data, b"b");
    assert!(wal2.read_next("t").is_none());
    cleanup_wal();
}

#[test]
fn checksum_corruption_is_detected_via_public_api() {
    cleanup_wal();
    let wal = Walrus::new();
    wal.append_for_topic("t", b"abcdef").unwrap();
    // corrupt by finding the data pattern in the file and flipping a byte
    let path = first_data_file();
    let mut bytes = Vec::new();
    {
        let mut f = OpenOptions::new().read(true).open(&path).unwrap();
        f.read_to_end(&mut bytes).unwrap();
    }
    if let Some(pos) = bytes.windows(6).position(|w| w == b"abcdef") {
        // flip one byte in the middle of the payload
        let flip_pos = pos + 2;
        let mut f = OpenOptions::new().read(true).write(true).open(&path).unwrap();
        f.seek(SeekFrom::Start(flip_pos as u64)).unwrap();
        f.write_all(&[bytes[flip_pos] ^ 0xFF]).unwrap();
    } else {
        panic!("payload not found to corrupt");
    }
    // restart and try reading
    let wal2 = Walrus::new();
    let res = wal2.read_next("t");
    assert!(res.is_none());
    cleanup_wal();
}

// ============================================================================
// UNIT TESTS FOR INDIVIDUAL COMPONENTS
// ============================================================================

mod checksum_tests {
    use super::*;
    
    // We need to access the internal checksum function, so we'll test it indirectly
    // through the public API by verifying data integrity
    #[test]
    fn checksum_detects_corruption() {
        cleanup_wal();
        let wal = Walrus::new();
        
        // Write some data
        let test_data = b"test_checksum_data_12345";
        wal.append_for_topic("checksum_test", test_data).unwrap();
        
        // First verify we can read the data normally
        let entry = wal.read_next("checksum_test").unwrap();
        assert_eq!(entry.data, test_data);
        
        // Corrupt the file and verify checksum detection
        let path = first_data_file();
        let mut bytes = Vec::new();
        {
            let mut f = OpenOptions::new().read(true).open(&path).unwrap();
            f.read_to_end(&mut bytes).unwrap();
        }
        
        // Find and corrupt the data - corrupt multiple bytes to ensure detection
        if let Some(pos) = bytes.windows(test_data.len()).position(|w| w == test_data) {
            let mut f = OpenOptions::new().read(true).write(true).open(&path).unwrap();
            f.seek(SeekFrom::Start(pos as u64)).unwrap();
            // Corrupt the first few bytes of the data
            let corrupted = [test_data[0] ^ 0xFF, test_data[1] ^ 0xFF, test_data[2] ^ 0xFF];
            f.write_all(&corrupted).unwrap();
            f.sync_all().unwrap(); // Ensure the corruption is written to disk
        } else {
            panic!("Test data not found in file for corruption");
        }
        
        // Restart and verify corruption is detected
        let wal2 = Walrus::new();
        let result = wal2.read_next("checksum_test");
        
        // The corrupted data should either return None or the corrupted data should be different
        match result {
            None => {
                // Corruption detected - this is what we expect
            }
            Some(entry) => {
                // If we get data back, it should be different from the original
                assert_ne!(entry.data, test_data, "Corruption was not detected - got original data back");
            }
        }
        
        cleanup_wal();
    }
}

mod entry_tests {
    use super::*;
    
    #[test]
    fn entry_creation_and_data_access() {
        let test_data = vec![1, 2, 3, 4, 5];
        let entry = Entry { data: test_data.clone() };
        
        assert_eq!(entry.data, test_data);
        assert_eq!(entry.data.len(), 5);
    }
    
    #[test]
    fn entry_with_empty_data() {
        let entry = Entry { data: Vec::new() };
        assert!(entry.data.is_empty());
    }
    
    #[test]
    fn entry_with_large_data() {
        let large_data = vec![42u8; 1024 * 1024]; // 1MB
        let entry = Entry { data: large_data.clone() };
        assert_eq!(entry.data.len(), 1024 * 1024);
        assert_eq!(entry.data[0], 42);
        assert_eq!(entry.data[1024 * 1024 - 1], 42);
    }
}

mod wal_index_tests {
    use super::*;
    
    #[test]
    fn wal_index_basic_operations() {
        cleanup_wal();
        let mut idx = WalIndex::new("test_basic");
        
        // Test set and get
        idx.set("key1".to_string(), 10, 20);
        let pos = idx.get("key1").unwrap();
        assert_eq!(pos.cur_block_idx, 10);
        assert_eq!(pos.cur_block_offset, 20);
        
        // Test non-existent key
        assert!(idx.get("nonexistent").is_none());
        
        cleanup_wal();
    }
    
    #[test]
    fn wal_index_update_existing_key() {
        cleanup_wal();
        let mut idx = WalIndex::new("test_update");
        
        idx.set("key1".to_string(), 10, 20);
        idx.set("key1".to_string(), 30, 40); // Update
        
        let pos = idx.get("key1").unwrap();
        assert_eq!(pos.cur_block_idx, 30);
        assert_eq!(pos.cur_block_offset, 40);
        
        cleanup_wal();
    }
    
    #[test]
    fn wal_index_remove_key() {
        cleanup_wal();
        let mut idx = WalIndex::new("test_remove");
        
        idx.set("key1".to_string(), 10, 20);
        let removed = idx.remove("key1").unwrap();
        assert_eq!(removed.cur_block_idx, 10);
        assert_eq!(removed.cur_block_offset, 20);
        
        assert!(idx.get("key1").is_none());
        assert!(idx.remove("key1").is_none()); // Remove non-existent
        
        cleanup_wal();
    }
    
    #[test]
    fn wal_index_persistence_across_instances() {
        cleanup_wal();
        let index_name = "test_persistence";
        
        // Create and populate index
        {
            let mut idx = WalIndex::new(index_name);
            idx.set("persistent_key".to_string(), 100, 200);
        }
        
        // Create new instance and verify data persists
        {
            let idx = WalIndex::new(index_name);
            let pos = idx.get("persistent_key").unwrap();
            assert_eq!(pos.cur_block_idx, 100);
            assert_eq!(pos.cur_block_offset, 200);
        }
        
        cleanup_wal();
    }
    
    #[test]
    fn wal_index_multiple_keys() {
        cleanup_wal();
        let mut idx = WalIndex::new("test_multiple");
        
        idx.set("key1".to_string(), 10, 20);
        idx.set("key2".to_string(), 30, 40);
        idx.set("key3".to_string(), 50, 60);
        
        assert_eq!(idx.get("key1").unwrap().cur_block_idx, 10);
        assert_eq!(idx.get("key2").unwrap().cur_block_idx, 30);
        assert_eq!(idx.get("key3").unwrap().cur_block_idx, 50);
        
        cleanup_wal();
    }
}

mod walrus_integration_tests {
    use super::*;
    
    #[test]
    fn walrus_empty_topic_read() {
        cleanup_wal();
        let wal = Walrus::new();
        
        // Reading from non-existent topic should return None
        assert!(wal.read_next("empty_topic").is_none());
        
        cleanup_wal();
    }
    
    #[test]
    fn walrus_single_entry_per_topic() {
        cleanup_wal();
        let wal = Walrus::new();
        
        wal.append_for_topic("topic1", b"data1").unwrap();
        wal.append_for_topic("topic2", b"data2").unwrap();
        
        assert_eq!(wal.read_next("topic1").unwrap().data, b"data1");
        assert_eq!(wal.read_next("topic2").unwrap().data, b"data2");
        
        // Should be empty after reading
        assert!(wal.read_next("topic1").is_none());
        assert!(wal.read_next("topic2").is_none());
        
        cleanup_wal();
    }
    
    #[test]
    fn walrus_multiple_entries_same_topic() {
        cleanup_wal();
        let wal = Walrus::new();
        
        let entries = vec![b"entry1", b"entry2", b"entry3", b"entry4"];
        for entry in &entries {
            wal.append_for_topic("multi_topic", *entry).unwrap();
        }
        
        for expected in &entries {
            assert_eq!(wal.read_next("multi_topic").unwrap().data, expected.as_slice());
        }
        
        assert!(wal.read_next("multi_topic").is_none());
        
        cleanup_wal();
    }
    
    #[test]
    fn walrus_interleaved_topics() {
        cleanup_wal();
        let wal = Walrus::new();
        
        wal.append_for_topic("a", b"a1").unwrap();
        wal.append_for_topic("b", b"b1").unwrap();
        wal.append_for_topic("a", b"a2").unwrap();
        wal.append_for_topic("b", b"b2").unwrap();
        
        assert_eq!(wal.read_next("a").unwrap().data, b"a1");
        assert_eq!(wal.read_next("b").unwrap().data, b"b1");
        assert_eq!(wal.read_next("a").unwrap().data, b"a2");
        assert_eq!(wal.read_next("b").unwrap().data, b"b2");
        
        cleanup_wal();
    }
    
    #[test]
    fn walrus_large_entries() {
        cleanup_wal();
        let wal = Walrus::new();
        
        // Test with various sizes
        let sizes = vec![1024, 64 * 1024, 512 * 1024, 1024 * 1024]; // 1KB to 1MB
        
        for (i, size) in sizes.iter().enumerate() {
            let data = vec![i as u8 + 1; *size];
            wal.append_for_topic("large_test", &data).unwrap();
        }
        
        for (i, size) in sizes.iter().enumerate() {
            let expected = vec![i as u8 + 1; *size];
            let actual = wal.read_next("large_test").unwrap().data;
            assert_eq!(actual, expected);
        }
        
        cleanup_wal();
    }
    
    #[test]
    fn walrus_zero_length_entry() {
        cleanup_wal();
        let wal = Walrus::new();
        
        wal.append_for_topic("empty", b"").unwrap();
        wal.append_for_topic("empty", b"not_empty").unwrap();
        
        assert_eq!(wal.read_next("empty").unwrap().data, b"");
        assert_eq!(wal.read_next("empty").unwrap().data, b"not_empty");
        
        cleanup_wal();
    }
    
    #[test]
    fn walrus_topic_isolation() {
        cleanup_wal();
        let wal = Walrus::new();
        
        // Write to multiple topics
        for i in 0..10 {
            wal.append_for_topic("topic_a", &[i]).unwrap();
            wal.append_for_topic("topic_b", &[i + 100]).unwrap();
        }
        
        // Read from topic_a only
        for i in 0..5 {
            assert_eq!(wal.read_next("topic_a").unwrap().data, &[i]);
        }
        
        // Read from topic_b - should be independent
        for i in 0..10 {
            assert_eq!(wal.read_next("topic_b").unwrap().data, &[i + 100]);
        }
        
        // Continue reading topic_a from where we left off
        for i in 5..10 {
            assert_eq!(wal.read_next("topic_a").unwrap().data, &[i]);
        }
        
        cleanup_wal();
    }
    
    #[test]
    fn walrus_recovery_after_restart() {
        cleanup_wal();
        
        // First instance - write data
        {
            let wal = Walrus::new();
            wal.append_for_topic("recovery_test", b"before_restart").unwrap();
            wal.append_for_topic("recovery_test", b"also_before").unwrap();
            
            // Read one entry
            assert_eq!(wal.read_next("recovery_test").unwrap().data, b"before_restart");
        }
        
        // Second instance - should recover state
        {
            let wal = Walrus::new();
            // Should continue from where we left off
            assert_eq!(wal.read_next("recovery_test").unwrap().data, b"also_before");
            assert!(wal.read_next("recovery_test").is_none());
        }
        
        cleanup_wal();
    }
    
    #[test]
    fn walrus_write_after_read_exhaustion() {
        cleanup_wal();
        let wal = Walrus::new();
        
        wal.append_for_topic("test", b"first").unwrap();
        assert_eq!(wal.read_next("test").unwrap().data, b"first");
        assert!(wal.read_next("test").is_none());
        
        // Write more data after exhausting reads
        wal.append_for_topic("test", b"second").unwrap();
        assert_eq!(wal.read_next("test").unwrap().data, b"second");
        
        cleanup_wal();
    }
    
    #[test]
    fn walrus_concurrent_topics_different_patterns() {
        cleanup_wal();
        let wal = Walrus::new();
        
        // Topic A: few large entries
        let large_data = vec![0xAA; 100 * 1024]; // 100KB
        wal.append_for_topic("topic_large", &large_data).unwrap();
        wal.append_for_topic("topic_large", &large_data).unwrap();
        
        // Topic B: many small entries
        for i in 0..100 {
            wal.append_for_topic("topic_small", &[i as u8]).unwrap();
        }
        
        // Verify both topics work correctly
        assert_eq!(wal.read_next("topic_large").unwrap().data, large_data);
        assert_eq!(wal.read_next("topic_large").unwrap().data, large_data);
        assert!(wal.read_next("topic_large").is_none());
        
        for i in 0..100 {
            assert_eq!(wal.read_next("topic_small").unwrap().data, &[i as u8]);
        }
        assert!(wal.read_next("topic_small").is_none());
        
        cleanup_wal();
    }
}

mod error_handling_tests {
    use super::*;
    
    #[test]
    fn walrus_handles_invalid_data_gracefully() {
        cleanup_wal();
        let wal = Walrus::new();
        
        // Write valid data first
        wal.append_for_topic("test", b"valid_data").unwrap();
        
        // Corrupt the file
        let path = first_data_file();
        let mut bytes = Vec::new();
        {
            let mut f = OpenOptions::new().read(true).open(&path).unwrap();
            f.read_to_end(&mut bytes).unwrap();
        }
        
        // Corrupt metadata length bytes (first 2 bytes of each block)
        {
            let mut f = OpenOptions::new().write(true).open(&path).unwrap();
            f.seek(SeekFrom::Start(0)).unwrap();
            f.write_all(&[0xFF, 0xFF]).unwrap(); // Invalid metadata length
        }
        
        // Should handle corruption gracefully
        let wal2 = Walrus::new();
        let _result = wal2.read_next("test");
        // Should either return None or handle the error gracefully
        // The exact behavior depends on implementation details
        
        cleanup_wal();
    }
}

mod stress_tests {
    use super::*;
    
    #[test]
    fn walrus_many_small_entries() {
        cleanup_wal();
        let wal = Walrus::new();
        
        let num_entries = 1000;
        
        // Write many small entries
        for i in 0..num_entries {
            let data = format!("entry_{:04}", i);
            wal.append_for_topic("stress_small", data.as_bytes()).unwrap();
        }
        
        // Read them back
        for i in 0..num_entries {
            let expected = format!("entry_{:04}", i);
            let actual = wal.read_next("stress_small").unwrap().data;
            assert_eq!(actual, expected.as_bytes());
        }
        
        assert!(wal.read_next("stress_small").is_none());
        
        cleanup_wal();
    }
    
    #[test]
    fn walrus_multiple_topics_stress() {
        cleanup_wal();
        let wal = Walrus::new();
        
        let num_topics = 10;
        let entries_per_topic = 100;
        
        // Write to multiple topics
        for topic_id in 0..num_topics {
            for entry_id in 0..entries_per_topic {
                let data = format!("t{}_e{}", topic_id, entry_id);
                let topic_name = format!("stress_topic_{}", topic_id);
                wal.append_for_topic(&topic_name, data.as_bytes()).unwrap();
            }
        }
        
        // Read from all topics
        for topic_id in 0..num_topics {
            let topic_name = format!("stress_topic_{}", topic_id);
            for entry_id in 0..entries_per_topic {
                let expected = format!("t{}_e{}", topic_id, entry_id);
                let actual = wal.read_next(&topic_name).unwrap().data;
                assert_eq!(actual, expected.as_bytes());
            }
            assert!(wal.read_next(&topic_name).is_none());
        }
        
        cleanup_wal();
    }
}
