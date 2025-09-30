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
    let mut idx = WalIndex::new(&name).unwrap();
    idx.set("k".to_string(), 7, 99).unwrap();
    drop(idx);
    let idx2 = WalIndex::new(&name).unwrap();
    let bp = idx2.get("k").unwrap();
    assert_eq!(bp.cur_block_idx, 7);
    assert_eq!(bp.cur_block_offset, 99);
    cleanup_wal();
}

#[test]
fn large_entry_forces_block_seal() {
    cleanup_wal();
    let wal = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
    
    let large_data_1 = vec![0x42u8; 9 * 1024 * 1024]; // 9MB of 0x42
    let large_data_2 = vec![0x43u8; 9 * 1024 * 1024]; // 9MB of 0x43
    let large_data_3 = vec![0x43u8; 9 * 1024 * 1024]; // 9MB of 0x43

    
    wal.append_for_topic("t", &large_data_1).unwrap();
    wal.append_for_topic("t", &large_data_2).unwrap();
    wal.append_for_topic("t", &large_data_3).unwrap();
    

    assert_eq!(wal.read_next("t").unwrap().unwrap().data, large_data_1);
    assert_eq!(wal.read_next("t").unwrap().unwrap().data, large_data_2);
    assert_eq!(wal.read_next("t").unwrap().unwrap().data, large_data_3); // it will fail because it's in the write block still :))
    
    cleanup_wal();
}

#[test]
fn basic_roundtrip_single_topic() {
    cleanup_wal();
    let wal = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
    wal.append_for_topic("t", b"x").unwrap();
    wal.append_for_topic("t", b"y").unwrap();
    assert_eq!(wal.read_next("t").unwrap().unwrap().data, b"x");
    assert_eq!(wal.read_next("t").unwrap().unwrap().data, b"y");
    assert!(wal.read_next("t").unwrap().is_none());
    cleanup_wal();
}

#[test]
fn basic_roundtrip_multi_topic() {
    cleanup_wal();
    let wal = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
    wal.append_for_topic("a", b"1").unwrap();
    wal.append_for_topic("b", b"2").unwrap();
    assert_eq!(wal.read_next("a").unwrap().unwrap().data, b"1");
    assert_eq!(wal.read_next("b").unwrap().unwrap().data, b"2");
    cleanup_wal();
}

#[test]
fn persists_read_offsets_across_restart() {
    cleanup_wal();
    let wal = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
    wal.append_for_topic("t", b"a").unwrap();
    wal.append_for_topic("t", b"b").unwrap();
    assert_eq!(wal.read_next("t").unwrap().unwrap().data, b"a");
    let wal2 = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
    assert_eq!(wal2.read_next("t").unwrap().unwrap().data, b"b");
    assert!(wal2.read_next("t").unwrap().is_none());
    cleanup_wal();
}

#[test]
fn checksum_corruption_is_detected_via_public_api() {
    cleanup_wal();
    let wal = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
    wal.append_for_topic("t", b"abcdef").unwrap();
    let path = first_data_file();
    let mut bytes = Vec::new();
    {
        let mut f = OpenOptions::new().read(true).open(&path).unwrap();
        f.read_to_end(&mut bytes).unwrap();
    }
    if let Some(pos) = bytes.windows(6).position(|w| w == b"abcdef") {
        let flip_pos = pos + 2;
        let mut f = OpenOptions::new().read(true).write(true).open(&path).unwrap();
        f.seek(SeekFrom::Start(flip_pos as u64)).unwrap();
        f.write_all(&[bytes[flip_pos] ^ 0xFF]).unwrap();
    } else {
        panic!("payload not found to corrupt");
    }
    let wal2 = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
    let res = wal2.read_next("t").unwrap();
    assert!(res.is_none());
    cleanup_wal();
}


#[test]
fn stress_massive_single_entry() {
    cleanup_wal();
    let wal = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
    
    let size = 100 * 1024 * 1024; // 100MB
    let mut massive_data = Vec::with_capacity(size);
    
    for i in 0..size {
        massive_data.push((i % 256) as u8);
    }
    
    wal.append_for_topic("massive", &massive_data).unwrap();
    
    let entry = wal.read_next("massive").unwrap().unwrap();
    assert_eq!(entry.data.len(), size);
    
    for (i, &byte) in entry.data.iter().enumerate() {
        assert_eq!(byte, (i % 256) as u8, "Data corruption at byte {}", i);
    }
    
    cleanup_wal();
}

#[test]
fn stress_many_topics_with_validation() {
    cleanup_wal();
    let wal = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
    
    let num_topics = 1000;
    let entries_per_topic = 100;
    
    for topic_id in 0..num_topics {
        let topic = format!("topic_{:04}", topic_id);
        
        for entry_id in 0..entries_per_topic {
            let mut data = Vec::new();
            data.extend_from_slice(&(topic_id as u32).to_le_bytes());
            data.extend_from_slice(&(entry_id as u32).to_le_bytes());
            
            let payload = format!("data_{}_{}_", topic_id, entry_id).repeat(10);
            data.extend_from_slice(payload.as_bytes());
            
            wal.append_for_topic(&topic, &data).unwrap();
        }
    }
    
    for topic_id in 0..num_topics {
        let topic = format!("topic_{:04}", topic_id);
        
        for entry_id in 0..entries_per_topic {
            let entry = wal.read_next(&topic).unwrap().unwrap();
            
            let read_topic_id = u32::from_le_bytes([
                entry.data[0], entry.data[1], entry.data[2], entry.data[3]
            ]);
            let read_entry_id = u32::from_le_bytes([
                entry.data[4], entry.data[5], entry.data[6], entry.data[7]
            ]);
            
            assert_eq!(read_topic_id, topic_id as u32);
            assert_eq!(read_entry_id, entry_id as u32);
            
            let expected_payload = format!("data_{}_{}_", topic_id, entry_id).repeat(10);
            let actual_payload = String::from_utf8(entry.data[8..].to_vec()).unwrap();
            assert_eq!(actual_payload, expected_payload);
        }
        
        assert!(wal.read_next(&topic).unwrap().is_none());
    }
    
    cleanup_wal();
}

#[test]
fn stress_rapid_write_read_cycles() {
    cleanup_wal();
    let wal = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
    
    let cycles = 10000;
    let topic = "rapid_cycles";
    
    for cycle in 0..cycles {
        let mut data = Vec::new();
        data.extend_from_slice(&(cycle as u64).to_le_bytes());
        data.extend_from_slice(&[0xAA, 0xBB, 0xCC, 0xDD]); // Magic bytes
        
        let payload_size = (cycle % 100) + 1;
        for i in 0..payload_size {
            data.push((cycle + i) as u8);
        }
        
        wal.append_for_topic(topic, &data).unwrap();
        
        let entry = wal.read_next(topic).unwrap().unwrap();
        
        let read_cycle = u64::from_le_bytes([
            entry.data[0], entry.data[1], entry.data[2], entry.data[3],
            entry.data[4], entry.data[5], entry.data[6], entry.data[7]
        ]);
        assert_eq!(read_cycle, cycle as u64);
        
        assert_eq!(&entry.data[8..12], &[0xAA, 0xBB, 0xCC, 0xDD]);
        
        let expected_payload_size = (cycle % 100) + 1;
        assert_eq!(entry.data.len(), 8 + 4 + expected_payload_size);
        
        for (i, &byte) in entry.data[12..].iter().enumerate() {
            assert_eq!(byte, ((cycle + i) % 256) as u8);
        }
    }
    
    cleanup_wal();
}

#[test]
fn stress_boundary_conditions() {
    cleanup_wal();
    let wal = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
    
    let test_sizes = vec![
        0,                    // Empty
        1,                    // Single byte
        63,                   // Just under metadata size
        64,                   // Exactly metadata size
        65,                   // Just over metadata size
        1023,                 // Just under 1KB
        1024,                 // Exactly 1KB
        1025,                 // Just over 1KB
        65535,                // Just under 64KB
        65536,                // Exactly 64KB
        65537,                // Just over 64KB
        1024 * 1024 - 1,      // Just under 1MB
        1024 * 1024,          // Exactly 1MB
        1024 * 1024 + 1,      // Just over 1MB
    ];
    
    for (i, &size) in test_sizes.iter().enumerate() {
        let topic = format!("boundary_{}", i);
        
        let mut data = Vec::with_capacity(size);
        for j in 0..size {
            data.push(((i + j) % 256) as u8);
        }
        
        wal.append_for_topic(&topic, &data).unwrap();
        
        let entry = wal.read_next(&topic).unwrap().unwrap();
        assert_eq!(entry.data.len(), size);
        
        for (j, &byte) in entry.data.iter().enumerate() {
            assert_eq!(byte, ((i + j) % 256) as u8, 
                      "Mismatch at size {} byte {}", size, j);
        }
    }
    
    cleanup_wal();
}

#[test]
fn stress_data_integrity_patterns() {
    cleanup_wal();
    let wal = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
    
    let patterns = vec![
        ("zeros", vec![0u8; 1000]),
        ("ones", vec![0xFF; 1000]),
        ("alternating", (0..1000).map(|i| if i % 2 == 0 { 0xAA } else { 0x55 }).collect()),
        ("sequential", (0..1000).map(|i| (i % 256) as u8).collect()),
        ("reverse", (0..1000).map(|i| (255 - (i % 256)) as u8).collect()),
        ("random_seed", {
            let mut data = Vec::new();
            let mut seed = 12345u32;
            for _ in 0..1000 {
                seed = seed.wrapping_mul(1103515245).wrapping_add(12345);
                data.push((seed >> 16) as u8);
            }
            data
        }),
    ];
    
    for (pattern_name, data) in patterns {
        wal.append_for_topic(pattern_name, &data).unwrap();
        
        let entry = wal.read_next(pattern_name).unwrap().unwrap();
        assert_eq!(entry.data, data, "Pattern {} corrupted", pattern_name);
    }
    
    cleanup_wal();
}

#[test]
fn stress_concurrent_topic_validation() {
    cleanup_wal();
    let wal = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
    
    let num_topics = 50;
    let entries_per_topic = 200;
    
    for round in 0..entries_per_topic {
        for topic_id in 0..num_topics {
            let topic = format!("concurrent_{}", topic_id);
            
            let mut data = Vec::new();
            data.extend_from_slice(&(topic_id as u32).to_le_bytes());
            data.extend_from_slice(&(round as u32).to_le_bytes());
            
            let checksum = (topic_id + round) % 256;
            data.push(checksum as u8);
            
            let payload = format!("T{}R{}", topic_id, round);
            data.extend_from_slice(payload.as_bytes());
            
            wal.append_for_topic(&topic, &data).unwrap();
        }
    }
    
    for topic_id in 0..num_topics {
        let topic = format!("concurrent_{}", topic_id);
        
        for round in 0..entries_per_topic {
            let entry = wal.read_next(&topic).unwrap().unwrap();
            
            let read_topic_id = u32::from_le_bytes([
                entry.data[0], entry.data[1], entry.data[2], entry.data[3]
            ]);
            let read_round = u32::from_le_bytes([
                entry.data[4], entry.data[5], entry.data[6], entry.data[7]
            ]);
            let read_checksum = entry.data[8];
            
            assert_eq!(read_topic_id, topic_id as u32);
            assert_eq!(read_round, round as u32);
            assert_eq!(read_checksum, ((topic_id + round) % 256) as u8);
            
            let expected_payload = format!("T{}R{}", topic_id, round);
            let actual_payload = String::from_utf8(entry.data[9..].to_vec()).unwrap();
            assert_eq!(actual_payload, expected_payload);
        }
    }
    
    cleanup_wal();
}

#[test]
fn stress_extreme_topic_names() {
    cleanup_wal();
    let wal = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
    
    let extreme_topics = vec![
        "a".to_string(),                                    // Single char
        "a".repeat(10),                                     // Short
        "topic_with_underscores_and_numbers_123".to_string(), // Mixed
        "UPPERCASE_TOPIC".to_string(),                      // Uppercase
        "mixed_Case_Topic_123".to_string(),                // Mixed case
        "topic.with.dots".to_string(),                     // Dots
        "topic-with-dashes".to_string(),                   // Dashes
        "0123456789".to_string(),                          // Numbers only
        "topic_with_unicode_café".to_string(),             // Unicode (if supported)
    ];
    
    for (i, topic) in extreme_topics.iter().enumerate() {
        let data = format!("data_for_topic_{}", i).as_bytes().to_vec();
        
        match wal.append_for_topic(topic, &data) {
            Ok(_) => {
                let entry = wal.read_next(topic).unwrap().unwrap();
                assert_eq!(entry.data, data);
            }
            Err(_) => {
                println!("Topic '{}' rejected (expected for some cases)", topic);
            }
        }
    }
    
    cleanup_wal();
}


mod checksum_tests {
    use super::*;
    
    #[test]
    fn checksum_detects_corruption() {
        cleanup_wal();
        let wal = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
        
        let test_data = b"test_checksum_data_12345";
        wal.append_for_topic("checksum_test", test_data).unwrap();
        
        let entry = wal.read_next("checksum_test").unwrap().unwrap();
        assert_eq!(entry.data, test_data);
        
        let path = first_data_file();
        let mut bytes = Vec::new();
        {
            let mut f = OpenOptions::new().read(true).open(&path).unwrap();
            f.read_to_end(&mut bytes).unwrap();
        }
        
        if let Some(pos) = bytes.windows(test_data.len()).position(|w| w == test_data) {
            let mut f = OpenOptions::new().read(true).write(true).open(&path).unwrap();
            f.seek(SeekFrom::Start(pos as u64)).unwrap();
            let corrupted = [test_data[0] ^ 0xFF, test_data[1] ^ 0xFF, test_data[2] ^ 0xFF];
            f.write_all(&corrupted).unwrap();
            f.sync_all().unwrap(); // Ensure the corruption is written to disk
        } else {
            panic!("Test data not found in file for corruption");
        }
        
        let wal2 = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
        let result = wal2.read_next("checksum_test").unwrap();
        
        match result {
            None => {
            }
            Some(entry) => {
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
        let mut idx = WalIndex::new("test_basic").unwrap();
        
        idx.set("key1".to_string(), 10, 20).unwrap();
        let pos = idx.get("key1").unwrap();
        assert_eq!(pos.cur_block_idx, 10);
        assert_eq!(pos.cur_block_offset, 20);
        
        assert!(idx.get("nonexistent").is_none());
        
        cleanup_wal();
    }
    
    #[test]
    fn wal_index_update_existing_key() {
        cleanup_wal();
        let mut idx = WalIndex::new("test_update").unwrap();
        
        idx.set("key1".to_string(), 10, 20).unwrap();
        idx.set("key1".to_string(), 30, 40).unwrap(); // Update
        
        let pos = idx.get("key1").unwrap();
        assert_eq!(pos.cur_block_idx, 30);
        assert_eq!(pos.cur_block_offset, 40);
        
        cleanup_wal();
    }
    
    #[test]
    fn wal_index_remove_key() {
        cleanup_wal();
        let mut idx = WalIndex::new("test_remove").unwrap();
        
        idx.set("key1".to_string(), 10, 20).unwrap();
        let removed = idx.remove("key1").unwrap().unwrap();
        assert_eq!(removed.cur_block_idx, 10);
        assert_eq!(removed.cur_block_offset, 20);
        
        assert!(idx.get("key1").is_none());
        assert!(idx.remove("key1").unwrap().is_none()); // Remove non-existent
        
        cleanup_wal();
    }
    
    #[test]
    fn wal_index_persistence_across_instances() {
        cleanup_wal();
        let index_name = "test_persistence";
        
        {
            let mut idx = WalIndex::new(index_name).unwrap();
            idx.set("persistent_key".to_string(), 100, 200).unwrap();
        }
        
        {
            let idx = WalIndex::new(index_name).unwrap();
            let pos = idx.get("persistent_key").unwrap();
            assert_eq!(pos.cur_block_idx, 100);
            assert_eq!(pos.cur_block_offset, 200);
        }
        
        cleanup_wal();
    }
    
    #[test]
    fn wal_index_multiple_keys() {
        cleanup_wal();
        let mut idx = WalIndex::new("test_multiple").unwrap();
        
        idx.set("key1".to_string(), 10, 20).unwrap();
        idx.set("key2".to_string(), 30, 40).unwrap();
        idx.set("key3".to_string(), 50, 60).unwrap();
        
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
        let wal = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
        
        assert!(wal.read_next("empty_topic").unwrap().is_none());
        
        cleanup_wal();
    }
    
    #[test]
    fn walrus_single_entry_per_topic() {
        cleanup_wal();
        let wal = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
        
        wal.append_for_topic("topic1", b"data1").unwrap();
        wal.append_for_topic("topic2", b"data2").unwrap();
        
        assert_eq!(wal.read_next("topic1").unwrap().unwrap().data, b"data1");
        assert_eq!(wal.read_next("topic2").unwrap().unwrap().data, b"data2");
        
        assert!(wal.read_next("topic1").unwrap().is_none());
        assert!(wal.read_next("topic2").unwrap().is_none());
        
        cleanup_wal();
    }
    
    #[test]
    fn walrus_multiple_entries_same_topic() {
        cleanup_wal();
        let wal = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
        
        let entries = vec![b"entry1", b"entry2", b"entry3", b"entry4"];
        for entry in &entries {
            wal.append_for_topic("multi_topic", *entry).unwrap();
        }
        
        for expected in &entries {
            assert_eq!(wal.read_next("multi_topic").unwrap().unwrap().data, expected.as_slice());
        }
        
        assert!(wal.read_next("multi_topic").unwrap().is_none());
        
        cleanup_wal();
    }
    
    #[test]
    fn walrus_interleaved_topics() {
        cleanup_wal();
        let wal = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
        
        wal.append_for_topic("a", b"a1").unwrap();
        wal.append_for_topic("b", b"b1").unwrap();
        wal.append_for_topic("a", b"a2").unwrap();
        wal.append_for_topic("b", b"b2").unwrap();
        
        assert_eq!(wal.read_next("a").unwrap().unwrap().data, b"a1");
        assert_eq!(wal.read_next("b").unwrap().unwrap().data, b"b1");
        assert_eq!(wal.read_next("a").unwrap().unwrap().data, b"a2");
        assert_eq!(wal.read_next("b").unwrap().unwrap().data, b"b2");
        
        cleanup_wal();
    }
    
    #[test]
    fn walrus_large_entries() {
        cleanup_wal();
        let wal = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
        
        let sizes = vec![1024, 64 * 1024, 512 * 1024, 1024 * 1024]; // 1KB to 1MB
        
        for (i, size) in sizes.iter().enumerate() {
            let data = vec![i as u8 + 1; *size];
            wal.append_for_topic("large_test", &data).unwrap();
        }
        
        for (i, size) in sizes.iter().enumerate() {
            let expected = vec![i as u8 + 1; *size];
            let actual = wal.read_next("large_test").unwrap().unwrap().data;
            assert_eq!(actual, expected);
        }
        
        cleanup_wal();
    }
    
    #[test]
    fn walrus_zero_length_entry() {
        cleanup_wal();
        let wal = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
        
        wal.append_for_topic("empty", b"").unwrap();
        wal.append_for_topic("empty", b"not_empty").unwrap();
        
        assert_eq!(wal.read_next("empty").unwrap().unwrap().data, b"");
        assert_eq!(wal.read_next("empty").unwrap().unwrap().data, b"not_empty");
        
        cleanup_wal();
    }
    
    #[test]
    fn walrus_topic_isolation() {
        cleanup_wal();
        let wal = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
        
        for i in 0..10 {
            wal.append_for_topic("topic_a", &[i]).unwrap();
            wal.append_for_topic("topic_b", &[i + 100]).unwrap();
        }
        
        for i in 0..5 {
            assert_eq!(wal.read_next("topic_a").unwrap().unwrap().data, &[i]);
        }
        
        for i in 0..10 {
            assert_eq!(wal.read_next("topic_b").unwrap().unwrap().data, &[i + 100]);
        }
        
        for i in 5..10 {
            assert_eq!(wal.read_next("topic_a").unwrap().unwrap().data, &[i]);
        }
        
        cleanup_wal();
    }
    
    #[test]
    fn walrus_recovery_after_restart() {
        cleanup_wal();
        
        {
            let wal = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
            wal.append_for_topic("recovery_test", b"before_restart").unwrap();
            wal.append_for_topic("recovery_test", b"also_before").unwrap();
            
            assert_eq!(wal.read_next("recovery_test").unwrap().unwrap().data, b"before_restart");
        }
        
        {
            let wal = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
            assert_eq!(wal.read_next("recovery_test").unwrap().unwrap().data, b"also_before");
            assert!(wal.read_next("recovery_test").unwrap().is_none());
        }
        
        cleanup_wal();
    }
    
    #[test]
    fn walrus_write_after_read_exhaustion() {
        cleanup_wal();
        let wal = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
        
        wal.append_for_topic("test", b"first").unwrap();
        assert_eq!(wal.read_next("test").unwrap().unwrap().data, b"first");
        assert!(wal.read_next("test").unwrap().is_none());
        
        wal.append_for_topic("test", b"second").unwrap();
        assert_eq!(wal.read_next("test").unwrap().unwrap().data, b"second");
        
        cleanup_wal();
    }
    
    #[test]
    fn walrus_concurrent_topics_different_patterns() {
        cleanup_wal();
        let wal = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
        
        let large_data = vec![0xAA; 100 * 1024]; // 100KB
        wal.append_for_topic("topic_large", &large_data).unwrap();
        wal.append_for_topic("topic_large", &large_data).unwrap();
        
        for i in 0..100 {
            wal.append_for_topic("topic_small", &[i as u8]).unwrap();
        }
        
        assert_eq!(wal.read_next("topic_large").unwrap().unwrap().data, large_data);
        assert_eq!(wal.read_next("topic_large").unwrap().unwrap().data, large_data);
        assert!(wal.read_next("topic_large").unwrap().is_none());
        
        for i in 0..100 {
            assert_eq!(wal.read_next("topic_small").unwrap().unwrap().data, &[i as u8]);
        }
        assert!(wal.read_next("topic_small").unwrap().is_none());
        
        cleanup_wal();
    }
}

mod error_handling_tests {
    use super::*;
    
    #[test]
    fn walrus_handles_invalid_data_gracefully() {
        cleanup_wal();
        let wal = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
        
        wal.append_for_topic("test", b"valid_data").unwrap();
        
        let path = first_data_file();
        let mut bytes = Vec::new();
        {
            let mut f = OpenOptions::new().read(true).open(&path).unwrap();
            f.read_to_end(&mut bytes).unwrap();
        }
        
        {
            let mut f = OpenOptions::new().write(true).open(&path).unwrap();
            f.seek(SeekFrom::Start(0)).unwrap();
            f.write_all(&[0xFF, 0xFF]).unwrap(); // Invalid metadata length
        }
        
        let wal2 = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
        let _result = wal2.read_next("test").unwrap();
        
        cleanup_wal();
    }
}

mod stress_tests {
    use super::*;
    
    #[test]
    fn walrus_many_small_entries() {
        cleanup_wal();
        let wal = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
        
        let num_entries = 1000;
        
        for i in 0..num_entries {
            let data = format!("entry_{:04}", i);
            wal.append_for_topic("stress_small", data.as_bytes()).unwrap();
        }
        
        for i in 0..num_entries {
            let expected = format!("entry_{:04}", i);
            let actual = wal.read_next("stress_small").unwrap().unwrap().data;
            assert_eq!(actual, expected.as_bytes());
        }
        
        assert!(wal.read_next("stress_small").unwrap().is_none());
        
        cleanup_wal();
    }
    
    #[test]
    fn walrus_multiple_topics_stress() {
        cleanup_wal();
        let wal = Walrus::with_consistency(walrus::ReadConsistency::StrictlyAtOnce).unwrap();
        
        let num_topics = 10;
        let entries_per_topic = 100;
        
        for topic_id in 0..num_topics {
            for entry_id in 0..entries_per_topic {
                let data = format!("t{}_e{}", topic_id, entry_id);
                let topic_name = format!("stress_topic_{}", topic_id);
                wal.append_for_topic(&topic_name, data.as_bytes()).unwrap();
            }
        }
        
        for topic_id in 0..num_topics {
            let topic_name = format!("stress_topic_{}", topic_id);
            for entry_id in 0..entries_per_topic {
                let expected = format!("t{}_e{}", topic_id, entry_id);
                let actual = wal.read_next(&topic_name).unwrap().unwrap().data;
                assert_eq!(actual, expected.as_bytes());
            }
            assert!(wal.read_next(&topic_name).unwrap().is_none());
        }
        
        cleanup_wal();
    }
}
