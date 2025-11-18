use walrus_rust::wal::kv_store::store::KvStore;
use std::path::Path;
use std::fs;

#[test]
fn test_kv_compaction() {
    let namespace = "test_compaction";
    let kv_dir = Path::new("wal_kv").join(namespace);
    let _ = fs::remove_dir_all(&kv_dir);

    let store = KvStore::new(namespace).expect("failed to create store");

    // 1. Create Garbage
    // We need to trigger file rotation to have "old" files.
    // Block size is default 10MB.
    // We can force rotation by writing enough data or using a small block size?
    // We can't easily change block size from here (it's const).
    // But we can rely on the fact that `BlockAllocator` creates a NEW file on every restart/new() 
    // if we use `wal_data_dir` logic.
    // Wait, `KvStore::new` calls `BlockAllocator::new` which calls `paths.create_new_file()`.
    // So every time we restart the store, we get a NEW active file. The old ones become immutable.
    
    // Write Key1 in File 1
    store.put(b"key1", b"val1_old").unwrap();
    
    // Restart -> File 2
    drop(store);
    let store = KvStore::new(namespace).expect("reopen 1");
    
    // Overwrite Key1 in File 2 (File 1 entry is now garbage)
    store.put(b"key1", b"val1_new").unwrap();
    store.put(b"key2", b"val2").unwrap();
    
    // Restart -> File 3 (Active)
    drop(store);
    let store = KvStore::new(namespace).expect("reopen 2");
    
    // Check we have multiple files
    let count_files = || {
        fs::read_dir(&kv_dir).unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_none() || e.path().extension().unwrap() != "hint")
            .count()
    };
    
    let file_count_before = count_files();
    assert!(file_count_before >= 2, "Should have at least 2 data files (1 old, 1 active)");

    // 2. Run Compaction
    store.compact().expect("compaction failed");

    // 3. Verify Garbage Collection
    // We expect the old file (File 1) to be gone.
    // File 2 had "val1_new" and "val2". Both are live.
    // Wait, File 1 had "val1_old". Overwritten in File 2. So File 1 entry is DEAD.
    // File 2 has "val1_new". Is it overwritten in File 3? No. So it is LIVE.
    // So `compact` should:
    // - Read File 1. "key1" -> Index says File 2. Mismatch. Skip.
    // - File 1 empty/done. Delete File 1.
    // - Read File 2. "key1" -> Index says File 2. Match. Rewrite to File 3.
    // - "key2" -> Index says File 2. Match. Rewrite to File 3.
    // - File 2 done. Delete File 2.
    
    // So we expect only File 3 (Active) to remain?
    // Or File 3 and maybe a new File 4 if rewriting filled File 3? (Unlikely for small data).
    
    let file_count_after = count_files();
    assert_eq!(file_count_after, 1, "Compaction should leave only the active file");
    
    // 4. Verify Data Integrity
    let v1 = store.get(b"key1").unwrap();
    assert_eq!(v1, Some(b"val1_new".to_vec()));
    
    let v2 = store.get(b"key2").unwrap();
    assert_eq!(v2, Some(b"val2".to_vec()));
}
