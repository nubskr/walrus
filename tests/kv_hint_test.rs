use walrus_rust::wal::kv_store::store::KvStore;
use std::path::Path;
use std::fs;

#[test]
fn test_hint_file_generation_and_recovery() {
    let namespace = "test_hint_gen";
    let kv_dir = Path::new("wal_kv").join(namespace);
    let _ = fs::remove_dir_all(&kv_dir);

    // 1. Write data (Slow path, no hints initially)
    {
        let store = KvStore::new(namespace).expect("failed to create store");
        store.put(b"key1", b"value1").unwrap();
        store.put(b"key2", b"value2").unwrap();
        // Dropping store closes it. The file remains "active" or "closed" on disk.
        // Allocator creates a file. 
    }

    // 2. Reopen (First Recovery)
    // This should detect the existing data file, scan it, AND generate a .hint file.
    {
        let store = KvStore::new(namespace).expect("failed to reopen store 1");
        let val = store.get(b"key1").unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
    }

    // 3. Verify Hint File Exists
    // We need to find the file ID.
    let mut files: Vec<_> = fs::read_dir(&kv_dir).unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().map_or(false, |e| e == "hint"))
        .collect();
    
    assert!(!files.is_empty(), "Hint file should have been generated during first recovery");

    // 4. Reopen (Second Recovery)
    // This should use the hint file (Fast path).
    {
        let store = KvStore::new(namespace).expect("failed to reopen store 2");
        let val = store.get(b"key2").unwrap();
        assert_eq!(val, Some(b"value2".to_vec()));
    }
}
