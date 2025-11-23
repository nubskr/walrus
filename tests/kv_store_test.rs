use walrus_rust::wal::kv_store::store::KvStore;

#[test]
fn test_kv_store_basic() {
    let namespace = "test_kv_basic";
    // cleanup previous run (assuming default location relative to CWD)
    // In tests, CWD is typically the crate root.
    // We need to be careful about paths.
    let _ = std::fs::remove_dir_all(format!("wal_kv/{}", namespace));

    let store = KvStore::new(namespace).expect("failed to create store");

    store.put(b"key1", b"value1").expect("put failed");
    store.put(b"key2", b"value2").expect("put failed");

    let val1 = store.get(b"key1").expect("get failed");
    assert_eq!(val1, Some(b"value1".to_vec()));

    let val2 = store.get(b"key2").expect("get failed");
    assert_eq!(val2, Some(b"value2".to_vec()));
    
    let val3 = store.get(b"key3").expect("get failed");
    assert_eq!(val3, None);
}

#[test]
fn test_kv_store_persistence() {
    let namespace = "test_kv_persistence";
    let _ = std::fs::remove_dir_all(format!("wal_kv/{}", namespace));

    {
        let store = KvStore::new(namespace).expect("failed to create store");
        store.put(b"persist_key", b"persist_val").expect("put failed");
    } // store dropped

    // Re-open
    let store = KvStore::new(namespace).expect("failed to reopen store");
    let val = store.get(b"persist_key").expect("get failed");
    assert_eq!(val, Some(b"persist_val".to_vec()));
}

#[test]
fn test_kv_store_update() {
    let namespace = "test_kv_update";
    let _ = std::fs::remove_dir_all(format!("wal_kv/{}", namespace));

    let store = KvStore::new(namespace).expect("failed to create store");
    store.put(b"key", b"val1").expect("put failed");
    
    let v1 = store.get(b"key").expect("get failed");
    assert_eq!(v1, Some(b"val1".to_vec()));

    store.put(b"key", b"val2").expect("put failed");
    
    let v2 = store.get(b"key").expect("get failed");
    assert_eq!(v2, Some(b"val2".to_vec()));
}
