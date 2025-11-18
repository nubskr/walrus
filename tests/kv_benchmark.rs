use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use walrus_rust::wal::Walrus;

// Simple benchmark for the KV store
// Usage: cargo test --test kv_benchmark -- --nocapture

#[test]
fn benchmark_kv_write_throughput() {
    let namespace = "bench_write";
    let _ = std::fs::remove_dir_all(format!("wal_kv/{}", namespace));

    let wal = Walrus::new().unwrap();
    let store = Arc::new(wal.kv_store(namespace).unwrap());

    let entry_count = 100_000;
    let key_size = 16;
    let val_size = 100;
    let value = vec![b'x'; val_size];

    println!("Starting Write Benchmark: {} entries, {} byte values", entry_count, val_size);

    let start = Instant::now();
    for i in 0..entry_count {
        let key = format!("k_{:012}", i);
        store.put(key.as_bytes(), &value).unwrap();
    }
    let duration = start.elapsed();

    let ops = entry_count as f64 / duration.as_secs_f64();
    let mb = (entry_count * (key_size + val_size)) as f64 / 1_000_000.0;
    let mbps = mb / duration.as_secs_f64();

    println!("Write Result: {:.2} ops/sec, {:.2} MB/s, Total Time: {:.2?}", ops, mbps, duration);
}

#[test]
fn benchmark_kv_read_latency() {
    let namespace = "bench_read";
    let _ = std::fs::remove_dir_all(format!("wal_kv/{}", namespace));

    let wal = Walrus::new().unwrap();
    let store = Arc::new(wal.kv_store(namespace).unwrap());

    let entry_count = 50_000;
    let value = vec![b'y'; 100];

    // Pre-fill
    for i in 0..entry_count {
        let key = format!("k_{:012}", i);
        store.put(key.as_bytes(), &value).unwrap();
    }

    println!("Starting Read Benchmark: {} random lookups", entry_count);
    
    let start = Instant::now();
    use rand::Rng;
    let mut rng = rand::thread_rng();

    for _ in 0..entry_count {
        let i = rng.gen_range(0..entry_count);
        let key = format!("k_{:012}", i);
        let res = store.get(key.as_bytes()).unwrap();
        assert!(res.is_some());
    }
    let duration = start.elapsed();

    let ops = entry_count as f64 / duration.as_secs_f64();
    let latency_us = duration.as_micros() as f64 / entry_count as f64;

    println!("Read Result: {:.2} ops/sec, Latency: {:.2} µs/op", ops, latency_us);
}
