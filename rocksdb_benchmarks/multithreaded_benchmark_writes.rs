use rand::Rng;
use rocksdb::{Options, WriteOptions, DB};
use std::env;
use std::fs;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

fn get_memory_info() -> (u64, u64, f64) {
    #[cfg(target_os = "macos")]
    {
        let total_memory = get_macos_total_memory();
        let dirty_pages = get_macos_dirty_pages();
        let dirty_ratio = if total_memory > 0 {
            (dirty_pages as f64 / total_memory as f64) * 100.0
        } else {
            0.0
        };
        (total_memory, dirty_pages, dirty_ratio)
    }

    #[cfg(target_os = "linux")]
    {
        get_linux_memory_info()
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        (0, 0, 0.0)
    }
}

#[cfg(target_os = "macos")]
fn get_macos_total_memory() -> u64 {
    use std::process::Command;

    if let Ok(output) = Command::new("sysctl").args(&["-n", "hw.memsize"]).output() {
        if let Ok(memsize_str) = String::from_utf8(output.stdout) {
            if let Ok(memsize_bytes) = memsize_str.trim().parse::<u64>() {
                return memsize_bytes / 1024;
            }
        }
    }
    0
}

#[cfg(target_os = "macos")]
fn get_macos_dirty_pages() -> u64 {
    use std::process::Command;

    if let Ok(output) = Command::new("vm_stat").output() {
        if let Ok(vm_stat_str) = String::from_utf8(output.stdout) {
            for line in vm_stat_str.lines() {
                if line.contains("Pages modified:") {
                    if let Some(pages_str) = line.split_whitespace().nth(2) {
                        if let Ok(pages) = pages_str.trim_end_matches('.').parse::<u64>() {
                            return pages * 4;
                        }
                    }
                }
            }
        }
    }
    0
}

#[cfg(target_os = "linux")]
fn get_linux_memory_info() -> (u64, u64, f64) {
    let mut total_memory = 0u64;
    let mut dirty_pages = 0u64;

    if let Ok(meminfo) = fs::read_to_string("/proc/meminfo") {
        for line in meminfo.lines() {
            if line.starts_with("MemTotal:") {
                if let Some(kb_str) = line.split_whitespace().nth(1) {
                    total_memory = kb_str.parse().unwrap_or(0);
                }
            } else if line.starts_with("Dirty:") {
                if let Some(kb_str) = line.split_whitespace().nth(1) {
                    dirty_pages = kb_str.parse().unwrap_or(0);
                }
            }
        }
    }

    let dirty_ratio = if total_memory > 0 {
        (dirty_pages as f64 / total_memory as f64) * 100.0
    } else {
        0.0
    };

    (total_memory, dirty_pages, dirty_ratio)
}

#[derive(Clone)]
enum RocksdbFsyncMode {
    SyncEach,
    NoFsync,
    Async(Duration),
}

fn parse_fsync_mode() -> RocksdbFsyncMode {
    if let Ok(fsync_env) = env::var("WALRUS_FSYNC") {
        if let Some(mode) = parse_fsync_value(&fsync_env) {
            return mode;
        }
    }

    let args: Vec<String> = env::args().collect();
    for i in 0..args.len() {
        if args[i] == "--fsync" && i + 1 < args.len() {
            if let Some(mode) = parse_fsync_value(&args[i + 1]) {
                return mode;
            }
        }
    }

    RocksdbFsyncMode::Async(Duration::from_millis(1000))
}

fn parse_fsync_value(value: &str) -> Option<RocksdbFsyncMode> {
    match value {
        "sync-each" => Some(RocksdbFsyncMode::SyncEach),
        "no-fsync" | "none" => Some(RocksdbFsyncMode::NoFsync),
        "async" => Some(RocksdbFsyncMode::Async(Duration::from_millis(1000))),
        other if other.ends_with("ms") => other[..other.len() - 2]
            .parse::<u64>()
            .ok()
            .map(|ms| RocksdbFsyncMode::Async(Duration::from_millis(ms))),
        other => other
            .parse::<u64>()
            .ok()
            .map(|ms| RocksdbFsyncMode::Async(Duration::from_millis(ms))),
    }
}

fn parse_duration() -> Duration {
    if let Ok(duration_env) = env::var("WALRUS_DURATION") {
        if let Some(duration) = parse_duration_string(&duration_env) {
            return duration;
        }
    }

    let args: Vec<String> = env::args().collect();
    for i in 0..args.len() {
        if args[i] == "--duration" && i + 1 < args.len() {
            if let Some(duration) = parse_duration_string(&args[i + 1]) {
                return duration;
            }
        }
    }

    Duration::from_secs(120)
}

fn parse_duration_string(duration_str: &str) -> Option<Duration> {
    if duration_str.ends_with("s") {
        duration_str[..duration_str.len() - 1]
            .parse::<u64>()
            .ok()
            .map(Duration::from_secs)
    } else if duration_str.ends_with("m") {
        duration_str[..duration_str.len() - 1]
            .parse::<u64>()
            .ok()
            .map(|mins| Duration::from_secs(mins * 60))
    } else if duration_str.ends_with("h") {
        duration_str[..duration_str.len() - 1]
            .parse::<u64>()
            .ok()
            .map(|hours| Duration::from_secs(hours * 3600))
    } else {
        duration_str
            .parse::<u64>()
            .ok()
            .map(Duration::from_secs)
    }
}

fn print_usage() {
    println!("Usage: WALRUS_FSYNC=<schedule> WALRUS_DURATION=<duration> cargo test rocksdb_multithreaded_benchmark_writes");
    println!("   or: cargo test rocksdb_multithreaded_benchmark_writes -- --fsync <schedule> --duration <duration>");
    println!();
    println!("Fsync Schedule Options:");
    println!("  sync-each    Sync WAL on every write (most durable)");
    println!("  no-fsync     Rely on OS buffering (fastest, least durable)");
    println!("  async        Background WAL sync every 1000ms (default)");
    println!("  <number>ms   Background WAL sync every N milliseconds (e.g., 500ms)");
    println!("  <number>     Same as above without the ms suffix");
    println!();
    println!("Duration Options:");
    println!("  <number>s    Duration in seconds (e.g., 30s, 120s)");
    println!("  <number>m    Duration in minutes (e.g., 2m, 5m)");
    println!("  <number>h    Duration in hours (e.g., 1h, 2h)");
    println!("  <number>     Duration in seconds (e.g., 120, 300)");
    println!("  Default: 2m (120 seconds)");
    println!();
    println!("Examples:");
    println!("  WALRUS_FSYNC=sync-each WALRUS_DURATION=30s cargo test rocksdb_multithreaded_benchmark_writes");
    println!("  WALRUS_FSYNC=no-fsync WALRUS_DURATION=1m cargo test rocksdb_multithreaded_benchmark_writes");
    println!("  WALRUS_FSYNC=500ms WALRUS_DURATION=5m cargo test rocksdb_multithreaded_benchmark_writes");
    println!("  cargo test rocksdb_multithreaded_benchmark_writes -- --fsync async --duration 1m");
}

fn cleanup_path(path: &str) {
    let _ = fs::remove_dir_all(path);
    thread::sleep(Duration::from_millis(100));
}

#[test]
fn rocksdb_multithreaded_benchmark() {
    let args: Vec<String> = env::args().collect();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        print_usage();
        return;
    }

    let db_path = "rocksdb_benchmark_db";
    let wal_path = format!("{}/wal", db_path);
    cleanup_path(db_path);

    fs::create_dir_all(&wal_path).ok();

    let fsync_mode = parse_fsync_mode();
    let write_duration = parse_duration();

    let mut options = Options::default();
    options.create_if_missing(true);
    options.set_wal_dir(&wal_path);
    options.set_keep_log_file_num(10);
    options.optimize_level_style_compaction(512 * 1024 * 1024);

    let db = Arc::new(DB::open(&options, db_path).expect("Failed to open RocksDB"));

    let num_threads = 10;
    let total_writes = Arc::new(AtomicU64::new(0));
    let total_write_bytes = Arc::new(AtomicU64::new(0));
    let write_errors = Arc::new(AtomicU64::new(0));

    let csv_path = "rocksdb_benchmark_throughput.csv";
    let mut csv_file = fs::File::create(csv_path).expect("Failed to create CSV file");
    writeln!(
        csv_file,
        "timestamp,elapsed_seconds,writes_per_second,bytes_per_second,total_writes,total_bytes,dirty_pages_kb,dirty_ratio_percent"
    )
    .expect("Failed to write CSV header");

    let (throughput_tx, throughput_rx) = mpsc::channel::<()>();
    let start_barrier = Arc::new(Barrier::new(num_threads + 1));
    let write_end_barrier = Arc::new(Barrier::new(num_threads + 1));

    let topics: Vec<String> = (0..num_threads)
        .map(|i| format!("topic_{}", i))
        .collect();

    println!("=== Multi-threaded RocksDB WAL Benchmark ===");
    println!(
        "Configuration: {} threads, {:.0}s write phase only",
        num_threads,
        write_duration.as_secs()
    );
    let fsync_mode_label = match &fsync_mode {
        RocksdbFsyncMode::SyncEach => "sync-each".to_string(),
        RocksdbFsyncMode::NoFsync => "no-fsync".to_string(),
        RocksdbFsyncMode::Async(d) => {
            if *d == Duration::from_millis(1000) {
                "async (1000ms)".to_string()
            } else {
                format!("async ({}ms)", d.as_millis())
            }
        }
    };
    println!("Fsync mode: {}", fsync_mode_label);
    println!("Duration: {:?}", write_duration);

    let total_writes_monitor = Arc::clone(&total_writes);
    let total_write_bytes_monitor = Arc::clone(&total_write_bytes);
    let monitor_duration = write_duration;

    let monitor_handle = thread::spawn(move || {
        let mut csv_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open("rocksdb_benchmark_throughput.csv")
            .expect("Failed to open CSV file");

        let mut last_writes = 0u64;
        let mut last_bytes = 0u64;
        let mut tick_index: u64 = 0;

        let _ = throughput_rx.recv();

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let (_, initial_dirty_kb, initial_dirty_ratio) = get_memory_info();
        writeln!(
            csv_file,
            "{},{:.2},{:.0},{:.0},{},{},{},{:.2}",
            timestamp, 0.0, 0.0, 0.0, 0, 0, initial_dirty_kb, initial_dirty_ratio
        )
        .expect("Failed to write initial CSV entry");
        csv_file.flush().expect("Failed to flush CSV");

        thread::sleep(Duration::from_millis(500));

        loop {
            thread::sleep(Duration::from_millis(500));

            tick_index += 1;
            let interval_s = 0.5f64;
            let elapsed_total = tick_index as f64 * interval_s;

            let current_writes = total_writes_monitor.load(Ordering::Relaxed);
            let current_bytes = total_write_bytes_monitor.load(Ordering::Relaxed);

            let writes_per_second = (current_writes - last_writes) as f64 / interval_s;
            let bytes_per_second = (current_bytes - last_bytes) as f64 / interval_s;

            let (_, dirty_kb, dirty_ratio) = get_memory_info();
            let should_log = (current_writes != last_writes) || (tick_index % 4 == 0);

            if should_log {
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                writeln!(
                    csv_file,
                    "{},{:.2},{:.0},{:.0},{},{},{},{:.2}",
                    timestamp,
                    elapsed_total,
                    writes_per_second,
                    bytes_per_second,
                    current_writes,
                    current_bytes,
                    dirty_kb,
                    dirty_ratio
                )
                .expect("Failed to write to CSV");
                csv_file.flush().expect("Failed to flush CSV");

                if current_writes > last_writes {
                    println!(
                        "[Monitor] {:.1}s: {:.0} writes/sec, {:.2} MB/sec, total: {} writes, dirty: {:.2}% ({} KB)",
                        elapsed_total,
                        writes_per_second,
                        bytes_per_second / (1024.0 * 1024.0),
                        current_writes,
                        dirty_ratio,
                        dirty_kb
                    );
                }
            }

            last_writes = current_writes;
            last_bytes = current_bytes;

            let max_monitor_time = monitor_duration.as_secs_f64() + 30.0;
            if elapsed_total > max_monitor_time {
                break;
            }
        }
    });

    let wal_sync_stop = Arc::new(AtomicBool::new(false));
    let wal_sync_handle = if let RocksdbFsyncMode::Async(interval) = fsync_mode.clone() {
        let db_clone = Arc::clone(&db);
        let stop_flag = Arc::clone(&wal_sync_stop);
        Some(thread::spawn(move || {
            while !stop_flag.load(Ordering::Relaxed) {
                thread::sleep(interval);
                if let Err(err) = db_clone.flush_wal(true) {
                    eprintln!("Error flushing WAL: {}", err);
                }
            }
            let _ = db_clone.flush_wal(true);
        }))
    } else {
        None
    };

    let mut handles = Vec::new();
    for thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let total_writes_clone = Arc::clone(&total_writes);
        let total_write_bytes_clone = Arc::clone(&total_write_bytes);
        let write_errors_clone = Arc::clone(&write_errors);
        let start_barrier_clone = Arc::clone(&start_barrier);
        let write_end_barrier_clone = Arc::clone(&write_end_barrier);
        let topic = topics[thread_id].clone();
        let fsync_mode_clone = fsync_mode.clone();

        let handle = thread::spawn(move || {
            start_barrier_clone.wait();

            let start_time = Instant::now();
            let mut local_writes = 0u64;
            let mut local_write_bytes = 0u64;
            let mut local_errors = 0u64;
            let mut counter = 0u64;

            let mut rng = rand::thread_rng();
            let batch_delay = Duration::from_millis(500);
            let mut batch_number = 0;

            let mut write_opts = WriteOptions::default();
            write_opts.disable_wal(false);
            if let RocksdbFsyncMode::SyncEach = fsync_mode_clone {
                write_opts.set_sync(true);
            } else {
                write_opts.set_sync(false);
            }

            while start_time.elapsed() < write_duration {
                let current_batch_size = match batch_number {
                    0 => 50_000,
                    1 => 100_000,
                    2 => 150_000,
                    3 => 200_000,
                    4 => 250_000,
                    5 => 300_000,
                    6 => 350_000,
                    7 => 400_000,
                    8 => 450_000,
                    _ => 500_000,
                };

                for _ in 0..current_batch_size {
                    if start_time.elapsed() >= write_duration {
                        break;
                    }

                    let size = rng.gen_range(500..=1024);
                    let data = vec![(counter % 256) as u8; size];
                    let key = format!("{}:{}", topic, counter);

                    match db_clone.put_opt(key.as_bytes(), data.as_slice(), &write_opts) {
                        Ok(_) => {
                            local_writes += 1;
                            local_write_bytes += data.len() as u64;
                            total_writes_clone.fetch_add(1, Ordering::Relaxed);
                            total_write_bytes_clone
                                .fetch_add(data.len() as u64, Ordering::Relaxed);
                        }
                        Err(err) => {
                            eprintln!(
                                "Thread {} put error: {} (topic {})",
                                thread_id, err, topic
                            );
                            local_errors += 1;
                        }
                    }

                    counter += 1;
                }

                batch_number += 1;

                if start_time.elapsed() < write_duration {
                    thread::sleep(batch_delay);
                }
            }

            write_errors_clone.fetch_add(local_errors, Ordering::Relaxed);

            println!(
                "Thread {} ({}): {} writes, {} KB, {} errors",
                thread_id,
                topic,
                local_writes,
                local_write_bytes / 1024,
                local_errors
            );

            write_end_barrier_clone.wait();
        });

        handles.push(handle);
    }

    let benchmark_start = Instant::now();
    start_barrier.wait();
    println!("All threads started! Write phase beginning...");
    let _ = throughput_tx.send(());

    write_end_barrier.wait();
    let write_elapsed = benchmark_start.elapsed();
    println!("Write phase completed in {:?}", write_elapsed);

    let final_writes = total_writes.load(Ordering::Relaxed);
    let final_write_bytes = total_write_bytes.load(Ordering::Relaxed);
    let final_errors = write_errors.load(Ordering::Relaxed);

    println!("\n=== Write Phase Results ===");
    println!("Write Duration: {:?}", write_elapsed);
    println!("Total Operations: {}", final_writes);
    println!("Total Bytes: {} MB", final_write_bytes / (1024 * 1024));
    println!("Write Errors: {}", final_errors);
    println!(
        "Write Throughput: {:.0} ops/sec",
        final_writes as f64 / write_elapsed.as_secs_f64()
    );
    println!(
        "Write Bandwidth: {:.2} MB/sec",
        (final_write_bytes as f64 / (1024.0 * 1024.0)) / write_elapsed.as_secs_f64()
    );

    for handle in handles {
        handle.join().expect("Writer thread panicked");
    }

    wal_sync_stop.store(true, Ordering::Relaxed);
    if let Some(handle) = wal_sync_handle {
        let _ = handle.join();
    }

    if let Err(err) = monitor_handle.join() {
        eprintln!("Monitor thread terminated with error: {:?}", err);
    }

    if let RocksdbFsyncMode::SyncEach = fsync_mode {
        if let Err(err) = db.flush_wal(true) {
            eprintln!("Error during final WAL flush: {}", err);
        }
    } else if let RocksdbFsyncMode::Async(_) = fsync_mode {
        if let Err(err) = db.flush_wal(true) {
            eprintln!("Error during final WAL flush: {}", err);
        }
    }

    let total_elapsed = benchmark_start.elapsed();
    println!("\n=== Final Summary ===");
    println!("Total Benchmark Duration: {:?}", total_elapsed);

    assert!(
        final_writes > 1000,
        "Write throughput too low: {} ops",
        final_writes
    );
    assert!(
        final_errors < final_writes / 10,
        "Too many write errors: {} out of {}",
        final_errors,
        final_writes
    );

    println!("RocksDB multi-threaded benchmark completed successfully!");

    drop(db);
    cleanup_path(db_path);
}
