use rand::Rng;
use std::env;
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use walrus_rust::wal::{FsyncSchedule, ReadConsistency, Walrus};

fn parse_thread_range() -> (usize, usize) {
    if let Ok(threads_env) = env::var("WALRUS_THREADS") {
        if let Some((start_str, end_str)) = threads_env.split_once('-') {
            if let (Ok(start), Ok(end)) = (start_str.parse::<usize>(), end_str.parse::<usize>()) {
                if start > 0 && end >= start && end <= 128 {
                    return (start, end);
                }
            }
        } else if let Ok(max_threads) = threads_env.parse::<usize>() {
            if max_threads > 0 && max_threads <= 128 {
                return (1, max_threads);
            }
        }
    }

    let args: Vec<String> = env::args().collect();

    for i in 0..args.len() {
        if args[i] == "--threads" && i + 1 < args.len() {
            let threads_arg = &args[i + 1];
            if let Some((start_str, end_str)) = threads_arg.split_once('-') {
                if let (Ok(start), Ok(end)) = (start_str.parse::<usize>(), end_str.parse::<usize>())
                {
                    if start > 0 && end >= start && end <= 128 {
                        return (start, end);
                    }
                }
            } else if let Ok(max_threads) = threads_arg.parse::<usize>() {
                if max_threads > 0 && max_threads <= 128 {
                    return (1, max_threads);
                }
            }
        }
    }

    (1, 10)
}

fn parse_fsync_schedule() -> FsyncSchedule {
    if let Ok(fsync_env) = env::var("WALRUS_FSYNC") {
        match fsync_env.as_str() {
            "sync-each" => return FsyncSchedule::SyncEach,
            "no-fsync" | "none" => return FsyncSchedule::NoFsync,
            "async" => return FsyncSchedule::Milliseconds(1000),
            ms_str if ms_str.ends_with("ms") => {
                if let Ok(ms) = ms_str[..ms_str.len() - 2].parse::<u64>() {
                    return FsyncSchedule::Milliseconds(ms);
                }
            }
            ms_str => {
                if let Ok(ms) = ms_str.parse::<u64>() {
                    return FsyncSchedule::Milliseconds(ms);
                }
            }
        }
    }

    let args: Vec<String> = env::args().collect();

    for i in 0..args.len() {
        if args[i] == "--fsync" && i + 1 < args.len() {
            match args[i + 1].as_str() {
                "sync-each" => return FsyncSchedule::SyncEach,
                "no-fsync" | "none" => return FsyncSchedule::NoFsync,
                "async" => return FsyncSchedule::Milliseconds(1000),
                ms_str if ms_str.ends_with("ms") => {
                    if let Ok(ms) = ms_str[..ms_str.len() - 2].parse::<u64>() {
                        return FsyncSchedule::Milliseconds(ms);
                    }
                }
                ms_str => {
                    if let Ok(ms) = ms_str.parse::<u64>() {
                        return FsyncSchedule::Milliseconds(ms);
                    }
                }
            }
        }
    }

    FsyncSchedule::Milliseconds(1000)
}

fn parse_batch_size() -> usize {
    if let Ok(batch_env) = env::var("WALRUS_BATCH_SIZE") {
        if let Ok(size) = batch_env.parse::<usize>() {
            if size > 0 && size <= 10_000 {
                return size;
            }
        }
    }

    let args: Vec<String> = env::args().collect();

    for i in 0..args.len() {
        if args[i] == "--batch-size" && i + 1 < args.len() {
            if let Ok(size) = args[i + 1].parse::<usize>() {
                if size > 0 && size <= 10_000 {
                    return size;
                }
            }
        }
    }

    256
}

fn parse_storage_backend() -> Option<String> {
    fn normalize(value: &str) -> Option<String> {
        match value.to_ascii_lowercase().as_str() {
            "fd" | "io_uring" | "uring" | "file" => Some("fd".to_string()),
            "mmap" | "memory" => Some("mmap".to_string()),
            _ => None,
        }
    }

    if let Ok(backend_env) = env::var("WALRUS_BACKEND") {
        if let Some(norm) = normalize(&backend_env) {
            return Some(norm);
        }
    }

    let args: Vec<String> = env::args().collect();
    for i in 0..args.len() {
        if args[i] == "--backend" && i + 1 < args.len() {
            if let Some(norm) = normalize(&args[i + 1]) {
                return Some(norm);
            }
        }
    }

    None
}

fn configure_storage_backend() {
    let selection = parse_storage_backend();
    #[cfg(target_os = "linux")]
    {
        match selection.as_deref() {
            Some("mmap") => {
                walrus_rust::disable_fd_backend();
                println!("Storage backend: mmap");
            }
            Some("fd") | Some("io_uring") | Some("uring") | Some("file") | None => {
                walrus_rust::enable_fd_backend();
                println!("Storage backend: fd");
            }
            Some(other) => {
                println!(
                    "Unknown storage backend '{}'; defaulting to fd (io_uring) backend.",
                    other
                );
                walrus_rust::enable_fd_backend();
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        if let Some(choice) = selection {
            println!(
                "Storage backend '{}' requested, but only the mmap backend is available on this platform.",
                choice
            );
        }
    }
}

fn print_usage() {
    println!(
        "Usage: WALRUS_FSYNC=<schedule> WALRUS_THREADS=<range> WALRUS_BATCH_SIZE=<entries> WALRUS_BACKEND=<fd|mmap>"
    );
    println!(
        "       cargo test batch_scaling_benchmark -- --fsync <schedule> --threads <range> --batch-size <entries> --backend <fd|mmap>"
    );
    println!();
    println!("Fsync Schedule Options:");
    println!("  sync-each    Fsync after every write (slowest, most durable)");
    println!("  no-fsync     Disable fsyncing entirely (fastest, no durability)");
    println!("  none         Same as no-fsync");
    println!("  async        Async fsync every 1000ms (default)");
    println!("  <number>ms   Async fsync every N milliseconds (e.g., 500ms)");
    println!("  <number>     Async fsync every N milliseconds (e.g., 500)");
    println!();
    println!("Thread Range Options:");
    println!("  <number>     Test from 1 to N threads (e.g., 8 means 1-8)");
    println!("  <start-end>  Test from start to end threads (e.g., 2-16)");
    println!("  Default: 1-10 threads");
    println!();
    println!("Batch Size Options:");
    println!("  <number>     Entries per batch (1-10000, default: 256)");
    println!();
    println!("Storage Backend Options (Linux only):");
    println!("  fd           Use fd/io_uring backend (default)");
    println!("  mmap         Use memory-mapped backend");
    println!("  Set via WALRUS_BACKEND=<fd|mmap> or --backend <value>");
    println!();
    println!("Examples:");
    println!(
        "  WALRUS_FSYNC=sync-each WALRUS_THREADS=12 WALRUS_BATCH_SIZE=128 cargo test batch_scaling_benchmark"
    );
    println!("  WALRUS_THREADS=4-32 cargo test batch_scaling_benchmark -- --batch-size 512");
    println!("  make bench-batch-scaling  # Uses Makefile convenience target");
}

fn cleanup_wal() {
    let _ = fs::remove_dir_all("wal_files");
    let _ = fs::remove_file("wal_files/read_offset_idx_index.db");
    let _ = fs::remove_file("wal_files/read_offset_idx_index.db.tmp");
    thread::sleep(Duration::from_millis(200));
    std::hint::black_box(());
}

#[derive(Debug)]
struct BatchScalingResult {
    batches_per_sec: f64,
    entries_per_sec: f64,
    mb_per_sec: f64,
}

fn run_benchmark_with_threads(num_threads: usize, batch_size: usize) -> BatchScalingResult {
    cleanup_wal();

    unsafe {
        std::env::set_var("WALRUS_QUIET", "1");
    }

    thread::sleep(Duration::from_millis(100));

    let fsync_schedule = parse_fsync_schedule();
    let wal = Arc::new(
        Walrus::with_consistency_and_schedule(
            ReadConsistency::AtLeastOnce {
                persist_every: 5000,
            },
            fsync_schedule,
        )
        .expect("Failed to create Walrus"),
    );

    let test_duration = Duration::from_secs(30);

    let total_batches = Arc::new(AtomicU64::new(0));
    let total_entries = Arc::new(AtomicU64::new(0));
    let total_bytes = Arc::new(AtomicU64::new(0));
    let batch_errors = Arc::new(AtomicU64::new(0));

    let start_barrier = Arc::new(Barrier::new(num_threads + 1));
    let write_end_barrier = Arc::new(Barrier::new(num_threads + 1));
    let topics: Vec<String> = (0..num_threads)
        .map(|i| format!("batch_scaling_topic_{}", i))
        .collect();

    let mut handles = Vec::new();

    for thread_id in 0..num_threads {
        let wal_clone = Arc::clone(&wal);
        let total_batches_clone = Arc::clone(&total_batches);
        let total_entries_clone = Arc::clone(&total_entries);
        let total_bytes_clone = Arc::clone(&total_bytes);
        let batch_errors_clone = Arc::clone(&batch_errors);
        let start_barrier_clone = Arc::clone(&start_barrier);
        let write_end_barrier_clone = Arc::clone(&write_end_barrier);
        let topic = topics[thread_id].clone();

        let handle = thread::spawn(move || {
            start_barrier_clone.wait();

            let start_time = Instant::now();
            let mut rng = rand::thread_rng();
            let mut local_batches = 0u64;
            let mut local_entries = 0u64;
            let mut local_bytes = 0u64;
            let mut local_errors = 0u64;
            let mut counter = 0u64;

            while start_time.elapsed() < test_duration {
                let mut batch_data = Vec::with_capacity(batch_size);
                let mut batch_bytes = 0usize;

                for _ in 0..batch_size {
                    let size = rng.gen_range(500..=1024);
                    let byte = (counter & 0xFF) as u8;
                    batch_data.push(vec![byte; size]);
                    batch_bytes += size;
                    counter += 1;
                }

                let batch_refs: Vec<&[u8]> = batch_data.iter().map(|v| v.as_slice()).collect();

                match wal_clone.batch_append_for_topic(&topic, &batch_refs) {
                    Ok(_) => {
                        local_batches += 1;
                        local_entries += batch_size as u64;
                        local_bytes += batch_bytes as u64;

                        total_batches_clone.fetch_add(1, Ordering::Relaxed);
                        total_entries_clone.fetch_add(batch_size as u64, Ordering::Relaxed);
                        total_bytes_clone.fetch_add(batch_bytes as u64, Ordering::Relaxed);
                    }
                    Err(e) => {
                        local_errors += 1;
                        batch_errors_clone.fetch_add(1, Ordering::Relaxed);
                        if e.kind() == std::io::ErrorKind::WouldBlock {
                            thread::sleep(Duration::from_micros(500));
                        } else {
                            eprintln!("Thread {} batch error: {}", thread_id, e);
                        }
                    }
                }
            }

            println!(
                "Thread {}: {} batches, {} entries, {:.2} MB, {} errors",
                thread_id,
                local_batches,
                local_entries,
                local_bytes as f64 / (1024.0 * 1024.0),
                local_errors
            );

            write_end_barrier_clone.wait();
        });

        handles.push(handle);
    }

    let benchmark_start = Instant::now();
    start_barrier.wait();
    write_end_barrier.wait();
    let elapsed = benchmark_start.elapsed();

    for handle in handles {
        let _ = handle.join();
    }

    let batches = total_batches.load(Ordering::Relaxed);
    let entries = total_entries.load(Ordering::Relaxed);
    let bytes = total_bytes.load(Ordering::Relaxed);
    let errors = batch_errors.load(Ordering::Relaxed);

    println!(
        "{} threads summary: {} batches, {} entries, {:.2} MB, {} errors in {:?}",
        num_threads,
        batches,
        entries,
        bytes as f64 / (1024.0 * 1024.0),
        errors,
        elapsed
    );

    drop(wal);
    cleanup_wal();

    let seconds = elapsed.as_secs_f64().max(0.001);
    BatchScalingResult {
        batches_per_sec: batches as f64 / seconds,
        entries_per_sec: entries as f64 / seconds,
        mb_per_sec: (bytes as f64 / (1024.0 * 1024.0)) / seconds,
    }
}

#[test]
fn batch_scaling_benchmark() {
    let args: Vec<String> = env::args().collect();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        print_usage();
        return;
    }

    let fsync_schedule = parse_fsync_schedule();
    let (start_threads, end_threads) = parse_thread_range();
    let batch_size = parse_batch_size();

    configure_storage_backend();

    println!("=== WAL Batch Scaling Benchmark ===");
    println!(
        "Testing batch throughput scaling from {} to {} threads",
        start_threads, end_threads
    );
    println!(
        "Each test runs for 30 seconds with batch size {}",
        batch_size
    );
    println!("Fsync schedule: {:?}", fsync_schedule);
    println!();

    let mut results = Vec::new();

    for num_threads in start_threads..=end_threads {
        println!("Testing with {} thread(s)...", num_threads);
        let result = run_benchmark_with_threads(num_threads, batch_size);
        println!(
            "{} threads throughput: {:.0} entries/sec, {:.2} MB/sec ({:.1} batches/sec)",
            num_threads, result.entries_per_sec, result.mb_per_sec, result.batches_per_sec
        );
        println!();
        results.push((num_threads, result));
        thread::sleep(Duration::from_millis(800));
    }

    cleanup_wal();

    println!("=== Batch Scaling Results ===");
    println!("Threads | Entries/sec | MB/sec | Batches/sec");
    println!("--------|-------------|--------|-------------");
    for (threads, result) in &results {
        println!(
            "{:7} | {:11.0} | {:6.2} | {:11.1}",
            threads, result.entries_per_sec, result.mb_per_sec, result.batches_per_sec
        );
    }

    let csv_rows = results
        .iter()
        .map(|(threads, res)| {
            format!(
                "{},{:.0},{:.2},{:.1}",
                threads, res.entries_per_sec, res.mb_per_sec, res.batches_per_sec
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    let csv_data = format!(
        "threads,entries_per_second,mb_per_second,batches_per_second\n{}",
        csv_rows
    );
    fs::write("batch_scaling_results.csv", csv_data).expect("Failed to write batch scaling CSV");

    println!("\nResults saved to: batch_scaling_results.csv");
    println!("Run 'make show-batch-scaling' to visualize the scaling curve");

    let best = results
        .iter()
        .map(|(_, res)| res.entries_per_sec)
        .fold(0.0_f64, f64::max);
    let baseline = results
        .first()
        .map(|(_, res)| res.entries_per_sec)
        .unwrap_or(0.0);

    assert!(
        best > baseline,
        "Multi-threading should improve entries/sec throughput"
    );
    assert!(
        baseline > 10_000.0,
        "Single thread throughput too low: {:.0} entries/sec",
        baseline
    );

    println!(
        "Best entries throughput: {:.0} entries/sec ({:.1}x over single thread)",
        best,
        if baseline > 0.0 { best / baseline } else { 0.0 }
    );
}
