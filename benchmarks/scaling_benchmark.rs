use walrus::wal::Walrus;
use std::fs;
use std::thread;
use std::time::{Duration, Instant};
use std::sync::{Arc, Barrier};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::io::Write;
use rand::Rng;

fn cleanup_wal() {
    // Remove WAL files directory
    let _ = fs::remove_dir_all("wal_files");
    
    // Remove any leftover index files
    let _ = fs::remove_file("wal_files/read_offset_idx_index.db");
    let _ = fs::remove_file("wal_files/read_offset_idx_index.db.tmp");
    
    // Give filesystem time to clean up completely
    thread::sleep(Duration::from_millis(200));
    
    // Force garbage collection to clean up any lingering memory
    // This helps ensure each test starts with a clean slate
    std::hint::black_box(());
}

fn run_benchmark_with_threads(num_threads: usize) -> f64 {
    // Thorough cleanup before each test
    cleanup_wal();
    
    // Enable quiet mode to suppress debug output
    unsafe {
        std::env::set_var("WALRUS_QUIET", "1");
    }
    
    // Small delay to ensure complete cleanup
    thread::sleep(Duration::from_millis(100));
    
    let wal = Arc::new(Walrus::with_consistency(walrus::ReadConsistency::AtLeastOnce { persist_every: 50 }).expect("Failed to create Walrus"));
    let test_duration = Duration::from_secs(30); // 30 seconds per test
    
    // Shared counters for statistics
    let total_writes = Arc::new(AtomicU64::new(0));
    let write_errors = Arc::new(AtomicU64::new(0));
    
    // Barrier to synchronize thread start
    let start_barrier = Arc::new(Barrier::new(num_threads + 1)); // +1 for main thread
    let write_end_barrier = Arc::new(Barrier::new(num_threads + 1));
    
    // Generate topic names for each thread
    let topics: Vec<String> = (0..num_threads)
        .map(|i| format!("topic_{}", i))
        .collect();
    
    // Spawn writer threads
    let mut handles = Vec::new();
    for thread_id in 0..num_threads {
        let wal_clone = Arc::clone(&wal);
        let total_writes_clone = Arc::clone(&total_writes);
        let write_errors_clone = Arc::clone(&write_errors);
        let start_barrier_clone = Arc::clone(&start_barrier);
        let write_end_barrier_clone = Arc::clone(&write_end_barrier);
        let topic = topics[thread_id].clone();
        
        let handle = thread::spawn(move || {
            // Wait for all threads to be ready
            start_barrier_clone.wait();
            
            let start_time = Instant::now();
            let mut local_writes = 0u64;
            let mut local_errors = 0u64;
            let mut counter = 0u64;
            let mut rng = rand::thread_rng();
            
            // Write phase - write as fast as possible for test duration
            while start_time.elapsed() < test_duration {
                // Random entry size between 500B and 1KB
                let size = rng.gen_range(500..=1024);
                let data = vec![(counter % 256) as u8; size];
                
                match wal_clone.append_for_topic(&topic, &data) {
                    Ok(_) => {
                        local_writes += 1;
                        // Update global counter in real-time
                        total_writes_clone.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        local_errors += 1;
                        write_errors_clone.fetch_add(1, Ordering::Relaxed);
                    }
                }
                
                counter += 1;
                
                // Small gap after every 100k writes
                if counter % 100000 == 0 {
                    thread::sleep(Duration::from_micros(50));
                }
            }
            
            // Wait for all threads to finish writing
            write_end_barrier_clone.wait();
        });
        
        handles.push(handle);
    }
    
    // Start all threads simultaneously
    let benchmark_start = Instant::now();
    start_barrier.wait();
    
    // Wait for write phase to complete
    write_end_barrier.wait();
    let write_elapsed = benchmark_start.elapsed();
    
    // Wait for all threads to complete
    for handle in handles {
        let _ = handle.join().unwrap();
    }
    
    let final_writes = total_writes.load(Ordering::Relaxed);
    let final_errors = write_errors.load(Ordering::Relaxed);
    
    // Calculate throughput (ops/sec)
    let throughput = final_writes as f64 / write_elapsed.as_secs_f64();
    
    println!("{} threads: {} writes, {} errors, {:.0} ops/sec", 
        num_threads, final_writes, final_errors, throughput);
    
    // Explicit cleanup after test to ensure no residue
    drop(wal);
    
    // Additional cleanup to ensure no lingering state
    cleanup_wal();
    
    throughput
}

fn create_live_plot_script() {
    let plot_script = r#"#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import os
import time

class LiveScalingPlot:
    def __init__(self):
        self.fig, self.ax = plt.subplots(figsize=(10, 6))
        self.ax.set_xlabel('Number of Threads')
        self.ax.set_ylabel('Throughput (ops/sec)')
        self.ax.set_title('WAL Write Throughput Scaling (Live)')
        self.ax.grid(True, alpha=0.3)
        self.ax.set_xlim(0.5, 10.5)
        self.ax.set_xticks(range(1, 11))
        
        # Format Y-axis to avoid scientific notation
        self.ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
        
        plt.tight_layout()
        
    def update_plot(self, frame):
        if not os.path.exists('scaling_results_live.csv'):
            return
            
        try:
            df = pd.read_csv('scaling_results_live.csv')
            if df.empty:
                return
                
            self.ax.clear()
            
            # Plot the data
            self.ax.plot(df['threads'], df['throughput'], 'bo-', linewidth=2, markersize=8)
            
            # Styling
            self.ax.set_xlabel('Number of Threads')
            self.ax.set_ylabel('Throughput (ops/sec)')
            self.ax.set_title(f'WAL Write Throughput Scaling (Live) - {len(df)}/10 tests complete')
            self.ax.grid(True, alpha=0.3)
            self.ax.set_xlim(0.5, 10.5)
            self.ax.set_xticks(range(1, 11))
            
            # Format Y-axis
            self.ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
            
            # Set Y-axis limits with some padding
            if len(df) > 0:
                max_throughput = df['throughput'].max()
                self.ax.set_ylim(0, max_throughput * 1.1)
            
            plt.tight_layout()
            
        except Exception as e:
            print(f"Error updating plot: {e}")
    
    def start_monitoring(self):
        print("🚀 Starting live scaling visualization...")
        print("📊 Graph will update as each test completes")
        print("💡 Close the plot window to stop monitoring")
        
        ani = animation.FuncAnimation(self.fig, self.update_plot, 
                                    interval=1000, blit=False, cache_frame_data=False)
        
        try:
            plt.show()
        except KeyboardInterrupt:
            print("\n⏹️  Monitoring stopped by user")

if __name__ == '__main__':
    plotter = LiveScalingPlot()
    plotter.start_monitoring()
"#;
    
    fs::write("live_scaling_plot.py", plot_script).expect("Failed to write live plot script");
}

#[test]
fn scaling_benchmark() {
    println!("=== WAL Scaling Benchmark ===");
    println!("Testing throughput scaling from 1 to 10 threads");
    println!("Each test runs for 30 seconds with random 500B-1KB entries");
    println!();
    
    // Create the live plotting script
    create_live_plot_script();
    
    // Initialize CSV file for live updates
    fs::write("scaling_results_live.csv", "threads,throughput\n").expect("Failed to create live CSV");
    
    // Create channel for non-blocking CSV updates
    let (csv_tx, csv_rx) = mpsc::channel::<(usize, f64)>();
    
    // Spawn dedicated CSV writer thread
    let csv_handle = thread::spawn(move || {
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open("scaling_results_live.csv")
            .expect("Failed to open live CSV");
            
        while let Ok((threads, throughput)) = csv_rx.recv() {
            if let Err(e) = writeln!(file, "{},{:.0}", threads, throughput) {
                eprintln!("Failed to write to CSV: {}", e);
            }
            if let Err(e) = file.flush() {
                eprintln!("Failed to flush CSV: {}", e);
            }
        }
    });
    
    println!("📊 Live graph available! Run in another terminal:");
    println!("   python live_scaling_plot.py");
    println!();
    
    let mut results = Vec::new();
    
    // Test from 1 to 10 threads
    for num_threads in 1..=10 {
        println!("Testing with {} thread(s)...", num_threads);
        let throughput = run_benchmark_with_threads(num_threads);
        results.push((num_threads, throughput));
        
        // Send to CSV writer thread (non-blocking)
        let _ = csv_tx.send((num_threads, throughput));
        
        println!("✅ {} threads: {:.0} ops/sec (added to live graph)", num_threads, throughput);
        
        // Longer delay between tests to ensure complete cleanup
        thread::sleep(Duration::from_millis(1000));
    }
    
    // Close CSV writer thread
    drop(csv_tx);
    let _ = csv_handle.join();
    
    // Final cleanup
    cleanup_wal();
    
    println!("\n=== Scaling Results ===");
    println!("Threads | Throughput (ops/sec)");
    println!("--------|-------------------");
    for (threads, throughput) in &results {
        println!("{:7} | {:>17.0}", threads, throughput);
    }
    
    // Create a simple CSV for plotting
    let csv_content = results.iter()
        .map(|(threads, throughput)| format!("{},{:.0}", threads, throughput))
        .collect::<Vec<_>>()
        .join("\n");
    
    let csv_data = format!("threads,throughput\n{}", csv_content);
    fs::write("scaling_results.csv", csv_data).expect("Failed to write scaling results");
    
    // Create a simple Python script to show the graph
    let plot_script = r#"#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt

# Read the data
df = pd.read_csv('scaling_results.csv')

# Create the plot
plt.figure(figsize=(10, 6))
plt.plot(df['threads'], df['throughput'], 'bo-', linewidth=2, markersize=8)
plt.xlabel('Number of Threads')
plt.ylabel('Throughput (ops/sec)')
plt.title('WAL Write Throughput Scaling')
plt.grid(True, alpha=0.3)

# Format Y-axis to avoid scientific notation
ax = plt.gca()
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))

# Set integer ticks on X-axis
plt.xticks(range(1, 11))

# Add some styling
plt.tight_layout()

# Show the plot
plt.show()

print("Scaling benchmark complete!")
print("Data saved to: scaling_results.csv")
"#;
    
    fs::write("show_scaling_graph.py", plot_script).expect("Failed to write plot script");
    
    println!("\n📊 Results saved to: scaling_results.csv");
    println!("📈 Run 'python show_scaling_graph.py' to see the scaling graph");
    
    // Basic performance assertions
    let max_throughput = results.iter().map(|(_, t)| *t).fold(0.0, f64::max);
    let single_thread_throughput = results[0].1;
    
    assert!(max_throughput > single_thread_throughput, 
        "Multi-threading should improve performance");
    assert!(single_thread_throughput > 1000.0, 
        "Single thread throughput too low: {:.0} ops/sec", single_thread_throughput);
    
    println!("\n✅ Scaling benchmark completed successfully!");
    println!("📈 Best throughput: {:.0} ops/sec", max_throughput);
    println!("🚀 Scaling factor: {:.1}x", max_throughput / single_thread_throughput);
}
