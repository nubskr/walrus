#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import os

if not os.path.exists('read_benchmark_throughput.csv'):
    print("❌ read_benchmark_throughput.csv not found")
    print("💡 Run the read benchmark first:")
    print("   cargo test --release multithreaded_benchmark_reads -- --nocapture")
    exit(1)

# Read the data
df = pd.read_csv('read_benchmark_throughput.csv')

# Create subplots: 2x2 grid for throughput and bandwidth
fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))

# Write phase throughput
write_data = df[df['phase'] == 'write']
if not write_data.empty:
    # Adjust time to start from 0 for write phase
    write_start_time = write_data['elapsed_seconds'].min()
    write_time_adjusted = write_data['elapsed_seconds'] - write_start_time
    
    ax1.plot(write_time_adjusted, write_data['writes_per_second'], 'b-', linewidth=2)
    ax1.set_xlabel('Time (seconds)')
    ax1.set_ylabel('Writes/sec')
    ax1.set_title('Write Phase - Throughput')
    ax1.grid(True, alpha=0.3)
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))

# Write phase bandwidth
if not write_data.empty:
    ax2.plot(write_time_adjusted, write_data['write_bytes_per_second'] / (1024*1024), 'g-', linewidth=2)
    ax2.set_xlabel('Time (seconds)')
    ax2.set_ylabel('Write MB/sec')
    ax2.set_title('Write Phase - Bandwidth')
    ax2.grid(True, alpha=0.3)
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:.1f}'))

# Read phase throughput
read_data = df[df['phase'] == 'read']
if not read_data.empty:
    # Adjust time to start from 0 for read phase
    read_start_time = read_data['elapsed_seconds'].min()
    read_time_adjusted = read_data['elapsed_seconds'] - read_start_time
    
    ax3.plot(read_time_adjusted, read_data['reads_per_second'], 'r-', linewidth=2)
    ax3.set_xlabel('Time (seconds)')
    ax3.set_ylabel('Reads/sec')
    ax3.set_title('Read Phase - Throughput')
    ax3.grid(True, alpha=0.3)
    ax3.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))

# Read phase bandwidth
if not read_data.empty:
    ax4.plot(read_time_adjusted, read_data['read_bytes_per_second'] / (1024*1024), 'orange', linewidth=2)
    ax4.set_xlabel('Time (seconds)')
    ax4.set_ylabel('Read MB/sec')
    ax4.set_title('Read Phase - Bandwidth')
    ax4.grid(True, alpha=0.3)
    ax4.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:.1f}'))

plt.tight_layout()
plt.show()

# Print summary stats
print("\n📊 Read Benchmark Summary:")
if not write_data.empty:
    max_writes = write_data['writes_per_second'].max()
    avg_writes = write_data['writes_per_second'].mean()
    print(f"Write Phase - Max: {max_writes:,.0f} ops/sec, Avg: {avg_writes:,.0f} ops/sec")

if not read_data.empty:
    max_reads = read_data['reads_per_second'].max()
    avg_reads = read_data['reads_per_second'].mean()
    print(f"Read Phase  - Max: {max_reads:,.0f} ops/sec, Avg: {avg_reads:,.0f} ops/sec")

print("Data saved to: read_benchmark_throughput.csv")
