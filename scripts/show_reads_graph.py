#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import os

if not os.path.exists('read_benchmark_throughput.csv'):
    print("read_benchmark_throughput.csv not found")
    print("Run the read benchmark first:")
    print("   cargo test --release multithreaded_benchmark_reads -- --nocapture")
    exit(1)

# Read the data
df = pd.read_csv('read_benchmark_throughput.csv')

# Ensure we have expected columns
required_cols = {
    'elapsed_seconds',
    'phase',
    'write_mb_per_sec',
    'read_mb_per_sec',
    'total_write_mb',
    'total_read_mb',
}
missing = required_cols - set(df.columns)
if missing:
    raise SystemExit(
        f"CSV file is missing expected columns: {', '.join(sorted(missing))}. "
        "Re-run the benchmark to regenerate the file."
    )

# Create subplots: bandwidth + cumulative volume for both phases
fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 10))

write_data = df[df['phase'] == 'write']
read_data = df[df['phase'] == 'read']

def adjust_time(series):
    if series.empty:
        return series
    return series - series.min()

write_time = adjust_time(write_data['elapsed_seconds'])
read_time = adjust_time(read_data['elapsed_seconds'])

# Write bandwidth
if not write_data.empty:
    ax1.plot(write_time, write_data['write_mb_per_sec'], color='tab:blue', linewidth=2)
    ax1.set_title('Write Phase – Bandwidth')
    ax1.set_xlabel('Time (s)')
    ax1.set_ylabel('MB / s')
    ax1.grid(True, alpha=0.3)
    ax1.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{x:.1f}'))

# Write cumulative volume
if not write_data.empty:
    ax2.plot(write_time, write_data['total_write_mb'] / 1024, color='tab:green', linewidth=2)
    ax2.set_title('Write Phase – Cumulative Volume')
    ax2.set_xlabel('Time (s)')
    ax2.set_ylabel('Total GB')
    ax2.grid(True, alpha=0.3)
    ax2.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{x:.2f}'))

# Read bandwidth
if not read_data.empty:
    ax3.plot(read_time, read_data['read_mb_per_sec'], color='tab:orange', linewidth=2)
    ax3.set_title('Read Phase – Bandwidth')
    ax3.set_xlabel('Time (s)')
    ax3.set_ylabel('MB / s')
    ax3.grid(True, alpha=0.3)
    ax3.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{x:.1f}'))

# Read cumulative volume
if not read_data.empty:
    ax4.plot(read_time, read_data['total_read_mb'] / 1024, color='tab:red', linewidth=2)
    ax4.set_title('Read Phase – Cumulative Volume')
    ax4.set_xlabel('Time (s)')
    ax4.set_ylabel('Total GB')
    ax4.grid(True, alpha=0.3)
    ax4.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{x:.2f}'))

plt.tight_layout()
plt.show()

print("\nRead Benchmark Summary:")
if not write_data.empty:
    print(
        f"Write Phase - Max: {write_data['write_mb_per_sec'].max():.2f} MB/s, "
        f"Avg: {write_data['write_mb_per_sec'].mean():.2f} MB/s, "
        f"Total: {write_data['total_write_mb'].max() / 1024:.2f} GB"
    )
if not read_data.empty:
    print(
        f"Read Phase  - Max: {read_data['read_mb_per_sec'].max():.2f} MB/s, "
        f"Avg: {read_data['read_mb_per_sec'].mean():.2f} MB/s, "
        f"Total: {read_data['total_read_mb'].max() / 1024:.2f} GB"
    )

print("Data saved to: read_benchmark_throughput.csv")
