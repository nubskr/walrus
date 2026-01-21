#!/usr/bin/env python3
"""
Non-interactive WAL Batch Benchmark Visualizer

This script monitors the batch benchmark CSV file and generates
real-time graphs of batch throughput, cumulative statistics, and performance metrics.
Designed for headless environments - saves images instead of displaying interactive plots.

Usage:
    # Real-time monitoring (default)
    python visualize_batch_benchmark_noninteractive.py --file batch_benchmark_throughput.csv
    
    # Single-shot mode with custom output prefix
    python visualize_batch_benchmark_noninteractive.py --file batch_benchmark_throughput.csv --single-shot --output-prefix batch_benchmark_
    
    # Real-time monitoring with custom interval
    python visualize_batch_benchmark_noninteractive.py --file batch_benchmark_throughput.csv --interval 2
    
    # Real-time monitoring with max frames
    python visualize_batch_benchmark_noninteractive.py --file batch_benchmark_throughput.csv --max-frames 100

Requirements:
    pip install matplotlib pandas
"""

import matplotlib
matplotlib.use('Agg')
import pandas as pd
import matplotlib.pyplot as plt
import argparse
import os
import time
from datetime import datetime

class BatchBenchmarkVisualizer:
    def __init__(self, csv_file, output_prefix='batch_benchmark'):
        self.csv_file = csv_file
        self.output_prefix = output_prefix
        self.timestamp = None
        plt.style.use('seaborn-v0_8' if 'seaborn-v0_8' in plt.style.available else 'default')
    
    def load_csv_data(self):
        if not os.path.exists(self.csv_file):
            return None
        
        try:
            df = pd.read_csv(self.csv_file)
            if df.empty:
                return None
            return df
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            return None
    
    def plot_throughput(self, df, output_file=None):
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"{self.output_prefix}throughput_{timestamp}.png"
        
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Batch Benchmark Throughput Analysis', fontsize=16, fontweight='bold')
        
        ax1 = axes[0, 0]
        ax1.plot(df['elapsed_seconds'], df['batches_per_second'], 'b-', linewidth=2, label='Batches/sec')
        ax1.set_xlabel('Time (seconds)')
        ax1.set_ylabel('Batches per second')
        ax1.set_title('Batch Throughput Over Time')
        ax1.grid(True, alpha=0.3)
        ax1.legend()
        
        ax2 = axes[0, 1]
        ax2.plot(df['elapsed_seconds'], df['entries_per_second'], 'g-', linewidth=2, label='Entries/sec')
        ax2.set_xlabel('Time (seconds)')
        ax2.set_ylabel('Entries per second')
        ax2.set_title('Entry Throughput Over Time')
        ax2.grid(True, alpha=0.3)
        ax2.legend()
        
        ax3 = axes[1, 0]
        ax3.plot(df['elapsed_seconds'], df['bytes_per_second'] / (1024 * 1024), 'r-', linewidth=2, label='MB/sec')
        ax3.set_xlabel('Time (seconds)')
        ax3.set_ylabel('Bandwidth (MB/sec)')
        ax3.set_title('Write Bandwidth Over Time')
        ax3.grid(True, alpha=0.3)
        ax3.legend()
        
        ax4 = axes[1, 1]
        ax4.plot(df['elapsed_seconds'], df['dirty_ratio_percent'], 'orange', linewidth=2, label='Dirty Page Ratio %')
        ax4_twin = ax4.twinx()
        ax4_twin.plot(df['elapsed_seconds'], df['dirty_pages_kb'] / 1024, 'purple', linewidth=1.5, linestyle='--', label='Dirty Pages (MB)')
        ax4.set_xlabel('Time (seconds)')
        ax4.set_ylabel('Dirty Page Ratio (%)', color='orange')
        ax4_twin.set_ylabel('Dirty Pages (MB)', color='purple')
        ax4.set_title('Memory Dirty Pages Over Time')
        ax4.grid(True, alpha=0.3)
        ax4.tick_params(axis='y', labelcolor='orange')
        ax4_twin.tick_params(axis='y', labelcolor='purple')
        
        lines1, labels1 = ax4.get_legend_handles_labels()
        lines2, labels2 = ax4_twin.get_legend_handles_labels()
        ax4.legend(lines1 + lines2, labels1 + labels2, loc='upper right')
        
        plt.tight_layout()
        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        plt.close()
        return output_file
    
    def plot_cumulative(self, df, output_file=None):
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"{self.output_prefix}cumulative_{timestamp}.png"
        
        fig, axes = plt.subplots(1, 3, figsize=(18, 6))
        fig.suptitle('Batch Benchmark Cumulative Statistics', fontsize=16, fontweight='bold')
        
        ax1 = axes[0]
        ax1.plot(df['elapsed_seconds'], df['total_batches'], 'b-', linewidth=2)
        ax1.set_xlabel('Time (seconds)')
        ax1.set_ylabel('Total Batches')
        ax1.set_title('Cumulative Batches')
        ax1.grid(True, alpha=0.3)
        
        ax2 = axes[1]
        ax2.plot(df['elapsed_seconds'], df['total_entries'], 'g-', linewidth=2)
        ax2.set_xlabel('Time (seconds)')
        ax2.set_ylabel('Total Entries')
        ax2.set_title('Cumulative Entries')
        ax2.grid(True, alpha=0.3)
        
        ax3 = axes[2]
        ax3.plot(df['elapsed_seconds'], df['total_bytes'] / (1024 * 1024), 'r-', linewidth=2)
        ax3.set_xlabel('Time (seconds)')
        ax3.set_ylabel('Total Bytes (MB)')
        ax3.set_title('Cumulative Bytes Written')
        ax3.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        plt.close()
        return output_file
    
    def plot_statistics_summary(self, df, output_file=None):
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"{self.output_prefix}statistics_{timestamp}.png"
        
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Batch Benchmark Statistics Summary', fontsize=16, fontweight='bold')
        
        throughput_data = df['entries_per_second'].dropna()
        bandwidth_data = df['bytes_per_second'].dropna() / (1024 * 1024)
        
        ax1 = axes[0, 0]
        ax1.hist(throughput_data, bins=50, color='green', alpha=0.7, edgecolor='black')
        ax1.set_xlabel('Entries/sec')
        ax1.set_ylabel('Frequency')
        ax1.set_title('Entry Throughput Distribution')
        ax1.grid(True, alpha=0.3, axis='y')
        ax1.axvline(throughput_data.mean(), color='red', linestyle='--', linewidth=2, label=f'Mean: {throughput_data.mean():.0f}')
        ax1.legend()
        
        ax2 = axes[0, 1]
        ax2.hist(bandwidth_data, bins=50, color='red', alpha=0.7, edgecolor='black')
        ax2.set_xlabel('Bandwidth (MB/sec)')
        ax2.set_ylabel('Frequency')
        ax2.set_title('Write Bandwidth Distribution')
        ax2.grid(True, alpha=0.3, axis='y')
        ax2.axvline(bandwidth_data.mean(), color='blue', linestyle='--', linewidth=2, label=f'Mean: {bandwidth_data.mean():.2f}')
        ax2.legend()
        
        ax3 = axes[1, 0]
        ax3.boxplot([throughput_data, bandwidth_data], tick_labels=['Entries/sec', 'MB/sec'])
        ax3.set_ylabel('Value')
        ax3.set_title('Performance Metrics Box Plot')
        ax3.grid(True, alpha=0.3, axis='y')
        
        ax4 = axes[1, 1]
        ax4.plot(df['elapsed_seconds'], df['entries_per_second'], 'g-', alpha=0.5, linewidth=1, label='Raw Data')
        ax4.plot(df['elapsed_seconds'], df['entries_per_second'].rolling(window=10, min_periods=1).mean(), 'b-', linewidth=2, label='Moving Avg (10)')
        ax4.plot(df['elapsed_seconds'], df['entries_per_second'].rolling(window=30, min_periods=1).mean(), 'r-', linewidth=2, label='Moving Avg (30)')
        ax4.set_xlabel('Time (seconds)')
        ax4.set_ylabel('Entries/sec')
        ax4.set_title('Throughput with Moving Average')
        ax4.grid(True, alpha=0.3)
        ax4.legend()
        
        plt.tight_layout()
        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        plt.close()
        return output_file
    
    def update_and_save_plots(self, frame):
        df = self.load_csv_data()
        
        if df is None:
            return False, None
        
        try:
            if self.timestamp is None:
                self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            throughput_file = f"{self.output_prefix}throughput_{self.timestamp}.png"
            cumulative_file = f"{self.output_prefix}cumulative_{self.timestamp}.png"
            statistics_file = f"{self.output_prefix}statistics_{self.timestamp}.png"
            
            self.plot_throughput(df, throughput_file)
            self.plot_cumulative(df, cumulative_file)
            self.plot_statistics_summary(df, statistics_file)
            
            return True, (throughput_file, cumulative_file, statistics_file)
        except Exception as e:
            print(f"Error updating plots: {e}")
            return False, None
    
    def generate_single_shot(self):
        df = self.load_csv_data()
        
        if df is None:
            print("Error: No data available or file not found")
            return False
        
        print(f"\nGenerating single-shot plots from {self.csv_file}...")
        
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            throughput_file = f"{self.output_prefix}throughput_{timestamp}.png"
            cumulative_file = f"{self.output_prefix}cumulative_{timestamp}.png"
            statistics_file = f"{self.output_prefix}statistics_{timestamp}.png"
            
            self.plot_throughput(df, throughput_file)
            print(f"Saved throughput chart to: {throughput_file}")
            
            self.plot_cumulative(df, cumulative_file)
            print(f"Saved cumulative chart to: {cumulative_file}")
            
            self.plot_statistics_summary(df, statistics_file)
            print(f"Saved statistics chart to: {statistics_file}")
            
            self.print_summary(df)
            return True
        except Exception as e:
            print(f"Error generating plots: {e}")
            return False
    
    def start_monitoring(self, interval=1, max_frames=None):
        print(f"Starting real-time monitoring of {self.csv_file}")
        print(f"Output prefix: {self.output_prefix}")
        print(f"Update interval: {interval} second(s)")
        print("Press Ctrl+C to stop monitoring\n")
        
        frame_count = 0
        last_data_time = None
        
        try:
            while True:
                if max_frames is not None and frame_count >= max_frames:
                    print(f"\nReached maximum frame count ({max_frames})")
                    break
                
                data_updated, files = self.update_and_save_plots(frame_count)
                
                if data_updated and files:
                    last_data_time = datetime.now()
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Frame {frame_count}: Plots updated")
                    print(f"  - {files[0]}")
                    print(f"  - {files[1]}")
                    print(f"  - {files[2]}")
                else:
                    if last_data_time:
                        elapsed = (datetime.now() - last_data_time).total_seconds()
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] Frame {frame_count}: Waiting for data... (last update {elapsed:.0f}s ago)")
                    else:
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] Frame {frame_count}: Waiting for data...")
                
                frame_count += 1
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print(f"\n\nMonitoring stopped by user")
            print(f"Total frames generated: {frame_count}")
            return True
        
        return True
    
    def print_summary(self, df):
        print("\n" + "="*80)
        print("Benchmark Summary")
        print("="*80)
        
        throughput = df['entries_per_second'].dropna()
        bandwidth = df['bytes_per_second'].dropna() / (1024 * 1024)
        
        print(f"\nDuration: {df['elapsed_seconds'].max():.2f} seconds")
        print(f"Total Batches: {df['total_batches'].max():,}")
        print(f"Total Entries: {df['total_entries'].max():,}")
        print(f"Total Bytes: {df['total_bytes'].max() / (1024**3):.2f} GB")
        
        print(f"\nThroughput Statistics (entries/sec):")
        print(f"  Mean: {throughput.mean():.0f}")
        print(f"  Median:  {throughput.median():.0f}")
        print(f"  Min:     {throughput.min():.0f}")
        print(f"  Max:     {throughput.max():.0f}")
        print(f"  Std Dev: {throughput.std():.0f}")
        
        print(f"\nBandwidth Statistics (MB/sec):")
        print(f"  Mean: {bandwidth.mean():.2f}")
        print(f"  Median:  {bandwidth.median():.2f}")
        print(f"  Min:     {bandwidth.min():.2f}")
        print(f"  Max:     {bandwidth.max():.2f}")
        print(f"  Std Dev: {bandwidth.std():.2f}")
        
        print(f"\nMemory Statistics:")
        print(f"  Max Dirty Pages: {df['dirty_pages_kb'].max() / 1024:.2f} MB")
        print(f"  Max Dirty Page Ratio: {df['dirty_ratio_percent'].max():.2f}%")
        
        print("="*80 + "\n")

def main():
    parser = argparse.ArgumentParser(
        description='Visualize batch benchmark results (non-interactive, headless-friendly)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Real-time monitoring with default settings
  python visualize_batch_benchmark_noninteractive.py --file batch_benchmark_throughput.csv
  
  # Real-time monitoring with custom interval (2 seconds)
  python visualize_batch_benchmark_noninteractive.py --file batch_benchmark_throughput.csv --interval 2
  
  # Real-time monitoring with max frames
  python visualize_batch_benchmark_noninteractive.py --file batch_benchmark_throughput.csv --max-frames 100
  
  # Single-shot mode with custom output prefix
  python visualize_batch_benchmark_noninteractive.py --file batch_benchmark_throughput.csv --single-shot --output-prefix batch_benchmark_
        """)
    
    parser.add_argument('--file', '-f', required=True,
                       help='CSV file to visualize')
    parser.add_argument('--output-prefix', '-o', default='batch_benchmark_',
                       help='Output file prefix (default: batch_benchmark_)')
    parser.add_argument('--interval', '-i', type=int, default=1,
                       help='Update interval in seconds (default: 1)')
    parser.add_argument('--single-shot', '-s', action='store_true',
                       help='Generate a single plot and exit (no monitoring)')
    parser.add_argument('--max-frames', type=int,
                       help='Maximum number of frames to generate before stopping (realtime mode only)')
    
    args = parser.parse_args()
    
    print("=" * 50)
    print("WAL Batch Benchmark Visualizer (Non-Interactive)")
    print("=" * 50)
    
    if not os.path.exists(args.file):
        print(f"\nError: CSV file '{args.file}' not found.")
        return
    
    visualizer = BatchBenchmarkVisualizer(args.file, args.output_prefix)
    
    if args.single_shot:
        visualizer.generate_single_shot()
    else:
        visualizer.start_monitoring(
            interval=args.interval,
            max_frames=args.max_frames
        )

if __name__ == '__main__':
    main()
