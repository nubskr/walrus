#!/usr/bin/env python3
"""
Real-time WAL Benchmark Throughput Visualizer

This script monitors the benchmark_throughput.csv file and displays
real-time graphs of write throughput and bandwidth.

Usage:
    python visualize_throughput.py [--file benchmark_throughput.csv]

Requirements:
    pip install matplotlib pandas
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import matplotlib.ticker as ticker
import argparse
import os
import time
from datetime import datetime

class ThroughputVisualizer:
    def __init__(self, csv_file='benchmark_throughput.csv'):
        self.csv_file = csv_file
        self.fig, (self.ax1, self.ax2) = plt.subplots(2, 1, figsize=(12, 8))
        self.fig.suptitle('WAL Benchmark Throughput Monitor', fontsize=16, fontweight='bold')
        
        # Configure subplots
        self.ax1.set_title('Write Throughput (Operations/Second)')
        self.ax1.set_xlabel('Time (seconds)')
        self.ax1.set_ylabel('Writes/sec')
        self.ax1.grid(True, alpha=0.3)
        
        self.ax2.set_title('Write Bandwidth (MB/Second)')
        self.ax2.set_xlabel('Time (seconds)')
        self.ax2.set_ylabel('MB/sec')
        self.ax2.grid(True, alpha=0.3)
        
        # Set up better Y-axis formatting
        self.setup_axis_formatting()
        
        # Style
        plt.style.use('seaborn-v0_8' if 'seaborn-v0_8' in plt.style.available else 'default')
    
    def setup_axis_formatting(self):
        """Set up better Y-axis formatting to avoid scientific notation"""
        def format_thousands(x, pos):
            """Format large numbers with K, M suffixes instead of scientific notation"""
            if x >= 1_000_000:
                return f'{x/1_000_000:.1f}M'
            elif x >= 1_000:
                return f'{x/1_000:.1f}K'
            else:
                return f'{x:.0f}'
        
        def format_bandwidth(x, pos):
            """Format bandwidth numbers with proper MB formatting"""
            if x >= 1_000:
                return f'{x/1_000:.1f}GB/s'
            elif x >= 1:
                return f'{x:.1f}MB/s'
            else:
                return f'{x*1000:.0f}KB/s'
        
        # Apply formatters to both axes
        self.ax1.yaxis.set_major_formatter(ticker.FuncFormatter(format_thousands))
        self.ax2.yaxis.set_major_formatter(ticker.FuncFormatter(format_bandwidth))
        
        # Set reasonable tick spacing
        self.ax1.yaxis.set_major_locator(ticker.MaxNLocator(nbins=8, integer=False))
        self.ax2.yaxis.set_major_locator(ticker.MaxNLocator(nbins=8, integer=False))
        
    def update_plot(self, frame):
        """Update the plot with new data from CSV"""
        try:
            if not os.path.exists(self.csv_file):
                return
                
            # Read CSV data
            df = pd.read_csv(self.csv_file)
            
            if df.empty:
                return
                
            # Clear previous plots
            self.ax1.clear()
            self.ax2.clear()
            
            # Plot throughput
            self.ax1.plot(df['elapsed_seconds'], df['writes_per_second'], 
                         'b-', linewidth=2, label='Writes/sec')
            self.ax1.fill_between(df['elapsed_seconds'], df['writes_per_second'], 
                                 alpha=0.3, color='blue')
            
            # Plot bandwidth (convert bytes to MB)
            bandwidth_mb = df['bytes_per_second'] / (1024 * 1024)
            self.ax2.plot(df['elapsed_seconds'], bandwidth_mb, 
                         'r-', linewidth=2, label='MB/sec')
            self.ax2.fill_between(df['elapsed_seconds'], bandwidth_mb, 
                                 alpha=0.3, color='red')
            
            # Styling
            self.ax1.set_title('Write Throughput (Operations/Second)')
            self.ax1.set_xlabel('Time (seconds)')
            self.ax1.set_ylabel('Writes/sec')
            self.ax1.grid(True, alpha=0.3)
            self.ax1.legend()
            
            self.ax2.set_title('Write Bandwidth (MB/Second)')
            self.ax2.set_xlabel('Time (seconds)')
            self.ax2.set_ylabel('MB/sec')
            self.ax2.grid(True, alpha=0.3)
            self.ax2.legend()
            
            # Reapply formatting after clearing
            self.setup_axis_formatting()
            
            # Add statistics text
            if not df.empty:
                latest = df.iloc[-1]
                max_throughput = df['writes_per_second'].max()
                max_bandwidth = bandwidth_mb.max()
                avg_throughput = df['writes_per_second'].mean()
                avg_bandwidth = bandwidth_mb.mean()
                
                stats_text = f"""Current: {latest['writes_per_second']:.0f} writes/sec, {bandwidth_mb.iloc[-1]:.2f} MB/sec
Max: {max_throughput:.0f} writes/sec, {max_bandwidth:.2f} MB/sec
Avg: {avg_throughput:.0f} writes/sec, {avg_bandwidth:.2f} MB/sec
Total: {latest['total_writes']:,} writes"""
                
                self.fig.text(0.02, 0.02, stats_text, fontsize=10, 
                             bbox=dict(boxstyle="round,pad=0.3", facecolor="lightgray", alpha=0.8))
            
            plt.tight_layout()
            
        except Exception as e:
            print(f"Error updating plot: {e}")
    
    def start_monitoring(self, interval=1000):
        """Start real-time monitoring"""
        print(f"🚀 Starting real-time monitoring of {self.csv_file}")
        print("📊 Waiting for benchmark data...")
        print("💡 Close the plot window to stop monitoring")
        
        # Animation
        ani = animation.FuncAnimation(self.fig, self.update_plot, 
                                    interval=interval, blit=False, cache_frame_data=False)
        
        try:
            plt.show()
        except KeyboardInterrupt:
            print("\n⏹️  Monitoring stopped by user")

def main():
    parser = argparse.ArgumentParser(description='Visualize WAL benchmark throughput in real-time')
    parser.add_argument('--file', '-f', default='benchmark_throughput.csv',
                       help='CSV file to monitor (default: benchmark_throughput.csv)')
    parser.add_argument('--interval', '-i', type=int, default=1000,
                       help='Update interval in milliseconds (default: 1000)')
    
    args = parser.parse_args()
    
    print("🎯 WAL Benchmark Throughput Visualizer")
    print("=" * 40)
    
    if not os.path.exists(args.file):
        print(f"⚠️  CSV file '{args.file}' not found.")
        print("💡 Run the benchmark first to generate data:")
        print("   cargo test --test multithreaded_benchmark -- --nocapture")
        return
    
    visualizer = ThroughputVisualizer(args.file)
    visualizer.start_monitoring(args.interval)

if __name__ == '__main__':
    main()
