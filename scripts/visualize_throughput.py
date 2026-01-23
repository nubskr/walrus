#!/usr/bin/env python3
"""
Real-time WAL Benchmark Throughput Visualizer

This script monitors the benchmark_throughput.csv file and displays
real-time graphs of write throughput and bandwidth, or analyzes thread scaling.

Usage:
    python visualize_throughput.py [--file benchmark_throughput.csv]
    python visualize_throughput.py --mode scaling --thread-files 1:bench_1t.csv 2:bench_2t.csv 4:bench_4t.csv
    python visualize_throughput.py --headless --output plot.png

Requirements:
    pip install matplotlib pandas
"""

import pandas as pd
import matplotlib.ticker as ticker
import argparse
import os
import time
from datetime import datetime

# These will be imported after backend configuration in main()
plt = None
animation = None

class ThroughputVisualizer:
    def __init__(self, csv_file='benchmark_throughput.csv', mode='realtime'):
        self.csv_file = csv_file
        self.mode = mode
        
        if mode == 'realtime':
            self.fig, (self.ax1, self.ax2) = plt.subplots(2, 1, figsize=(12, 8))
            self.fig.suptitle('WAL Benchmark Throughput Monitor', fontsize=16, fontweight='bold')
            
            self.ax1.set_title('Write Throughput (Operations/Second)')
            self.ax1.set_xlabel('Time (seconds)')
            self.ax1.set_ylabel('Writes/sec')
            self.ax1.grid(True, alpha=0.3)
            
            self.ax2.set_title('Write Bandwidth (MB/Second)')
            self.ax2.set_xlabel('Time (seconds)')
            self.ax2.set_ylabel('MB/sec')
            self.ax2.grid(True, alpha=0.3)
            
            self.setup_axis_formatting()
            
        elif mode == 'thread-scaling':
            self.fig, self.ax = plt.subplots(1, 1, figsize=(12, 7))
            self.fig.suptitle('WAL Thread Scaling Analysis', fontsize=16, fontweight='bold')
        
        plt.style.use('seaborn-v0_8' if 'seaborn-v0_8' in plt.style.available else 'default')
    
    def setup_axis_formatting(self):
        def format_thousands(x, pos):
            if x >= 1_000_000:
                return f'{x/1_000_000:.1f}M'
            elif x >= 1_000:
                return f'{x/1_000:.1f}K'
            else:
                return f'{x:.0f}'
        
        def format_bandwidth(x, pos):
            if x >= 1_000:
                return f'{x/1_000:.1f}GB/s'
            elif x >= 1:
                return f'{x:.1f}MB/s'
            else:
                return f'{x*1000:.0f}KB/s'
        
        self.ax1.yaxis.set_major_formatter(ticker.FuncFormatter(format_thousands))
        self.ax2.yaxis.set_major_formatter(ticker.FuncFormatter(format_bandwidth))
        
        self.ax1.yaxis.set_major_locator(ticker.MaxNLocator(nbins=8, integer=False))
        self.ax2.yaxis.set_major_locator(ticker.MaxNLocator(nbins=8, integer=False))
        
    def update_plot(self, frame):
        try:
            if not os.path.exists(self.csv_file):
                return
                
            df = pd.read_csv(self.csv_file)
            
            if df.empty:
                return
                
            self.ax1.clear()
            self.ax2.clear()
            
            self.ax1.plot(df['elapsed_seconds'], df['writes_per_second'], 
                         'b-', linewidth=2, label='Writes/sec')
            self.ax1.fill_between(df['elapsed_seconds'], df['writes_per_second'], 
                                 alpha=0.3, color='blue')
            
            bandwidth_mb = df['bytes_per_second'] / (1024 * 1024)
            self.ax2.plot(df['elapsed_seconds'], bandwidth_mb, 
                         'r-', linewidth=2, label='MB/sec')
            self.ax2.fill_between(df['elapsed_seconds'], bandwidth_mb, 
                                 alpha=0.3, color='red')
            
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
            
            self.setup_axis_formatting()
            
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
    
    def plot_thread_scaling(self, csv_files, output_file=None):
        if self.mode != 'thread-scaling':
            print("Warning: Visualizer not in thread-scaling mode")
            return
            
        thread_counts = []
        avg_throughputs = []
        max_throughputs = []
        avg_bandwidths = []
        max_bandwidths = []
        
        print("\nAnalyzing thread scaling data...")
        print("-" * 50)
        
        for thread_count in sorted(csv_files.keys()):
            csv_file = csv_files[thread_count]
            
            if not os.path.exists(csv_file):
                print(f"Warning: File not found: {csv_file}")
                continue
            
            try:
                df = pd.read_csv(csv_file)
                if df.empty:
                    print(f"Warning: Empty data in {csv_file}")
                    continue
                
                avg_tput = df['writes_per_second'].mean()
                max_tput = df['writes_per_second'].max()
                avg_bw = (df['bytes_per_second'] / (1024 * 1024)).mean()
                max_bw = (df['bytes_per_second'] / (1024 * 1024)).max()
                
                thread_counts.append(thread_count)
                avg_throughputs.append(avg_tput)
                max_throughputs.append(max_tput)
                avg_bandwidths.append(avg_bw)
                max_bandwidths.append(max_bw)
                
                print(f"{thread_count:2d} threads: Avg {avg_tput:>10,.0f} writes/s, "
                      f"Max {max_tput:>10,.0f} writes/s, Avg BW {avg_bw:>6.1f} MB/s")
                
            except Exception as e:
                print(f"Error reading {csv_file}: {e}")
                continue
        
        if not thread_counts:
            print("\nError: No valid data found")
            return
        
        print("-" * 50)
        
        self.ax.clear()
        
        ax2 = self.ax.twinx()
        
        line1 = self.ax.plot(thread_counts, avg_throughputs, 'b-o', 
                            linewidth=2.5, markersize=10, label='Avg Throughput',
                            markerfacecolor='blue', markeredgewidth=2, markeredgecolor='darkblue')
        line2 = self.ax.plot(thread_counts, max_throughputs, 'b--s', 
                            linewidth=2, markersize=8, label='Max Throughput', 
                            alpha=0.7, markerfacecolor='lightblue', markeredgewidth=1.5, 
                            markeredgecolor='darkblue')
        
        if len(thread_counts) > 1:
            linear_scaling = [avg_throughputs[0] * t for t in thread_counts]
            self.ax.plot(thread_counts, linear_scaling, 'g:', 
                        linewidth=2, label='Perfect Linear Scaling', alpha=0.6)
        
        line3 = ax2.plot(thread_counts, avg_bandwidths, 'r-^', 
                        linewidth=2.5, markersize=10, label='Avg Bandwidth',
                        markerfacecolor='red', markeredgewidth=2, markeredgecolor='darkred')
        
        self.ax.set_xlabel('Number of Threads', fontsize=13, fontweight='bold')
        self.ax.set_ylabel('Writes/Second', fontsize=12, color='b', fontweight='bold')
        self.ax.tick_params(axis='y', labelcolor='b', labelsize=10)
        self.ax.tick_params(axis='x', labelsize=10)
        
        ax2.set_ylabel('Bandwidth (MB/s)', fontsize=12, color='r', fontweight='bold')
        ax2.tick_params(axis='y', labelcolor='r', labelsize=10)
        
        self.ax.grid(True, alpha=0.3, linestyle='--')
        self.ax.set_title('Throughput Scaling vs Thread Count', fontsize=14, pad=15)
        
        def format_throughput(x, pos):
            if x >= 1_000_000:
                return f'{x/1_000_000:.1f}M'
            elif x >= 1_000:
                return f'{x/1_000:.0f}K'
            else:
                return f'{x:.0f}'
        
        self.ax.yaxis.set_major_formatter(ticker.FuncFormatter(format_throughput))
        ax2.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: f'{x:.0f}'))
        
        self.ax.set_xticks(thread_counts)
        
        lines = line1 + line2 + line3
        labels = [l.get_label() for l in lines]
        self.ax.legend(lines, labels, loc='upper left', fontsize=10, framealpha=0.9)
        
        if len(thread_counts) > 1:
            scaling_efficiency = (avg_throughputs[-1] / avg_throughputs[0]) / thread_counts[-1] * 100
            speedup = avg_throughputs[-1] / avg_throughputs[0]
            
            stats_text = f"""Scaling Analysis
━━━━━━━━━━━━━━━━━━━━
Threads: {thread_counts[0]} → {thread_counts[-1]}
Speedup: {speedup:.2f}x
Efficiency: {scaling_efficiency:.1f}%

Single Thread:
  {avg_throughputs[0]:,.0f} writes/s
  {avg_bandwidths[0]:.1f} MB/s

Max ({thread_counts[-1]} threads):
  {avg_throughputs[-1]:,.0f} writes/s
  {avg_bandwidths[-1]:.1f} MB/s
  Peak: {max_throughputs[-1]:,.0f} writes/s"""
            
            self.ax.text(0.98, 0.03, stats_text, 
                        transform=self.ax.transAxes,
                        verticalalignment='bottom',
                        horizontalalignment='right',
                        fontsize=9,
                        family='monospace',
                        bbox=dict(boxstyle='round,pad=0.5', 
                                facecolor='lightgray', 
                                alpha=0.9,
                                edgecolor='black',
                                linewidth=1.5))
            
            print(f"\nScaling Summary:")
            print(f"  Speedup: {speedup:.2f}x ({thread_counts[0]} → {thread_counts[-1]} threads)")
            print(f"  Efficiency: {scaling_efficiency:.1f}%")
            print(f"  Peak Throughput: {max(max_throughputs):,.0f} writes/s at {thread_counts[max_throughputs.index(max(max_throughputs))]} threads")
        
        plt.tight_layout()
        
        if output_file:
            plt.savefig(output_file, dpi=300, bbox_inches='tight')
            print(f"\nPlot saved to: {output_file}")
        else:
            plt.show()
    
    def start_monitoring(self, interval=1000, headless=False, output_file=None, single_shot=False, max_frames=None):
        if self.mode != 'realtime':
            print("Error: start_monitoring only works in realtime mode")
            return
        
        if headless:
            if output_file is None:
                output_file = 'throughput_monitor.png'
            
            print(f"Starting real-time monitoring of {self.csv_file} (headless)")
            print(f"Output file: {output_file}")
            print(f"Update interval: {interval} ms")
            print("Press Ctrl+C to stop monitoring\n")
            
            frame_count = 0
            
            try:
                while True:
                    if single_shot:
                        self.update_plot(frame_count)
                        plt.savefig(output_file, dpi=150, bbox_inches='tight')
                        timestamp = datetime.now().strftime('%H:%M:%S')
                        print(f"[{timestamp}] Single-shot plot saved to {output_file}")
                        break
                    
                    if max_frames is not None and frame_count >= max_frames:
                        print(f"\nReached maximum frame count ({max_frames})")
                        break
                    
                    self.update_plot(frame_count)
                    plt.savefig(output_file, dpi=150, bbox_inches='tight')
                    timestamp = datetime.now().strftime('%H:%M:%S')
                    print(f"[{timestamp}] Frame {frame_count}: Plot updated and saved to {output_file}")
                    
                    frame_count += 1
                    time.sleep(interval / 1000.0)
            except KeyboardInterrupt:
                print("\nMonitoring stopped by user")
                print(f"Total frames generated: {frame_count}")
                print(f"Final plot saved to: {output_file}")
            return
            
        print(f"Starting real-time monitoring of {self.csv_file}")
        print("Waiting for benchmark data...")
        print("Close the plot window to stop monitoring")
        
        ani = animation.FuncAnimation(self.fig, self.update_plot, 
                                    interval=interval, blit=False, cache_frame_data=False)
        
        try:
            plt.show()
        except KeyboardInterrupt:
            print("\nMonitoring stopped by user")

def main():
    parser = argparse.ArgumentParser(
        description='Visualize WAL benchmark throughput in real-time or analyze thread scaling',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python visualize_throughput.py --file benchmark_throughput.csv
  python visualize_throughput.py --mode scaling --thread-files 1:bench_1t.csv 2:bench_2t.csv 4:bench_4t.csv
  python visualize_throughput.py --mode scaling --thread-files 1:bench_1t.csv 2:bench_2t.csv --output scaling.png
  python visualize_throughput.py --single-shot --output plot.png
        """)
    
    parser.add_argument('--file', '-f', default='benchmark_throughput.csv',
                       help='CSV file to monitor (default: benchmark_throughput.csv)')
    parser.add_argument('--interval', '-i', type=int, default=1000,
                       help='Update interval in milliseconds (default: 1000)')
    parser.add_argument('--mode', '-m', choices=['realtime', 'scaling'],
                       default='realtime',
                       help='Visualization mode (default: realtime)')
    parser.add_argument('--thread-files', nargs='+', metavar='THREADS:FILE',
                       help='Thread scaling files in format "1:file1.csv 2:file2.csv 4:file4.csv"')
    parser.add_argument('--output', '-o',
                       help='Output file path to save the plot (implies --headless)')
    parser.add_argument('--headless', action='store_true',
                       help='Save to file instead of displaying (for environments without a display)')
    parser.add_argument('--single-shot', action='store_true',
                       help='Generate a single plot and exit (realtime mode only)')
    parser.add_argument('--max-frames', type=int,
                       help='Maximum number of frames to generate before stopping (realtime mode only)')
    
    args = parser.parse_args()
    
    # Configure matplotlib backend before importing pyplot/animation
    if args.headless or args.output:
        import matplotlib
        matplotlib.use('Agg')
    
    global plt, animation
    import matplotlib.pyplot as plt
    import matplotlib.animation as animation
    
    print("=" * 50)
    print("WAL Benchmark Throughput Visualizer")
    print("=" * 50)
    
    if args.mode == 'realtime':
        if not os.path.exists(args.file):
            print(f"\nError: CSV file '{args.file}' not found.")
            print("\nRun the benchmark first to generate data:")
            print("   cargo test --test multithreaded_benchmark_writes -- --nocapture")
            print("   make bench-writes")
            return
        
        visualizer = ThroughputVisualizer(args.file, mode='realtime')
        
        headless = bool(args.headless or args.output)
        output_file = args.output if args.output else None
        
        visualizer.start_monitoring(
            interval=args.interval,
            headless=headless,
            output_file=output_file,
            single_shot=args.single_shot,
            max_frames=args.max_frames,
        )
        
    elif args.mode == 'scaling':
        if not args.thread_files:
            print("\nError: --thread-files required for scaling mode")
            print("\nExample usage:")
            print("   python visualize_throughput.py --mode scaling \\")
            print("       --thread-files 1:bench_1t.csv 2:bench_2t.csv 4:bench_4t.csv 8:bench_8t.csv")
            return
        
        thread_files = {}
        for tf in args.thread_files:
            try:
                threads_str, filepath = tf.split(':', 1)
                threads = int(threads_str)
                if threads <= 0:
                    print(f"Error: Thread count must be positive: {threads}")
                    return
                thread_files[threads] = filepath
            except ValueError as e:
                print(f"Error: Invalid format '{tf}'. Expected THREADS:FILE (e.g., '4:bench_4t.csv')")
                return
        
        if not thread_files:
            print("Error: No valid thread files provided")
            return
        
        visualizer = ThroughputVisualizer(mode='thread-scaling')
        
        headless = bool(args.headless or args.output)
        if args.output:
            output_file = args.output
        elif headless:
            output_file = 'thread_scaling_analysis.png'
        else:
            output_file = None
        
        visualizer.plot_thread_scaling(thread_files, output_file)

if __name__ == '__main__':
    main()
