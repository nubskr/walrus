#!/usr/bin/env python3
"""
Compare benchmark results from Walrus, RocksDB, and Kafka

This script compares throughput and bandwidth metrics across multiple systems
and generates a comparison visualization.

Usage:
    python compare_walrus_rocksdb.py --walrus walrus.csv --rocksdb rocksdb.csv [--kafka kafka.csv] --out comparison.png

Requirements:
    pip install matplotlib pandas
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import argparse
import sys
import os


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


def load_benchmark_csv(filepath, label):
    """Load a benchmark CSV and add a label column"""
    if not os.path.exists(filepath):
        print(f"Error: {filepath} not found")
        return None

    df = pd.read_csv(filepath)
    df['system'] = label
    return df


def create_comparison_plot(walrus_df, rocksdb_df, kafka_df, output_file):
    """Create a comparison plot of throughput and bandwidth"""

    # Set up the figure with 2 subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))

    # Style
    plt.style.use('seaborn-v0_8' if 'seaborn-v0_8' in plt.style.available else 'default')

    # Colors for each system
    colors = {
        'Walrus': '#2E86AB',      # Blue
        'RocksDB': '#A23B72',     # Purple
        'Kafka': '#F18F01'        # Orange
    }

    # Plot 1: Throughput comparison
    ax1.set_title('Write Throughput Comparison', fontsize=14, fontweight='bold')
    ax1.set_xlabel('Time (seconds)', fontsize=11)
    ax1.set_ylabel('Writes/sec', fontsize=11)
    ax1.grid(True, alpha=0.3)

    # Plot each dataset
    datasets = []
    labels = []

    if walrus_df is not None:
        ax1.plot(walrus_df['elapsed_seconds'], walrus_df['writes_per_second'],
                linewidth=2.5, label='Walrus', color=colors['Walrus'], alpha=0.9)
        datasets.append(walrus_df)
        labels.append('Walrus')

    if rocksdb_df is not None:
        ax1.plot(rocksdb_df['elapsed_seconds'], rocksdb_df['writes_per_second'],
                linewidth=2.5, label='RocksDB', color=colors['RocksDB'], alpha=0.9)
        datasets.append(rocksdb_df)
        labels.append('RocksDB')

    if kafka_df is not None:
        ax1.plot(kafka_df['elapsed_seconds'], kafka_df['writes_per_second'],
                linewidth=2.5, label='Kafka', color=colors['Kafka'], alpha=0.9)
        datasets.append(kafka_df)
        labels.append('Kafka')

    ax1.yaxis.set_major_formatter(ticker.FuncFormatter(format_thousands))
    ax1.yaxis.set_major_locator(ticker.MaxNLocator(nbins=8, integer=False))
    ax1.legend(loc='best', fontsize=11, framealpha=0.9)

    # Plot 2: Bandwidth comparison
    ax2.set_title('Write Bandwidth Comparison', fontsize=14, fontweight='bold')
    ax2.set_xlabel('Time (seconds)', fontsize=11)
    ax2.set_ylabel('MB/sec', fontsize=11)
    ax2.grid(True, alpha=0.3)

    if walrus_df is not None:
        walrus_bandwidth = walrus_df['bytes_per_second'] / (1024 * 1024)
        ax2.plot(walrus_df['elapsed_seconds'], walrus_bandwidth,
                linewidth=2.5, label='Walrus', color=colors['Walrus'], alpha=0.9)

    if rocksdb_df is not None:
        rocksdb_bandwidth = rocksdb_df['bytes_per_second'] / (1024 * 1024)
        ax2.plot(rocksdb_df['elapsed_seconds'], rocksdb_bandwidth,
                linewidth=2.5, label='RocksDB', color=colors['RocksDB'], alpha=0.9)

    if kafka_df is not None:
        kafka_bandwidth = kafka_df['bytes_per_second'] / (1024 * 1024)
        ax2.plot(kafka_df['elapsed_seconds'], kafka_bandwidth,
                linewidth=2.5, label='Kafka', color=colors['Kafka'], alpha=0.9)

    ax2.yaxis.set_major_formatter(ticker.FuncFormatter(format_bandwidth))
    ax2.yaxis.set_major_locator(ticker.MaxNLocator(nbins=8, integer=False))
    ax2.legend(loc='best', fontsize=11, framealpha=0.9)

    # Add statistics summary
    stats_lines = []
    stats_lines.append("Average Throughput & Bandwidth:")

    for df, label in zip(datasets, labels):
        if df is not None and not df.empty:
            avg_throughput = df['writes_per_second'].mean()
            max_throughput = df['writes_per_second'].max()
            avg_bandwidth = (df['bytes_per_second'] / (1024 * 1024)).mean()
            max_bandwidth = (df['bytes_per_second'] / (1024 * 1024)).max()

            stats_lines.append(
                f"{label:>8}: Avg {avg_throughput:>9,.0f} writes/s ({avg_bandwidth:>7.2f} MB/s) | "
                f"Max {max_throughput:>9,.0f} writes/s ({max_bandwidth:>7.2f} MB/s)"
            )

    stats_text = '\n'.join(stats_lines)

    fig.text(0.02, 0.01, stats_text, fontsize=9, family='monospace',
             bbox=dict(boxstyle="round,pad=0.5", facecolor="lightgray", alpha=0.9))

    plt.tight_layout(rect=[0, 0.08, 1, 1])  # Leave space for stats at bottom

    # Save the plot
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"Comparison plot saved to: {output_file}")

    # Also display statistics in console
    print("\n" + "="*80)
    print("BENCHMARK COMPARISON SUMMARY")
    print("="*80)
    for line in stats_lines:
        print(line)
    print("="*80 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description='Compare Walrus, RocksDB, and Kafka benchmark results',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--walrus', required=True,
                       help='Path to Walrus benchmark CSV file')
    parser.add_argument('--rocksdb', required=True,
                       help='Path to RocksDB benchmark CSV file')
    parser.add_argument('--kafka', required=False,
                       help='Path to Kafka benchmark CSV file (optional)')
    parser.add_argument('--out', '-o', default='comparison.png',
                       help='Output file for comparison plot (default: comparison.png)')

    args = parser.parse_args()

    print("Walrus vs RocksDB vs Kafka Benchmark Comparison")
    print("=" * 50)

    # Load datasets
    walrus_df = load_benchmark_csv(args.walrus, 'Walrus')
    rocksdb_df = load_benchmark_csv(args.rocksdb, 'RocksDB')
    kafka_df = None

    if args.kafka:
        kafka_df = load_benchmark_csv(args.kafka, 'Kafka')

    # Check if we have at least one valid dataset
    if walrus_df is None and rocksdb_df is None and kafka_df is None:
        print("Error: No valid benchmark data found")
        sys.exit(1)

    # Create comparison plot
    create_comparison_plot(walrus_df, rocksdb_df, kafka_df, args.out)


if __name__ == '__main__':
    main()
