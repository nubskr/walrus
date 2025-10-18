#!/usr/bin/env python3
"""
Compare Walrus WAL benchmark results against RocksDB WAL benchmark results.

Reads the CSV outputs from:
  - benchmarks/multithreaded_benchmark_writes.rs   -> benchmark_throughput.csv
  - rocksdb_benchmarks/multithreaded_benchmark_writes.rs -> rocksdb_benchmark_throughput.csv

Produces a combined line chart showing writes/sec and MB/sec for both systems.

Usage:
    python3 scripts/compare_walrus_rocksdb.py \
        --walrus benchmark_throughput.csv \
        --rocksdb rocksdb_benchmark_throughput.csv \
        --out walrus_vs_rocksdb.png

Requirements:
    pip install pandas matplotlib
"""

import argparse
import os
from typing import Optional

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd


def load_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV file not found: {path}")
    df = pd.read_csv(path)
    if df.empty:
        raise ValueError(f"CSV file '{path}' is empty")
    return df


def format_axis(ax):
    def format_thousands(x, _pos):
        if x >= 1_000_000:
            return f"{x/1_000_000:.1f}M"
        if x >= 1_000:
            return f"{x/1_000:.1f}K"
        return f"{x:.0f}"

    ax.yaxis.set_major_formatter(ticker.FuncFormatter(format_thousands))
    ax.yaxis.set_major_locator(ticker.MaxNLocator(nbins=8, integer=False))


def create_plot(walrus_df: pd.DataFrame, rocks_df: pd.DataFrame, output: Optional[str]):
    plt.style.use("seaborn-v0_8" if "seaborn-v0_8" in plt.style.available else "default")
    fig, (ax_writes, ax_bw) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
    fig.suptitle("Walrus vs RocksDB WAL Throughput", fontsize=16, fontweight="bold")

    # Throughput
    ax_writes.plot(
        walrus_df["elapsed_seconds"],
        walrus_df["writes_per_second"],
        label="Walrus writes/sec",
        color="#1f77b4",
        linewidth=2,
    )
    ax_writes.plot(
        rocks_df["elapsed_seconds"],
        rocks_df["writes_per_second"],
        label="RocksDB writes/sec",
        color="#d62728",
        linewidth=2,
    )
    ax_writes.set_ylabel("Writes per second")
    ax_writes.set_title("Write Throughput")
    ax_writes.grid(True, alpha=0.3)
    ax_writes.legend()
    format_axis(ax_writes)

    # Bandwidth
    walrus_bw = walrus_df["bytes_per_second"] / (1024 * 1024)
    rocks_bw = rocks_df["bytes_per_second"] / (1024 * 1024)

    ax_bw.plot(
        walrus_df["elapsed_seconds"],
        walrus_bw,
        label="Walrus MB/sec",
        color="#1f77b4",
        linewidth=2,
    )
    ax_bw.plot(
        rocks_df["elapsed_seconds"],
        rocks_bw,
        label="RocksDB MB/sec",
        color="#d62728",
        linewidth=2,
        linestyle="--",
    )
    ax_bw.set_xlabel("Elapsed Seconds")
    ax_bw.set_ylabel("MB per second")
    ax_bw.set_title("Write Bandwidth")
    ax_bw.grid(True, alpha=0.3)
    ax_bw.legend()
    ax_bw.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _pos: f"{x:.1f} MB/s"))
    ax_bw.yaxis.set_major_locator(ticker.MaxNLocator(nbins=8, integer=False))

    plt.tight_layout(rect=[0, 0.03, 1, 0.97])

    if output:
        plt.savefig(output, dpi=150)
        print(f"Saved comparison plot to {output}")
    else:
        plt.show()


def summarize(df: pd.DataFrame, label: str):
    max_writes = df["writes_per_second"].max()
    avg_writes = df["writes_per_second"].mean()
    total_ops = df["total_writes"].max()
    duration = df["elapsed_seconds"].max()
    print(
        f"{label} — duration ~{duration:.1f}s, total ops {total_ops:,}, "
        f"avg {avg_writes:.0f} ops/s, peak {max_writes:.0f} ops/s"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Superimpose Walrus and RocksDB WAL benchmark CSVs."
    )
    parser.add_argument(
        "--walrus",
        default="benchmark_throughput.csv",
        help="CSV produced by Walrus benchmark (default: benchmark_throughput.csv)",
    )
    parser.add_argument(
        "--rocksdb",
        default="rocksdb_benchmark_throughput.csv",
        help="CSV produced by RocksDB benchmark (default: rocksdb_benchmark_throughput.csv)",
    )
    parser.add_argument(
        "--out",
        default="walrus_vs_rocksdb.png",
        help="Output image path (default: walrus_vs_rocksdb.png)",
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Do not save to disk; display interactively instead.",
    )

    args = parser.parse_args()

    walrus_df = load_csv(args.walrus)
    rocks_df = load_csv(args.rocksdb)

    summarize(walrus_df, "Walrus")
    summarize(rocks_df, "RocksDB")

    output_path = None if args.no_save else args.out
    create_plot(walrus_df, rocks_df, output_path)


if __name__ == "__main__":
    main()
