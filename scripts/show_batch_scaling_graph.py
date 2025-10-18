#!/usr/bin/env python3
"""
Batch Scaling Graph

Reads batch_scaling_results.csv and plots entries/sec scaling vs threads
with a secondary axis for write bandwidth (MB/sec).
"""

import argparse
import os

import matplotlib.pyplot as plt
import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser(description="Show Walrus batch scaling results.")
    parser.add_argument(
        "--file",
        "-f",
        default="batch_scaling_results.csv",
        help="Path to CSV produced by batch_scaling_benchmark.",
    )
    args = parser.parse_args()

    if not os.path.exists(args.file):
        raise SystemExit(
            f"{args.file} not found. Run 'cargo test --test batch_scaling_benchmark -- --nocapture' first."
        )

    df = pd.read_csv(args.file)
    if df.empty:
        raise SystemExit(f"{args.file} is empty. Re-run the benchmark to collect data.")

    plt.style.use("seaborn-v0_8" if "seaborn-v0_8" in plt.style.available else "default")
    fig, ax_entries = plt.subplots(figsize=(10, 6))

    ax_entries.plot(
        df["threads"], df["entries_per_second"], "o-", color="tab:green", linewidth=2, markersize=7
    )
    ax_entries.set_xlabel("Number of Threads")
    ax_entries.set_ylabel("Entries per Second", color="tab:green")
    ax_entries.tick_params(axis="y", labelcolor="tab:green")
    ax_entries.grid(True, alpha=0.3)

    # Provide readable y-axis formatting
    ax_entries.yaxis.set_major_formatter(
        plt.FuncFormatter(lambda x, _: f"{x/1_000:.1f}K" if x >= 1_000 else f"{x:.0f}")
    )

    ax_bandwidth = ax_entries.twinx()
    ax_bandwidth.plot(
        df["threads"], df["mb_per_second"], "s--", color="tab:red", linewidth=2, markersize=6
    )
    ax_bandwidth.set_ylabel("Write Bandwidth (MB/sec)", color="tab:red")
    ax_bandwidth.tick_params(axis="y", labelcolor="tab:red")

    plt.title("Walrus Batch Throughput Scaling")
    fig.tight_layout()

    stats = (
        f"Best entries/sec: {df['entries_per_second'].max():.0f}\n"
        f"Best bandwidth: {df['mb_per_second'].max():.2f} MB/s\n"
        f"Single-thread entries/sec: {df['entries_per_second'].iloc[0]:.0f}"
    )
    fig.text(
        0.02,
        0.02,
        stats,
        fontsize=10,
        bbox=dict(boxstyle="round", facecolor="lightgray", alpha=0.75),
    )

    plt.show()


if __name__ == "__main__":
    main()
