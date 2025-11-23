#!/usr/bin/env python3
"""
Batch Benchmark Visualizer

Reads batch_benchmark_throughput.csv and renders time-series plots for:
  * entries/sec
  * write bandwidth (MB/sec)

Usage:
    python scripts/visualize_batch_benchmark.py --file batch_benchmark_throughput.csv

Requires pandas and matplotlib (pip install pandas matplotlib).
"""

import argparse
import os

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd


class BatchBenchmarkVisualizer:
    def __init__(self, csv_path: str) -> None:
        self.csv_path = csv_path

        plt.style.use("seaborn-v0_8" if "seaborn-v0_8" in plt.style.available else "default")
        self.fig, (self.ax_entries, self.ax_bw) = plt.subplots(2, 1, figsize=(12, 8))
        self.fig.suptitle("Walrus Batch Benchmark Throughput", fontsize=16, fontweight="bold")

        self._configure_axes()

    def _configure_axes(self) -> None:
        self.ax_entries.set_title("Entries per Second")
        self.ax_entries.set_xlabel("Elapsed Seconds")
        self.ax_entries.set_ylabel("entries/sec")
        self.ax_entries.grid(True, alpha=0.3)

        self.ax_bw.set_title("Write Bandwidth")
        self.ax_bw.set_xlabel("Elapsed Seconds")
        self.ax_bw.set_ylabel("MB/sec")
        self.ax_bw.grid(True, alpha=0.3)

        thousands = ticker.FuncFormatter(lambda x, _: f"{x/1_000:.1f}K" if x >= 1_000 else f"{x:.0f}")
        self.ax_entries.yaxis.set_major_formatter(thousands)

        bandwidth_fmt = ticker.FuncFormatter(
            lambda x, _: f"{x/1024:.1f} GB/s" if x >= 1024 else f"{x:.1f} MB/s"
        )
        self.ax_bw.yaxis.set_major_formatter(bandwidth_fmt)

    def render(self) -> None:
        if not os.path.exists(self.csv_path):
            raise FileNotFoundError(f"{self.csv_path} not found; run the benchmark first.")

        df = pd.read_csv(self.csv_path)
        if df.empty:
            raise ValueError(f"{self.csv_path} is empty; rerun the benchmark to collect data.")

        elapsed = df["elapsed_seconds"]

        self.ax_entries.plot(elapsed, df["entries_per_second"], color="tab:green", linewidth=2)

        bandwidth_mb = df["bytes_per_second"] / (1024 * 1024)
        self.ax_bw.plot(elapsed, bandwidth_mb, color="tab:red", linewidth=2)

        stats_text = (
            f"Total entries: {int(df['total_entries'].iloc[-1]):,}\n"
            f"Total bytes: {df['total_bytes'].iloc[-1] / (1024 * 1024):.1f} MB\n"
            f"Peak entries/sec: {df['entries_per_second'].max():.0f}\n"
            f"Peak bandwidth: {bandwidth_mb.max():.2f} MB/s\n"
            f"Average entries/sec: {df['entries_per_second'].mean():.0f}\n"
            f"Average bandwidth: {bandwidth_mb.mean():.2f} MB/s"
        )
        self.fig.text(
            0.02,
            0.02,
            stats_text,
            fontsize=10,
            bbox=dict(boxstyle="round", facecolor="lightgray", alpha=0.75),
        )

        self.fig.tight_layout()
        plt.show()


def main() -> None:
    parser = argparse.ArgumentParser(description="Visualize Walrus batch benchmark CSV output.")
    parser.add_argument(
        "--file",
        "-f",
        default="batch_benchmark_throughput.csv",
        help="Path to CSV produced by multithreaded batch benchmark.",
    )
    args = parser.parse_args()

    visualizer = BatchBenchmarkVisualizer(args.file)
    try:
        visualizer.render()
    except (FileNotFoundError, ValueError) as exc:
        print(exc)


if __name__ == "__main__":
    main()
