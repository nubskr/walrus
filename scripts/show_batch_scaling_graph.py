#!/usr/bin/env python3

import argparse
import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, MultipleLocator


def format_axis(x, _):
    if x >= 1_000_000:
        return f"{x/1_000_000:.1f}M"
    if x >= 1_000:
        return f"{x/1_000:.0f}k"
    return str(int(x))


def choose_step(max_val: float) -> int:
    if max_val <= 200_000:
        return 50_000
    if max_val <= 1_000_000:
        return 100_000
    if max_val <= 2_000_000:
        return 200_000
    if max_val <= 5_000_000:
        return 500_000
    return 1_000_000


def choose_step_bw(max_val: float) -> float:
    if max_val <= 50:
        return 10
    if max_val <= 100:
        return 20
    if max_val <= 250:
        return 50
    if max_val <= 500:
        return 100
    return 200


def main():
    parser = argparse.ArgumentParser(description="Show Walrus batch scaling results.")
    parser.add_argument(
        "-f", "--file",
        default="batch_scaling_results.csv",
        help="Path to CSV produced by batch_scaling_benchmark.",
    )
    args = parser.parse_args()

    if not os.path.exists(args.file):
        raise SystemExit(f"{args.file} not found.")
    df = pd.read_csv(args.file)
    if df.empty:
        raise SystemExit(f"{args.file} is empty.")

    fig, ax_entries = plt.subplots(figsize=(10, 6), dpi=110)

    # X axis: 1..N ticks with half-step padding so first tick isn't glued to Y axis
    x_min = int(df["threads"].min())
    x_max = int(df["threads"].max())
    ax_entries.set_xlim(left=x_min - 0.5, right=x_max + 0.5)
    ax_entries.xaxis.set_major_locator(MultipleLocator(1))
    ax_entries.set_xlabel("Threads")
    ax_entries.tick_params(axis="x", pad=6)

    # Entries/sec — red, dotted line + solid circle markers
    ax_entries.plot(
        df["threads"], df["entries_per_second"],
        color="red", linestyle=":", linewidth=2,
        marker="o", markersize=6,
        markerfacecolor="red", markeredgecolor="red", markeredgewidth=0.8,
    )
    ax_entries.set_ylabel("Entries per Second (ops/sec)", color="black")
    ax_entries.tick_params(axis="y", labelcolor="black")
    ax_entries.spines["top"].set_visible(False)
    ax_entries.spines["right"].set_visible(False)
    ax_entries.grid(False)

    # Y axis (entries): zero baseline + clean steps + k/M formatting
    ax_entries.set_ylim(bottom=0)
    step_e = choose_step(float(df["entries_per_second"].max()))
    ax_entries.yaxis.set_major_locator(MultipleLocator(step_e))
    ax_entries.yaxis.set_major_formatter(FuncFormatter(format_axis))

    # Bandwidth — red, dashed line + solid square markers
    ax_bw = ax_entries.twinx()
    ax_bw.plot(
        df["threads"], df["mb_per_second"],
        color="red", linestyle="--", linewidth=2,
        marker="s", markersize=5.5,
        markerfacecolor="red", markeredgecolor="red", markeredgewidth=0.8,
    )
    ax_bw.set_ylabel("Write Bandwidth (MB/sec)", color="black")
    ax_bw.tick_params(axis="y", labelcolor="black")
    ax_bw.spines["top"].set_visible(False)

    # Y axis (bandwidth): zero baseline + clean steps + same formatter
    ax_bw.set_ylim(bottom=0)
    step_b = choose_step_bw(float(df["mb_per_second"].max()))
    ax_bw.yaxis.set_major_locator(MultipleLocator(step_b))
    ax_bw.yaxis.set_major_formatter(FuncFormatter(format_axis))

    plt.title("Batch Throughput Scaling", loc="left")
    fig.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
