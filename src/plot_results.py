import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from common import ROOT


def read_table(name):
    with (ROOT / "results" / name).open(encoding="utf-8-sig") as stream:
        return list(csv.DictReader(stream))


def plot_growth(output):
    rows = read_table("growth.csv")
    positions = list(range(len(rows)))
    figure, axes = plt.subplots(figsize=(8, 3.2))
    for size, label in ((1, "Навыки"), (2, "Пары"), (3, "Тройки")):
        values = [float(row[f"events_{size}"]) for row in rows]
        axes.plot(positions, values, marker="o", label=label)
    axes.set_xticks(positions, ["5", "10", "20", "Весь словарь"])
    axes.set_yscale("log")
    axes.set_ylabel("Число эмиссий")
    axes.set_xlabel("Размер словаря")
    axes.grid(axis="y", alpha=0.2)
    axes.legend(ncols=3, loc="upper left")
    figure.tight_layout()
    figure.savefig(output / "growth.png", dpi=180)
    plt.close(figure)


def plot_performance(output):
    rows = read_table("profile_summary.csv")
    sizes = ("5", "10", "20", "all")
    methods = (
        ("mr_baseline", "MR базовый"),
        ("mr_optimized", "MR оптимизированный"),
        ("duckdb", "DuckDB"),
    )
    figure, axes = plt.subplots(figsize=(8, 3.2))
    bar_width = 0.25
    for index, (method, label) in enumerate(methods):
        values = [
            next(
                float(row["median_seconds"])
                for row in rows
                if row["method"] == method and row["stack_size"] == size
            )
            for size in sizes
        ]
        positions = [position + (index - 1) * bar_width for position in range(len(sizes))]
        axes.bar(positions, values, width=bar_width, label=label)
    axes.set_xticks(range(len(sizes)), ["5", "10", "20", "Весь словарь"])
    axes.set_ylabel("Время, с")
    axes.grid(axis="y", alpha=0.2)
    axes.legend(fontsize=8)
    figure.tight_layout()
    figure.savefig(output / "performance.png", dpi=180)
    plt.close(figure)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT / "report/figures")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 10,
            "axes.spines.top": False,
            "axes.spines.right": False,
        }
    )
    plot_growth(args.output)
    plot_performance(args.output)


if __name__ == "__main__":
    main()
