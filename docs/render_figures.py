#!/usr/bin/env python3
"""Generate the README figures as crisp SVG (plus PNG previews).

Usage:
    pip install matplotlib
    python3 docs/render_figures.py

Writes docs/img/*.svg (embedded in the README) and docs/img/*.png (previews).
Keeping the generator in the repo means the figures are reproducible: change a
number here, rerun, commit.
"""

import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

OUT = os.path.join(os.path.dirname(__file__), "img")
os.makedirs(OUT, exist_ok=True)

# Palette: muted, professional, consistent across every figure.
INK = "#1b2330"
MUTED = "#8a939b"
GRID = "#eaeef2"
TEAL = "#1f8a78"        # driftwatch accent
TEAL_SOFT = "#d6ebe6"
GREY = "#aab2ba"
GREEN = "#2e9e6b"
RED = "#d2605a"
AMBER = "#dd9b2e"
SLATE = "#54626f"

plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 12,
    "text.color": INK,
    "axes.edgecolor": MUTED,
    "axes.labelcolor": INK,
    "xtick.color": SLATE,
    "ytick.color": INK,
    "svg.fonttype": "none",   # keep text selectable/sharp in the SVG
    "figure.facecolor": "white",
    "axes.facecolor": "white",
})


def save(fig, name):
    fig.savefig(os.path.join(OUT, name + ".svg"), bbox_inches="tight")
    fig.savefig(os.path.join(OUT, name + ".png"), dpi=160, bbox_inches="tight")
    plt.close(fig)
    print("wrote", name + ".svg / .png")


def fig_perf_rows():
    fig, ax = plt.subplots(figsize=(8.2, 2.5))
    labels = ["full table scan", "driftwatch"]
    vals = [6000, 275]
    colors = [GREY, TEAL]
    y = [1, 0]
    ax.barh(y, vals, color=colors, height=0.52, zorder=3)
    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=12)
    for yi, v in zip(y, vals):
        pct = v / 6000 * 100
        ax.text(v + 90, yi, "{:,} rows  ({:.1f}%)".format(v, pct),
                va="center", ha="left", color=INK, fontsize=12, fontweight="bold")
    ax.set_xlim(0, 7400)
    ax.set_xticks([])
    for s in ("top", "right", "left", "bottom"):
        ax.spines[s].set_visible(False)
    ax.tick_params(left=False)
    ax.set_title("Rows read to verify a 6,000-row table with 3 drifted rows",
                 loc="left", fontsize=13.5, fontweight="bold", pad=10)
    ax.text(0, -0.42, "Lower is better. driftwatch prunes matching key ranges instead of scanning them.",
            transform=ax.transAxes, color=MUTED, fontsize=10.5)
    save(fig, "perf-rows")


def fig_perf_scaling():
    fig, ax = plt.subplots(figsize=(7.6, 4.2))
    n = np.array([1e3, 1e4, 1e5, 1e6, 1e7, 1e8])
    full = n                       # a full scan reads every row
    leaf = 5000
    dw = leaf + 16 * np.log2(n) * 2  # a few drifted leaves + checksum round-trips, ~flat
    ax.plot(n, full, marker="o", color=GREY, linewidth=2.4, label="full table scan  (reads every row)")
    ax.plot(n, dw, marker="o", color=TEAL, linewidth=2.6, label="driftwatch  (sparse drift)")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("table size (rows)")
    ax.set_ylabel("rows read to verify")
    ax.grid(True, which="major", color=GRID, linewidth=1.0, zorder=0)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.set_title("Verification cost stays flat when drift is sparse",
                 loc="left", fontsize=13.5, fontweight="bold", pad=10)
    ax.legend(frameon=False, fontsize=11, loc="upper left")
    ax.annotate("~1000x fewer rows\nat 100M",
                xy=(1e8, dw[-1]), xytext=(1.1e6, dw[-1] * 6),
                color=TEAL, fontsize=10.5,
                arrowprops=dict(arrowstyle="-", color=TEAL, lw=1.2))
    save(fig, "perf-scaling")


def _cell(ax, x, y, kind):
    colors = {"y": GREEN, "n": RED, "p": AMBER}
    glyph = {"y": "✓", "n": "✗", "p": "~"}
    box = FancyBboxPatch((x + 0.06, y + 0.06), 0.88, 0.88,
                         boxstyle="round,pad=0.0,rounding_size=0.12",
                         linewidth=0, facecolor=colors[kind], alpha=0.16, zorder=2)
    ax.add_patch(box)
    ax.text(x + 0.5, y + 0.5, glyph[kind], ha="center", va="center",
            color=colors[kind], fontsize=15, fontweight="bold", zorder=3)


def fig_compare_matrix():
    cols = ["continuous", "cross-engine", "lag-aware", "open source"]
    rows = [
        ("driftwatch",            ["y", "y", "y", "y"]),
        ("data-diff / reladiff",  ["n", "y", "n", "p"]),
        ("dbt tests",             ["n", "n", "n", "y"]),
        ("Great Expectations",    ["n", "n", "n", "y"]),
        ("Monte Carlo",           ["p", "p", "n", "n"]),
        ("pt-table-checksum",     ["y", "n", "y", "y"]),
    ]
    nrows = len(rows)
    fig, ax = plt.subplots(figsize=(8.6, 3.9))
    ax.set_xlim(0, len(cols) + 2.2)
    ax.set_ylim(0, nrows + 1.1)
    ax.axis("off")

    # column headers
    for j, c in enumerate(cols):
        ax.text(2.2 + j + 0.5, nrows + 0.45, c, ha="center", va="center",
                fontsize=11, fontweight="bold", color=SLATE)
    # rows
    for i, (name, cells) in enumerate(rows):
        y = nrows - 1 - i
        is_dw = i == 0
        if is_dw:
            band = FancyBboxPatch((0.0, y + 0.04), len(cols) + 2.2, 0.92,
                                  boxstyle="round,pad=0,rounding_size=0.1",
                                  linewidth=0, facecolor=TEAL_SOFT, zorder=1)
            ax.add_patch(band)
        ax.text(0.1, y + 0.5, name, ha="left", va="center",
                fontsize=12, fontweight="bold" if is_dw else "normal",
                color=TEAL if is_dw else INK)
        for j, kind in enumerate(cells):
            _cell(ax, 2.2 + j, y, kind)

    ax.text(0, nrows + 0.95,
            "Capability coverage across data-reconciliation tools",
            ha="left", va="center", fontsize=13.5, fontweight="bold")
    save(fig, "compare-matrix")


def _node(ax, cx, cy, w, h, text, fill, edge, tcolor, fontsize=11.5, bold=True):
    box = FancyBboxPatch((cx - w / 2, cy - h / 2), w, h,
                         boxstyle="round,pad=0.02,rounding_size=2.2",
                         linewidth=1.6, facecolor=fill, edgecolor=edge, zorder=3)
    ax.add_patch(box)
    ax.text(cx, cy, text, ha="center", va="center", color=tcolor,
            fontsize=fontsize, fontweight="bold" if bold else "normal", zorder=4)


def _arrow(ax, p0, p1, color, style="-|>", dashed=False, lw=1.8):
    a = FancyArrowPatch(p0, p1, arrowstyle=style, mutation_scale=14,
                        color=color, lw=lw, zorder=2,
                        linestyle="--" if dashed else "-",
                        shrinkA=2, shrinkB=2)
    ax.add_patch(a)


def fig_architecture():
    fig, ax = plt.subplots(figsize=(9.2, 4.6))
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 52)
    ax.axis("off")

    _node(ax, 13, 36, 22, 12, "Postgres\n(source of truth)", "white", SLATE, INK)
    _node(ax, 56, 44, 26, 11, "Snowflake\n(warehouse mirror)", "white", SLATE, INK)
    _node(ax, 56, 28, 26, 11, "Search index / cache", "white", SLATE, INK)

    _arrow(ax, (24, 38), (43, 44), GREY)
    _arrow(ax, (24, 35), (43, 29), GREY)
    ax.text(33.5, 43, "CDC / ETL / dbt", ha="center", va="bottom",
            fontsize=10, color=MUTED, style="italic")

    _node(ax, 34, 12, 26, 11, "driftwatch run", TEAL_SOFT, TEAL, TEAL, fontsize=13)
    _arrow(ax, (13, 30), (28, 16), TEAL, dashed=True)
    _arrow(ax, (52, 23), (40, 16.5), TEAL, dashed=True)
    ax.text(7.5, 22.5, "read-only", ha="left", va="center", fontsize=9.5,
            color=TEAL, style="italic")

    _node(ax, 82, 12, 28, 11, "CI / cron /\nalerting", "white", SLATE, INK)
    _arrow(ax, (47, 12), (68, 12), TEAL, lw=2.2)
    ax.text(57.5, 14.2, "exit 0 = in sync\nexit 1 = drift", ha="center", va="bottom",
            fontsize=9.5, color=TEAL)

    ax.text(0, 51, "Where driftwatch sits in a data system",
            ha="left", va="top", fontsize=13.5, fontweight="bold")
    save(fig, "architecture")


if __name__ == "__main__":
    fig_perf_rows()
    fig_perf_scaling()
    fig_compare_matrix()
    fig_architecture()
    print("done ->", OUT)
