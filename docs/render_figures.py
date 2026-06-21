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
from matplotlib.patches import Circle, FancyArrowPatch, FancyBboxPatch

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
    total = 10_000_000   # measured: see examples/benchmark-results.json
    labels = ["full table scan", "driftwatch"]
    vals = [total, 14651]
    colors = [GREY, TEAL]
    y = [1, 0]
    ax.barh(y, vals, color=colors, height=0.52, zorder=3)
    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=12)
    for yi, v in zip(y, vals):
        pct = v / total * 100
        pct_str = "%.0f%%" % pct if pct >= 10 else "%.2f%%" % pct
        ax.text(v + total * 0.012, yi, "{:,} rows  ({})".format(v, pct_str),
                va="center", ha="left", color=INK, fontsize=12, fontweight="bold")
    ax.set_xlim(0, total * 1.24)
    ax.set_xticks([])
    for s in ("top", "right", "left", "bottom"):
        ax.spines[s].set_visible(False)
    ax.tick_params(left=False)
    ax.set_title("Rows read to find 7 drifted rows in a 10,000,000-row table (measured)",
                 loc="left", fontsize=13.5, fontweight="bold", pad=10)
    ax.text(0, -0.42, "Lower is better. driftwatch only reads the key ranges that disagree.",
            transform=ax.transAxes, color=MUTED, fontsize=10.5)
    save(fig, "perf-rows")


def fig_perf_scaling():
    fig, ax = plt.subplots(figsize=(7.6, 4.2))
    # measured (examples/benchmark-results.json): rows read to find sparse drift
    n = np.array([1e5, 1e6, 1e7])
    full = n                       # a full scan reads every row
    dw = np.array([2346, 23440, 14651])
    ax.plot(n, full, marker="o", color=GREY, linewidth=2.4, label="full table scan  (reads every row)")
    ax.plot(n, dw, marker="o", color=TEAL, linewidth=2.6, label="driftwatch  (measured)")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("table size (rows)")
    ax.set_ylabel("rows read to find drift")
    ax.grid(True, which="major", color=GRID, linewidth=1.0, zorder=0)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.set_title("Rows read to find sparse drift stays tiny as the table grows",
                 loc="left", fontsize=13.5, fontweight="bold", pad=10)
    ax.legend(frameon=False, fontsize=11, loc="lower right")
    ax.text(1.1e5, 4.5e4, "~15,000 rows checks 10M  (0.15%)",
            color=TEAL, fontsize=10.5, ha="left", va="center")
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


def fig_terminal():
    """A terminal-style screenshot of a real `make demo` session (postgres -> duckdb)."""
    L = [
        ("cmd", "$ make demo"),
        ("dim", "starting Postgres (postgres:16) ... ready"),
        ("dim", "loaded 1,000 orders into Postgres and the DuckDB warehouse"),
        ("blank", ""),
        ("cmt", "# 1) the copies match"),
        ("cmd", "$ driftwatch run -c orders.yaml"),
        ("ok", "driftwatch: orders - IN SYNC          (exit 0)"),
        ("blank", ""),
        ("cmt", "# 2) the warehouse drifts"),
        ("cmd", "$ driftwatch run -c orders.yaml"),
        ("bad", "driftwatch: orders - DRIFT            (exit 1)"),
        ("key", "  drift keys: 3  (missing=1, extra=1, changed=1)"),
        ("key", "    [changed] 250"),
        ("key", "    [missing] 500"),
        ("key", "    [extra]   99999"),
        ("blank", ""),
        ("cmt", "# 3) a fresh row is still syncing"),
        ("cmd", "$ driftwatch run -c orders.yaml          # 15-min grace window"),
        ("ok", "driftwatch: orders - IN SYNC          (exit 0, lag ignored)"),
        ("cmd", "$ driftwatch run -c orders-no-grace.yaml"),
        ("bad", "driftwatch: orders - DRIFT            (exit 1, [missing] 1001)"),
    ]
    colors = {"cmd": "#7ee787", "dim": "#8b949e", "cmt": "#6e7681",
              "ok": "#3fb950", "bad": "#f85149", "key": "#c9d1d9"}
    n = len(L)
    fig_w, lh, bar = 8.8, 0.34, 0.5
    fig_h = bar + n * lh + 0.3
    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    ax.add_patch(FancyBboxPatch((0.006, 0.006), 0.988, 0.988,
                                boxstyle="round,pad=0,rounding_size=0.02",
                                facecolor="#0d1117", edgecolor="#30363d", lw=1.3, zorder=1))
    bar_h = bar / fig_h
    ax.plot([0.006, 0.994], [1 - bar_h, 1 - bar_h], color="#21262d", lw=1.0, zorder=2)
    cy = 1 - bar_h / 2
    for i, c in enumerate(["#ff5f56", "#ffbd2e", "#27c93f"]):
        ax.add_patch(Circle((0.028 + i * 0.022, cy), 0.0075, color=c, zorder=3))
    ax.text(0.5, cy, "driftwatch demo    postgres -> duckdb", ha="center", va="center",
            color="#8b949e", fontsize=10, family="monospace", zorder=3)
    y0 = 1 - bar_h - 0.012
    for i, (kind, text) in enumerate(L):
        if kind == "blank":
            continue
        y = y0 - (i + 0.6) * (lh / fig_h)
        ax.text(0.028, y, text, ha="left", va="center", color=colors[kind],
                fontsize=11, family="monospace", zorder=3)
    save(fig, "demo-terminal")


if __name__ == "__main__":
    fig_perf_rows()
    fig_perf_scaling()
    fig_compare_matrix()
    fig_architecture()
    fig_terminal()
    print("done ->", OUT)
