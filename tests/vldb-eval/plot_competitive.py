"""
plot_competitive.py — builds Figure 6.4.1 (competitive latency benchmark).

Currently ELK-only; SIESTA data is added by passing --siesta-dir once
those JSONL files are ready.  The layout, colors, and legend are designed
so adding two more lines per panel requires zero restructuring.

Usage:
    python plot_competitive.py \
        --elk-dir /path/to/results \
        [--siesta-dir /path/to/siesta_results] \
        --out competitive_latency.pdf
"""
from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.rcParams.update({
    "font.family":       "serif",
        "font.serif":         ["STIX", "STIXGeneral", "Liberation Serif",
                               "DejaVu Serif", "Times New Roman"], 
                                     "mathtext.fontset":  "stix",
    "font.size":         9,
    "axes.titlesize":    10,
    "axes.labelsize":    10,
    "xtick.labelsize":   8.5,
    "ytick.labelsize":   8.5,
    "legend.fontsize":   8,
    "figure.dpi":        300,
    "pdf.fonttype":      42,
    "ps.fonttype":       42,
})
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import numpy as np
from scipy.stats import spearmanr

# ── Okabe-Ito palette ───────────────────────────────────────────────────────
ELK_COLOR    = "#0072B2"   # blue
SIESTA_COLOR = "#A90702"   # red

DATASETS = [
    ("bpic2011", "BPIC 2011"),
    ("bpic2015", "BPIC 2015"),
    ("bpic2017", "BPIC 2017"),
    ("bpic2018", "BPIC 2018"),
]
LENGTHS = list(range(8, 16))


def load_jsonl(path: Path) -> list[dict]:
    return [json.loads(l) for l in path.read_text().splitlines() if l.strip()]


def parse_len(row: dict) -> int:
    return len(row["pattern"].split(";;"))


def is_attr(row: dict) -> bool:
    return "[" in row["pattern"]


def median_iqr_by_length(rows, lengths, attr_filter=None):
    """Returns (medians, lo, hi) arrays aligned to `lengths`."""
    by_len = defaultdict(list)
    for r in rows:
        if attr_filter is not None and is_attr(r) != attr_filter:
            continue
        by_len[parse_len(r)].append(r["elapsed_sec"])
    medians, lo, hi, ns = [], [], [], []
    for L in lengths:
        vals = by_len.get(L, [])
        if vals:
            medians.append(float(np.median(vals)))
            lo.append(float(np.percentile(vals, 25)))
            hi.append(float(np.percentile(vals, 75)))
            ns.append(len(vals))
        else:
            medians.append(np.nan); lo.append(np.nan)
            hi.append(np.nan); ns.append(0)
    return np.array(medians), np.array(lo), np.array(hi), ns


def plot_series(ax, xs, med, lo, hi, color, ls, alpha_band=0.15, lw=1.4, **kw):
    mask = ~np.isnan(med)
    x, m, l, h = xs[mask], med[mask], lo[mask], hi[mask]
    ax.plot(x, m, color=color, ls=ls, lw=lw, **kw)
    ax.fill_between(x, l, h, color=color, alpha=alpha_band)


def build_figure(elk_dir: Path, siesta_dir: Path | None, out: Path):
    xs = np.array(LENGTHS)

    # ── layout: 2 rows × 2 columns ────────────────────────────────────────
    fig, axes = plt.subplots(
        2, 2,
        figsize=(4.5, 4.5),            # fits two-column VLDB page
        sharey=False, sharex=True,
    )
    fig.subplots_adjust(left=0.10, right=0.99, top=0.88, bottom=0.14,
                    wspace=0.25, hspace=0.28)

    # Flatten to a 1-D list for easy iteration; zip with datasets
    flat_axes = axes.flatten()

    for idx, (ax, (ds_key, ds_label)) in enumerate(zip(flat_axes, DATASETS)):
        row, col = divmod(idx, 2)

        # ── load ELK ──────────────────────────────────────────────────────
        elk_path = elk_dir / f"elk_results{ds_key}.jsonl"
        if not elk_path.exists():
            ax.text(0.5, 0.5, "no data", ha="center", va="center",
                    transform=ax.transAxes, fontsize=7, color="gray")
            ax.set_title(ds_label)
            continue
        elk_rows = load_jsonl(elk_path)

        # ELK structural
        m, l, h, _ = median_iqr_by_length(elk_rows, LENGTHS, attr_filter=False)
        plot_series(ax, xs, m, l, h, ELK_COLOR, ls="-")
        # ELK attr-aware
        m, l, h, _ = median_iqr_by_length(elk_rows, LENGTHS, attr_filter=True)
        plot_series(ax, xs, m, l, h, ELK_COLOR, ls="--")

        # ── load SIESTA (optional) ────────────────────────────────────────
        if siesta_dir is not None:
            siesta_path = siesta_dir / f"siesta_results{ds_key}.jsonl"
            if siesta_path.exists():
                siesta_rows = load_jsonl(siesta_path)
                m, l, h, _ = median_iqr_by_length(siesta_rows, LENGTHS, attr_filter=False)
                plot_series(ax, xs, m, l, h, SIESTA_COLOR, ls="-")
                m, l, h, _ = median_iqr_by_length(siesta_rows, LENGTHS, attr_filter=True)
                plot_series(ax, xs, m, l, h, SIESTA_COLOR, ls="--")

        # ── per-panel cosmetics ───────────────────────────────────────────
        ax.set_title(ds_label, pad=2)
        ax.set_xticks(LENGTHS[::2])
        ax.set_xticklabels([str(L) for L in LENGTHS[::2]], ha="right")
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.yaxis.grid(True, linestyle=":", linewidth=0.5, color="#cccccc", zorder=0)
        ax.set_axisbelow(True)

        # y-axis label only on left column
        if col == 0:
            ax.set_ylabel("Latency (s)")

        # x-axis label only on bottom row
        if row == 1:
            ax.set_xlabel("Pattern length")

        # Spearman ρ annotation
        lats = [r["elapsed_sec"] for r in elk_rows]
        lens = [parse_len(r) for r in elk_rows]
        rho, _ = spearmanr(lens, lats)
        ax.annotate(f"ρ={rho:.2f}", xy=(0.97, 0.05), xycoords="axes fraction",
                    ha="right", va="bottom", fontsize=6.5,
                    color=ELK_COLOR, style="italic")

    # ── legend ───────────────────────────────────────────────────────────
    handles = [
        mlines.Line2D([], [], color=ELK_COLOR,    ls="-",  lw=1.4, label="ELK structural"),
        mlines.Line2D([], [], color=ELK_COLOR,    ls="--", lw=1.4, label="ELK attr-aware"),
    ]
    if siesta_dir is not None:
        handles += [
            mlines.Line2D([], [], color=SIESTA_COLOR, ls="-",  lw=1.4, label="SIESTA structural"),
            mlines.Line2D([], [], color=SIESTA_COLOR, ls="--", lw=1.4, label="SIESTA attr-aware"),
        ]
    fig.legend(handles=handles, loc="upper center",
               ncol=len(handles), frameon=False,
               bbox_to_anchor=(0.5, 0.97), fontsize=7)

    fig.savefig(out, bbox_inches="tight")
    print(f"Saved: {out}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--elk-dir",    default="/home/balaktsis/Projects/siesta-framework/")
    ap.add_argument("--siesta-dir", default="/home/balaktsis/Downloads")
    ap.add_argument("--out",        default="./competitive_latency.pdf")
    args = ap.parse_args()
    build_figure(
        Path(args.elk_dir),
        Path(args.siesta_dir) if args.siesta_dir else None,
        Path(args.out),
    )