"""
tests/eval/plot_warmup.py

Generate the warm-up curve figure for Experiment 6.3.1.

Reads one or more JSONL result files (one per dataset) and produces
a two-panel PDF:

  Left panel  — Learning curve: mean HOT-pair latency by exposure
                number (hot_rep 1-4), one line per dataset.  COLD
                baselines shown as horizontal dashed lines.

  Right panel — Latency by query-stream position (seq) for the
                largest dataset, showing HOT vs COLD interleaving
                and the per-pair latency trajectory.

Usage:
    python tests/eval/plot_warmup.py \
        results/6_3_1_warmup_bpic2017.jsonl \
        results/6_3_1_warmup_bpic2012.jsonl \
        results/6_3_1_warmup_sepsis.jsonl \
        -o figures/fig_warmup.pdf
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


# ═══════════════════════════════════════════════════════════════════════════
# Style  (matches plot_maintenance_savings.py / plot_lru_eviction.py)
# ═══════════════════════════════════════════════════════════════════════════

def apply_style() -> None:
    plt.rcParams.update({
        "font.family":        "serif",
        "font.serif":         ["STIX", "STIXGeneral", "Liberation Serif",
                               "DejaVu Serif", "Times New Roman"],
        "mathtext.fontset":   "stix",
        "font.size":          9,
        "axes.labelsize":     10,
        "axes.titlesize":     10,
        "legend.fontsize":    8,
        "xtick.labelsize":    8.5,
        "ytick.labelsize":    8.5,
        "axes.linewidth":     0.6,
        "grid.linewidth":     0.4,
        "lines.linewidth":    1.4,
        "patch.linewidth":    0.5,
        "xtick.major.width":  0.5,
        "ytick.major.width":  0.5,
        "xtick.major.pad":    3,
        "ytick.major.pad":    3,
        "axes.grid":          False,
        "figure.dpi":         300,
        "savefig.bbox":       "tight",
        "savefig.pad_inches": 0.05,
    })


# ── Palette (matches plot_maintenance_savings.py) ──────────────────────────

PAL = {
    "blue":   "#3C76AF",
    "orange": "#E07B39",
    "green":  "#4DA54A",
    "red":    "#C44E52",
    "purple": "#8E6DB5",
    "brown":  "#937860",
    "pink":   "#D57EBF",
    "grey":   "#7F7F7F",
}

# Okabe-Ito colorblind-safe palette for per-dataset lines.
_DATASET_ORDER   = ["bpic2011", "bpic2012", "bpic2015", "bpic2017", "bpic2018",
                    "sepsis", "synthetic"]
_DATASET_COLOURS = [
    "#0072B2",   # blue       — bpic2011
    "#D55E00",   # vermillion — bpic2012
    "#009E73",   # green      — bpic2015
    "#CC79A7",   # pink       — bpic2017
    "#E69F00",   # orange     — bpic2018
    "#56B4E9",   # sky blue   — sepsis
    "#F0E442",   # yellow     — synthetic
]
_DATASET_MARKERS = ["o", "s", "D", "^", "v", "P", "X"]


def dataset_style(label: str) -> dict:
    """Return {color, marker} for a display label or log_name."""
    clean = label.lower().replace("-", "").replace("_", "").replace(" ", "")
    for i, ref in enumerate(_DATASET_ORDER):
        if ref in clean:
            return {
                "color":  _DATASET_COLOURS[i % len(_DATASET_COLOURS)],
                "marker": _DATASET_MARKERS[i % len(_DATASET_MARKERS)],
            }
    idx = hash(label) % len(_DATASET_COLOURS)
    return {"color": _DATASET_COLOURS[idx], "marker": _DATASET_MARKERS[idx]}


def pretty_name(log_name: str) -> str:
    import re
    m = re.match(r"(?i)(bpic)\s*(\d{4})", log_name)
    if m:
        return f"BPIC {m.group(2)}"
    if "sepsis" in log_name.lower():
        return "Sepsis"
    return log_name.replace("_", " ").title()


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _label_from_path(path: Path) -> str:
    name = path.stem.lower()
    if "2011" in name:   return "BPIC 2011"
    if "2012" in name:   return "BPIC 2012"
    if "2015" in name:   return "BPIC 2015"
    if "2017" in name:   return "BPIC 2017"
    if "2018" in name:   return "BPIC 2018"
    if "2019" in name:   return "BPIC 2019"
    if "2020" in name:   return "BPIC 2020"
    if "sepsis" in name: return "Sepsis"
    return path.stem


def load_results(path: Path) -> tuple[str, list[dict]]:
    recs  = [json.loads(l) for l in path.open() if l.strip()]
    label = _label_from_path(path)
    for r in recs:
        if r.get("event") == "dataset":
            p = r.get("path", "")
            if "2011" in p:                label = "BPIC 2011"
            elif "2012" in p:              label = "BPIC 2012"
            elif "2015" in p:              label = "BPIC 2015"
            elif "2017" in p:              label = "BPIC 2017"
            elif "2018" in p:              label = "BPIC 2018"
            elif "sepsis" in p.lower():    label = "Sepsis"
            break
    queries = [
        r for r in recs
        if r.get("event") == "query" and r.get("system") == "adaptive"
    ]
    return label, queries


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

def plot(datasets: list[tuple[str, list[dict]]], out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(5, 3.2))

    reps       = [1, 2, 3, 4, 5]
    rep_labels = ["1\n(scan)", "2\n(LRU)", "3\n(LRU)", "4\n(Delta)", "5\n(Delta)"]

    cold_means: dict[str, float] = {}

    for label, qs in datasets:
        st     = dataset_style(label)
        color  = st["color"]
        marker = st["marker"]

        hot  = [q for q in qs if q.get("bucket") == "HOT"]
        cold = [q for q in qs if q.get("bucket") == "COLD"]

        # Per-rep mean and jittered scatter for HOT queries.
        means = []
        for rep in reps:
            rep_lats = [q["latency_s"] for q in hot if q.get("hot_rep") == rep]
            if rep_lats:
                means.append(np.mean(rep_lats))
                ax.scatter(
                    [rep] * len(rep_lats), rep_lats,
                    color=color, alpha=0.22, s=14,
                    marker=marker, zorder=2, linewidths=0,
                )
            else:
                means.append(means[-1] if means else 0.0)

        ax.plot(reps, means,
                color=color, marker=marker, label=label,
                linewidth=1.4, zorder=3,
                markerfacecolor="white", markeredgewidth=1.2,
                markeredgecolor=color)

        if cold:
            cold_means[label] = np.mean([q["latency_s"] for q in cold])

    # COLD baselines — drawn after HOT curves (z-order).
    for label, cm in cold_means.items():
        color = dataset_style(label)["color"]
        ax.axhline(cm, color=color, linestyle=":", linewidth=0.9,
                   alpha=0.35, zorder=1)

    if cold_means:
        top_cold = max(cold_means.values())
        ax.annotate(
            "cold\nbaselines",
            xy=(4.6, top_cold),
            fontsize=6.5, color="#999999",
            va="center", ha="left", style="italic",
        )

    # ── Axes ──────────────────────────────────────────────────────────────
    ax.set_xticks(reps)
    ax.set_xticklabels(rep_labels)
    ax.set_xlabel("Query repetition (exposure)")
    ax.set_ylabel("Query latency (s)")
    ax.set_xlim(0.8, 5.1)
    ax.set_ylim(bottom=0)
    # ax.spines["top"].set_visible(False)
    # ax.spines["right"].set_visible(False)
    ax.set_axisbelow(True)
    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:g}"))
    ax.set_ylim(bottom=0.3)   # avoid log(0); adjust to your minimum latency

    # ── Lifecycle stage brackets ───────────────────────────────────────────
    ymin, ymax = ax.get_ylim()
    # bracket_y  = ymax * 0.025
    # label_y    = ymax * 0.065
    
    # bracket_props = dict(
    #     arrowstyle="|-|", color="#aaaaaa", lw=0.7, mutation_scale=4,
    # )

    # for (x0, x1), stage in [
    #     ((0.95, 2.0), "ABSENT"),
    #     ((2.0,  4.0), "TRANSIENT"),
    #     ((4.0,  5.0), "PERSISTENT"),
    # ]:
    #     ax.annotate("", xy=(x0, bracket_y), xytext=(x1, bracket_y),
    #                 arrowprops=bracket_props)
    #     ax.text((x0 + x1) / 2, label_y, stage,
    #             ha="center", va="top", fontsize=6.5,
    #             color="#aaaaaa", style="italic")
    bracket_y_frac = 0.04   # axes fraction (0 = bottom, 1 = top)
    label_y_frac   = 0.10
    bracket_props  = dict(
        arrowstyle="|-|", color="#aaaaaa", lw=0.7, mutation_scale=4,
    )
    xform = ax.get_xaxis_transform()   # x=data coords, y=axes fraction

    for (x0, x1), stage in [
        ((0.95, 2.0), "ABSENT"),
        ((2.0,  4.0), "TRANSIENT"),
        ((4.0,  5.0), "PERSISTENT"),
    ]:
        ax.annotate("",
                    xy=(x0, bracket_y_frac), xytext=(x1, bracket_y_frac),
                    xycoords=xform, textcoords=xform,
                    arrowprops=bracket_props)
        ax.text((x0 + x1) / 2, label_y_frac, stage,
                ha="center", va="bottom", fontsize=6.5,
                color="#aaaaaa", style="italic",
                transform=xform)

    # ── Legend ────────────────────────────────────────────────────────────
    leg = ax.legend(loc="upper center", framealpha=0.92, edgecolor="#cccccc",
                    handlelength=1.6, handletextpad=0.4, labelspacing=0.3)
    leg.get_frame().set_linewidth(0.4)

    # ── Save ──────────────────────────────────────────────────────────────
    fig.tight_layout()
    fig.savefig(str(out_path), format=out_path.suffix.lstrip("."))
    print(f"  → {out_path}")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Plot the warm-up curve (Experiment 6.3.1).",
    )
    ap.add_argument("jsonl", nargs="+", type=Path,
                    help="One or more JSONL result files to plot.")
    ap.add_argument("-o", "--output", type=Path,
                    default=Path("figures/fig_warmup.pdf"),
                    help="Output PDF path (default: figures/fig_warmup.pdf).")
    args = ap.parse_args()

    apply_style()

    datasets = []
    for p in args.jsonl:
        if not p.exists():
            print(f"WARNING: {p} not found, skipping.", file=sys.stderr)
            continue
        label, qs = load_results(p)
        if not qs:
            print(f"WARNING: {p} has no adaptive queries, skipping.",
                  file=sys.stderr)
            continue
        datasets.append((label, qs))
        print(f"  loaded {label}: {len(qs)} queries")

    if not datasets:
        print("ERROR: no data to plot.", file=sys.stderr)
        return 1

    args.output.parent.mkdir(parents=True, exist_ok=True)
    plot(datasets, args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())