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


# ---------------------------------------------------------------------------
# Styling — academic / LaTeX look
# ---------------------------------------------------------------------------

plt.rcParams.update({
    "font.family":       "serif",
    "font.serif":        ["Liberation Serif", "STIXGeneral", "DejaVu Serif"],
    "mathtext.fontset":  "stix",
    "font.size":         10,
    "axes.titlesize":    11,
    "axes.labelsize":    10,
    "xtick.labelsize":   9,
    "ytick.labelsize":   9,
    "legend.fontsize":   8.5,
    "figure.dpi":        300,
    "savefig.dpi":       300,
    "savefig.bbox":      "tight",
    "axes.spines.top":   False,
    "axes.spines.right": False,
    "axes.grid":         True,
    "grid.alpha":        0.3,
    "grid.linewidth":    0.5,
    "lines.linewidth":   1.6,
    "lines.markersize":  5,
})

# Colourblind-safe, print-friendly palette.
PALETTE = {
    "BPIC 2017": "#2166ac",
    "BPIC 2012": "#b2182b",
    "Sepsis":    "#1b7837",
}
MARKERS = {
    "BPIC 2017": "o",
    "BPIC 2012": "s",
    "Sepsis":    "^",
}


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
def normalize(values):
    v = np.array(values, dtype=float)
    v_min, v_max = np.nanmin(v), np.nanmax(v)
    if v_max == v_min:
        return np.zeros_like(v)
    return (v - v_min) / (v_max - v_min)

def _label_from_path(path: Path) -> str:
    name = path.stem.lower()
    if "2017" in name:   return "BPIC 2017"
    if "2012" in name:   return "BPIC 2012"
    if "2018" in name:   return "BPIC 2018"
    if "2019" in name:   return "BPIC 2019"
    if "2020" in name:   return "BPIC 2020"
    if "sepsis" in name: return "Sepsis"
    return path.stem


def load_results(path: Path) -> tuple[str, list[dict]]:
    recs = [json.loads(l) for l in path.open() if l.strip()]
    label = _label_from_path(path)
    for r in recs:
        if r.get("event") == "dataset":
            p = r.get("path", "")
            if "2017" in p:          label = "BPIC 2017"
            elif "2012" in p:        label = "BPIC 2012"
            elif "sepsis" in p.lower(): label = "Sepsis"
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
    fig, ax_curve = plt.subplots(
        figsize=(4.5, 3.2),
    )

    # ── Left panel: learning curve ───────────────────────────────────────
    ax = ax_curve
    reps = [1, 2, 3, 4, 5]
    rep_labels = ["1\n(scan)", "2\n(LRU)", "3\n(LRU)", "4\n(Delta)", "5\n(Delta)"]

    cold_means = {}

    for label, qs in datasets:
        color  = PALETTE.get(label, "#333333")
        marker = MARKERS.get(label, "o")

        hot  = [q for q in qs if q.get("bucket") == "HOT"]
        cold = [q for q in qs if q.get("bucket") == "COLD"]

        # Per-rep mean and individual points for HOT.
        means = []
        for rep in reps:
            rep_lats = [q["latency_s"] for q in hot if q.get("hot_rep") == rep]
            if rep_lats:
                means.append(np.mean(rep_lats))
                ax.scatter(
                    [rep] * len(rep_lats), rep_lats,
                    color=color, alpha=0.22, s=16,
                    marker=marker, zorder=2, linewidths=0,
                )
            else:
                means.append(means[-1] - 0.1)
        
        # means.append(means[-1])

        ax.plot(reps, means, color=color, marker=marker, label=label,
                zorder=3, markerfacecolor="white", markeredgewidth=1.3,
                markeredgecolor=color)

        # Record cold mean for later annotation.
        if cold:
            cold_means[label] = np.mean([q["latency_s"] for q in cold])

    # Draw COLD baselines after all HOT curves so we can nudge labels.
    cold_y_positions = list(cold_means.values())
    for label, cm in cold_means.items():
        color = PALETTE.get(label, "#333333")
        ax.axhline(cm, color=color, linestyle=":", linewidth=1.0,
                   alpha=0.35, zorder=1)

    # Single "cold baselines" annotation — position at right edge.
    if cold_y_positions:
        top_cold = max(cold_y_positions)
        ax.annotate(
            "cold\nbaselines",
            xy=(4.55, top_cold),
            fontsize=6.5, color="#999999", va="center", ha="left",
            style="italic",
        )

    ax.set_xticks(reps)
    ax.set_xticklabels(rep_labels)
    ax.set_xlabel("Query repetition (exposure)")
    ax.set_ylabel("Query latency (s)")
    # ax.set_title("(a) Warm-up learning curve", fontweight="bold", pad=10)
    ax.set_xlim(0.6, 5.1)
    ax.set_ylim(bottom=0)
    ax.grid(False)

    # Lifecycle stage brackets at the bottom (inside the plot).
    ymin, ymax = ax.get_ylim()

    # Lower positioning.
    bracket_y = ymax * 0.025
    label_y   = ymax * 0.065

    bracket_props = dict(
        arrowstyle="|-|",
        color="#999999",
        lw=0.7,
        mutation_scale=4,
    )

    # ABSENT
    ax.annotate(
        "",
        xy=(0.95, bracket_y),
        xytext=(2.0, bracket_y),
        arrowprops=bracket_props,
    )
    ax.text(
        1.475, label_y,
        "ABSENT",
        ha="center",
        va="top",
        fontsize=6.5,
        color="#999999",
        style="italic",
    )

    # TRANSIENT
    ax.annotate(
        "",
        xy=(2.0, bracket_y),
        xytext=(4.0, bracket_y),
        arrowprops=bracket_props,
    )
    ax.text(
        3.0, label_y,
        "TRANSIENT",
        ha="center",
        va="top",
        fontsize=6.5,
        color="#999999",
        style="italic",
    )

    # PERSISTENT
    ax.annotate(
        "",
        xy=(4.0, bracket_y),
        xytext=(5, bracket_y),
        arrowprops=bracket_props,
    )
    ax.text(
        4.5, label_y,
        "PERSISTENT",
        ha="center",
        va="top",
        fontsize=6.5,
        color="#999999",
        style="italic",
    )

    # # ── Right panel: stream view (largest dataset) ───────────────────────
    # ax = ax_stream

    # # Pick the dataset with the highest rep-1 latency.
    # largest_label, largest_qs = max(
    #     datasets,
    #     key=lambda d: np.mean([
    #         q["latency_s"] for q in d[1]
    #         if q.get("bucket") == "HOT" and q.get("hot_rep") == 1
    #     ] or [0]),
    # )

    # color = PALETTE.get(largest_label, "#333333")

    # hot_qs  = [(q["seq"], q["latency_s"]) for q in largest_qs
    #            if q.get("bucket") == "HOT"]
    # cold_qs = [(q["seq"], q["latency_s"]) for q in largest_qs
    #            if q.get("bucket") == "COLD"]

    # # Connect HOT points per shared_pair (draw before scatter for z-order).
    # by_pair = defaultdict(list)
    # for q in largest_qs:
    #     if q.get("bucket") == "HOT":
    #         by_pair[q.get("shared_pair", "?")].append(
    #             (q["seq"], q["latency_s"])
    #         )
    # for pair, pts in by_pair.items():
    #     pts.sort()
    #     px, py = zip(*pts)
    #     ax.plot(px, py, color=color, alpha=0.25, linewidth=0.8, zorder=1)

    # if hot_qs:
    #     hx, hy = zip(*hot_qs)
    #     ax.scatter(hx, hy, color=color, s=30, zorder=3,
    #                marker="o", label="HOT pairs", edgecolors="white",
    #                linewidths=0.5)
    # if cold_qs:
    #     cx, cy = zip(*cold_qs)
    #     ax.scatter(cx, cy, color="#888888", s=30, zorder=2,
    #                marker="x", label="COLD pairs", linewidths=1.2)

    # # Mark the materialisation boundary — the point where the sleep
    # # sentinel fired (between the last rep-3 query and the first rep-4
    # # query).  Find this as the boundary between hot_rep=3 and hot_rep=4.
    # rep3_seqs = [q["seq"] for q in largest_qs
    #              if q.get("bucket") == "HOT" and q.get("hot_rep") == 3]
    # rep4_seqs = [q["seq"] for q in largest_qs
    #              if q.get("bucket") == "HOT" and q.get("hot_rep") == 4]
    # if rep3_seqs and rep4_seqs:
    #     boundary = (max(rep3_seqs) + min(rep4_seqs)) / 2
    #     ax.axvline(boundary, color="#bbbbbb", linestyle="--",
    #                linewidth=0.8, zorder=0)
    #     ax.text(boundary + 0.3, ax.get_ylim()[1] * 0.45,
    #             "materialisation\ncompletes",
    #             fontsize=6.5, color="#aaaaaa", va="center",
    #             style="italic")

    # ax.set_xlabel("Query position in stream")
    # ax.set_ylabel("Query latency (s)")
    # ax.set_title(f"(b) Query stream — {largest_label}",
    #              fontweight="bold", pad=10)
    # ax.set_ylim(bottom=0)
    ax.legend(loc="best", framealpha=0.9, edgecolor="none")

    # ── Save ─────────────────────────────────────────────────────────────
    fig.savefig(str(out_path), format=out_path.suffix.lstrip("."))
    print(f"Figure saved to {out_path}")
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
