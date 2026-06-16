"""
tests/eval/plot_maintenance_savings.py

Reads maintenance_savings_<log_name>.jsonl output from
exp_maintenance_savings.py and produces publication-quality PDF figures.

Figures
-------
fig_maintenance_per_batch_<log_name>.pdf   (one per dataset)
    Eager bars (stacked by perspective) vs adaptive bar, per batch.
    Speedup factor annotated above each adaptive bar.

fig_maintenance_lines.pdf
    All datasets on one compact log-scale plot.
    Solid = eager, dashed = adaptive, distinct marker per dataset.

fig_maintenance_savings_bar.pdf
    Horizontal bar chart: savings % and avg speedup per dataset.

Usage
-----
    python -m tests.eval.plot_maintenance_savings \
        --results-dir tests/eval/results/ \
        --output-dir  figures/

Options
-------
    --results-dir   DIR   directory with maintenance_savings_*.jsonl files
    --output-dir    DIR   where to write PDFs (default: figures/)
    --datasets      LIST  comma-separated log_names to include (default: all)
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import OrderedDict
from pathlib import Path

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import matplotlib.ticker as mtick
    from matplotlib.lines import Line2D
    import numpy as np
except ImportError:
    print("matplotlib and numpy required:  pip install matplotlib numpy")
    sys.exit(1)


# ═══════════════════════════════════════════════════════════════════════════
# Style
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


# ── Palette ────────────────────────────────────────────────────────────────

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

PERSP_STACK  = [PAL["pink"],   PAL["brown"],  PAL["purple"],
                PAL["orange"], PAL["blue"],   PAL["red"]]
ADAPTIVE_COL = PAL["green"]   # green now belongs exclusively to adaptive

_DATASET_ORDER   = ["bpic2011", "bpic2012", "bpic2015", "bpic2017", "bpic2018"]
_DATASET_COLOURS = [PAL["blue"], PAL["orange"], PAL["red"],
                    PAL["purple"], PAL["brown"]]
_DATASET_MARKERS = ["o", "s", "D", "^", "v"]


def dataset_style(log_name: str) -> dict:
    clean = log_name.lower().replace("-", "").replace("_", "")
    for i, ref in enumerate(_DATASET_ORDER):
        if ref in clean:
            return {"color":  _DATASET_COLOURS[i % len(_DATASET_COLOURS)],
                    "marker": _DATASET_MARKERS[i % len(_DATASET_MARKERS)]}
    idx = hash(log_name) % len(_DATASET_COLOURS)
    return {"color": _DATASET_COLOURS[idx], "marker": _DATASET_MARKERS[idx]}


def pretty_name(log_name: str) -> str:
    import re
    m = re.match(r"(?i)(bpic)\s*(\d{4})", log_name)
    return f"BPIC {m.group(2)}" if m else log_name.replace("_", " ").title()


def shorten_persp(lbl: str) -> str:
    return (lbl
            .replace("lifecycle:transition", "lifecycle")
            .replace("EventOrigin",          "origin")
            .replace("Producer code",        "producer")
            .replace("activityNameEN",       "actNameEN")
            .replace("activityNameNL",       "actNameNL"))


# ═══════════════════════════════════════════════════════════════════════════
# Data loading
# ═══════════════════════════════════════════════════════════════════════════

def load_jsonl(path: Path) -> list[dict]:
    out = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return out


def load_all_results(results_dir: Path) -> OrderedDict[str, list[dict]]:
    raw: dict[str, list[dict]] = {}
    for jsonl in sorted(results_dir.glob("maintenance_savings_*.jsonl")):
        records = load_jsonl(jsonl)
        if not records:
            continue
        log_name = next(
            (r["log_name"] for r in records if r.get("event") == "dataset"),
            jsonl.stem.replace("maintenance_savings_", ""),
        )
        if any(r.get("event") == "batch_maintenance" for r in records):
            raw[log_name] = records

    def _order(name: str) -> int:
        clean = name.lower().replace("-", "").replace("_", "")
        for i, ref in enumerate(_DATASET_ORDER):
            if ref in clean:
                return i
        return 100

    return OrderedDict(sorted(raw.items(), key=lambda kv: _order(kv[0])))


def get_batches(records: list[dict]) -> list[dict]:
    return sorted([r for r in records if r.get("event") == "batch_maintenance"],
                  key=lambda r: r["batch"])


def get_summary(records: list[dict]) -> dict | None:
    return next((r for r in records if r.get("event") == "summary"), None)


# ═══════════════════════════════════════════════════════════════════════════
# Figure 1 — per-batch stacked bars (ONE file per dataset)
# ═══════════════════════════════════════════════════════════════════════════

def plot_per_batch_single(log_name: str, records: list[dict],
                          output: Path) -> None:
    bs = get_batches(records)
    s  = get_summary(records)
    n  = len(bs)
    persp_labels = list(bs[0]["eager_per_perspective"].keys())
    batch_idxs   = np.arange(n)
    bar_w        = 0.32

    fig, ax = plt.subplots(figsize=(4.5, 2.8))

    # ── Stacked eager bars ────────────────────────────────────────────
    bottoms = np.zeros(n)
    patches = []
    for p_idx, lbl in enumerate(persp_labels):
        col  = PERSP_STACK[p_idx % len(PERSP_STACK)]
        vals = np.array([r["eager_per_perspective"].get(lbl, 0.0) for r in bs])
        ax.bar(batch_idxs - bar_w/2, vals, bar_w, bottom=bottoms,
               color=col, edgecolor="white", linewidth=0.3, zorder=3)
        patches.append(mpatches.Patch(color=col, label=shorten_persp(lbl)))
        bottoms += vals

    # ── Adaptive bars ─────────────────────────────────────────────────
    adpt_vals = np.array([r["adaptive_s"] for r in bs])
    ax.bar(batch_idxs + bar_w/2, adpt_vals, bar_w,
           color=ADAPTIVE_COL, edgecolor="white", linewidth=0.3,
           zorder=3, label="adaptive")

    # ── Speedup annotations ───────────────────────────────────────────
    # Extend y-limit first so annotations fit inside the axes.
    ax.set_ylim(0, bottoms.max() * 1.22)
    for i, (r, av) in enumerate(zip(bs, adpt_vals)):
        ratio = r["eager_total_s"] / av
        ax.text(batch_idxs[i] + bar_w/2,
                av + bottoms.max() * 0.02,
                f"{ratio:.0f}\u00d7",
                ha="center", va="bottom", fontsize=6.5,
                color=ADAPTIVE_COL, fontweight="bold",
                clip_on=True)

    # ── Axes labels ───────────────────────────────────────────────────
    ax.set_xticks(batch_idxs)
    ax.set_xticklabels([f"$B_{{{r['batch']}}}$" for r in bs])
    ax.set_xlabel("Batch")
    ax.set_ylabel("Time (s)")
    # ax.grid(axis="y", alpha=0.25, zorder=0)
    ax.set_axisbelow(True)

    # ── Legend ────────────────────────────────────────────────────────
    legend_h = patches + [mpatches.Patch(color=ADAPTIVE_COL, label="adaptive")]
    leg = ax.legend(handles=legend_h, loc="best", fontsize=6.5,
                    framealpha=0.92, edgecolor="#cccccc",
                    handlelength=1.2, handletextpad=0.4,
                    title="eager (perspective) / adaptive",
                    title_fontsize=6.5)
    leg.get_frame().set_linewidth(0.4)

    # ── Savings annotation (axis-coord → stays inside) ────────────────
    if s and s.get("savings_ratio") is not None:
        sr  = s["savings_ratio"] * 100
        # ann = f"{pretty_name(log_name)}"
        # ax.annotate(ann,
        #             xy=(0.5, 0.97), xycoords="axes fraction",
        #             ha="center", va="top", fontsize=7.5,
        #             color="#222222",
        #             bbox=dict(boxstyle="round,pad=0.25",
        #                       facecolor="white", edgecolor="#cccccc",
        #                       alpha=0.9, linewidth=0.4))

    fig.tight_layout()
    fig.savefig(str(output), format="pdf")
    print(f"  → {output}")
    plt.close(fig)


def plot_per_batch(data: OrderedDict[str, list[dict]],
                   output_dir: Path) -> None:
    for log_name, records in data.items():
        out = output_dir / f"fig_maintenance_per_batch_{log_name}.pdf"
        plot_per_batch_single(log_name, records, out)


# ═══════════════════════════════════════════════════════════════════════════
# Figure 2 — per-batch lines, all datasets, log scale
# ═══════════════════════════════════════════════════════════════════════════

def plot_lines(data: OrderedDict[str, list[dict]], output: Path) -> None:
    fig, ax = plt.subplots(figsize=(4.5, 3.2))

    # Collect all x values to compute right-margin padding.
    all_xs: set[int] = set()
    for records in data.values():
        for r in get_batches(records):
            all_xs.add(r["batch"])
    x_max = max(all_xs)

    for log_name, records in data.items():
        bs  = get_batches(records)
        st  = dataset_style(log_name)
        xs  = [r["batch"] for r in bs]
        eagr = [r["eager_total_s"] for r in bs]
        adpt = [r["adaptive_s"]    for r in bs]

        ax.plot(xs, eagr, color=st["color"], linewidth=1.5, linestyle="-",
                marker=st["marker"], markersize=5,
                markeredgecolor="white", markeredgewidth=0.4,
                zorder=3, label=pretty_name(log_name))
        ax.plot(xs, adpt, color=st["color"], linewidth=1.5, linestyle="--",
                marker=st["marker"], markersize=5,
                markeredgecolor="white", markeredgewidth=0.4,
                zorder=3, alpha=0.85)

    # Extra right margin so labels don't clip.
    ax.set_xlim(min(all_xs) - 0.3, x_max + 0.5)
    ax.set_xticks(sorted(all_xs))
    ax.set_xticklabels([f"$B_{{{r['batch']}}}$" for r in bs])
    


    # ── Legend ────────────────────────────────────────────────────────
    # Dataset handles (colour/marker) from the eager lines plotted above.
    handles, _ = ax.get_legend_handles_labels()
    # Add line-style guide handles.
    guides = handles + [
        Line2D([0], [0], color=PAL["grey"], lw=1.5, ls="-",
               label="eager (sum)"),
        Line2D([0], [0], color=PAL["grey"], lw=1.5, ls="--",
               label="adaptive"),
    ]
    leg = ax.legend(handles=guides, loc="best", fontsize=7,
                    framealpha=0.92, edgecolor="#cccccc",
                    ncol=2, columnspacing=1,
                    handlelength=1.8, handletextpad=0.4)
    leg.get_frame().set_linewidth(0.4)
    ax.set_xticklabels([f"$B_{{{r['batch']}}}$" for r in bs])

    ax.set_yscale("log")
    ax.set_xlabel("Batch")
    ax.legend(loc="best")
    ax.set_ylabel("Maintenance time per batch (s)")
    # ax.grid(axis="y", alpha=0.25, which="both", zorder=0)
    # ax.grid(axis="x", alpha=0.15, zorder=0)
    ax.set_axisbelow(True)
    ax.yaxis.set_minor_formatter(mtick.NullFormatter())

    fig.tight_layout()
    fig.savefig(str(output), format="pdf")
    print(f"  → {output}")
    plt.close(fig)


# ═══════════════════════════════════════════════════════════════════════════
# Figure 3 — savings summary bar
# ═══════════════════════════════════════════════════════════════════════════

def plot_savings_bar(data: OrderedDict[str, list[dict]],
                     output: Path) -> None:
    labels, svals, spdups, cols = [], [], [], []
    for log_name, records in data.items():
        s = get_summary(records)
        if not s or s.get("savings_ratio") is None:
            continue
        bs = get_batches(records)
        labels.append(pretty_name(log_name))
        svals.append(s["savings_ratio"] * 100)
        spdups.append(np.mean([r["eager_total_s"] / r["adaptive_s"]
                               for r in bs]))
        cols.append(dataset_style(log_name)["color"])

    if not labels:
        print("  No summary records — skipping savings bar.")
        return

    fig, ax = plt.subplots(figsize=(3.8, max(1.6, len(labels) * 0.55 + 0.5)))

    y    = np.arange(len(labels))
    bars = ax.barh(y, svals, color=cols, height=0.48, zorder=3,
                   edgecolor="white", linewidth=0.3)

    # Compute max annotation width to set xlim before annotating.
    ax.set_xlim(0, 116)
    for bar, val, sp in zip(bars, svals, spdups):
        ax.text(bar.get_width() + 0.6,
                bar.get_y() + bar.get_height() / 2,
                f"{val:.1f}%  ({sp:.0f}\u00d7)",
                va="center", fontsize=7.5, clip_on=False)

    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=8.5)
    ax.set_xlabel("Maintenance savings (%)")
    ax.xaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
    ax.grid(axis="x", alpha=0.25, zorder=0)
    ax.set_axisbelow(True)
    ax.invert_yaxis()

    fig.tight_layout()
    fig.savefig(str(output), format="pdf")
    print(f"  → {output}")
    plt.close(fig)


# ═══════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Plot maintenance savings results.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--results-dir", type=Path,
                    default=Path("tests/eval/results"),
                    help="Directory with maintenance_savings_*.jsonl files.")
    ap.add_argument("--output-dir",  type=Path,
                    default=Path("figures"),
                    help="Output directory for PDFs.")
    ap.add_argument("--datasets",    type=str, default=None,
                    help="Comma-separated log_names to include (default: all).")
    args = ap.parse_args()

    apply_style()

    all_data = load_all_results(args.results_dir)
    if not all_data:
        print(f"No maintenance_savings_*.jsonl with batch records in "
              f"{args.results_dir}")
        sys.exit(1)

    if args.datasets:
        keep = {d.strip() for d in args.datasets.split(",")}
        all_data = OrderedDict(
            (k, v) for k, v in all_data.items() if k in keep)
        if not all_data:
            print(f"No matching datasets. Available: {list(all_data.keys())}")
            sys.exit(1)

    print(f"Datasets: {list(all_data.keys())}")
    args.output_dir.mkdir(parents=True, exist_ok=True)

    plot_per_batch(all_data,    args.output_dir)           # one PDF per dataset
    plot_lines(all_data,        args.output_dir / "fig_maintenance_lines.pdf")
    plot_savings_bar(all_data,  args.output_dir / "fig_maintenance_savings_bar.pdf")

    print("Done.")


if __name__ == "__main__":
    main()