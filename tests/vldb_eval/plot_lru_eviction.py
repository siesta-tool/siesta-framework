"""
tests/eval/plot_lru_eviction.py

Reads 6_3_3_lru_eviction_<log_name>.jsonl output from exp_lru_eviction.py
and produces two publication-quality PDFs per dataset.

Figures
-------
fig_lru_eviction_stream_<log_name>.pdf   (one per dataset)
    Per-query latency vs. stream position for both workloads.
    Skewed stream (left): HOT queries converge from cold-scan to Delta in
    ~13 positions; COLD queries remain at cold-scan baseline throughout.
    Uniform stream (full width): flat at cold-scan baseline — 619 unique
    pairs exceed the 128-slot LRU, so no pair benefits from caching.
    A rolling mean overlays the scatter for each workload.

fig_lru_eviction_lines.pdf   (all datasets combined)
    All datasets on one compact plot.
    Solid = skewed HOT rolling mean, dashed = uniform rolling mean.
    Distinct colour per dataset (analogous to fig_maintenance_lines.pdf).

fig_lru_speedup_bar.pdf   (all datasets combined)
    Horizontal bar chart: speedup factor (uniform mean / skewed
    post-convergence mean) per dataset, analogous to the savings bar
    in plot_maintenance_savings.py.

Usage
-----
    python -m tests.eval.plot_lru_eviction \\
        --results-dir tests/eval/results/ \\
        --output-dir  figures/

Options
-------
    --results-dir   DIR   directory with 6_3_3_lru_eviction_*.jsonl files
    --output-dir    DIR   where to write PDFs (default: figures/)
    --datasets      LIST  comma-separated log_names to include (default: all)
"""

from __future__ import annotations

import argparse
import json
import re
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
# Style  (identical to plot_maintenance_savings.py)
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


# ── Palette (identical to plot_maintenance_savings.py) ─────────────────────

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

_DATASET_ORDER   = ["bpic2011", "bpic2012", "bpic2015", "bpic2017", "bpic2018",
                    "synthetic"]
# Okabe-Ito colorblind-safe palette: maximises perceptual distance,
# distinguishable in greyscale print.
_DATASET_COLOURS = [
    "#0072B2",   # blue         — bpic2011
    "#D55E00",   # vermillion   — bpic2012
    "#009E73",   # green        — bpic2015
    "#CC79A7",   # pink/purple  — bpic2017
    "#E69F00",   # orange       — bpic2018
    "#56B4E9",   # sky blue     — synthetic
]
_DATASET_MARKERS = ["o", "s", "D", "^", "v", "P"]

# ── Series colours (NONE of these appear in _DATASET_COLOURS) ─────────────
COL_HOT     = "#C44E52"   # PAL red       — skewed hot pairs
COL_COLD    = "#937860"   # PAL brown     — skewed cold pairs
COL_UNIFORM = "#7F7F7F"   # PAL grey      — uniform stream
COL_PERSIST = "#4DA54A"   # PAL green     — PERSISTENT tier highlight
                           #   (distinct from Okabe #009E73 used for bpic2015)

# ── Skewness-level progression (most → least concentrated) ────────────────
_SKEWNESS_PAL: list[str] = [
    "#C44E52",   # red        — most concentrated (matches COL_HOT)
    "#4DA54A",   # green
    "#8E6DB5",   # purple
    "#937860",   # brown
    "#D57EBF",   # pink
]

def dataset_style(log_name: str) -> dict:
    clean = log_name.lower().replace("-", "").replace("_", "")
    for i, ref in enumerate(_DATASET_ORDER):
        if ref in clean:
            return {"color":  _DATASET_COLOURS[i % len(_DATASET_COLOURS)],
                    "marker": _DATASET_MARKERS[i % len(_DATASET_MARKERS)]}
    idx = hash(log_name) % len(_DATASET_COLOURS)
    return {"color": _DATASET_COLOURS[idx], "marker": _DATASET_MARKERS[idx]}


def pretty_name(log_name: str) -> str:
    m = re.match(r"(?i)(bpic)\s*(\d{4})", log_name)
    if m:
        return f"BPIC {m.group(2)}"
    if "synthetic" in log_name.lower():
        return "Synthetic"
    return log_name.replace("_", " ").title()


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


def load_all_results(results_dir: Path) -> OrderedDict[str, dict]:
    raw: dict[str, dict] = {}
    for jsonl in sorted(results_dir.glob("6_3_3_lru_eviction_*.jsonl")):
        records = load_jsonl(jsonl)
        if not records:
            continue
        log_name = next(
            (r["log_name"] for r in records if r.get("event") == "dataset"),
            jsonl.stem.replace("6_3_3_lru_eviction_", ""),
        )
        parsed = parse_streams(records)
        if parsed.get("n_hot_levels") or parsed.get("uniform"):
            raw[log_name] = parsed

    def _order(name: str) -> int:
        clean = name.lower().replace("-", "").replace("_", "")
        for i, ref in enumerate(_DATASET_ORDER):
            if ref in clean:
                return i
        return 100

    return OrderedDict(sorted(raw.items(), key=lambda kv: _order(kv[0])))


# Colour progression for skewness levels (most concentrated → most diffuse).
# Keys are n_hot values; unlisted values get a fallback.
# Skewness levels: cool → warm as concentration decreases.
_SKEWNESS_PAL: list[str] = [
    "#0072B2",   # blue       — most concentrated
    "#009E73",   # green
    "#E69F00",   # orange
    "#D55E00",   # vermillion
    "#CC79A7",   # pink
]

def _skewness_color(n_hot: int, all_levels: list[int]) -> str:
    """Map an n_hot value to a colour, ordered by concentration."""
    levels = sorted(all_levels)
    idx    = levels.index(n_hot) if n_hot in levels else len(levels)
    return _SKEWNESS_PAL[idx % len(_SKEWNESS_PAL)]


def parse_streams(records: list[dict]) -> dict:
    """
    Extract per-stream data from one JSONL file.

    Handles both the new format (workload = "hot_N") and the old format
    (workload = "skewed", with n_hot taken from the dataset record).

    Returns:
      {
        "hot_3":   {"queries": [...], "sleep_after_seq": int | None, "n_hot": 3},
        "hot_10":  {...},
        "uniform": {"queries": [...], "sleep_after_seq": None,       "n_hot": N},
        "n_pairs": int,
        "lru_cap": int,
        "n_hot_levels": [3, 10, 30],   # discovered from records
      }
    """
    ds_rec  = next((r for r in records if r.get("event") == "dataset"), {})
    queries = [r for r in records if r.get("event") == "query"]

    # Discover all workload labels present in the file.
    all_labels = sorted({q.get("workload") for q in queries if q.get("workload")})

    # Normalise old "skewed" label → "hot_N" using dataset n_hot field.
    legacy_n_hot = ds_rec.get("n_hot")
    if "skewed" in all_labels and legacy_n_hot is not None:
        for q in queries:
            if q.get("workload") == "skewed":
                q["workload"] = f"hot_{legacy_n_hot}"
        all_labels = sorted({q.get("workload") for q in queries if q.get("workload")})

    result: dict = {
        "n_pairs":      ds_rec.get("n_pairs",      0),
        "lru_cap":      ds_rec.get("lru_capacity", 128),
        "n_hot_levels": [],
    }

    for label in all_labels:
        qs = sorted([q for q in queries if q.get("workload") == label],
                    key=lambda q: q["seq"])
        if not qs:
            continue

        if label == "uniform":
            result["uniform"] = {
                "queries":         qs,
                "sleep_after_seq": None,
                "n_hot":           result["n_pairs"],
            }
        elif label.startswith("hot_"):
            try:
                n_hot = int(label.split("_", 1)[1])
            except ValueError:
                continue
            # Sleep boundary: seq just before first PERSISTENT query.
            pers_seqs = [q["seq"] for q in qs if q.get("tier") == "PERSISTENT"]
            sleep_seq = (min(pers_seqs) - 1) if pers_seqs else None
            result[label] = {
                "queries":         qs,
                "sleep_after_seq": sleep_seq,
                "n_hot":           n_hot,
            }
            result["n_hot_levels"].append(n_hot)

    result["n_hot_levels"] = sorted(result["n_hot_levels"])
    return result


def _rolling_mean(xs: list, ys: list, window: int) -> tuple[np.ndarray, np.ndarray]:
    """Compute centred rolling mean; uses minimum periods = 1."""
    if not xs:
        return np.array([]), np.array([])
    xs_arr = np.array(xs, dtype=float)
    ys_arr = np.array(ys, dtype=float)
    half = window // 2
    smoothed = np.array([
        ys_arr[max(0, i - half):i + half + 1].mean()
        for i in range(len(ys_arr))
    ])
    return xs_arr, smoothed


# ═══════════════════════════════════════════════════════════════════════════
# Figure 1 — per-query scatter + rolling mean  (one file per dataset)
# ═══════════════════════════════════════════════════════════════════════════

def plot_stream_single(log_name: str, data: dict, output: Path) -> None:
    """
    Detailed scatter plot for the most concentrated skewness level (lowest
    n_hot), showing individual query transitions HOT TRANSIENT → PERSISTENT
    alongside the uniform cold-scan baseline.
    """
    levels  = data.get("n_hot_levels", [])
    if not levels:
        print(f"  SKIP {output.name}: no hot streams found.")
        return
    level_key = f"hot_{min(levels)}"          # most concentrated level
    stream    = data.get(level_key, {})
    skewed    = stream.get("queries", [])
    uniform   = data.get("uniform", {}).get("queries", [])
    sleep_x   = stream.get("sleep_after_seq")

    fig, ax = plt.subplots(figsize=(4.5, 2.8))

    # ── Uniform scatter (drawn first, behind skewed) ──────────────────
    sk_max = max((q["seq"] for q in skewed), default=0)

    if uniform:
        # Hard-clip to the same length as the skewed stream so both
        # streams share identical x-axis range.
        uniform_vis = [q for q in uniform if q["seq"] <= sk_max]
        ux = [q["seq"] for q in uniform_vis]
        uy = [q["latency_s"] for q in uniform_vis]
        if ux:
            ax.scatter(ux, uy, color=COL_UNIFORM, s=8, alpha=0.40,
                       linewidths=0, zorder=2, label="Uniform")
            rx, ry = _rolling_mean(ux, uy, window=5)
            ax.plot(rx, ry, color=COL_UNIFORM, linewidth=1.3, linestyle="--",
                    zorder=4)

    # ── Skewed: COLD queries ──────────────────────────────────────────
    cold_qs = [q for q in skewed if q.get("tag") == "COLD"]
    if cold_qs:
        ax.scatter([q["seq"] for q in cold_qs],
                   [q["latency_s"] for q in cold_qs],
                   color=COL_COLD, s=22, marker="s", alpha=0.85,
                   linewidths=0.3, edgecolors="white",
                   zorder=5, label="Skewed — cold")

    # ── Skewed: HOT queries, classified by tier ───────────────────────
    hot_trans = [q for q in skewed
                 if q.get("tag") == "HOT" and q.get("tier") == "TRANSIENT"]
    hot_pers  = [q for q in skewed
                 if q.get("tag") == "HOT" and q.get("tier") == "PERSISTENT"]

    if hot_trans:
        ax.scatter([q["seq"] for q in hot_trans],
                   [q["latency_s"] for q in hot_trans],
                   color=COL_HOT, s=22, marker="o", alpha=0.75,
                   linewidths=0.3, edgecolors="white",
                   zorder=5, label="skewed (TRANSIENT)")
    if hot_pers:
        ax.scatter([q["seq"] for q in hot_pers],
                   [q["latency_s"] for q in hot_pers],
                   color=COL_PERSIST, s=28, marker="D", alpha=0.95,
                   linewidths=0.3, edgecolors="white",
                   zorder=6, label="skewed (PERSISTENT)")

    # Rolling mean for skewed: HOT queries only, to show the convergence
    # of the hot set without the cold-pair spikes distorting the line.
    hot_qs = [q for q in skewed if q.get("tag") == "HOT"]
    if hot_qs:
        sx = [q["seq"] for q in hot_qs]
        sy = [q["latency_s"] for q in hot_qs]
        rx, ry = _rolling_mean(sx, sy, window=3)
        ax.plot(rx, ry, color=COL_HOT, linewidth=1.4, linestyle="-",
                zorder=4)

    # ── Sleep / materialisation boundary ─────────────────────────────
    if sleep_x is not None:
        bx = sleep_x + 0.5
        ax.axvline(bx, color="#bbbbbb", linewidth=0.7, linestyle=":",
                   zorder=1)
        ax.text(bx + 0.5, ax.get_ylim()[1] * 0.97,
                "materialisation",
                fontsize=6.5, color="#aaaaaa", va="top", ha="left",
                style="italic")

    # ── Axes ──────────────────────────────────────────────────────────
    x_right = sk_max + 1
    ax.set_xlim(-1, x_right)
    ax.set_ylim(bottom=0)
    ax.set_xlabel("Query position")
    ax.set_ylabel("Latency (s)")
    ax.set_axisbelow(True)

    # ── Legend ────────────────────────────────────────────────────────
    handles, labels = ax.get_legend_handles_labels()
    # Deduplicate (scatter + line produce separate handles per series).
    seen, h2, l2 = set(), [], []
    for h, l in zip(handles, labels):
        if l not in seen:
            h2.append(h); l2.append(l); seen.add(l)
    leg = ax.legend(h2, l2, loc="center right", framealpha=0.92,
                    edgecolor="#cccccc", fontsize=7,
                    handlelength=1.4, handletextpad=0.4,
                    labelspacing=0.3)
    leg.get_frame().set_linewidth(0.4)

    fig.tight_layout()
    fig.savefig(str(output), format="pdf")
    print(f"  → {output}")
    plt.close(fig)


def plot_stream(all_data: OrderedDict[str, dict], output_dir: Path) -> None:
    for log_name, data in all_data.items():
        out = output_dir / f"fig_lru_eviction_stream_{log_name}.pdf"
        plot_stream_single(log_name, data, out)


# ═══════════════════════════════════════════════════════════════════════════
# Figure 2a — skewness-level comparison  (one file per dataset)
# ═══════════════════════════════════════════════════════════════════════════

def plot_skewness_lines(log_name: str, data: dict, output: Path) -> None:
    """
    Rolling-mean latency curves for each n_hot level on one axes.
    Most concentrated level (small n_hot) converges earliest; uniform
    stays flat at the cold-scan baseline throughout.
    Analogous to plot_per_batch_single but in line form.
    """
    levels  = sorted(data.get("n_hot_levels", []))
    uniform = data.get("uniform", {}).get("queries", [])

    if not levels and not uniform:
        print(f"  SKIP {output.name}: no stream data.")
        return

    fig, ax = plt.subplots(figsize=(5, 3.2))

    # x_right = actual maximum seq across all streams + small margin.
    all_seqs = []
    for n in levels:
        if f"hot_{n}" in data:
            qs = data[f"hot_{n}"]["queries"]
            if qs:
                all_seqs.append(max(q["seq"] for q in qs))
    if uniform:
        un_seqs = [q["seq"] for q in uniform]
        if un_seqs:
            all_seqs.append(max(un_seqs))
    x_right = (max(all_seqs) + 5) if all_seqs else 250

    sleep_xs = [
        data[f"hot_{n}"]["sleep_after_seq"]
        for n in levels
        if f"hot_{n}" in data and data[f"hot_{n}"]["sleep_after_seq"] is not None
    ]

    # One line per hot level.
    for n_hot in levels:
        key    = f"hot_{n_hot}"
        stream = data.get(key, {})
        qs     = stream.get("queries", [])
        hot_qs = [q for q in qs if q.get("tag") == "HOT"]
        if not hot_qs:
            continue
        color = _skewness_color(n_hot, levels)
        sx    = [q["seq"] for q in hot_qs]
        sy    = [q["latency_s"] for q in hot_qs]
        # Adaptive smoothing window: more points → smoother line.
        window = max(3, len(hot_qs) // 15)
        rx, ry = _rolling_mean(sx, sy, window=window)
        # Sparse markers so the line doesn't look like a scatter plot.
        markevery = max(1, len(rx) // 6)
        if data["n_pairs"]:
            pv = n_hot / data["n_pairs"] * 100
            pct = f"{pv:.1f}%" if pv >= 0.1 else (f"{pv:.2f}%" if pv >= 0.01 else f"{pv:.3f}%")
        else:
            pct = "?"
        ax.plot(rx, ry,
                color=color, linewidth=1.5, linestyle="-",
                marker="o", markersize=4, markevery=markevery,
                markeredgecolor="white", markeredgewidth=0.4,
                label=f"$n_{{\\mathrm{{hot}}}}={n_hot}$ ({pct})",
                zorder=3)

    # Uniform baseline.
    if uniform:
        un_clip = [q for q in uniform if q["seq"] <= x_right]
        if un_clip:
            ux  = [q["seq"] for q in un_clip]
            uy  = [q["latency_s"] for q in un_clip]
            window_u = max(3, len(un_clip) // 15)
            markevery_u = max(1, len(un_clip) // 6)
            rx, ry = _rolling_mean(ux, uy, window=window_u)
            ax.plot(rx, ry,
                    color=COL_UNIFORM, linewidth=1.5, linestyle="--",
                    marker="^", markersize=4, markevery=markevery_u,
                    markerfacecolor="white", markeredgecolor=COL_UNIFORM,
                    markeredgewidth=0.8,
                    label=f"Uniform ({data['n_pairs']} pairs)",
                    zorder=3)

    # Sleep boundary.
    if sleep_xs:
        bx = float(np.median(sleep_xs)) + 0.5
        ax.axvline(bx, color="#bbbbbb", linewidth=0.7, linestyle=":", zorder=1)
        _, ymax = ax.get_ylim()
        ax.text(bx + 0.5, ymax * 0.97,
                "materialisation",
                fontsize=6.5, color="#aaaaaa",
                va="top", ha="left", style="italic")

    ax.set_xlim(-2, x_right)
    ax.set_ylim(bottom=0)
    ax.set_xlabel("Query position")
    ax.set_ylabel("Latency (s)")
    ax.set_axisbelow(True)


    leg = ax.legend(loc="center right", framealpha=0.92,
                    edgecolor="#cccccc", fontsize=7,
                    handlelength=1.6, handletextpad=0.4, labelspacing=0.3)
    leg.get_frame().set_linewidth(0.4)

    fig.tight_layout()
    fig.savefig(str(output), format="pdf")
    print(f"  → {output}")
    plt.close(fig)


def plot_skewness(all_data: OrderedDict[str, dict], output_dir: Path) -> None:
    for log_name, data in all_data.items():
        out = output_dir / f"fig_lru_eviction_skewness_{log_name}.pdf"
        plot_skewness_lines(log_name, data, out)


# ═══════════════════════════════════════════════════════════════════════════
# Figure 2 — all datasets on one plot  (analogous to plot_lines)
# ═══════════════════════════════════════════════════════════════════════════

def plot_streams_lines(all_data: OrderedDict[str, dict], output: Path) -> None:
    """
    Overlay rolling-mean latency curves for all datasets on one axes.

    Solid line  = skewed HOT queries (convergence curve).
    Dashed line = uniform queries (cold-scan baseline).
    One colour per dataset, matching dataset_style().

    Mirrors the structure of plot_lines() in plot_maintenance_savings.py.
    """
    fig, ax = plt.subplots(figsize=(5, 3.2))

    # Common x-right: max seq of the most concentrated level + small margin.
    # Matches plot_stream_single so both figures share the same x-axis range.
    x_right = max(
        max(
            (q["seq"]
             for n in d.get("n_hot_levels", [])
             for q in d.get(f"hot_{min(d['n_hot_levels'])}", {}).get("queries", [])
             ),
            default=0,
        ) + 1
        for d in all_data.values()
    ) if all_data else 31

    for log_name, data in all_data.items():
        st     = dataset_style(log_name)
        color  = st["color"]
        marker = st["marker"]

        # Use the most concentrated level as the skewed representative.
        levels = data.get("n_hot_levels", [])
        if levels:
            key    = f"hot_{min(levels)}"
            hot_qs = [q for q in data.get(key, {}).get("queries", [])
                      if q.get("tag") == "HOT"]
            if hot_qs:
                sx = [q["seq"] for q in hot_qs]
                sy = [q["latency_s"] for q in hot_qs]
                rx, ry = _rolling_mean(sx, sy, window=3)
                ax.plot(rx, ry,
                        color=color, linewidth=1.5, linestyle="-",
                        marker=marker, markersize=4,
                        markeredgecolor="white", markeredgewidth=0.4,
                        label=pretty_name(log_name), zorder=3)

        # Uniform rolling mean — clip to the same x window.
        un_clip = [q for q in data.get("uniform", {}).get("queries", [])
                   if q["seq"] <= x_right]
        if un_clip:
            ux = [q["seq"] for q in un_clip]
            uy = [q["latency_s"] for q in un_clip]
            rx, ry = _rolling_mean(ux, uy, window=5)
            ax.plot(rx, ry,
                    color=color, linewidth=1.6, linestyle="--",
                    marker=marker, markersize=4, markevery=5,
                    markerfacecolor="white", markeredgecolor=color,
                    markeredgewidth=0.8, zorder=3, alpha=0.85)

    # Sleep boundary at the median sleep position across datasets.
    sleep_xs = []
    for d in all_data.values():
        for n in d.get("n_hot_levels", []):
            sx = d.get(f"hot_{n}", {}).get("sleep_after_seq")
            if sx is not None:
                sleep_xs.append(sx)
                break   # one per dataset
    if sleep_xs:
        bx = float(np.median(sleep_xs)) + 0.5
        ax.axvline(bx, color="#bbbbbb", linewidth=0.7, linestyle=":", zorder=1)
        _, ymax = ax.get_ylim()
        ax.text(bx + 0.5, ymax * 0.97,
                "materialisation",
                fontsize=6.5, color="#aaaaaa", va="top", ha="left",
                style="italic")

    ax.set_xlim(-2, x_right)
    ax.set_yscale("log")
    ax.set_ylim(bottom=1)
    ax.yaxis.set_major_formatter(
        matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:g}")
    )
    ax.set_xlabel("Query position")
    ax.set_ylabel("Latency (s)")

    # Legend: dataset colour/marker handles + line-style guide.
    ds_handles, _ = ax.get_legend_handles_labels()
    guides = ds_handles + [
        Line2D([0], [0], color=PAL["grey"], lw=1.5, ls="-",
               label="skewed"),
        Line2D([0], [0], color=PAL["grey"], lw=1.5, ls="--",
               label="uniform"),
    ]
    leg = ax.legend(handles=guides, loc="upper right", framealpha=0.92,
                    edgecolor="#cccccc", ncol=2, columnspacing=1,
                    fontsize=7, handlelength=1.8, handletextpad=0.4)
    leg.get_frame().set_linewidth(0.4)

    fig.tight_layout()
    fig.savefig(str(output), format="pdf")
    print(f"  → {output}")
    plt.close(fig)


# ═══════════════════════════════════════════════════════════════════════════
# Figure 2 — speedup summary bar  (all datasets, one file)
# ═══════════════════════════════════════════════════════════════════════════

def plot_speedup_bar(all_data: OrderedDict[str, dict], output: Path) -> None:
    labels, speedups, cols = [], [], []

    for log_name, data in all_data.items():
        uniform  = data.get("uniform", {}).get("queries", [])
        levels   = data.get("n_hot_levels", [])
        if not uniform or not levels:
            continue
        # Use the most concentrated level for the PERSISTENT mean.
        hot_pers = [q for q in data.get(f"hot_{min(levels)}", {}).get("queries", [])
                    if q.get("tag") == "HOT" and q.get("tier") == "PERSISTENT"]
        if not uniform or not hot_pers:
            continue

        mean_unif = np.mean([q["latency_s"] for q in uniform])
        mean_pers = np.mean([q["latency_s"] for q in hot_pers])
        speedup   = mean_unif / mean_pers

        labels.append(pretty_name(log_name))
        speedups.append(speedup)
        cols.append(dataset_style(log_name)["color"])

    if not labels:
        print("  No PERSISTENT hot queries found — skipping speedup bar.")
        return

    fig, ax = plt.subplots(
        figsize=(3.8, max(1.6, len(labels) * 0.55 + 0.5))
    )

    y    = np.arange(len(labels))
    bars = ax.barh(y, speedups, color=cols, height=0.48, zorder=3,
                   edgecolor="white", linewidth=0.3)

    x_max = max(speedups) * 1.35
    ax.set_xlim(0, x_max)
    for bar, sp in zip(bars, speedups):
        ax.text(bar.get_width() + x_max * 0.02,
                bar.get_y() + bar.get_height() / 2,
                f"{sp:.0f}\u00d7",
                va="center", fontsize=7.5, clip_on=False)

    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=8.5)
    ax.set_xlabel("Speedup (uniform latency / PERSISTENT latency)")
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
        description="Plot LRU eviction experiment results.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--results-dir", type=Path,
                    default=Path("tests/eval/results"),
                    help="Directory with 6_3_3_lru_eviction_*.jsonl files.")
    ap.add_argument("--output-dir",  type=Path,
                    default=Path("figures"),
                    help="Output directory for PDFs.")
    ap.add_argument("--datasets",    type=str, default=None,
                    help="Comma-separated log_names to include (default: all).")
    args = ap.parse_args()

    apply_style()

    all_data = load_all_results(args.results_dir)
    if not all_data:
        print(f"No 6_3_3_lru_eviction_*.jsonl files with query records "
              f"in {args.results_dir}")
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

    plot_stream(all_data,        args.output_dir)
    plot_skewness(all_data,      args.output_dir)
    plot_streams_lines(all_data, args.output_dir / "fig_lru_eviction_lines.pdf")
    plot_speedup_bar(all_data,   args.output_dir / "fig_lru_speedup_bar.pdf")

    print("Done.")


if __name__ == "__main__":
    main()