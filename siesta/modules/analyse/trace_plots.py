#!/usr/bin/env python3
"""
trace_plots.py — Per-trace size and working-hours duration distributions,
broken down by trace label, as violin + boxplot overlays.

WHAT IT PRODUCES
----------------
A two-panel PDF figure:

  Left  – Trace size: number of events per trace.
  Right – Trace duration: working-hours span (Mon–Fri, within business hours),
          consistent with the duration model in loop_analysis.py and
          time_bottleneck.py.

Each panel shows one violin + embedded box per label group.  Groups with
fewer than MIN_VIOLIN_N (default 20) traces fall back to a strip plot.

USAGE
-----
  python trace_plots.py \
      --log        event_log.csv      \
      [--sep-key   activity]          \
      [--sep-groups '[["reject"]]']   \
      [--activities focus.txt]        \
      [--label-names '["OK","NOK"]']  \
      [--work-start  08:00]           \
      [--work-end    18:00]           \
      [--trace-col    trace_id]       \
      [--activity-col activity]       \
      [--time-col     start_timestamp]\
      [--end-time-col end_timestamp]  \
      [--output   figures/trace_dist] \
      [--fmt      pdf|png|both]

SIESTA WIRING
-------------
    from trace_plots import compute_trace_stats, plot_distributions

    stats_df = compute_trace_stats(log_df, trace_labels,
                                   trace_col="trace_id",
                                   activity_col="activity",
                                   time_col="start_timestamp",
                                   work_start=..., work_end=...)
    plot_distributions(stats_df, "figures/trace_dist")
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, time as dtime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Okabe-Ito colourblind-safe palette ──────────────────────────────────────
OI = ["#0072B2", "#D55E00", "#009E73", "#CC79A7", "#E69F00", "#56B4E9",
      "#F0E442", "#000000"]

MIN_VIOLIN_N = 20    # below this, use strip plot instead of violin

# ── Shared rcParams (mirrors plot_warmup.py) ─────────────────────────────────
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
})


# ═══════════════════════════════════════════════════════════════════════════
# 1.  Working-hours helpers  (identical to loop_analysis.py / time_bottleneck.py)
# ═══════════════════════════════════════════════════════════════════════════

def _working_seconds(dt1: datetime, dt2: datetime,
                     work_start: dtime, work_end: dtime) -> float:
    # Expects timezone-aware datetime objects directly from pandas
    if dt2 <= dt1:
        return 0.0
    total = 0.0
    day = dt1.date()
    while day <= dt2.date():
        if day.weekday() < 5:
            ds = datetime.combine(day, work_start, tzinfo=timezone.utc)
            de = datetime.combine(day, work_end,   tzinfo=timezone.utc)
            seg_s = max(dt1, ds)
            seg_e = min(dt2, de)
            if seg_e > seg_s:
                total += (seg_e - seg_s).total_seconds()
        day += timedelta(days=1)
    return total


def _derive_working_hours(timestamps: pd.Series) -> Tuple[dtime, dtime]:
    times = timestamps.dt.time.dropna()
    return min(times), max(times)


def _parse_timestamps(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_datetime(series, unit='s', utc=True)
    else:
        return pd.to_datetime(series, errors="coerce", utc=True)


# ═══════════════════════════════════════════════════════════════════════════
# 2.  Trace labeling  (identical to loop_analysis.py)
# ═══════════════════════════════════════════════════════════════════════════

def build_trace_labels(
    log_df:            pd.DataFrame,
    separating_key:    str,
    separating_groups: List[List[str]],
    trace_col:         str = "trace_id",
) -> pd.Series:
    if separating_key not in log_df.columns:
        raise ValueError(
            f"separating_key '{separating_key}' not found in log. "
            f"Available: {list(log_df.columns)}"
        )
    trace_ids = log_df[trace_col].unique()
    labels    = pd.Series(0, index=trace_ids, name="label", dtype=int)
    if not separating_groups:
        return labels
    trace_key_vals = log_df.groupby(trace_col)[separating_key].apply(set)
    for group_idx, group_values in enumerate(separating_groups, start=1):
        gs      = set(str(v) for v in group_values)
        matches = trace_key_vals.apply(lambda v, gs=gs: bool({str(x) for x in v} & gs))
        labels  = labels.where(~((labels == 0) & matches), group_idx)
    return labels


# ═══════════════════════════════════════════════════════════════════════════
# 3.  Activity whitelist (mirrors loop_analysis.py)
# ═══════════════════════════════════════════════════════════════════════════

def load_activity_filter(path: str) -> Set[str]:
    acts: Set[str] = set()
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            name = line.strip()
            if name and not name.startswith("#"):
                acts.add(name)
    if not acts:
        raise ValueError(f"Activity filter '{path}' contains no activities.")
    return acts


# ═══════════════════════════════════════════════════════════════════════════
# 4.  Per-trace statistics
# ═══════════════════════════════════════════════════════════════════════════

def compute_trace_stats(
    log_df:        pd.DataFrame,
    trace_labels:  pd.Series,
    trace_col:     str           = "trace_id",
    activity_col:  str           = "activity",
    time_col:      str           = "start_timestamp",
    end_time_col:  Optional[str] = None,
    work_start:    dtime         = dtime(0, 0),
    work_end:      dtime         = dtime(23, 59, 59),
) -> pd.DataFrame:
    """Compute per-trace size and working-hours duration.

    Args:
        log_df:       Flat event log (may be pre-filtered by activity).
        trace_labels: Series[trace_id → int label] from build_trace_labels.
        trace_col:    Trace ID column.
        activity_col: Activity column (used only for counting events).
        time_col:     Start-timestamp column (already parsed to datetime).
        end_time_col: End-timestamp column (optional).
        work_start:   Working-day start time.
        work_end:     Working-day end   time.

    Returns:
        DataFrame with columns:
            trace_id, label, size, duration_hours
    """
    # Aggregate per trace
    agg: Dict[str, Any] = {
        "size":     (activity_col, "count"),
        "first_ts": (time_col,     "min"),
    }
    if end_time_col and end_time_col in log_df.columns:
        agg["last_ts"] = (end_time_col, "max")
    else:
        agg["last_ts"] = (time_col, "max")

    trace_agg = log_df.groupby(trace_col).agg(**agg).reset_index()
    trace_agg[trace_col] = trace_agg[trace_col].astype(str)

    # Working-hours duration (vectorised row-by-row with pure datetimes)
    trace_agg["duration_hours"] = trace_agg.apply(
        lambda r: _working_seconds(
            r["first_ts"], r["last_ts"], work_start, work_end
        ) / 3600.0,
        axis=1,
    )

    # Merge with labels (fill missing traces with size=0, dur=0)
    label_df = trace_labels.reset_index()
    label_df.columns = [trace_col, "label"]
    label_df[trace_col] = label_df[trace_col].astype(str)

    stats = label_df.merge(
        trace_agg[[trace_col, "size", "duration_hours"]],
        on=trace_col, how="left",
    ).fillna({"size": 0, "duration_hours": 0.0})
    stats["size"] = stats["size"].astype(int)

    return stats.reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════════════════
# 5.  Plotting
# ═══════════════════════════════════════════════════════════════════════════

def _violin_or_strip(
    ax:        plt.Axes,
    data:      List[np.ndarray],
    positions: List[int],
    colors:    List[str],
    min_n:     int = MIN_VIOLIN_N,
) -> None:
    """Draw violin + box overlay, or strip + box for small groups."""
    rng = np.random.default_rng(42)

    for i, (pos, d, col) in enumerate(zip(positions, data, colors)):
        if len(d) == 0:
            continue

        if len(d) >= min_n:
            # ── Violin ──────────────────────────────────────────────────
            vp = ax.violinplot(d, positions=[pos],
                               showmeans=False, showmedians=False,
                               showextrema=False, widths=0.7)
            for body in vp["bodies"]:
                body.set_facecolor(col)
                body.set_edgecolor(col)
                body.set_alpha(0.45)
                body.set_zorder(1)
        else:
            # ── Strip (jittered points) for small n ─────────────────────
            jitter = rng.uniform(-0.18, 0.18, len(d))
            ax.scatter(pos + jitter, d,
                       color=col, alpha=0.55, s=14, zorder=2,
                       linewidths=0, label=f"_nolegend_")
            logger.warning(
                "Group at position %d has n=%d < %d; "
                "using strip plot instead of violin.", pos, len(d), min_n
            )

        # ── Boxplot (narrow, on top of violin or strip) ──────────────────
        bp = ax.boxplot(
            d,
            positions=[pos],
            widths=0.13,
            patch_artist=True,
            showfliers=False,
            zorder=3,
            boxprops=dict(
                facecolor=col, edgecolor="#333333",
                linewidth=0.8, alpha=0.85,
            ),
            medianprops=dict(color="white", linewidth=2.0),
            whiskerprops=dict(color="#444444", linewidth=0.9),
            capprops=dict(color="#444444",    linewidth=0.9),
        )


def _col_stats(arr: np.ndarray) -> Dict[str, float]:
    return {
        "median": float(np.median(arr)),
        "q1":     float(np.percentile(arr, 25)),
        "q3":     float(np.percentile(arr, 75)),
        "min":    float(arr.min()),
        "max":    float(arr.max()),
    }


def _summary_table(stats_df: pd.DataFrame,
                   label_names: List[str]) -> str:
    """Return a formatted summary table string for stdout."""
    labels = sorted(stats_df["label"].unique())
    col_w  = 20
    sep    = "─" * (12 + col_w * (len(labels) + 1))

    # Header
    hdr_vals = [("All traces", len(stats_df))]
    for lv in labels:
        name = label_names[lv] if lv < len(label_names) else f"Label {lv}"
        hdr_vals.append((name, len(stats_df[stats_df["label"] == lv])))

    lines = [
        "\nTRACE STATISTICS SUMMARY",
        sep,
        "             " + "".join(
            f"{h}  (n={n})".rjust(col_w) for h, n in hdr_vals
        ),
        sep,
    ]

    def _rows(metric: str, fmt: str) -> List[str]:
        all_arr  = stats_df[metric].values.astype(float)
        all_s    = _col_stats(all_arr)
        lv_stats = {lv: _col_stats(
            stats_df.loc[stats_df["label"] == lv, metric].values.astype(float)
        ) for lv in labels}

        if fmt == "int":
            def _v(x):  return f"{x:.1f}"
            def _rng(s): return f"{s['min']:.0f}\u2013{s['max']:.0f}"
            def _iqr(s): return f"[{s['q1']:.1f}, {s['q3']:.1f}]"
        else:
            def _v(x):  return f"{x:.2f}"
            def _rng(s): return f"{s['min']:.2f}\u2013{s['max']:.2f}"
            def _iqr(s): return f"[{s['q1']:.2f}, {s['q3']:.2f}]"

        row_med = (f"  {'median':<10}" + _v(all_s['median']).rjust(col_w)
                   + "".join(_v(lv_stats[lv]['median']).rjust(col_w) for lv in labels))
        row_iqr = (f"  {'IQR':<10}" + _iqr(all_s).rjust(col_w)
                   + "".join(_iqr(lv_stats[lv]).rjust(col_w) for lv in labels))
        row_rng = (f"  {'min\u2013max':<10}" + _rng(all_s).rjust(col_w)
                   + "".join(_rng(lv_stats[lv]).rjust(col_w) for lv in labels))
        return [row_med, row_iqr, row_rng]

    lines.append("Trace size (events)")
    lines.extend(_rows("size", "int"))
    lines.append("Trace duration (working hours)")
    lines.extend(_rows("duration_hours", "float"))
    lines.append(sep + "\n")
    return "\n".join(lines)


def plot_distributions(
    stats_df:    pd.DataFrame,
    output:      str,
    label_names: Optional[List[str]] = None,
    fmt:         str                 = "pdf",
    figsize:     Tuple[float, float] = (7.0, 3.8),
    min_n:       int                 = MIN_VIOLIN_N,
) -> List[Path]:
    """Produce the violin + box distribution figure."""
    labels   = sorted(stats_df["label"].unique())
    n_groups = len(labels)
    colors   = [OI[i % len(OI)] for i in range(n_groups)]

    if label_names is None:
        label_names = [f"Label {lv}" for lv in range(max(labels) + 1)]

    def _xticklabels() -> List[str]:
        out = []
        for lv in labels:
            n    = len(stats_df[stats_df["label"] == lv])
            name = label_names[lv] if lv < len(label_names) else f"Label {lv}"
            out.append(f"{name}\n$(n={n:,})$")
        return out

    positions = list(range(n_groups))
    xlim      = (-0.6, n_groups - 0.4)

    # ── Data groups ──────────────────────────────────────────────────────
    size_groups = [
        stats_df.loc[stats_df["label"] == lv, "size"].values.astype(float)
        for lv in labels
    ]
    dur_groups = [
        stats_df.loc[stats_df["label"] == lv, "duration_hours"].values.astype(float)
        for lv in labels
    ]

    # ── Figure ───────────────────────────────────────────────────────────
    fig, (ax_size, ax_dur) = plt.subplots(
        1, 2, figsize=figsize,
        gridspec_kw={"wspace": 0.38},
    )

    for ax, data_groups, ylabel, yscale in [
        (ax_size, size_groups, "Events per trace",      "log"),
        (ax_dur,  dur_groups,  "Duration (working h)",  "log"),
    ]:
        _violin_or_strip(ax, data_groups, positions, colors, min_n)

        ax.set_xticks(positions)
        ax.set_xticklabels(_xticklabels())
        ax.set_xlim(xlim)
        ax.set_ylabel(ylabel)
        ax.set_yscale(yscale)
        ax.yaxis.grid(True, alpha=0.3, linewidth=0.5)
        ax.xaxis.grid(False)
        ax.set_axisbelow(True)

    # ── Legend patch ──────────────────────────────────────────────────────
    patches = [
        mpatches.Patch(facecolor=colors[i], alpha=0.7,
                       label=label_names[labels[i]] if labels[i] < len(label_names)
                             else f"Label {labels[i]}")
        for i in range(n_groups)
    ]
    if n_groups > 1:
        fig.legend(
            handles=patches, loc="upper center",
            ncol=min(n_groups, 4),
            bbox_to_anchor=(0.5, 1.02),
            framealpha=0.0, edgecolor="none",
            fontsize=8.5,
        )

    # ── Save ─────────────────────────────────────────────────────────────
    stem  = output.rstrip(".pdf").rstrip(".png")
    saved = []
    fmts  = ["pdf", "png"] if fmt == "both" else [fmt]
    for ext in fmts:
        p = Path(f"{stem}.{ext}")
        p.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(str(p), format=ext)
        print(f"Figure saved : {p}")
        saved.append(p)

    plt.close(fig)
    return saved


# ═══════════════════════════════════════════════════════════════════════════
# 6.  Main entry point  (exposed for SIESTA wiring)
# ═══════════════════════════════════════════════════════════════════════════

def run_trace_plots(
    log_df:            pd.DataFrame,
    separating_key:    Optional[str]       = None,
    separating_groups: Optional[List[List[str]]] = None,
    activity_filter:   Optional[Set[str]]  = None,
    trace_col:         str                 = "trace_id",
    activity_col:      str                 = "activity",
    time_col:          str                 = "start_timestamp",
    end_time_col:      Optional[str]       = None,
    work_start:        Optional[dtime]     = None,
    work_end:          Optional[dtime]     = None,
    label_names:       Optional[List[str]] = None,
    output:            str                 = "trace_dist",
    fmt:               str                 = "pdf",
) -> Tuple[pd.DataFrame, List[Path]]:
    """End-to-end pipeline: label → filter → stats → plots."""
    log_df = log_df.copy()
    log_df[trace_col] = log_df[trace_col].astype(str)
    
    # Unified, robust datetime parsing
    log_df[time_col]  = _parse_timestamps(log_df[time_col])
    if end_time_col and end_time_col in log_df.columns:
        log_df[end_time_col] = _parse_timestamps(log_df[end_time_col])
    else:
        end_time_col = None

    # Auto-derive working hours
    if work_start is None or work_end is None:
        ws, we = _derive_working_hours(log_df[time_col])
        work_start = work_start or ws
        work_end   = work_end   or we
    logger.info("Working hours: %s – %s", work_start, work_end)

    # Build trace labels (from full log, before activity filter)
    sep_key    = separating_key    or (log_df.columns[0] if not separating_key else None)
    sep_groups = separating_groups or []

    # Default sep_key to activity_col when groups are specified but key is omitted
    if not separating_key and sep_groups:
        separating_key = activity_col

    if separating_key:
        trace_labels = build_trace_labels(log_df, separating_key, sep_groups, trace_col)
    else:
        trace_labels = pd.Series(
            0, index=log_df[trace_col].unique(), name="label", dtype=int
        )

    # Apply activity filter (after labeling)
    if activity_filter:
        before = len(log_df)
        log_df = log_df[log_df[activity_col].isin(activity_filter)].copy()
        logger.info("Activity filter: %d → %d events.", before, len(log_df))

    # Compute per-trace stats
    stats_df = compute_trace_stats(
        log_df, trace_labels, trace_col, activity_col,
        time_col, end_time_col, work_start, work_end,
    )

    # Print summary
    _lnames = label_names or [f"Label {lv}" for lv in range(max(stats_df["label"]) + 1)]
    print(_summary_table(stats_df, _lnames))

    # Plot
    paths = plot_distributions(stats_df, output, _lnames, fmt)
    return stats_df, paths


# ═══════════════════════════════════════════════════════════════════════════
# 7.  CLI
# ═══════════════════════════════════════════════════════════════════════════

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--log",          required=True,
                   help="Event log CSV.")
    p.add_argument("--sep-key",      default=None, dest="sep_key",
                   help="Column used to separate trace groups (optional).")
    p.add_argument("--sep-groups",   default="[]", dest="sep_groups",
                   help='JSON array of arrays, e.g. \'[["reject"]]\'.')
    p.add_argument("--activities",   default=None,
                   help="Path to activity whitelist .txt (one per line).")
    p.add_argument("--label-names",  default=None, dest="label_names",
                   help='JSON array of display names, e.g. \'["OK","NOK"]\'.')
    p.add_argument("--trace-col",    default="trace_id",  dest="trace_col")
    p.add_argument("--activity-col", default="activity",  dest="activity_col")
    p.add_argument("--time-col",     default="start_timestamp", dest="time_col")
    p.add_argument("--end-time-col", default=None,        dest="end_time_col")
    p.add_argument("--work-start",   default=None,        dest="work_start",
                   help="Working-day start HH:MM (auto-derived when absent).")
    p.add_argument("--work-end",     default=None,        dest="work_end",
                   help="Working-day end   HH:MM (auto-derived when absent).")
    p.add_argument("--output",       default="trace_dist",
                   help="Output path prefix (default: trace_dist).")
    p.add_argument("--fmt",          choices=["pdf", "png", "both"],
                   default="pdf",
                   help="Output format (default: pdf).")
    return p


def main(argv: Optional[List[str]] = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
    args = _build_parser().parse_args(argv)

    def _parse_time(s):
        if s is None:
            return None
        try:
            h, m = map(int, s.split(":"))
            return dtime(h, m)
        except Exception:
            print(f"Error: cannot parse time '{s}' (expected HH:MM).", file=sys.stderr)
            sys.exit(1)

    try:
        sep_groups = json.loads(args.sep_groups)
    except json.JSONDecodeError as e:
        print(f"Error: --sep-groups is not valid JSON: {e}", file=sys.stderr)
        sys.exit(1)

    label_names = None
    if args.label_names:
        try:
            label_names = json.loads(args.label_names)
        except json.JSONDecodeError as e:
            print(f"Error: --label-names is not valid JSON: {e}", file=sys.stderr)
            sys.exit(1)

    act_filter = None
    if args.activities:
        act_filter = load_activity_filter(args.activities)
        print(f"Activity filter: {len(act_filter)} activities loaded.")

    print(f"Loading event log: {args.log}")
    log_df = pd.read_csv(args.log)
    log_df.columns = log_df.columns.str.strip()
    print(f"  {len(log_df):,} events, "
          f"{log_df[args.trace_col].nunique():,} traces.")

    run_trace_plots(
        log_df            = log_df,
        separating_key    = args.sep_key,
        separating_groups = sep_groups,
        activity_filter   = act_filter,
        trace_col         = args.trace_col,
        activity_col      = args.activity_col,
        time_col          = args.time_col,
        end_time_col      = args.end_time_col,
        work_start        = _parse_time(args.work_start),
        work_end          = _parse_time(args.work_end),
        label_names       = label_names,
        output            = args.output,
        fmt               = args.fmt,
    )


if __name__ == "__main__":
    main()