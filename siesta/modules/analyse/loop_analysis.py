#!/usr/bin/env python3
"""
loop_analysis.py — Self-contained loop analysis with trace-label comparison,
valid-transition filtering, and working-hours-aware duration statistics.

WHAT IT DOES
------------
In a single pass over a flat event-log CSV, this script:

  1. Assigns each trace a label (0 = reference, 1..N = user-defined groups)
     using the same separating_key / separating_groups logic as the SIESTA
     comparator module.
  2. Detects self-loops, minimal non-self-loops, and repeated blocks in
     every trace.
  3. Optionally removes "expected" loops that are valid single-occurrence
     process flows, using the same filtering rule as helper_filter_loops.py.
  4. Reports:
       a. Label distribution – % of cases per label.
       b. Loop prevalence   – % of cases with ≥1 loop, overall and per label.
       c. Loop time cost    – for each looping case, the fraction of its
                             working-hours duration consumed by loop
                             repetitions (2nd occurrence onward).

LOOP TYPES DETECTED
-------------------
  self_loop        – activity A followed immediately by itself  (A→A).
  non_self_loop    – minimal sub-sequence A→…→A where A does not appear
                     in the body (mirrors loop_detection.py / discover_loops).
  repeated_pattern – a block of ≥2 consecutive activities that repeats
                     immediately back-to-back (e.g. A→B→C→A→B→C).

DURATION MODEL
--------------
Working-hours durations mirror time_bottleneck.py:
  • Only Mon–Fri within [work_start, work_end] counts.
  • Weekends, nights, and public holidays are excluded entirely.
  • work_start / work_end can be supplied explicitly or auto-derived from
    the min/max time-of-day observed in the event log.
  • If the log uses an end-timestamp column, total case duration is
    max(end_ts) – min(start_ts) in working hours; otherwise it is
    last_start – first_start in working hours.

SIESTA WIRING
-------------
The core entry point ``run_loop_analysis`` accepts a plain pandas DataFrame
so it can be called from inside SIESTA after a ``events_df.toPandas()``:

    from loop_analysis import run_loop_analysis, load_valid_transitions

    valid = load_valid_transitions("transitions.csv")
    result = run_loop_analysis(
        log_df            = events_spark_df.toPandas(),
        separating_key    = "activity",
        separating_groups = [["reject", "cancel"]],
        valid_transitions = valid,
    )

STANDALONE USAGE
----------------
  python loop_analysis.py \
      --log        event_log.csv          \
      --sep-key    activity               \
      --sep-groups '[["reject","cancel"]]'\
      [--valid     valid_transitions.csv] \
      [--trace-col    trace_id]           \
      [--activity-col activity]           \
      [--time-col     start_timestamp]    \
      [--end-time-col end_timestamp]      \
      [--work-start   08:00]              \
      [--work-end     18:00]              \
      [--support      0.0]                \
      [--output       results]            \
      [--format       json|csv|both]
"""

from __future__ import annotations

import argparse
import csv as csv_mod
import json
import logging
import sys
from collections import defaultdict
from datetime import datetime, timedelta, time as dtime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# 1.  Working-hours helpers  (same logic as time_bottleneck.py)
# ═══════════════════════════════════════════════════════════════════════════

def _working_seconds(
    dt1: datetime,
    dt2: datetime,
    work_start: dtime,
    work_end: dtime,
) -> float:
    """Return elapsed working seconds between two datetime timestamps.

    Only Monday–Friday within [work_start, work_end] (UTC) is counted.
    Weekends and off-hours are excluded entirely.
    """
    if dt2 <= dt1:
        return 0.0
    total = 0.0
    day = dt1.date()
    while day <= dt2.date():
        if day.weekday() < 5:          # Mon=0 … Fri=4
            ds = datetime.combine(day, work_start, tzinfo=timezone.utc)
            de = datetime.combine(day, work_end,   tzinfo=timezone.utc)
            seg_s = max(dt1, ds)
            seg_e = min(dt2, de)
            if seg_e > seg_s:
                total += (seg_e - seg_s).total_seconds()
        day += timedelta(days=1)
    return total


def _derive_working_hours(timestamps: pd.Series) -> Tuple[dtime, dtime]:
    """Infer work_start / work_end from the earliest and latest time-of-day
    observed across all events, mirroring time_bottleneck.py's auto-detection.
    """
    times = timestamps.dt.time.dropna()
    return min(times), max(times)


def _parse_timestamps(series: pd.Series) -> pd.Series:
    """Return a Series of timezone-aware datetimes; handles numeric and ISO strings."""
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_datetime(series, unit='s', utc=True)
    else:
        return pd.to_datetime(series, errors="coerce", utc=True)


# ═══════════════════════════════════════════════════════════════════════════
# 2.  Trace labeling
# ═══════════════════════════════════════════════════════════════════════════

def build_trace_labels(
    log_df: pd.DataFrame,
    separating_key: str,
    separating_groups: List[List[str]],
    trace_col: str = "trace_id",
) -> pd.Series:
    """Assign an integer label to every trace."""
    if separating_key not in log_df.columns:
        raise ValueError(
            f"separating_key '{separating_key}' is not a column in the log. "
            f"Available columns: {list(log_df.columns)}"
        )

    trace_ids = log_df[trace_col].unique()
    labels = pd.Series(0, index=trace_ids, name="label", dtype=int)

    if not separating_groups:
        return labels

    trace_key_vals: pd.Series = (
        log_df.groupby(trace_col)[separating_key]
        .apply(set)
    )

    for group_idx, group_values in enumerate(separating_groups, start=1):
        group_set = set(str(v) for v in group_values)
        matches = trace_key_vals.apply(
            lambda vals, gs=group_set: bool({str(v) for v in vals} & gs)
        )
        unassigned = labels == 0
        labels = labels.where(~(unassigned & matches), group_idx)

    return labels


# ═══════════════════════════════════════════════════════════════════════════
# 3.  Valid-transition loading  (mirrors helper_filter_loops.py)
# ═══════════════════════════════════════════════════════════════════════════

def load_valid_transitions(
    path: str,
    from_col: Optional[str] = None,
    to_col:   Optional[str] = None,
) -> Set[Tuple[str, str]]:
    """Load (source, target) activity pairs from a CSV file."""
    _FROM = ["from", "source", "activity_from", "src", "a", "first"]
    _TO   = ["to",   "target", "activity_to",   "tgt", "b", "second"]

    with open(path, newline="", encoding="utf-8") as fh:
        reader  = csv_mod.DictReader(fh)
        headers = reader.fieldnames or []
        lower   = [h.strip().lower() for h in headers]

        if len(headers) == 2 and from_col is None and to_col is None:
            fc, tc = headers[0], headers[1]
        else:
            fc = from_col or next(
                (headers[i] for i, h in enumerate(lower) if h in _FROM), None
            )
            tc = to_col or next(
                (headers[i] for i, h in enumerate(lower) if h in _TO), None
            )
        if not fc or not tc:
            raise ValueError(
                f"Cannot detect source/target columns in '{path}'.  "
                f"Found: {headers}.  Use --from-col / --to-col."
            )

        valid: Set[Tuple[str, str]] = set()
        for row in reader:
            src = str(row.get(fc, "")).strip()
            tgt = str(row.get(tc, "")).strip()
            if src and tgt:
                valid.add((src, tgt))
    return valid


def load_activity_filter(path: str) -> Set[str]:
    """Load the activity whitelist from a plain-text file."""
    activities: Set[str] = set()
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            name = line.strip()
            if name and not name.startswith("#"):
                activities.add(name)
    if not activities:
        raise ValueError(f"Activity filter file '{path}' contains no activities.")
    return activities


# ═══════════════════════════════════════════════════════════════════════════
# 4.  Per-trace loop detection (pure Python / pandas, no Spark)
# ═══════════════════════════════════════════════════════════════════════════

def _detect_self_loops(activities: List[str]) -> Dict[str, int]:
    """Return {activity: occurrence_count} for all self-loops in the sequence."""
    result: Dict[str, int] = {}
    n = len(activities)
    i = 0
    while i < n:
        act = activities[i]
        j = i + 1
        while j < n and activities[j] == act:
            j += 1
        run_len = j - i
        if run_len >= 2:
            result[act] = result.get(act, 0) + (run_len - 1)
        i = j
    return result


def _detect_non_self_loops(activities: List[str]) -> Dict[str, int]:
    """Return {pattern_string: occurrence_count} for all minimal non-self-loops."""
    n = len(activities)
    patterns: Set[Tuple[str, ...]] = set()
    for i in range(n - 1):
        for j in range(i + 2, n):
            if activities[i] == activities[j]:
                if activities[i] not in activities[i + 1:j]:
                    patterns.add(tuple(activities[i:j + 1]))

    result: Dict[str, int] = {}
    for pat in patterns:
        L   = len(pat)
        cnt = 0
        i   = 0
        while i <= n - L:
            if activities[i:i + L] == list(pat):
                cnt += 1
                i   += max(L - 1, 1)
            else:
                i   += 1
        if cnt > 0:
            key = " -> ".join(pat)
            result[key] = result.get(key, 0) + cnt
    return result


def _detect_repeated_patterns(activities: List[str]) -> Dict[str, int]:
    """Return {pattern_string: occurrence_count} for consecutive block repeats."""
    n = len(activities)
    seen: Dict[Tuple[str, ...], int] = {}

    for k in range(2, n // 2 + 1):
        i = 0
        while i <= n - k:
            block = tuple(activities[i:i + k])
            run_len = 1
            j = i + k
            while j + k <= n and tuple(activities[j:j + k]) == block:
                run_len += 1
                j       += k
            if run_len >= 2:
                seen[block] = seen.get(block, 0) + run_len
            i = j if run_len >= 2 else i + 1

    return {" -> ".join(b): cnt for b, cnt in seen.items()}


def _detect_loops_in_trace(activities: List[str]) -> Dict[str, Dict[str, int]]:
    return {
        "self_loops":        _detect_self_loops(activities),
        "non_self_loops":    _detect_non_self_loops(activities),
        "repeated_patterns": _detect_repeated_patterns(activities),
    }


# ═══════════════════════════════════════════════════════════════════════════
# 5.  Valid-transition filtering  (mirrors helper_filter_loops.py)
# ═══════════════════════════════════════════════════════════════════════════

def _transitions_from_pattern(
    pattern:   str,
    loop_type: str,
) -> List[Tuple[str, str]]:
    if loop_type == "self_loop":
        act = pattern.strip()
        return [(act, act)]
    acts = [a.strip() for a in pattern.split(" -> ")]
    pairs = [(acts[i], acts[i + 1]) for i in range(len(acts) - 1)]
    if loop_type == "repeated_pattern":
        pairs.append((acts[-1], acts[0]))
    return pairs


def _all_transitions_valid(
    pattern:   str,
    loop_type: str,
    valid:     Set[Tuple[str, str]],
) -> bool:
    return all(t in valid for t in _transitions_from_pattern(pattern, loop_type))


def _filter_trace_occurrences(
    pattern:           str,
    loop_type:         str,
    trace_occurrences: Dict[str, int],
    valid:             Set[Tuple[str, str]],
) -> Dict[str, int]:
    if not _all_transitions_valid(pattern, loop_type, valid):
        return trace_occurrences
    return {tid: cnt for tid, cnt in trace_occurrences.items() if cnt > 1}


# ═══════════════════════════════════════════════════════════════════════════
# 6.  Per-trace detection → aggregated result structure
# ═══════════════════════════════════════════════════════════════════════════

_LOOP_LIST_KEYS: List[Tuple[str, str]] = [
    ("self_loops",        "self_loop"),
    ("non_self_loops",    "non_self_loop"),
    ("repeated_patterns", "repeated_pattern"),
]


def detect_and_aggregate(
    log_df:            pd.DataFrame,
    trace_labels:      pd.Series,
    valid_transitions: Optional[Set[Tuple[str, str]]] = None,
    trace_col:         str   = "trace_id",
    activity_col:      str   = "activity",
    sort_col:          Optional[str] = None,
    support_threshold: float = 0.0,
) -> Dict[str, Any]:
    label_map: Dict[str, int] = trace_labels.to_dict()
    total_traces = len(trace_labels)
    label_values = sorted(set(trace_labels.values))

    per_label_raw: Dict[int, Dict[str, Dict[str, Dict[str, int]]]] = {
        lv: {lk: defaultdict(dict) for lk, _ in _LOOP_LIST_KEYS}
        for lv in label_values
    }
    global_raw: Dict[str, Dict[str, Dict[str, int]]] = {
        lk: defaultdict(dict) for lk, _ in _LOOP_LIST_KEYS
    }

    for trace_id, grp in log_df.groupby(trace_col):
        tid_str   = str(trace_id)
        label     = label_map.get(tid_str, 0)
        activities = grp[activity_col].tolist()

        loops = _detect_loops_in_trace(activities)

        for list_key, loop_type in _LOOP_LIST_KEYS:
            for pattern, count in loops[list_key].items():
                if valid_transitions is not None:
                    surviving = _filter_trace_occurrences(
                        pattern, loop_type, {tid_str: count}, valid_transitions
                    )
                    if not surviving:
                        continue
                    count = surviving[tid_str]

                global_raw[list_key][pattern][tid_str]              = count
                per_label_raw[label][list_key][pattern][tid_str]    = count

    def _to_entries(
        raw:         Dict[str, Dict[str, int]],
        scope_total: int,
    ) -> List[Dict[str, Any]]:
        entries = []
        for pattern, tocc in raw.items():
            sc  = len(tocc)
            sup = round(sc / scope_total, 6) if scope_total else 0.0
            if sc > support_threshold * scope_total:
                entries.append({
                    "pattern":           pattern,
                    "support_count":     sc,
                    "support":           sup,
                    "trace_ids":         sorted(tocc.keys()),
                    "trace_occurrences": dict(tocc),
                })
        return sorted(entries, key=lambda e: e["support_count"], reverse=True)

    global_scope: Dict[str, Any] = {
        lk: _to_entries(global_raw[lk], total_traces)
        for lk, _ in _LOOP_LIST_KEYS
    }

    label_counts = trace_labels.value_counts().to_dict()
    per_label_scope: Dict[str, Any] = {}
    per_label_patterns: Dict[str, Set[Tuple[str, str]]] = {}

    for lv in label_values:
        lv_str   = str(lv)
        lv_total = label_counts.get(lv, 0)
        scope: Dict[str, Any] = {
            lk: _to_entries(per_label_raw[lv][lk], lv_total)
            for lk, _ in _LOOP_LIST_KEYS
        }
        per_label_scope[lv_str] = scope

        patterns: Set[Tuple[str, str]] = set()
        for lk, lt in _LOOP_LIST_KEYS:
            for e in scope[lk]:
                patterns.add((lt, e["pattern"]))
        per_label_patterns[lv_str] = patterns

    lookup: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for lv_str, label_scope in per_label_scope.items():
        for lk, lt in _LOOP_LIST_KEYS:
            for e in label_scope[lk]:
                lookup.setdefault((lt, e["pattern"]), {})[lv_str] = {
                    "support_count": e["support_count"],
                    "support":       e["support"],
                }
    _ZERO = {"support": 0.0, "support_count": 0}
    lv_strs = [str(lv) for lv in label_values]
    for lk, lt in _LOOP_LIST_KEYS:
        for e in global_scope[lk]:
            e["support_per_label"] = {
                lv_s: lookup.get((lt, e["pattern"]), {}).get(lv_s, _ZERO)
                for lv_s in lv_strs
            }

    exclusive_scope: Dict[str, Any] = {}
    for lv_str in per_label_patterns:
        other: Set[Tuple[str, str]] = set()
        for other_str, p_set in per_label_patterns.items():
            if other_str != lv_str:
                other |= p_set
        exc_set = per_label_patterns[lv_str] - other
        exclusive_scope[lv_str] = {
            lk: [
                e for e in per_label_scope[lv_str][lk]
                if (lt, e["pattern"]) in exc_set
            ]
            for lk, lt in _LOOP_LIST_KEYS
        }

    return {
        "global":    global_scope,
        "per_label": per_label_scope,
        "exclusive": exclusive_scope,
        "_label_counts": {str(lv): int(label_counts.get(lv, 0))
                          for lv in label_values},
        "_total_traces":  int(total_traces),
        "_trace_label_map": {str(tid): str(lbl)
                             for tid, lbl in label_map.items()},
    }


# ═══════════════════════════════════════════════════════════════════════════
# 7.  Working-hours loop time fractions
# ═══════════════════════════════════════════════════════════════════════════

def _merge_intervals(
    intervals: List[Tuple[datetime, datetime]],
) -> List[Tuple[datetime, datetime]]:
    if not intervals:
        return []
    merged = [list(sorted(intervals)[0])]
    for s, e in sorted(intervals)[1:]:
        if s <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], e)
        else:
            merged.append([s, e])
    return [(s, e) for s, e in merged]


def _self_loop_extra_raw_intervals(
    activities:     List[str],
    timestamps:     List[datetime],
    end_timestamps: Optional[List[datetime]],
    act:            str,
) -> List[Tuple[datetime, datetime]]:
    n = len(activities)
    intervals: List[Tuple[datetime, datetime]] = []
    i = 0
    while i < n:
        if activities[i] == act:
            j = i + 1
            while j < n and activities[j] == act:
                j += 1
            for k in range(i + 1, j):
                ts  = timestamps[k]
                te  = (end_timestamps[k]  if end_timestamps is not None
                       else timestamps[k + 1] if k + 1 < n
                       else ts)
                if te > ts:
                    intervals.append((ts, te))
            i = j
        else:
            i += 1
    return intervals


def _non_self_loop_extra_raw_intervals(
    activities:     List[str],
    timestamps:     List[datetime],
    end_timestamps: Optional[List[datetime]],
    pattern_acts:   List[str],
) -> List[Tuple[datetime, datetime]]:
    L = len(pattern_acts)
    n = len(activities)
    occ_starts: List[int] = []
    i = 0
    while i <= n - L:
        if activities[i:i + L] == pattern_acts:
            occ_starts.append(i)
            i += max(L - 1, 1)
        else:
            i += 1

    intervals: List[Tuple[datetime, datetime]] = []
    for pos in occ_starts[1:]:
        last = pos + L - 1
        ts   = timestamps[pos]
        te   = (end_timestamps[last] if end_timestamps is not None
                else timestamps[last + 1] if last + 1 < n
                else timestamps[last])
        if te > ts:
            intervals.append((ts, te))
    return intervals


def _repeated_pattern_extra_raw_intervals(
    activities:     List[str],
    timestamps:     List[datetime],
    end_timestamps: Optional[List[datetime]],
    block:          List[str],
) -> List[Tuple[datetime, datetime]]:
    k  = len(block)
    n  = len(activities)
    intervals: List[Tuple[datetime, datetime]] = []
    i  = 0
    while i <= n - k:
        if activities[i:i + k] == block:
            run_idxs = [i]
            j = i + k
            while j + k <= n and activities[j:j + k] == block:
                run_idxs.append(j)
                j += k
            for pos in run_idxs[1:]:
                last = pos + k - 1
                ts   = timestamps[pos]
                te   = (end_timestamps[last] if end_timestamps is not None
                        else timestamps[last + 1] if last + 1 < n
                        else timestamps[last])
                if te > ts:
                    intervals.append((ts, te))
            i = j
        else:
            i += 1
    return intervals


def _compute_trace_loop_fraction(
    activities:     List[str],
    timestamps:     List[datetime],
    end_timestamps: Optional[List[datetime]],
    patterns:       List[Tuple[str, str]],
    work_start:     dtime,
    work_end:       dtime,
) -> Optional[float]:
    ts_first = timestamps[0]
    ts_last  = (max(end_timestamps) if end_timestamps is not None
                else timestamps[-1])
    total_ws = _working_seconds(ts_first, ts_last, work_start, work_end)
    if total_ws <= 0:
        return None

    raw: List[Tuple[datetime, datetime]] = []
    for loop_type, pattern in patterns:
        if loop_type == "self_loop":
            raw.extend(_self_loop_extra_raw_intervals(
                activities, timestamps, end_timestamps, pattern.strip()
            ))
        elif loop_type == "non_self_loop":
            pacts = [a.strip() for a in pattern.split(" -> ")]
            raw.extend(_non_self_loop_extra_raw_intervals(
                activities, timestamps, end_timestamps, pacts
            ))
        else:
            block = [a.strip() for a in pattern.split(" -> ")]
            raw.extend(_repeated_pattern_extra_raw_intervals(
                activities, timestamps, end_timestamps, block
            ))

    merged      = _merge_intervals(raw)
    extra_ws    = sum(_working_seconds(s, e, work_start, work_end)
                      for s, e in merged)
    return min(extra_ws / total_ws, 1.0)


def compute_loop_time_fractions(
    log_df:            pd.DataFrame,
    detection_result:  Dict[str, Any],
    work_start:        dtime,
    work_end:          dtime,
    trace_col:         str           = "trace_id",
    activity_col:      str           = "activity",
    time_col:          str           = "start_timestamp",
    end_time_col:      Optional[str] = None,
    sort_col:          Optional[str] = None,
) -> Dict[str, Any]:
    trace_patterns: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
    for lk, lt in _LOOP_LIST_KEYS:
        for entry in detection_result.get("global", {}).get(lk, []):
            for tid in entry.get("trace_ids", []):
                trace_patterns[str(tid)].append((lt, entry["pattern"]))

    if not trace_patterns:
        return {
            "per_trace":           {},
            "aggregate":           None,
            "per_label_aggregate": {},
            "skipped_zero_ws":     0,
        }

    _sort_by = sort_col if (sort_col and sort_col in log_df.columns) else time_col
    has_end  = end_time_col and end_time_col in log_df.columns

    looping_ids    = set(trace_patterns.keys())
    log_looping    = log_df[log_df[trace_col].astype(str).isin(looping_ids)]

    per_trace: Dict[str, Any] = {}
    skipped = 0

    for trace_id, grp in log_looping.groupby(trace_col):
        tid_str     = str(trace_id)
        grp         = grp.sort_values(_sort_by).reset_index(drop=True)
        activities  = grp[activity_col].tolist()
        timestamps  = grp[time_col].tolist()
        end_ts      = grp[end_time_col].tolist() if has_end else None

        fraction = _compute_trace_loop_fraction(
            activities, timestamps, end_ts,
            trace_patterns[tid_str],
            work_start, work_end,
        )
        if fraction is None:
            skipped += 1
            continue

        ts_first = timestamps[0]
        ts_last  = (max(end_ts) if has_end else timestamps[-1])
        total_ws = _working_seconds(ts_first, ts_last, work_start, work_end)
        extra_ws = fraction * total_ws

        label = None
        if "_label_col_in_grp" in grp.columns:
            label = str(grp["_label_col_in_grp"].iloc[0])

        per_trace[tid_str] = {
            "total_working_sec":      round(total_ws,         2),
            "extra_loop_working_sec": round(extra_ws,         2),
            "loop_time_fraction":     round(fraction,         6),
            "label":                  label,
        }

    def _agg(vals: List[float]) -> Optional[Dict[str, Any]]:
        if not vals:
            return None
        a = np.asarray(vals, dtype=float)
        return {
            "count":  int(len(a)),
            "mean":   round(float(a.mean()),               4),
            "median": round(float(np.median(a)),           4),
            "min":    round(float(a.min()),                4),
            "max":    round(float(a.max()),                4),
            "pct25":  round(float(np.percentile(a, 25)),  4),
            "pct75":  round(float(np.percentile(a, 75)),  4),
        }

    all_fractions = [v["loop_time_fraction"] for v in per_trace.values()]
    aggregate     = _agg(all_fractions)

    label_fractions: Dict[str, List[float]] = defaultdict(list)
    trace_label_map: Dict[str, str] = detection_result.get("_trace_label_map", {})

    for tid, v in per_trace.items():
        lv = trace_label_map.get(str(tid), "0")
        label_fractions[lv].append(v["loop_time_fraction"])

    per_label_agg = {lv: _agg(fracs) for lv, fracs in sorted(label_fractions.items())}

    return {
        "per_trace":           per_trace,
        "aggregate":           aggregate,
        "per_label_aggregate": per_label_agg,
        "skipped_zero_ws":     skipped,
    }


# ═══════════════════════════════════════════════════════════════════════════
# 8.  Stats assembly
# ═══════════════════════════════════════════════════════════════════════════

def assemble_stats(
    detection_result:  Dict[str, Any],
    loop_time_result:  Dict[str, Any],
) -> Dict[str, Any]:
    total        = detection_result["_total_traces"]
    label_counts = detection_result["_label_counts"]

    label_dist: Dict[str, Any] = {
        "total_traces": total,
        "per_label": {
            lv: {
                "count": cnt,
                "pct":   round(cnt / total * 100, 2) if total else 0.0,
            }
            for lv, cnt in sorted(label_counts.items())
        },
    }

    def _looping_traces(scope: Dict[str, Any]) -> Set[str]:
        ids: Set[str] = set()
        for lk, _ in _LOOP_LIST_KEYS:
            for e in scope.get(lk, []):
                ids.update(e.get("trace_ids", []))
        return ids

    global_looping  = _looping_traces(detection_result.get("global", {}))
    cwl             = len(global_looping)
    prevalence: Dict[str, Any] = {
        "total_traces":     total,
        "cases_with_loops": cwl,
        "pct_with_loops":   round(cwl / total * 100, 2) if total else 0.0,
        "per_label": {},
    }
    for lv_str, label_scope in detection_result.get("per_label", {}).items():
        lv_ids  = _looping_traces(label_scope)
        lv_tot  = label_counts.get(lv_str, 0)
        prevalence["per_label"][lv_str] = {
            "total_traces":     lv_tot,
            "cases_with_loops": len(lv_ids),
            "pct_with_loops":   round(len(lv_ids) / lv_tot * 100, 2)
                                if lv_tot else 0.0,
        }

    return {
        "label_distribution": label_dist,
        "prevalence":         prevalence,
        "loop_time_fractions": {
            "aggregate":           loop_time_result.get("aggregate"),
            "per_label_aggregate": loop_time_result.get("per_label_aggregate", {}),
            "skipped_zero_ws":     loop_time_result.get("skipped_zero_ws", 0),
            "note": (
                f"{loop_time_result.get('skipped_zero_ws',0)} trace(s) had "
                "zero working-hours duration (weekend/off-hours only) and "
                "were excluded from the fraction computation."
                if loop_time_result.get("skipped_zero_ws", 0) else None
            ),
        },
    }


# ═══════════════════════════════════════════════════════════════════════════
# 9.  Main entrypoint
# ═══════════════════════════════════════════════════════════════════════════

def run_loop_analysis(
    log_df:            pd.DataFrame,
    separating_key:    str,
    separating_groups: List[List[str]],
    valid_transitions: Optional[Set[Tuple[str, str]]] = None,
    activity_filter:   Optional[Set[str]]             = None,
    trace_col:         str           = "trace_id",
    activity_col:      str           = "activity",
    time_col:          str           = "start_timestamp",
    end_time_col:      Optional[str] = None,
    work_start:        Optional[dtime] = None,
    work_end:          Optional[dtime] = None,
    sort_col:          Optional[str] = None,
    support_threshold: float         = 0.0,
) -> Dict[str, Any]:
    log_df = log_df.copy()
    log_df[trace_col] = log_df[trace_col].astype(str)
    
    # Robust unified datetime conversion
    log_df[time_col]  = _parse_timestamps(log_df[time_col])
    if end_time_col and end_time_col in log_df.columns:
        log_df[end_time_col] = _parse_timestamps(log_df[end_time_col])
    else:
        end_time_col = None

    _sort = sort_col if (sort_col and sort_col in log_df.columns) else time_col
    log_df = log_df.sort_values([trace_col, _sort])

    if work_start is None or work_end is None:
        ws, we = _derive_working_hours(log_df[time_col])
        if work_start is None:
            work_start = ws
        if work_end is None:
            work_end = we
    logger.info("Working hours: %s – %s", work_start, work_end)

    logger.info("Building trace labels (sep_key=%s, groups=%s) …",
                separating_key, separating_groups)
    trace_labels = build_trace_labels(
        log_df, separating_key, separating_groups, trace_col
    )
    logger.info("Label distribution: %s",
                trace_labels.value_counts().to_dict())

    if activity_filter:
        before = len(log_df)
        log_df = log_df[log_df[activity_col].isin(activity_filter)].copy()
        dropped = before - len(log_df)
        logger.info(
            "Activity filter: keeping %d of %d activities (%d events dropped, "
            "%d activities in whitelist).",
            len(activity_filter), log_df[activity_col].nunique() + dropped,
            dropped, len(activity_filter),
        )

    logger.info("Detecting loops …")
    detection = detect_and_aggregate(
        log_df, trace_labels, valid_transitions,
        trace_col, activity_col, _sort, support_threshold,
    )

    logger.info("Computing loop time fractions …")
    ltf = compute_loop_time_fractions(
        log_df, detection, work_start, work_end,
        trace_col, activity_col, time_col, end_time_col, _sort,
    )

    stats = assemble_stats(detection, ltf)

    detection_public = {
        k: v for k, v in detection.items()
        if not k.startswith("_")
    }

    return {
        "metadata": {
            "total_traces":               detection["_total_traces"],
            "work_start":                 str(work_start),
            "work_end":                   str(work_end),
            "separating_key":             separating_key,
            "separating_groups":          separating_groups,
            "valid_transitions_filtered": valid_transitions is not None,
            "activity_filter":            sorted(activity_filter)
                                          if activity_filter else None,
            "support_threshold":          support_threshold,
        },
        "loop_detection":       detection_public,
        "per_trace_fractions":  ltf.get("per_trace", {}),
        "stats":                stats,
    }


# ═══════════════════════════════════════════════════════════════════════════
# 10. Output helpers
# ═══════════════════════════════════════════════════════════════════════════

def _print_report(result: Dict[str, Any]) -> None:
    meta  = result.get("metadata", {})
    stats = result.get("stats",    {})

    print("\n" + "=" * 64)
    print("  LOOP ANALYSIS REPORT")
    print("=" * 64)
    print(f"  Working hours : {meta.get('work_start')} – {meta.get('work_end')}")
    print(f"  Total traces  : {meta.get('total_traces')}")
    vt = "yes" if meta.get("valid_transitions_filtered") else "no"
    print(f"  Filtered by valid transitions: {vt}")

    ld = stats.get("label_distribution", {})
    print("\n┌─ Label distribution ────────────────────────────────────────┐")
    for lv, d in sorted(ld.get("per_label", {}).items()):
        print(f"│  label {lv:>4s} : {d['count']:>6}  ({d['pct']:.1f}%)")
    print("└─────────────────────────────────────────────────────────────┘")

    prev = stats.get("prevalence", {})
    print("\n┌─ Stat 1: Cases with ≥1 loop ────────────────────────────────┐")
    print(f"│  Overall : {prev.get('cases_with_loops'):>6} / "
          f"{prev.get('total_traces')}  "
          f"({prev.get('pct_with_loops', 0):.1f}%)")
    for lv, d in sorted(prev.get("per_label", {}).items()):
        print(f"│  label {lv:>4s} : {d['cases_with_loops']:>6} / "
              f"{d['total_traces']}  ({d['pct_with_loops']:.1f}%)")
    print("└─────────────────────────────────────────────────────────────┘")

    ltf = stats.get("loop_time_fractions", {})
    agg = ltf.get("aggregate")
    print("\n┌─ Stat 2: Working-hours fraction in loop repetitions ─────────┐")
    if agg:
        print(f"│  (over {agg['count']} looping traces)")
        print(f"│  Mean   : {agg['mean']   * 100:.2f}%")
        print(f"│  Median : {agg['median'] * 100:.2f}%")
        print(f"│  Min    : {agg['min']    * 100:.2f}%")
        print(f"│  Max    : {agg['max']    * 100:.2f}%")
        print(f"│  IQR    : [{agg['pct25']*100:.2f}%,"
              f" {agg['pct75']*100:.2f}%]")
        for lv, d in sorted(ltf.get("per_label_aggregate", {}).items()):
            if d:
                print(f"│  label {lv:>4s} : "
                      f"mean={d['mean']*100:.2f}%  "
                      f"median={d['median']*100:.2f}%  "
                      f"(n={d['count']})")
        if ltf.get("skipped_zero_ws", 0):
            print(f"│  ⚠  {ltf['skipped_zero_ws']} trace(s) skipped"
                  " (zero working-hours duration)")
    else:
        print("│  No looping traces found.")
    print("└─────────────────────────────────────────────────────────────┘\n")


def save_results(result: Dict[str, Any], prefix: str, fmt: str = "json") -> None:
    stem = prefix.rstrip(".json").rstrip(".csv")

    if fmt in ("json", "both"):
        jp = stem + ".json"
        slim = {k: v for k, v in result.items() if k != "per_trace_fractions"}
        with open(jp, "w", encoding="utf-8") as fh:
            json.dump(slim, fh, indent=2, ensure_ascii=False)
        print(f"JSON written          : {jp}")

        ptf = result.get("per_trace_fractions", {})
        if ptf:
            ptj = stem + "_per_trace.json"
            with open(ptj, "w", encoding="utf-8") as fh:
                json.dump(ptf, fh, indent=2, ensure_ascii=False)
            print(f"Per-trace JSON written : {ptj}")

    if fmt in ("csv", "both"):
        stats = result.get("stats", {})
        rows: List[Dict[str, Any]] = []

        for lv, d in sorted(stats.get("label_distribution", {})
                            .get("per_label", {}).items()):
            rows.append({"section": "label_distribution", "label": lv,
                         "count": d["count"], "pct": d["pct"]})

        prev = stats.get("prevalence", {})
        rows.append({"section": "prevalence", "label": "all",
                     "cases_with_loops": prev.get("cases_with_loops"),
                     "total_traces": prev.get("total_traces"),
                     "pct_with_loops": prev.get("pct_with_loops")})
        for lv, d in sorted(prev.get("per_label", {}).items()):
            rows.append({"section": "prevalence", "label": lv,
                         "cases_with_loops": d["cases_with_loops"],
                         "total_traces": d["total_traces"],
                         "pct_with_loops": d["pct_with_loops"]})

        ltf = stats.get("loop_time_fractions", {})
        agg = ltf.get("aggregate")
        if agg:
            rows.append({"section": "loop_time_fraction", "label": "all", **agg})
        for lv, d in sorted(ltf.get("per_label_aggregate", {}).items()):
            if d:
                rows.append({"section": "loop_time_fraction", "label": lv, **d})

        fields = ["section", "label", "count", "pct",
                  "cases_with_loops", "total_traces", "pct_with_loops",
                  "mean", "median", "min", "max", "pct25", "pct75"]
        cp = stem + "_stats.csv"
        with open(cp, "w", newline="", encoding="utf-8") as fh:
            w = csv_mod.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        print(f"Stats CSV written     : {cp}")

        ptf = result.get("per_trace_fractions", {})
        if ptf:
            pcp = stem + "_per_trace.csv"
            pt_fields = ["trace_id", "label",
                         "total_working_sec", "extra_loop_working_sec",
                         "loop_time_fraction"]
            with open(pcp, "w", newline="", encoding="utf-8") as fh:
                w = csv_mod.DictWriter(fh, fieldnames=pt_fields)
                w.writeheader()
                for tid, d in sorted(ptf.items()):
                    w.writerow({"trace_id": tid, **d})
            print(f"Per-trace CSV written  : {pcp}")


# ═══════════════════════════════════════════════════════════════════════════
# 11. CLI
# ═══════════════════════════════════════════════════════════════════════════

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--log",          required=True, help="Event log CSV.")
    p.add_argument("--sep-key",      required=True, dest="sep_key")
    p.add_argument("--sep-groups",   required=True, dest="sep_groups")
    p.add_argument("--valid",        default=None)
    p.add_argument("--from-col",     default=None, dest="from_col")
    p.add_argument("--to-col",       default=None, dest="to_col")
    p.add_argument("--trace-col",    default="trace_id",  dest="trace_col")
    p.add_argument("--activity-col", default="activity",  dest="activity_col")
    p.add_argument("--time-col",     default="start_timestamp", dest="time_col")
    p.add_argument("--end-time-col", default=None,        dest="end_time_col")
    p.add_argument("--work-start",   default=None,        dest="work_start")
    p.add_argument("--work-end",     default=None,        dest="work_end")
    p.add_argument("--activities",   default=None,        dest="activities")
    p.add_argument("--support",      default=0.0, type=float)
    p.add_argument("--output",       default=None)
    p.add_argument("--format",       choices=["json", "csv", "both"], default="json")
    return p


def main(argv: Optional[List[str]] = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
    args = _build_parser().parse_args(argv)

    try:
        sep_groups: List[List[str]] = json.loads(args.sep_groups)
    except (json.JSONDecodeError, ValueError) as exc:
        print(f"Error: --sep-groups is not valid JSON: {exc}", file=sys.stderr)
        sys.exit(1)

    def _parse_time(s: Optional[str]) -> Optional[dtime]:
        if s is None:
            return None
        try:
            h, m = map(int, s.split(":"))
            return dtime(h, m)
        except Exception:
            print(f"Error: cannot parse time '{s}' (expected HH:MM).", file=sys.stderr)
            sys.exit(1)

    work_start = _parse_time(args.work_start)
    work_end   = _parse_time(args.work_end)

    valid = None
    if args.valid:
        print(f"Loading valid transitions from: {args.valid}")
        valid = load_valid_transitions(args.valid, args.from_col, args.to_col)
        print(f"  {len(valid)} valid transition(s) loaded.")

    act_filter = None
    if args.activities:
        print(f"Loading activity filter from  : {args.activities}")
        act_filter = load_activity_filter(args.activities)
        print(f"  {len(act_filter)} activity/activities to keep.")

    print(f"Loading event log from: {args.log}")
    log_df = pd.read_csv(args.log)
    log_df.columns = log_df.columns.str.strip()
    log_df[args.trace_col] = log_df[args.trace_col]
    log_df[args.activity_col] = log_df[args.activity_col].astype(str)
    print(f"  {len(log_df):,} events, {log_df[args.trace_col].nunique():,} traces loaded.")

    result = run_loop_analysis(
        log_df            = log_df,
        separating_key    = args.sep_key,
        separating_groups = sep_groups,
        valid_transitions = valid,
        activity_filter   = act_filter,
        trace_col         = args.trace_col,
        activity_col      = args.activity_col,
        time_col          = args.time_col,
        end_time_col      = args.end_time_col,
        work_start        = work_start,
        work_end          = work_end,
        support_threshold = args.support,
    )

    _print_report(result)
    if args.output:
        save_results(result, args.output, args.format)


if __name__ == "__main__":
    main()