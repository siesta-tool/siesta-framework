"""
tests/eval/batch_splitter.py

Utility for splitting a single event log into N realistic ingest batches.

Three modes
-----------
temporal
    Split by event timestamp.  The log is sorted by timestamp and cut
    into N equal-sized time windows.  Each window becomes one batch CSV.
    Requires that events span a meaningful time range — use this for
    6.3.3 (retention sensitivity) where the time axis matters.

trace_sample
    Partition traces into N disjoint random subsets (stratified by the
    first grouping-key attribute if available).  Each subset is one batch.
    This is the most realistic mode for process logs: each batch contains
    independent traces with their original timestamps.  Use this for 6.3.2
    (maintenance vs coverage).

synthetic
    Copy the full log N times.  In copy K, rewrite trace_id as
    ``<original>__b<K>`` and shift all timestamps forward by
    K × (max_ts − min_ts + 1 day).  Produces N batches of identical size
    with artificially non-overlapping timestamps.  Use this for small
    smoke-test datasets where correctness matters more than realism.

Output
------
Each batch is written as a CSV to ``<output_dir>/batch_<K>.csv``
(0-indexed).  The schema matches the input CSV schema exactly.  XES logs
are first converted to a flat CSV using the same field-mapping logic as
eval_common.

Usage
-----
    from tests.eval.batch_splitter import split_log

    paths = split_log(
        src=Path("datasets/bpic_2017.xes"),
        n_batches=5,
        mode="trace_sample",
        output_dir=Path("tests/eval/results/batches/bpic_2017"),
        grouping_key="org:resource",   # for stratified sampling
    )
    # paths is a list of Path objects, one per batch.

    # Or from the CLI:
    python tests/eval/batch_splitter.py \\
        --src datasets/bpic_2017.xes \\
        --n 5 --mode trace_sample \\
        --out tests/eval/results/batches/bpic_2017 \\
        --grouping-key org:resource
"""

from __future__ import annotations

import argparse
import csv
import os
import random
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator


# ---------------------------------------------------------------------------
# XES reader (minimal — extracts trace_id, activity, timestamp, attributes)
# ---------------------------------------------------------------------------

def _iter_xes(path: Path) -> Iterator[dict]:
    """
    Yield one dict per event from an XES file.  Reads line-by-line so
    large files are not loaded into memory.

    Recognised patterns (case-insensitive):
        <trace>  ...  </trace>  — outer trace envelope
        <string key="concept:name" value="..."/>  — trace ID or activity
        <date   key="time:timestamp" value="..."/>
        other <string/> or <int/> or <float/> keys -> attribute columns
    """
    _TRACE_START = re.compile(r'<trace\b', re.I)
    _TRACE_END   = re.compile(r'</trace>', re.I)
    _EVENT_START = re.compile(r'<event\b', re.I)
    _EVENT_END   = re.compile(r'</event>', re.I)
    _ATTR        = re.compile(
        r'<(?:string|int|float|boolean|id)\s+key="([^"]+)"\s+value="([^"]*)"',
        re.I,
    )
    _DATE        = re.compile(r'<date\s+key="([^"]+)"\s+value="([^"]*)"', re.I)

    trace_id: str = ""
    in_event: bool = False
    event_buf: dict = {}

    with path.open(encoding="utf-8", errors="replace") as f:
        for line in f:
            if _TRACE_START.search(line):
                trace_id = ""
                in_event = False
            elif _TRACE_END.search(line):
                trace_id = ""
                in_event = False
            elif _EVENT_START.search(line):
                in_event = True
                event_buf = {}
            elif _EVENT_END.search(line):
                if in_event and "activity" in event_buf and "timestamp" in event_buf:
                    event_buf["trace_id"] = trace_id
                    yield dict(event_buf)
                in_event = False
                event_buf = {}
            else:
                # Attribute line — applies to trace header or event body.
                m = _ATTR.search(line) or _DATE.search(line)
                if m:
                    key, val = m.group(1), m.group(2)
                    if key == "concept:name":
                        if not in_event:
                            trace_id = val
                        else:
                            event_buf["activity"] = val
                    elif key == "time:timestamp":
                        event_buf["timestamp"] = val
                    else:
                        if in_event:
                            event_buf[key] = val


def _iter_csv(path: Path) -> Iterator[dict]:
    with path.open(newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        # Normalise column names: map common variants to our canonical names.
        _RENAMES = {
            "case:concept:name": "trace_id",
            "concept:name":      "activity",
            "time:timestamp":    "timestamp",
        }
        for row in reader:
            normalised = {}
            for k, v in row.items():
                normalised[_RENAMES.get(k, k)] = v
            if "trace_id" in normalised and "activity" in normalised:
                yield normalised


def _iter_log(path: Path) -> Iterator[dict]:
    if path.suffix.lower() == ".xes":
        return _iter_xes(path)
    return _iter_csv(path)


# ---------------------------------------------------------------------------
# Timestamp handling
# ---------------------------------------------------------------------------

_TS_FORMATS = [
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
]


def _parse_ts(ts_str: str) -> float:
    """Parse ISO-ish timestamp string to Unix epoch float."""
    ts_str = ts_str.strip()
    for fmt in _TS_FORMATS:
        try:
            dt = datetime.strptime(ts_str, fmt)
            return dt.timestamp()
        except ValueError:
            continue
    # Last resort: try dateutil if available.
    try:
        from dateutil.parser import parse as du_parse
        return du_parse(ts_str).timestamp()
    except Exception:
        return 0.0


def _shift_ts(ts_str: str, delta_seconds: float) -> str:
    """Add delta_seconds to a timestamp string, return same ISO format."""
    ts_str = ts_str.strip()
    epoch = _parse_ts(ts_str)
    new_dt = datetime.utcfromtimestamp(epoch + delta_seconds)
    return new_dt.strftime("%Y-%m-%dT%H:%M:%S")


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------

def _write_batches(
    batches: list[list[dict]],
    output_dir: Path,
    all_keys: list[str],
) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for k, events in enumerate(batches):
        p = output_dir / f"batch_{k}.csv"
        with p.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(events)
        paths.append(p)
        print(f"  batch_{k}.csv: {len(events)} events")
    return paths


# ---------------------------------------------------------------------------
# Split modes
# ---------------------------------------------------------------------------

def _split_temporal(
    events: list[dict],
    n_batches: int,
) -> list[list[dict]]:
    """
    Sort all events by timestamp and cut into N equal-sized time windows.
    Each window is one batch.  Events with unparseable timestamps go into
    batch 0.
    """
    events_ts = []
    for ev in events:
        ts = _parse_ts(ev.get("timestamp", ""))
        events_ts.append((ts, ev))
    events_ts.sort(key=lambda x: x[0])

    chunk = max(1, len(events_ts) // n_batches)
    batches: list[list[dict]] = []
    for i in range(n_batches):
        start = i * chunk
        end   = start + chunk if i < n_batches - 1 else len(events_ts)
        batches.append([ev for _, ev in events_ts[start:end]])
    return batches


def _split_trace_sample(
    events: list[dict],
    n_batches: int,
    grouping_key: str | None,
    seed: int,
) -> list[list[dict]]:
    """
    Partition the log's traces into N disjoint random subsets.

    If `grouping_key` is provided, stratify by that attribute: each distinct
    value's traces are split proportionally across batches so every batch
    sees every group value (important for perspective maintenance correctness).
    """
    rng = random.Random(seed)

    # Group events by trace_id.
    trace_map: dict[str, list[dict]] = defaultdict(list)
    for ev in events:
        trace_map[ev["trace_id"]].append(ev)

    if grouping_key:
        # Stratify: bucket traces by their grouping-key value.
        strata: dict[str, list[str]] = defaultdict(list)
        for tid, evs in trace_map.items():
            gval = evs[0].get(grouping_key, "__unknown__")
            strata[gval].append(tid)
        # Shuffle within each stratum then interleave.
        ordered_tids: list[str] = []
        for gval in sorted(strata):
            tids = strata[gval]
            rng.shuffle(tids)
            ordered_tids.extend(tids)
    else:
        ordered_tids = list(trace_map.keys())
        rng.shuffle(ordered_tids)

    chunk = max(1, len(ordered_tids) // n_batches)
    batches: list[list[dict]] = []
    for i in range(n_batches):
        start = i * chunk
        end   = start + chunk if i < n_batches - 1 else len(ordered_tids)
        batch_events: list[dict] = []
        for tid in ordered_tids[start:end]:
            batch_events.extend(trace_map[tid])
        batches.append(batch_events)
    return batches


def _split_synthetic(
    events: list[dict],
    n_batches: int,
) -> list[list[dict]]:
    """
    Copy the full log N times.  In copy K:
      - trace_id becomes  <original>__b<K>
      - timestamps are shifted forward by K × (span + 1 day)

    Produces N batches of identical activity distribution with
    non-overlapping timestamps.
    """
    if not events:
        return [[] for _ in range(n_batches)]

    ts_vals = [_parse_ts(ev.get("timestamp", "")) for ev in events]
    ts_min, ts_max = min(ts_vals), max(ts_vals)
    span_s = max(ts_max - ts_min, 1.0) + 86_400  # +1 day padding

    batches: list[list[dict]] = []
    for k in range(n_batches):
        shift = k * span_s
        batch = []
        for ev in events:
            new_ev = dict(ev)
            new_ev["trace_id"]  = f"{ev['trace_id']}__b{k}"
            new_ev["timestamp"] = _shift_ts(ev.get("timestamp", ""), shift)
            batch.append(new_ev)
        batches.append(batch)
    return batches


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def split_log(
    src: Path,
    n_batches: int,
    mode: str,
    output_dir: Path,
    grouping_key: str | None = None,
    seed: int = 42,
) -> list[Path]:
    """
    Split `src` into `n_batches` CSV files under `output_dir`.

    Parameters
    ----------
    src          : path to source log (CSV or XES)
    n_batches    : number of output batches
    mode         : "temporal" | "trace_sample" | "synthetic"
    output_dir   : directory for batch_0.csv … batch_N-1.csv
    grouping_key : attribute key for stratified sampling (trace_sample only)
    seed         : random seed for trace_sample and synthetic

    Returns
    -------
    List of Path objects, one per batch, in ingest order.
    """
    print(f"Reading {src} ...")
    events = list(_iter_log(src))
    print(f"  {len(events)} events read")

    if not events:
        raise ValueError(f"No events read from {src}")

    # Collect all column names that appear in the data.
    all_keys_set: set[str] = set()
    for ev in events:
        all_keys_set.update(ev.keys())
    # Put canonical columns first.
    priority = ["trace_id", "activity", "timestamp"]
    all_keys = priority + sorted(all_keys_set - set(priority))

    print(f"  Splitting into {n_batches} batches using mode='{mode}' ...")
    if mode == "temporal":
        batches = _split_temporal(events, n_batches)
    elif mode == "trace_sample":
        batches = _split_trace_sample(events, n_batches, grouping_key, seed)
    elif mode == "synthetic":
        batches = _split_synthetic(events, n_batches)
    else:
        raise ValueError(f"Unknown mode '{mode}'. Choose: temporal, trace_sample, synthetic.")

    return _write_batches(batches, output_dir, all_keys)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Split an event log into N realistic ingest batches.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split("Usage")[1] if "Usage" in __doc__ else "",
    )
    ap.add_argument("--src",  required=True,  type=Path,
                    help="Source log (CSV or XES).")
    ap.add_argument("--n",    required=True,  type=int,  dest="n_batches",
                    help="Number of output batches.")
    ap.add_argument("--mode", required=True,
                    choices=["temporal", "trace_sample", "synthetic"],
                    help="Splitting strategy.")
    ap.add_argument("--out",  required=True,  type=Path,  dest="output_dir",
                    help="Output directory for batch CSVs.")
    ap.add_argument("--grouping-key", default=None, dest="grouping_key",
                    help="Attribute key for stratified sampling (trace_sample only).")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    paths = split_log(
        src=args.src,
        n_batches=args.n_batches,
        mode=args.mode,
        output_dir=args.output_dir,
        grouping_key=args.grouping_key,
        seed=args.seed,
    )
    print(f"\nWrote {len(paths)} batches to {args.output_dir}")
    for p in paths:
        print(f"  {p}")


if __name__ == "__main__":
    main()