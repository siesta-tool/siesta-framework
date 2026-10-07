"""
tests/vldb_eval/scaleout/replicate_bpic2017.py

Build replicated BPIC2017 CSV logs for the SCALE-OUT experiment (Exp 2).

The source XES is parsed ONCE into a canonical CSV (``bpic2017_r1.csv``) using
the same ElementTree iterator the rest of the eval suite relies on
(``batch_splitter._iter_xes`` — no pm4py needed).  Each replica factor F then
streams that base CSV F times, suffixing every ``trace_id`` with ``__r{k}`` so
the copies are independent traces with brand-new ids, as the experiment
requires ("sample traces and replicate them with new trace_ids").

Replicas keep the original timestamps by default (so every event stays inside
the query lookback window); ``--shift-days`` can spread copies across time if a
non-overlapping layout is wanted.  ``--sample-frac`` subsamples distinct traces
before replication.

Output (default dir ``/srv/datasets/scaleout``):
    bpic2017_r1.csv, bpic2017_r2.csv, bpic2017_r3.csv

Usage:
    python -m tests.vldb_eval.scaleout.replicate_bpic2017 --factors 1 2 3
    python -m tests.vldb_eval.scaleout.replicate_bpic2017 --factors 1 \
        --source /srv/datasets/bpic2017.xes --out-dir /srv/datasets/scaleout
"""

from __future__ import annotations

import argparse
import csv
import random
import sys
import time
from datetime import datetime
from pathlib import Path

# Allow running directly from the repo root.
_REPO = Path(__file__).resolve().parents[3]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from tests.vldb_eval.batch_splitter import _iter_xes, _parse_ts, _shift_ts

DEFAULT_SOURCE = Path("/srv/datasets/bpic2017.xes")
DEFAULT_OUT_DIR = Path("/srv/datasets/scaleout")

# Canonical columns always come first; attribute columns follow in first-seen
# order so the CSV header is stable and readable.
_LEAD_COLS = ["trace_id", "activity", "timestamp"]


def build_base_csv(source: Path, out_csv: Path, *, sample_frac: float = 1.0,
                   seed: int = 42) -> dict:
    """Parse the XES source once and write a flat canonical CSV.

    Returns a summary dict with the header, trace count and event count.
    """
    t0 = time.time()
    rows: list[dict] = []
    attr_cols: list[str] = []
    seen_attr: set[str] = set()
    trace_ids: set[str] = set()

    for ev in _iter_xes(source):
        tid = ev.get("trace_id")
        if tid is None or "activity" not in ev or "timestamp" not in ev:
            continue
        trace_ids.add(tid)
        for k in ev:
            if k not in _LEAD_COLS and k not in seen_attr:
                seen_attr.add(k)
                attr_cols.append(k)
        rows.append(ev)

    # Optional trace subsampling (sample distinct traces, keep all their events).
    if sample_frac < 1.0:
        rng = random.Random(seed)
        keep = set(rng.sample(sorted(trace_ids),
                              max(1, int(len(trace_ids) * sample_frac))))
        rows = [r for r in rows if r["trace_id"] in keep]
        trace_ids = keep

    header = _LEAD_COLS + attr_cols
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)

    dt = time.time() - t0
    print(f"[base] {out_csv.name}: {len(trace_ids):,} traces, "
          f"{len(rows):,} events, {len(attr_cols)} attr cols, {dt:.1f}s")
    return {"header": header, "n_traces": len(trace_ids), "n_events": len(rows)}


def _time_span_seconds(base_csv: Path) -> float:
    """Max-min timestamp span of the base CSV, for timestamp shifting."""
    lo = float("inf")
    hi = float("-inf")
    with base_csv.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            e = _parse_ts(row.get("timestamp", ""))
            if e:
                lo = min(lo, e)
                hi = max(hi, e)
    if lo == float("inf"):
        return 0.0
    return max(0.0, hi - lo)


def replicate(base_csv: Path, out_csv: Path, factor: int, *,
              shift_days: float = 0.0) -> dict:
    """Stream ``base_csv`` ``factor`` times into ``out_csv`` with new ids."""
    if factor == 1 and shift_days == 0.0:
        # r1 is just the base; avoid a pointless copy.
        if out_csv.resolve() != base_csv.resolve():
            out_csv.write_bytes(base_csv.read_bytes())
        print(f"[r{factor}] {out_csv.name}: identical to base")
        return {"factor": factor}

    t0 = time.time()
    span = _time_span_seconds(base_csv) + 86400.0 if shift_days else 0.0
    shift_unit = shift_days * 86400.0 if shift_days else 0.0

    with base_csv.open(newline="", encoding="utf-8") as fin:
        header = next(csv.reader(fin))
    ts_idx = header.index("timestamp")
    tid_idx = header.index("trace_id")

    n_rows = 0
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as fout:
        w = csv.writer(fout)
        w.writerow(header)
        for k in range(factor):
            suffix = f"__r{k}"
            delta = (span + shift_unit) * k if shift_days else 0.0
            with base_csv.open(newline="", encoding="utf-8") as fin:
                r = csv.reader(fin)
                next(r)  # skip header
                for row in r:
                    row[tid_idx] = row[tid_idx] + suffix
                    if delta:
                        row[ts_idx] = _shift_ts(row[ts_idx], delta)
                    w.writerow(row)
                    n_rows += 1

    dt = time.time() - t0
    print(f"[r{factor}] {out_csv.name}: {n_rows:,} events, {dt:.1f}s")
    return {"factor": factor, "n_events": n_rows}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    ap.add_argument("--factors", nargs="+", type=int, default=[1, 2, 3])
    ap.add_argument("--sample-frac", type=float, default=1.0,
                    help="fraction of distinct traces to keep in the base")
    ap.add_argument("--shift-days", type=float, default=0.0,
                    help="spread replicas forward in time by this many days each")
    ap.add_argument("--prefix", default="bpic2017")
    args = ap.parse_args()

    if not args.source.exists():
        ap.error(f"source not found: {args.source}")

    base_csv = args.out_dir / f"{args.prefix}_r1.csv"
    if not base_csv.exists():
        build_base_csv(args.source, base_csv, sample_frac=args.sample_frac)
    else:
        print(f"[base] reusing existing {base_csv}")

    for f in sorted(set(args.factors)):
        out = args.out_dir / f"{args.prefix}_r{f}.csv"
        if out.exists() and f != 1:
            print(f"[r{f}] reusing existing {out}")
            continue
        replicate(base_csv, out, f, shift_days=args.shift_days)

    print("done.")


if __name__ == "__main__":
    main()
