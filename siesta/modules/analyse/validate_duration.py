#!/usr/bin/env python3
"""
validate_duration.py — Quick diagnostic for per-trace working-hours duration.

Prints a plain-text breakdown of the duration distribution so you can verify
what the violin plot is actually showing.

Usage:
    python validate_duration.py \\
        --log        event_log.csv      \\
        [--trace-col trace_id]          \\
        [--time-col  start_timestamp]   \\
        [--work-start 08:00]            \\
        [--work-end   18:00]            \\
        [--n          10]               # show this many example traces
"""

import argparse
from datetime import datetime, timedelta, time as dtime, timezone

import numpy as np
import pandas as pd


def _working_seconds(dt1, dt2, work_start, work_end):
    # dt1 and dt2 are already timezone-aware datetime objects now
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


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--log",         required=True)
    ap.add_argument("--trace-col",   default="trace_id",       dest="trace_col")
    ap.add_argument("--time-col",    default="start_timestamp", dest="time_col")
    ap.add_argument("--work-start",  default=None, dest="work_start",
                    help="HH:MM  (auto-derived when absent)")
    ap.add_argument("--work-end",    default=None, dest="work_end",
                    help="HH:MM  (auto-derived when absent)")
    ap.add_argument("--n",           default=10, type=int,
                    help="Number of example traces to print (default 10)")
    args = ap.parse_args()

    # ── Load ────────────────────────────────────────────────────────────
    df = pd.read_csv(args.log)
    df.columns = df.columns.str.strip()
    
    # Cleanly parse string or numeric timestamps into Datetime objects
    if pd.api.types.is_numeric_dtype(df[args.time_col]):
        df[args.time_col] = pd.to_datetime(df[args.time_col], unit='s', utc=True)
    else:
        df[args.time_col] = pd.to_datetime(df[args.time_col], errors="coerce", utc=True)

    print(f"\nLoaded {len(df):,} events across "
          f"{df[args.trace_col].nunique():,} traces.")

    # ── Sanity-check raw timestamps ──────────────────────────────────────
    sample_dt = df[args.time_col].dropna().iloc[0]
    print(f"\nFirst timestamp raw value : {sample_dt}")
    print(f"  → interpreted as UTC    : {sample_dt.isoformat()}")

    # ── Auto-derive or parse working hours ───────────────────────────────
    def parse_time(s):
        h, m = map(int, s.split(":"))
        return dtime(h, m)

    # Clean extracting of the time component
    all_times = df[args.time_col].dt.time.dropna()
    auto_start, auto_end = min(all_times), max(all_times)

    work_start = parse_time(args.work_start) if args.work_start else auto_start
    work_end   = parse_time(args.work_end)   if args.work_end   else auto_end

    print(f"\nWorking hours used        : {work_start} – {work_end} UTC")
    if not args.work_start:
        print("  (auto-derived from data — override with --work-start / --work-end)")

    # ── Per-trace raw span and working-hours duration ────────────────────
    agg = df.groupby(args.trace_col)[args.time_col].agg(
        first_ts="min", last_ts="max", n_events="count"
    ).reset_index()

    agg["raw_span_min"] = (agg["last_ts"] - agg["first_ts"]).dt.total_seconds() / 60.0
    agg["work_hours"]   = agg.apply(
        lambda r: _working_seconds(
            r["first_ts"], r["last_ts"], work_start, work_end
        ) / 3600.0,
        axis=1,
    )
    
    agg["first_event_utc"] = agg["first_ts"].dt.strftime("%Y-%m-%d %H:%M UTC")

    # ── Summary ──────────────────────────────────────────────────────────
    wh = agg["work_hours"].values
    rs = agg["raw_span_min"].values

    print("\n" + "=" * 56)
    print("  RAW SPAN (calendar minutes, no calendar filter)")
    print("=" * 56)
    for label, pct in [("min", 0), ("p10", 10), ("p25", 25),
                        ("median", 50), ("p75", 75), ("p90", 90), ("max", 100)]:
        v = np.percentile(rs, pct)
        bar = "█" * int(v / max(rs) * 30) if max(rs) > 0 else ""
        print(f"  {label:>8} : {v:>10.1f} min   {bar}")

    zero_raw = (rs == 0).sum()
    print(f"\n  Traces with raw span = 0  : {zero_raw} "
          f"({zero_raw/len(agg)*100:.1f}%)  ← single-event traces")

    print("\n" + "=" * 56)
    print("  WORKING-HOURS DURATION")
    print("=" * 56)
    for label, pct in [("min", 0), ("p10", 10), ("p25", 25),
                        ("median", 50), ("p75", 75), ("p90", 90), ("max", 100)]:
        v = np.percentile(wh, pct)
        bar = "█" * int(v / max(wh) * 30) if max(wh) > 0 else ""
        print(f"  {label:>8} : {v:>10.4f} h     {bar}")

    zero_wh = (wh == 0).sum()
    print(f"\n  Traces with work_hours = 0 : {zero_wh} "
          f"({zero_wh/len(agg)*100:.1f}%)")
    print(f"  Possible causes:")
    print(f"    • Single-event traces (first = last timestamp)")
    print(f"    • Both events fall outside the working window")
    print(f"    • Timezone mismatch (events stored in local time, "
          f"filter applied in UTC)")

    # Breakdown by bracket
    print("\n" + "=" * 56)
    print("  WORKING-HOURS DISTRIBUTION BREAKDOWN")
    print("=" * 56)
    brackets = [
        ("= 0 h (instant / outside hours)", wh == 0),
        ("< 30 min",  (wh > 0)  & (wh < 0.5)),
        ("30–60 min", (wh >= 0.5) & (wh < 1.0)),
        ("1–4 h",     (wh >= 1.0) & (wh < 4.0)),
        ("4–8 h",     (wh >= 4.0) & (wh < 8.0)),
        ("> 8 h",     wh >= 8.0),
    ]
    for label, mask in brackets:
        n   = mask.sum()
        pct = n / len(agg) * 100
        bar = "█" * int(pct / 2)
        print(f"  {label:<30} {n:>5}  ({pct:5.1f}%)  {bar}")

    # ── Example traces ───────────────────────────────────────────────────
    print(f"\n" + "=" * 56)
    print(f"  SAMPLE TRACES  (sorted by raw span, showing {args.n})")
    print("=" * 56)
    sample = agg.nlargest(args.n, "raw_span_min")[
        [args.trace_col, "n_events", "first_event_utc",
         "raw_span_min", "work_hours"]
    ].reset_index(drop=True)
    print(sample.to_string(index=False,
                           float_format=lambda x: f"{x:.2f}"))
    print()


if __name__ == "__main__":
    main()