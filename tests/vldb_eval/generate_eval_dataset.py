"""
generate_eval_dataset.py

Generate a synthetic event log (CSV) designed to satisfy the distribution
requirements of ALL evaluation experiments in the SIESTA adaptive-indexing
paper (6.3.1 – 6.4).

Motivation
----------
Real process-mining datasets (BPIC 2017, BPIC 2020, etc.) have uncontrolled
attribute-value distributions that cause problems:

  - Too few distinct perspective groups → warm-up (6.3.1) skips perspectives
    because group_count < min_perspective_cardinality.
  - Too few distinct cold-pair templates relative to n_cold queries →
    maintenance (6.3.2) silently promotes cold pairs, blurring the signal.
  - Case-level metadata that passes the cardinality check but has
    avg_distinct_per_trace ≤ 1.2 → wrongly selected as perspectives.
  - Too few activities → insufficient pair/triple permutations for workloads.
  - Giant co-occurrence groups → 30-minute build times for certain pairs.

This generator produces a CSV log with *designed* properties:

  - N_ACTIVITIES activities with controlled co-occurrence structure.
  - PERSPECTIVE attributes that pass the discover_schema filter:
      • string-typed, non-numeric
      • cardinality in [cardinality_floor, cardinality_cap]
      • event-level (avg distinct values per trace >> 1.2)
      • sorted by cardinality ascending (3 perspectives at low/mid/high card.)
  - A CASE-LEVEL attribute that should be *rejected* by the DPT filter.
  - Enough traces and events per perspective group for:
      • pair_coverage to return ≥ 13 co-occurring pairs per perspective
      • trace_sample splitting into 5 balanced batches
      • stratified sampling to produce every perspective value in every batch
  - Bounded group sizes to avoid 30-minute pair builds.
  - Multiple attribute values per key for inline-constraint queries (6.4).

Schema
------
  trace_id      — case identifier (T0001 … T{N_TRACES})
  activity      — activity label from the controlled alphabet
  timestamp     — ISO 8601 timestamps spanning ~30 days
  resource      — event-level perspective, low cardinality (~8 values)
  department    — event-level perspective, medium cardinality (~20 values)
  region        — event-level perspective, higher cardinality (~50 values)
  priority      — case-level attribute (constant per trace), should be
                  filtered out by the DPT check
  cost          — numeric attribute, should be filtered out as numeric
  lifecycle     — event-level string attribute with few values (useful for
                  attribute-aware queries but not as a perspective due to
                  cardinality < floor)

Usage
-----
    python generate_eval_dataset.py [--output PATH] [--n-traces N]
                                     [--seed S] [--stats]

    # Quick check that discover_schema picks the right perspectives:
    python generate_eval_dataset.py --stats
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path


# ═══════════════════════════════════════════════════════════════════════════
# Tunable parameters
# ═══════════════════════════════════════════════════════════════════════════

# Activities — enough for rich pair/triple permutations but not so many
# that co-occurrence becomes sparse.  8 activities → 56 ordered pairs
# → 336 ordered triples.  Labels are simple identifiers (no quoting needed).
ACTIVITIES = [
    "Register",
    "Review",
    "Approve",
    "Reject",
    "Notify",
    "Escalate",
    "Complete",
    "Archive",
]

# Process templates: each is a possible trace (sequence of activities).
# Multiple templates ensure that different activity pairs co-occur in the
# data, giving pair_coverage rich results.  Templates are weighted so that
# some pairs are naturally "hot" (high co-occurrence) and others "cold".
PROCESS_TEMPLATES = [
    # Happy path — most common
    {"weight": 40, "seq": ["Register", "Review", "Approve", "Notify", "Complete", "Archive"]},
    # Rejection path
    {"weight": 20, "seq": ["Register", "Review", "Reject", "Notify", "Archive"]},
    # Escalation path
    {"weight": 15, "seq": ["Register", "Review", "Escalate", "Approve", "Notify", "Complete", "Archive"]},
    # Quick complete
    {"weight": 10, "seq": ["Register", "Approve", "Complete"]},
    # Double review
    {"weight": 8, "seq": ["Register", "Review", "Review", "Approve", "Complete", "Archive"]},
    # Reject then re-register
    {"weight": 7, "seq": ["Register", "Review", "Reject", "Register", "Review", "Approve", "Complete"]},
]

# ── Perspective definitions ──────────────────────────────────────────────
# Each perspective is event-level (assigned per-event, varies within traces).
# Cardinalities are chosen to land in different bands within [3, 500].

RESOURCES = [f"R{i:02d}" for i in range(1, 9)]         # card=8  (low)
DEPARTMENTS = [f"Dept_{chr(65+i)}" for i in range(20)]  # card=20 (mid)
REGIONS = [f"Region_{i:02d}" for i in range(1, 51)]     # card=50 (high)

# Lifecycle values — low cardinality (2), below the floor of 3.
# Should NOT be selected as a perspective.
LIFECYCLES = ["start", "complete"]

# Cost — numeric, should be filtered out by the numeric check.
COST_RANGE = (10.0, 500.0)

# Priority — CASE-LEVEL attribute (constant per trace).
# Should be filtered out by the DPT check (avg_distinct_per_trace ≈ 1.0).
PRIORITIES = ["low", "medium", "high", "critical"]

# ── Scale parameters ─────────────────────────────────────────────────────
DEFAULT_N_TRACES = 2000
TIME_ORIGIN = datetime(2025, 1, 1, 8, 0, 0)
TIME_SPAN_DAYS = 30
INTER_EVENT_MINUTES = (1, 120)  # min/max gap between events in a trace


# ═══════════════════════════════════════════════════════════════════════════
# Generator
# ═══════════════════════════════════════════════════════════════════════════

def _pick_weighted(templates: list[dict], rng: random.Random) -> dict:
    weights = [t["weight"] for t in templates]
    return rng.choices(templates, weights=weights, k=1)[0]


def generate_events(
    n_traces: int,
    seed: int,
) -> list[dict]:
    """
    Generate a flat list of event dicts ready for CSV output.

    Each event has: trace_id, activity, timestamp, resource, department,
    region, priority, cost, lifecycle.

    Key design choices:
      - resource/department/region are assigned PER-EVENT (not per-trace)
        so they are genuine event-level attributes with avg_dpt >> 1.2.
      - priority is assigned PER-TRACE (constant within a trace) so it
        has avg_dpt ≈ 1.0 and gets filtered out by discover_schema.
      - resource assignment is biased: each activity has a preferred
        resource pool (2-3 resources) that handles 70% of its events,
        with the remaining 30% distributed across others.  This creates
        natural per-resource co-occurrence clustering — essential for
        pair_coverage to return meaningful group counts.
      - department and region are assigned with moderate clustering per
        resource (each resource has a "home" department and region, but
        with 40% cross-assignment to ensure event-level variation).
    """
    rng = random.Random(seed)
    events: list[dict] = []

    # Build activity-to-preferred-resource mapping for clustering.
    # Each activity is primarily handled by 2-3 resources.
    act_resource_map: dict[str, list[str]] = {}
    for i, act in enumerate(ACTIVITIES):
        primary = RESOURCES[i % len(RESOURCES)]
        secondary = RESOURCES[(i + 1) % len(RESOURCES)]
        tertiary = RESOURCES[(i + 2) % len(RESOURCES)]
        act_resource_map[act] = [primary, secondary, tertiary]

    # Build resource-to-home-department/region for moderate clustering.
    res_dept_map = {r: DEPARTMENTS[i % len(DEPARTMENTS)] for i, r in enumerate(RESOURCES)}
    res_region_map = {r: REGIONS[i % len(REGIONS)] for i, r in enumerate(RESOURCES)}

    for trace_idx in range(n_traces):
        trace_id = f"T{trace_idx + 1:05d}"
        priority = rng.choice(PRIORITIES)

        # Pick a process template.
        template = _pick_weighted(PROCESS_TEMPLATES, rng)
        activities_seq = list(template["seq"])

        # Start time for this trace: distributed across the time span.
        trace_start = TIME_ORIGIN + timedelta(
            seconds=rng.uniform(0, TIME_SPAN_DAYS * 86400)
        )

        current_ts = trace_start
        for event_idx, activity in enumerate(activities_seq):
            # Advance timestamp.
            if event_idx > 0:
                gap_minutes = rng.uniform(*INTER_EVENT_MINUTES)
                current_ts += timedelta(minutes=gap_minutes)

            # Resource — biased per activity.
            if rng.random() < 0.70:
                resource = rng.choice(act_resource_map[activity])
            else:
                resource = rng.choice(RESOURCES)

            # Department — biased per resource.
            if rng.random() < 0.60:
                department = res_dept_map[resource]
            else:
                department = rng.choice(DEPARTMENTS)

            # Region — biased per resource.
            if rng.random() < 0.60:
                region = res_region_map[resource]
            else:
                region = rng.choice(REGIONS)

            # Cost — numeric.
            cost = round(rng.uniform(*COST_RANGE), 2)

            # Lifecycle — event-level but too low cardinality for perspective.
            lifecycle = rng.choice(LIFECYCLES)

            events.append({
                "trace_id":  trace_id,
                "activity":  activity,
                "timestamp": current_ts.strftime("%Y-%m-%dT%H:%M:%S"),
                "resource":  resource,
                "department": department,
                "region":    region,
                "priority":  priority,       # case-level
                "cost":      str(cost),
                "lifecycle": lifecycle,
            })

    return events


def write_csv(events: list[dict], path: Path) -> None:
    fieldnames = [
        "trace_id", "activity", "timestamp",
        "resource", "department", "region",
        "priority", "cost", "lifecycle",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(events)


# ═══════════════════════════════════════════════════════════════════════════
# Statistics / validation
# ═══════════════════════════════════════════════════════════════════════════

def compute_stats(events: list[dict]) -> dict:
    """
    Compute the same statistics that discover_schema would compute,
    and predict which attributes will be selected as perspectives.
    """
    # Activity distribution.
    act_counts: dict[str, int] = defaultdict(int)
    for ev in events:
        act_counts[ev["activity"]] += 1

    # Per-attribute stats.
    attr_keys = ["resource", "department", "region", "priority", "cost", "lifecycle"]
    distinct: dict[str, set] = {k: set() for k in attr_keys}
    numeric_count: dict[str, int] = defaultdict(int)
    total_count: dict[str, int] = defaultdict(int)

    # Per-trace distinct counting (for DPT).
    trace_events: dict[str, list[dict]] = defaultdict(list)
    for ev in events:
        trace_events[ev["trace_id"]].append(ev)
        for k in attr_keys:
            v = ev.get(k, "")
            distinct[k].add(v)
            total_count[k] += 1
            try:
                float(v)
                numeric_count[k] += 1
            except (ValueError, TypeError):
                pass

    # Compute avg distinct per trace (DPT).
    dpt_sums: dict[str, float] = defaultdict(float)
    trace_count = len(trace_events)
    for tid, tevs in trace_events.items():
        for k in attr_keys:
            vals = {ev.get(k) for ev in tevs}
            dpt_sums[k] += len(vals)

    avg_dpt = {k: dpt_sums[k] / trace_count if trace_count else 0 for k in attr_keys}

    # Predict perspective selection.
    numeric_keys = set()
    for k in attr_keys:
        if total_count[k] > 0 and numeric_count[k] / total_count[k] > 0.95:
            numeric_keys.add(k)

    blocked = {"concept:name", "time:timestamp", "activity", "trace_id",
               "timestamp", "position", "case:concept:name", "case_id"}

    perspectives = []
    for k in attr_keys:
        if k in blocked:
            continue
        if k in numeric_keys:
            continue
        card = len(distinct[k])
        if not (3 <= card <= 500):
            continue
        if avg_dpt[k] <= 1.2:
            continue
        perspectives.append((k, card))

    perspectives.sort(key=lambda x: (x[1], x[0]))

    # Co-occurrence pairs per perspective.
    pair_coverage_summary = {}
    for persp_key, _ in perspectives:
        # Group events by perspective value.
        groups: dict[str, list[str]] = defaultdict(list)
        for ev in events:
            gval = ev.get(persp_key, "")
            groups[gval].append(ev["activity"])

        # Count co-occurring pairs.
        pair_groups: dict[tuple[str, str], set[str]] = defaultdict(set)
        for gval, acts in groups.items():
            seen_pairs = set()
            for i in range(len(acts)):
                for j in range(i + 1, len(acts)):
                    pair = (acts[i], acts[j])
                    if pair not in seen_pairs:
                        seen_pairs.add(pair)
                        pair_groups[pair].add(gval)

        # Sort by number of groups descending.
        sorted_pairs = sorted(pair_groups.items(), key=lambda x: -len(x[1]))
        pair_coverage_summary[persp_key] = {
            "group_count": len(groups),
            "total_pairs": len(sorted_pairs),
            "top_5_pairs": [
                {"pair": f"{p[0]}->{p[1]}", "groups": len(gs)}
                for p, gs in sorted_pairs[:5]
            ],
            "bottom_5_pairs": [
                {"pair": f"{p[0]}->{p[1]}", "groups": len(gs)}
                for p, gs in sorted_pairs[-5:]
            ] if len(sorted_pairs) > 5 else [],
        }

    return {
        "n_events": len(events),
        "n_traces": trace_count,
        "activities": dict(act_counts),
        "n_activities": len(act_counts),
        "attribute_cardinalities": {k: len(v) for k, v in distinct.items()},
        "numeric_keys": list(numeric_keys),
        "avg_distinct_per_trace": {k: round(v, 3) for k, v in avg_dpt.items()},
        "predicted_perspectives": [
            {"key": k, "cardinality": c} for k, c in perspectives
        ],
        "filtered_out": {
            k: _filter_reason(k, distinct, numeric_keys, avg_dpt, blocked)
            for k in attr_keys
            if (k, len(distinct[k])) not in perspectives
        },
        "pair_coverage": pair_coverage_summary,
    }


def _filter_reason(
    k: str,
    distinct: dict[str, set],
    numeric_keys: set,
    avg_dpt: dict[str, float],
    blocked: set,
) -> str:
    if k in blocked:
        return "blocked key"
    if k in numeric_keys:
        return "numeric (>95% parseable as float)"
    card = len(distinct[k])
    if card < 3:
        return f"cardinality too low ({card} < 3)"
    if card > 500:
        return f"cardinality too high ({card} > 500)"
    if avg_dpt[k] <= 1.2:
        return f"case-level (avg_dpt={avg_dpt[k]:.3f} ≤ 1.2)"
    return "unknown"


def validate_for_experiments(stats: dict) -> list[str]:
    """
    Check that the generated dataset satisfies all experiment requirements.
    Returns a list of warnings (empty = all good).
    """
    warnings: list[str] = []

    perspectives = stats["predicted_perspectives"]
    if len(perspectives) < 1:
        warnings.append("CRITICAL: No perspectives selected — all experiments will fail.")
    if len(perspectives) < 2:
        warnings.append("WARNING: Fewer than 2 perspectives — some experiments use max_perspectives=4.")

    for p in perspectives:
        pc = stats["pair_coverage"].get(p["key"], {})
        group_count = pc.get("group_count", 0)
        total_pairs = pc.get("total_pairs", 0)

        if group_count < 5:
            warnings.append(
                f"WARNING [{p['key']}]: group_count={group_count} < 5 "
                f"(min_perspective_cardinality for warm-up)."
            )
        if total_pairs < 13:
            warnings.append(
                f"WARNING [{p['key']}]: only {total_pairs} co-occurring pairs "
                f"(warm-up needs ≥ 13 = n_hot=5 + n_cold=8)."
            )
        if total_pairs < 20:
            warnings.append(
                f"INFO [{p['key']}]: only {total_pairs} co-occurring pairs "
                f"(maintenance cold-pair blurring risk if < 20)."
            )

    # Check that 'priority' was correctly filtered out (case-level).
    if "priority" not in stats["filtered_out"]:
        warnings.append(
            "WARNING: 'priority' was NOT filtered out — it should be case-level."
        )

    # Check that 'cost' was correctly filtered out (numeric).
    if "cost" not in stats["filtered_out"]:
        warnings.append(
            "WARNING: 'cost' was NOT filtered out — it should be numeric."
        )

    # Check that 'lifecycle' was correctly filtered out (cardinality < 3).
    if "lifecycle" not in stats["filtered_out"]:
        warnings.append(
            "WARNING: 'lifecycle' was NOT filtered out — cardinality should be < 3."
        )

    # Check enough activities for experiments.
    n_act = stats["n_activities"]
    if n_act < 2:
        warnings.append("CRITICAL: fewer than 2 activities.")
    if n_act < 4:
        warnings.append("WARNING: fewer than 4 activities — L2 experiment uses up to 4.")
    if n_act < 6:
        warnings.append("WARNING: fewer than 6 activities — retention/external use up to 6.")

    # Check enough traces for 5-batch splitting.
    n_traces = stats["n_traces"]
    if n_traces < 50:
        warnings.append(
            f"WARNING: only {n_traces} traces — trace_sample into 5 batches "
            f"will produce very small batches."
        )

    return warnings


# ═══════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Generate a synthetic event log for SIESTA evaluation.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split("Usage")[1] if "Usage" in (__doc__ or "") else "",
    )
    ap.add_argument(
        "--output", "-o", type=Path, default=Path("eval_synthetic.csv"),
        help="Output CSV path (default: eval_synthetic.csv).",
    )
    ap.add_argument(
        "--n-traces", "-n", type=int, default=DEFAULT_N_TRACES,
        help=f"Number of traces to generate (default: {DEFAULT_N_TRACES}).",
    )
    ap.add_argument(
        "--seed", "-s", type=int, default=42,
        help="Random seed (default: 42).",
    )
    ap.add_argument(
        "--stats", action="store_true",
        help="Print dataset statistics and validation after generation.",
    )
    ap.add_argument(
        "--stats-only", action="store_true",
        help="Print statistics without writing the CSV (still generates in memory).",
    )
    ap.add_argument(
        "--stats-json", type=Path, default=None,
        help="Write statistics to a JSON file.",
    )
    args = ap.parse_args()

    print(f"Generating synthetic event log: {args.n_traces} traces, seed={args.seed}")
    events = generate_events(args.n_traces, args.seed)
    print(f"  Generated {len(events)} events across {args.n_traces} traces")

    if not args.stats_only:
        write_csv(events, args.output)
        print(f"  Written to {args.output}")

    if args.stats or args.stats_only or args.stats_json:
        stats = compute_stats(events)
        warnings = validate_for_experiments(stats)

        print(f"\n{'='*60}")
        print("DATASET STATISTICS")
        print(f"{'='*60}")
        print(f"Events:     {stats['n_events']}")
        print(f"Traces:     {stats['n_traces']}")
        print(f"Activities: {stats['n_activities']}  {list(stats['activities'].keys())}")
        print(f"\nActivity distribution:")
        for act, cnt in sorted(stats["activities"].items(), key=lambda x: -x[1]):
            pct = 100 * cnt / stats["n_events"]
            print(f"  {act:20s}  {cnt:5d}  ({pct:5.1f}%)")

        print(f"\nAttribute cardinalities:")
        for k, c in stats["attribute_cardinalities"].items():
            print(f"  {k:15s}  cardinality={c:4d}  "
                  f"avg_dpt={stats['avg_distinct_per_trace'][k]:.3f}  "
                  f"{'NUMERIC' if k in stats['numeric_keys'] else ''}")

        print(f"\nPredicted perspectives (discover_schema):")
        for p in stats["predicted_perspectives"]:
            print(f"  ✓ {p['key']:15s}  cardinality={p['cardinality']}")
        if stats["filtered_out"]:
            print(f"\nFiltered out:")
            for k, reason in stats["filtered_out"].items():
                print(f"  ✗ {k:15s}  {reason}")

        print(f"\nPair coverage per perspective:")
        for persp, pc in stats["pair_coverage"].items():
            print(f"\n  {persp}:")
            print(f"    group_count:  {pc['group_count']}")
            print(f"    total_pairs:  {pc['total_pairs']}")
            if pc["top_5_pairs"]:
                print(f"    top-5 (hot):  ", end="")
                print("  ".join(
                    f"{p['pair']}({p['groups']}g)"
                    for p in pc["top_5_pairs"]
                ))
            if pc["bottom_5_pairs"]:
                print(f"    bottom-5 (cold): ", end="")
                print("  ".join(
                    f"{p['pair']}({p['groups']}g)"
                    for p in pc["bottom_5_pairs"]
                ))

        print(f"\n{'='*60}")
        print("VALIDATION")
        print(f"{'='*60}")
        if warnings:
            for w in warnings:
                print(f"  {w}")
        else:
            print("  ✓ All experiment requirements satisfied.")

        if args.stats_json:
            stats["validation_warnings"] = warnings
            args.stats_json.write_text(json.dumps(stats, indent=2))
            print(f"\n  Stats written to {args.stats_json}")


if __name__ == "__main__":
    main()