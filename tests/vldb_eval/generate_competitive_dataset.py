"""
tests/eval/generate_competitive_dataset.py

Generate a synthetic event log purpose-built for the competitive (6.4.1)
and expressiveness (6.4.2) evaluation experiments.

Design goals
------------
1. GUARANTEED LONG CHAINS
   A six-activity "spine" (Register → Assess → Approve → Notify → Execute → Archive)
   appears in every trace, possibly with optional detours inserted between spine
   steps.  This guarantees that the k=2..6 length sweep always returns non-zero
   results without any pair_coverage discovery step.

2. CONTROLLED ATTRIBUTE DENSITY
   Each activity has a biased attribute distribution: 80% of events for activity A
   are handled by resources in A's preferred pool.  This ensures that
   A[resource="R01"] B returns a predictable, substantial number of results —
   not zero (too sparse) and not trivially equal to all traces (too dense).

   Target: each (activity, resource_value) combination covers ~10–25% of that
   activity's occurrences, giving attribute-constrained queries ~10–25% of the
   structural match count.

3. CONTROLLED PERSPECTIVE GROUP DENSITY
   Each perspective value (resource, department) appears in a controlled number
   of traces.  The per-group pair coverage is designed so that:
     - group_count ≥ MIN_GROUP_SIZE (enough for meaningful adaptive warm latency)
     - No single group is so large that CEP validation dominates
   This is achieved by assigning resources in a round-robin / balanced fashion
   rather than uniformly at random.

4. SCALE FOR TIMING CONTRAST
   Default 100K traces → ~550K events.
   At this scale:
     - SIESTA (eager index): sub-second for all queries
     - MATCH_RECOGNIZE: 30–120s per query (full partition scan)
     - ELK: ~5ms per query (term filter)
   This is the contrast you want in the paper.

Schema
------
  trace_id    — T000001 … T{N_TRACES}
  activity    — from the 10-activity alphabet (spine + optional)
  timestamp   — ISO 8601, spanning 90 days
  resource    — event-level, 10 values (dense bias per activity)
  department  — event-level, 25 values (moderate bias per resource)
  lifecycle   — event-level, 3 values: start / in_progress / complete
  cost        — numeric (gets filtered by discover_schema)
  priority    — case-level (gets filtered by discover_schema)

Perspective selection
---------------------
  resource   → selected (card=10, event-level, avg_dpt >> 1.2)
  department → selected (card=25, event-level, avg_dpt >> 1.2)
  lifecycle  → borderline (card=3, may be selected)
  cost       → filtered (numeric)
  priority   → filtered (case-level)

Usage
-----
  python generate_competitive_dataset.py --n-traces 100000 --stats
  python generate_competitive_dataset.py --n-traces 100000 -o datasets/synthetic_competitive.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path


# ═══════════════════════════════════════════════════════════════════════════
# Controlled process model
# ═══════════════════════════════════════════════════════════════════════════

# The SPINE is guaranteed to appear in every trace, in order.
# This ensures that the 6-activity length sweep chain is ALWAYS non-zero.
SPINE = [
    "Register",   # A0
    "Assess",     # A1
    "Approve",    # A2
    "Notify",     # A3
    "Execute",    # A4
    "Archive",    # A5
]

# OPTIONAL activities inserted at specific positions in the spine.
# These add variety without breaking the spine chain guarantee.
# Each entry: (position_after_spine_index, activity, probability)
OPTIONAL_ACTIVITIES = [
    (1, "Escalate",  0.20),   # after Assess, 20% of traces
    (1, "Reject",    0.15),   # after Assess (sometimes both Escalate+Reject)
    (2, "Review",    0.25),   # after Approve, 25% of traces
    (4, "Validate",  0.30),   # after Execute, 30% of traces
]

# All unique activities (spine + optional)
ALL_ACTIVITIES = SPINE + ["Escalate", "Reject", "Review", "Validate"]

# ── Attribute pools ──────────────────────────────────────────────────────

# 10 resources — card=10, enough for multiple per-resource groups
RESOURCES = [f"R{i:02d}" for i in range(1, 11)]

# 25 departments — card=25
DEPARTMENTS = [f"Dept_{chr(65 + i // 2)}{i % 2 + 1}" for i in range(25)]

# 3 lifecycle values — card=3, borderline perspective
LIFECYCLES = ["start", "in_progress", "complete"]

# Cost range — numeric, filtered by discover_schema
COST_RANGE = (10.0, 1000.0)

# Priority — case-level (4 values), filtered by DPT check
PRIORITIES = ["low", "medium", "high", "critical"]

# ── Activity → resource bias ─────────────────────────────────────────────
# Each activity has 2 preferred resources (80% probability).
# This creates controlled, substantial attribute-filtered pair results.

def _build_activity_resource_bias() -> dict[str, list[str]]:
    bias = {}
    for i, act in enumerate(ALL_ACTIVITIES):
        # Two preferred resources, offset so each resource covers 2-3 activities
        r1 = RESOURCES[i % len(RESOURCES)]
        r2 = RESOURCES[(i + 2) % len(RESOURCES)]
        bias[act] = [r1, r2]
    return bias

ACT_RESOURCE_BIAS = _build_activity_resource_bias()

# ── Resource → department bias ───────────────────────────────────────────
RES_DEPT_BIAS = {
    r: [DEPARTMENTS[i * 2 % len(DEPARTMENTS)],
        DEPARTMENTS[(i * 2 + 1) % len(DEPARTMENTS)]]
    for i, r in enumerate(RESOURCES)
}

# ── Scale ────────────────────────────────────────────────────────────────
DEFAULT_N_TRACES    = 100_000
TIME_ORIGIN         = datetime(2024, 1, 1, 8, 0, 0)
TIME_SPAN_DAYS      = 90
INTER_EVENT_MINUTES = (1, 60)

# Target: each resource value covers approximately this fraction of its
# activity's events.  With 2 preferred resources at 80%, each covers ~40%.
TARGET_ATTR_COVERAGE = 0.40   # documented, not enforced algorithmically


# ═══════════════════════════════════════════════════════════════════════════
# Generator
# ═══════════════════════════════════════════════════════════════════════════

def generate_trace(
    trace_id: str,
    rng: random.Random,
    trace_start: datetime,
) -> list[dict]:
    """
    Generate one trace: always contains the full spine, with optional
    activities inserted at controlled positions.

    Returns a list of event dicts.
    """
    # Build the activity sequence
    activities: list[str] = list(SPINE)

    # Insert optional activities (in reverse position order to preserve indices)
    inserts: list[tuple[int, str]] = []
    for spine_idx, opt_act, prob in OPTIONAL_ACTIVITIES:
        if rng.random() < prob:
            # Insert after spine position spine_idx (account for 0-based index)
            inserts.append((spine_idx + 1, opt_act))

    # Apply inserts in reverse order to not shift subsequent indices
    for insert_pos, opt_act in sorted(inserts, key=lambda x: -x[0]):
        activities.insert(insert_pos, opt_act)

    # Assign case-level attributes
    priority = rng.choice(PRIORITIES)

    # Generate events
    events: list[dict] = []
    current_ts = trace_start

    for event_idx, activity in enumerate(activities):
        if event_idx > 0:
            gap = rng.uniform(*INTER_EVENT_MINUTES)
            current_ts += timedelta(minutes=gap)

        # Resource: 80% probability of picking from the preferred pool
        if rng.random() < 0.80:
            resource = rng.choice(ACT_RESOURCE_BIAS[activity])
        else:
            resource = rng.choice(RESOURCES)

        # Department: 70% probability of picking from resource's preferred pool
        if rng.random() < 0.70:
            department = rng.choice(RES_DEPT_BIAS[resource])
        else:
            department = rng.choice(DEPARTMENTS)

        # Lifecycle: weighted toward complete (most events are completed steps)
        lifecycle = rng.choices(
            LIFECYCLES,
            weights=[0.15, 0.25, 0.60],
            k=1
        )[0]

        cost = round(rng.uniform(*COST_RANGE), 2)

        events.append({
            "trace_id":   trace_id,
            "activity":   activity,
            "timestamp":  current_ts.strftime("%Y-%m-%dT%H:%M:%S"),
            "resource":   resource,
            "department": department,
            "lifecycle":  lifecycle,
            "cost":       str(cost),
            "priority":   priority,
        })

    return events


def generate_events(n_traces: int, seed: int) -> list[dict]:
    rng = random.Random(seed)
    all_events: list[dict] = []

    for i in range(n_traces):
        trace_id = f"T{i + 1:06d}"
        trace_start = TIME_ORIGIN + timedelta(
            seconds=rng.uniform(0, TIME_SPAN_DAYS * 86400)
        )
        events = generate_trace(trace_id, rng, trace_start)
        all_events.extend(events)

    return all_events


FIELDNAMES = [
    "trace_id", "activity", "timestamp",
    "resource", "department", "lifecycle",
    "cost", "priority",
]


def write_csv(events: list[dict], path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(events)


# ═══════════════════════════════════════════════════════════════════════════
# Stats and validation
# ═══════════════════════════════════════════════════════════════════════════

def compute_stats(events: list[dict]) -> dict:
    from collections import Counter

    act_counts: dict[str, int] = Counter(ev["activity"] for ev in events)

    attr_keys = ["resource", "department", "lifecycle", "cost", "priority"]
    distinct:       dict[str, set] = {k: set() for k in attr_keys}
    numeric_count:  dict[str, int] = defaultdict(int)
    total_count:    dict[str, int] = defaultdict(int)
    trace_events:   dict[str, list[dict]] = defaultdict(list)

    for ev in events:
        trace_events[ev["trace_id"]].append(ev)
        for k in attr_keys:
            v = ev.get(k, "")
            distinct[k].add(v)
            total_count[k] += 1
            try:
                float(v); numeric_count[k] += 1
            except (ValueError, TypeError):
                pass

    n_traces = len(trace_events)
    dpt_sums: dict[str, float] = defaultdict(float)
    for tevs in trace_events.values():
        for k in attr_keys:
            dpt_sums[k] += len({ev.get(k) for ev in tevs})

    avg_dpt = {k: dpt_sums[k] / n_traces for k in attr_keys}
    numeric_keys = {
        k for k in attr_keys
        if total_count[k] and numeric_count[k] / total_count[k] > 0.95
    }
    blocked = {"trace_id", "timestamp", "activity", "position"}
    perspectives = []
    filtered_out = {}
    for k in attr_keys:
        card = len(distinct[k])
        if k in numeric_keys:
            filtered_out[k] = "numeric"
        elif not (3 <= card <= 500):
            filtered_out[k] = f"cardinality={card} outside [3,500]"
        elif avg_dpt[k] <= 1.2:
            filtered_out[k] = f"case-level (avg_dpt={avg_dpt[k]:.3f})"
        else:
            perspectives.append((k, card))

    # Attribute density: for each (activity, attr_value) how many traces?
    attr_density: dict[str, dict[str, int]] = {}
    for k in ["resource", "department"]:
        pairs: dict[str, dict[str, set]] = defaultdict(lambda: defaultdict(set))
        for ev in events:
            pairs[ev["activity"]][ev.get(k, "")].add(ev["trace_id"])
        attr_density[k] = {
            f"{act}[{k}={val}]": len(tids)
            for act, val_map in pairs.items()
            for val, tids in val_map.items()
        }

    # Verify spine co-occurrence
    spine_coverage: dict[str, int] = {}
    for i in range(len(SPINE) - 1):
        a, b = SPINE[i], SPINE[i + 1]
        count = sum(
            1 for tevs in trace_events.values()
            if any(e["activity"] == a for e in tevs)
            and any(e["activity"] == b for e in tevs)
        )
        spine_coverage[f"{a}→{b}"] = count

    return {
        "n_events": len(events),
        "n_traces": n_traces,
        "activities": dict(act_counts),
        "n_activities": len(act_counts),
        "avg_events_per_trace": round(len(events) / n_traces, 1),
        "attribute_cardinalities": {k: len(v) for k, v in distinct.items()},
        "avg_distinct_per_trace": {k: round(v, 3) for k, v in avg_dpt.items()},
        "predicted_perspectives": [{"key": k, "cardinality": c} for k, c in perspectives],
        "filtered_out": filtered_out,
        "spine_pair_coverage": spine_coverage,
        "sample_attr_density": {
            k: dict(sorted(v.items(), key=lambda x: -x[1])[:5])
            for k, v in attr_density.items()
        },
    }


def print_stats(stats: dict) -> None:
    print(f"\n{'='*62}")
    print("COMPETITIVE DATASET STATISTICS")
    print(f"{'='*62}")
    print(f"Events:          {stats['n_events']:,}")
    print(f"Traces:          {stats['n_traces']:,}")
    print(f"Avg events/trace:{stats['avg_events_per_trace']}")
    print(f"Activities ({stats['n_activities']}): {list(stats['activities'].keys())}")

    print(f"\nActivity distribution:")
    total = stats["n_events"]
    for act, cnt in sorted(stats["activities"].items(), key=lambda x: -x[1]):
        print(f"  {act:12s}  {cnt:8,}  ({100*cnt/total:5.1f}%)")

    print(f"\nAttribute cardinalities and avg distinct/trace:")
    for k, c in stats["attribute_cardinalities"].items():
        dpt = stats["avg_distinct_per_trace"][k]
        status = "✓ perspective" if any(
            p["key"] == k for p in stats["predicted_perspectives"]
        ) else "✗ filtered"
        print(f"  {k:12s}  card={c:3d}  avg_dpt={dpt:.3f}  {status}")

    print(f"\nSpine pair co-occurrence (all should be ~n_traces):")
    for pair, count in stats["spine_pair_coverage"].items():
        pct = 100 * count / stats["n_traces"]
        ok = "✓" if pct > 95 else "⚠"
        print(f"  {ok} {pair:25s}  {count:8,}  ({pct:.1f}% of traces)")

    print(f"\nTop attribute densities (traces with that activity+attr_value):")
    for k, top in stats["sample_attr_density"].items():
        print(f"  {k}:")
        for combo, cnt in top.items():
            pct = 100 * cnt / stats["n_traces"]
            print(f"    {combo:35s}  {cnt:8,}  ({pct:.1f}%)")

    print(f"\nPredicted perspectives: "
          f"{[p['key'] for p in stats['predicted_perspectives']]}")

    print(f"\nKEY EXPERIMENT PROPERTIES:")
    print(f"  Length sweep k=2..6: guaranteed non-zero (spine co-occurrence > 95%)")
    print(f"  Attribute constraints: ~{TARGET_ATTR_COVERAGE*100:.0f}% of structural matches")
    print(f"  MR scan target: ~{stats['n_events']:,} events across "
          f"{stats['n_traces']:,} partitions")
    print(f"  Expected MR latency: {'30–90s' if stats['n_traces'] >= 50_000 else '< 5s (too small)'}")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Generate a controlled synthetic event log for the "
                    "competitive (6.4.1) and expressiveness (6.4.2) experiments.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Spine guarantee:
  Register → Assess → Approve → Notify → Execute → Archive
  appears in 100% of traces.  The k=2..6 length sweep is always non-zero.

Attribute control:
  Each activity uses 2 preferred resources (80% probability), giving
  ~40% coverage per (activity, resource) combination.

Scale recommendation:
  --n-traces 100000  →  ~550K events  (MR: 30–90s, SIESTA: < 1s)
  --n-traces  50000  →  ~275K events  (MR: 15–45s, SIESTA: < 1s)
""",
    )
    ap.add_argument("--output", "-o", type=Path,
                    default=Path("datasets/synthetic_competitive.csv"))
    ap.add_argument("--n-traces", "-n", type=int, default=DEFAULT_N_TRACES)
    ap.add_argument("--seed", "-s", type=int, default=42)
    ap.add_argument("--stats", action="store_true",
                    help="Print statistics after generation.")
    ap.add_argument("--stats-only", action="store_true",
                    help="Compute and print stats without writing the CSV.")
    ap.add_argument("--stats-json", type=Path, default=None)
    args = ap.parse_args()

    print(f"Generating competitive synthetic dataset: "
          f"{args.n_traces:,} traces, seed={args.seed}")
    events = generate_events(args.n_traces, args.seed)
    print(f"  Generated {len(events):,} events")

    if not args.stats_only:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        write_csv(events, args.output)
        print(f"  Written to {args.output}")

    if args.stats or args.stats_only or args.stats_json:
        print("  Computing statistics ...")
        stats = compute_stats(events)
        print_stats(stats)
        if args.stats_json:
            args.stats_json.write_text(json.dumps(stats, indent=2, default=str))
            print(f"\n  Stats written to {args.stats_json}")


if __name__ == "__main__":
    main()