"""
Synthetic pattern-query generator for the evaluation harness.

Use cases
---------
1. Construct workloads with controlled skew for the maintenance experiment.
2. Enlarge the workloads beyond the standard sets when running on real
   datasets where the activity space is larger.
3. Generate retention-sensitivity workloads with explicit demand shifts.

The generator produces queries in the same dict schema as workload.py
so they can be fed directly to the experiment scripts.

Constraint encoding
-------------------
Attribute predicates are embedded inline in the SeQL pattern using
bracket notation, e.g. ``A[cost="5.0"] B`` or ``A[cost=$1] B[cost=$1]``
for cross-activity equality.  There is no separate `constraints` field.

CLI usage
---------
    python -m tests.eval.query_generator --type skewed --count 100 \\
        --hot-ratio 0.8 --dataset datasets/test.xes --out workload.json

Activities, perspectives, and attribute values are discovered from the
configured dataset (CSV or XES) by default.  Override with
``--activities``, ``--perspectives``, ``--attributes``.
"""

from __future__ import annotations

import argparse
import itertools
import json
import random
import sys
from pathlib import Path
from typing import Sequence

from tests.eval.eval_common import (
    DatasetSchema,
    discover_schema,
    resolve_dataset,
)


# ---------------------------------------------------------------------------
# Pattern templates
# ---------------------------------------------------------------------------

def _two_activity_patterns(activities: Sequence[str]) -> list[str]:
    """All ordered pairs as two-activity patterns."""
    return [f"{a} {b}" for a, b in itertools.permutations(activities, 2)]


def _three_activity_patterns(activities: Sequence[str]) -> list[str]:
    return [
        f"{a} {b} {c}"
        for a, b, c in itertools.permutations(activities, 3)
    ]


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------

def make_query(
    qid: str,
    pattern: str,
    grouping_keys: list[str],
    log_name: str = "test",
    tags: list[str] | None = None,
    category: str | None = None,
) -> dict:
    if category is None:
        category = "attribute_aware" if "[" in pattern else "structural"
    return {
        "id":            qid,
        "log_name":      log_name,
        "pattern":       pattern,
        "grouping_keys": grouping_keys,
        "category":      category,
        "tags":          tags or [],
    }


def gen_skewed(
    *,
    activities: Sequence[str],
    perspectives: Sequence[Sequence[str]],
    n_queries: int,
    hot_ratio: float,
    n_hot_combos: int = 4,
    seed: int = 42,
    log_name: str = "test",
) -> list[dict]:
    """
    Build a skewed workload by sampling from a small hot set with
    probability `hot_ratio` and a larger cold set otherwise.
    """
    rng = random.Random(seed)

    all_combos: list[tuple[Sequence[str], str]] = [
        (gk, p) for gk in perspectives for p in _two_activity_patterns(activities)
    ] + [
        (gk, p) for gk in perspectives for p in _three_activity_patterns(activities)
    ]
    rng.shuffle(all_combos)

    hot  = all_combos[:n_hot_combos]
    cold = all_combos[n_hot_combos:] or hot

    queries: list[dict] = []
    for i in range(n_queries):
        is_hot = rng.random() < hot_ratio
        gk, pat = rng.choice(hot if is_hot else cold)
        queries.append(make_query(
            qid=f"{'H' if is_hot else 'C'}{i}",
            pattern=pat,
            grouping_keys=list(gk),
            log_name=log_name,
            tags=["hot"] if is_hot else ["cold"],
        ))
    return queries


def gen_uniform(
    *,
    activities: Sequence[str],
    perspectives: Sequence[Sequence[str]],
    n_queries: int,
    seed: int = 42,
    log_name: str = "test",
) -> list[dict]:
    rng = random.Random(seed)
    all_combos = [
        (gk, p) for gk in perspectives for p in _two_activity_patterns(activities)
    ] + [
        (gk, p) for gk in perspectives for p in _three_activity_patterns(activities)
    ]
    queries = []
    for i in range(n_queries):
        gk, pat = rng.choice(all_combos)
        queries.append(make_query(
            qid=f"U{i}",
            pattern=pat,
            grouping_keys=list(gk),
            log_name=log_name,
            tags=["uniform"],
        ))
    return queries


def gen_demand_shift(
    *,
    activities: Sequence[str],
    perspectives: Sequence[Sequence[str]],
    n_queries: int,
    shift_at: float = 0.5,
    seed: int = 42,
    log_name: str = "test",
) -> list[dict]:
    """
    Hysteresis ablation workload.

    First half of the workload queries the "popular" pair set;
    after `shift_at` (fraction of total), demand abruptly switches to
    a different pair set.  Built from the supplied activity list so it
    works on any dataset.
    """
    rng = random.Random(seed)
    persp = list(perspectives[0]) if perspectives else []
    pairs = _two_activity_patterns(activities)
    if not pairs:
        return []

    half = max(1, len(pairs) // 2)
    pairs_phase1 = [(persp, p) for p in pairs[:half]]
    pairs_phase2 = [(persp, p) for p in pairs[half:]] or pairs_phase1

    cutoff = int(n_queries * shift_at)
    queries = []
    for i in range(n_queries):
        gk, pat = rng.choice(pairs_phase1 if i < cutoff else pairs_phase2)
        queries.append(make_query(
            qid=f"D{i}",
            pattern=pat,
            grouping_keys=list(gk),
            log_name=log_name,
            tags=["phase1" if i < cutoff else "phase2"],
        ))
    return queries


def gen_attribute_aware(
    *,
    activities: Sequence[str],
    perspectives: Sequence[Sequence[str]],
    attribute_values: dict[str, list[str]],
    n_queries: int,
    seed: int = 42,
    log_name: str = "test",
) -> list[dict]:
    """
    Generate queries with inline-bracket attribute constraints.

    Half use single-event equality (e.g. ``A[cost="5.0"] B``),
    half use cross-activity equality via variable bindings
    (``A[cost=$1] B[cost=$1]``).
    """
    rng = random.Random(seed)
    pair_patterns = list(itertools.permutations(activities, 2))
    available_attrs = [a for a, vals in attribute_values.items() if vals]
    if not pair_patterns or not available_attrs or not perspectives:
        return []

    queries: list[dict] = []
    for i in range(n_queries):
        a, b = rng.choice(pair_patterns)
        gk   = list(rng.choice(list(perspectives)))
        attr = rng.choice(available_attrs)
        if i % 2 == 0:
            value = rng.choice(attribute_values[attr])
            value_safe = value.replace('"', '\\"')
            pat = f'{a}[{attr}="{value_safe}"] {b}'
        else:
            pat = f'{a}[{attr}=$1] {b}[{attr}=$1]'
        queries.append(make_query(
            qid=f"AA{i}",
            pattern=pat,
            grouping_keys=gk,
            log_name=log_name,
            category="attribute_aware",
        ))
    return queries


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _resolve_inputs(args: argparse.Namespace) -> tuple[
    list[str], list[list[str]], dict[str, list[str]], str
]:
    """
    Resolve activities / perspectives / attribute values either from
    explicit CLI args or by discovering them from the configured dataset.
    """
    spec = resolve_dataset(args.dataset, args.log_name)
    schema: DatasetSchema | None = None

    if args.activities:
        activities = args.activities.split(",")
    else:
        schema = schema or discover_schema(spec.path)
        activities = schema.activities[:6]

    if args.perspectives:
        perspectives = [spec.split("+") for spec in args.perspectives.split(",")]
    else:
        schema = schema or discover_schema(spec.path)
        perspectives = (
            [[k] for k in schema.perspective_keys[:2]] or [[]]
        )

    if args.attributes:
        attr_keys = args.attributes.split(",")
        schema = schema or discover_schema(spec.path)
        attribute_values = {
            k: schema.attribute_values.get(k, []) for k in attr_keys
        }
    else:
        schema = schema or discover_schema(spec.path)
        attribute_values = schema.attribute_values

    return activities, perspectives, attribute_values, spec.log_name


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate synthetic query workloads.")
    ap.add_argument("--type", choices=["skewed", "uniform", "shift", "attribute"],
                    required=True)
    ap.add_argument("--count", type=int, default=100)
    ap.add_argument("--hot-ratio", type=float, default=0.8,
                    help="For --type skewed: fraction of queries hitting hot combos.")
    ap.add_argument("--n-hot", type=int, default=4,
                    help="For --type skewed: number of hot combinations.")
    ap.add_argument("--shift-at", type=float, default=0.5,
                    help="For --type shift: fraction at which demand shifts.")
    ap.add_argument("--dataset", default=None,
                    help="Path to dataset (CSV or XES). Defaults to "
                         "EVAL_DATASET or datasets/test.xes.")
    ap.add_argument("--log-name", default=None,
                    help="Log name to embed in queries (default: EVAL_LOG_NAME or 'test').")
    ap.add_argument("--activities", default=None,
                    help="Comma-separated activity labels. If omitted, "
                         "discovered from the dataset.")
    ap.add_argument("--perspectives", default=None,
                    help="Comma-separated grouping-key sets, '+' between keys "
                         "in a multi-key set.  E.g. 'resource,role+region'. "
                         "If omitted, discovered from the dataset.")
    ap.add_argument("--attributes", default=None,
                    help="Comma-separated attribute keys for --type attribute. "
                         "If omitted, all observed attributes are used.")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    activities, perspectives, attribute_values, log_name = _resolve_inputs(args)

    if args.type == "skewed":
        queries = gen_skewed(
            activities=activities, perspectives=perspectives,
            n_queries=args.count, hot_ratio=args.hot_ratio,
            n_hot_combos=args.n_hot, seed=args.seed, log_name=log_name,
        )
    elif args.type == "uniform":
        queries = gen_uniform(
            activities=activities, perspectives=perspectives,
            n_queries=args.count, seed=args.seed, log_name=log_name,
        )
    elif args.type == "shift":
        queries = gen_demand_shift(
            activities=activities, perspectives=perspectives,
            n_queries=args.count, shift_at=args.shift_at,
            seed=args.seed, log_name=log_name,
        )
    elif args.type == "attribute":
        queries = gen_attribute_aware(
            activities=activities, perspectives=perspectives,
            attribute_values=attribute_values,
            n_queries=args.count, seed=args.seed, log_name=log_name,
        )
    else:
        raise SystemExit(f"unknown --type {args.type!r}")

    out = json.dumps(queries, indent=2)
    if args.out:
        args.out.write_text(out)
        print(f"Wrote {len(queries)} queries to {args.out}")
    else:
        sys.stdout.write(out)


if __name__ == "__main__":
    main()
