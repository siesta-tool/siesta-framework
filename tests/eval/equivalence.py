"""
tests/eval/equivalence.py

Correctness-equivalence check between the eager and adaptive query paths.

For each query in a small probe workload we POST the *same* pattern to
both `/querying/detection` (eager / SIESTA) and
`/adaptive_querying/detection` (adaptive) and compare the returned
trace-id sets.  The script prints OK / DIFF per query and exits with
status 1 if any divergence is found, so it doubles as a CI smoke test.

Caveats — single-activity, lookback, and perspective
----------------------------------------------------
- A pattern with only one activity (``"A"``) returns 0 traces from
  *both* backends today: the detector reads from the pairs index and
  derives no pairs from a single activity.  We include "A" in the
  probe set anyway — empty == empty is still a valid equivalence.

- The adaptive endpoint applies a `lookback` window (default 7d server
  side, 30d historical client default).  Datasets with old timestamps
  (e.g. the bundled `test.xes` in 2025-01-01) fall outside that window
  and adaptive returns nothing while eager returns matches.  We pass a
  wide lookback ("3650d") here so the comparison is apples-to-apples.

- **Perspective must equal case**, otherwise the two backends answer
  different questions: eager keys by case (`trace_id`); adaptive keys
  by perspective group.  Using `grouping_keys=["resource"]` against a
  log where A and B are performed by different resources will produce
  an empty adaptive result even when eager finds matches, because no
  *single* resource performed A then B.  This script defaults
  `--grouping-key trace_id` for that reason.

Response field naming
---------------------
Eager returns ``{"trace_id": ...}`` per detected trace; adaptive
returns ``{"group_id": ...}`` per detected perspective group.  The
helper ``_collect_keys`` reads whichever is present so the comparison
works for both.

Usage
-----
    venv/bin/python -m tests.eval.equivalence
    venv/bin/python -m tests.eval.equivalence --dataset datasets/my.xes
    venv/bin/python -m tests.eval.equivalence --grouping-key trace_id
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tests.eval.eval_common import (
    CONFIG_DIR,
    detect_adaptive, detect_eager,
    discover_schema, health_check,
    ingest_adaptive, ingest_eager,
    resolve_dataset,
)


CONFIG = CONFIG_DIR / "adaptive_index.config.json"


def build_probe_queries(activities: list[str], attr_values: dict[str, list[str]]) -> list[dict]:
    """
    Small, dataset-agnostic probe set covering structural and
    attribute-aware patterns.  Returns dicts with `id` + `pattern`.

    Attribute-aware probes use whatever attribute keys the dataset
    actually exposes — keys are taken verbatim from the schema (e.g.
    ``org:resource`` from XES, ``resource`` from CSV) so they match the
    indexed event payloads exactly.
    """
    if len(activities) < 2:
        raise SystemExit(f"Need at least two activities, got {activities}")

    a, b = activities[0], activities[1]
    c = activities[2] if len(activities) >= 3 else None

    queries: list[dict] = [
        {"id": "Q1", "pattern": a},                              # degenerate single-activity
        {"id": "Q2", "pattern": f"{a} {b}"},
        {"id": "Q3", "pattern": f"{b} {a}"},
    ]
    if c:
        queries.append({"id": "Q4", "pattern": f"{a} {b} {c}"})

    # First non-numeric attribute with at least two distinct values gets
    # used for inline equality + variable-binding probes.
    str_attr = next(
        (
            k for k, vs in attr_values.items()
            if vs and not all(_is_num(v) for v in vs)
        ),
        None,
    )
    if str_attr:
        v = attr_values[str_attr][0].replace('"', '\\"')
        queries.append({"id": "Q5", "pattern": f'{a}[{str_attr}="{v}"] {b}'})
        queries.append({"id": "Q7", "pattern": f"{a}[{str_attr}=$1] {b}[{str_attr}=$1]"})

    # First numeric attribute drives a numeric inequality probe.
    num_attr = next(
        (
            k for k, vs in attr_values.items()
            if vs and all(_is_num(v) for v in vs)
        ),
        None,
    )
    if num_attr:
        values = sorted(float(v) for v in attr_values[num_attr])
        threshold = values[len(values) // 2]   # median, so some pass
        queries.append({"id": "Q6", "pattern": f"{a}[{num_attr}>={threshold}] {b}"})

    return queries


def _is_num(s: str) -> bool:
    try:
        float(s)
        return True
    except (TypeError, ValueError):
        return False


def _collect_keys(response: dict) -> list[str]:
    """
    Extract the per-row identifier from a detection response.

    Handles both backends:
      eager    -> {"detected": [{"trace_id": ...}, ...]}
      adaptive -> {"detected": [{"group_id": ...}, ...]}

    Rows missing both fields are reported as the literal "<missing>"
    so a divergence is visible rather than silently dropped.
    """
    out: list[str] = []
    for row in response.get("detected", []) or []:
        if "trace_id" in row and row["trace_id"] is not None:
            out.append(str(row["trace_id"]))
        elif "group_id" in row and row["group_id"] is not None:
            out.append(str(row["group_id"]))
        else:
            out.append("<missing>")
    return sorted(out)


def run_pair(log_name: str, pattern: str, grouping_keys: list[str], lookback: str) -> tuple[list[str], list[str], dict]:
    """Return (eager_keys, adaptive_keys, raw responses)."""
    eager = detect_eager(log_name, pattern)
    adaptive = detect_adaptive(
        log_name, pattern, grouping_keys, lookback=lookback,
    )
    return _collect_keys(eager), _collect_keys(adaptive), {"eager": eager, "adaptive": adaptive}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[2])
    ap.add_argument("--dataset", default=None)
    ap.add_argument("--log-name", default=None)
    ap.add_argument("--lookback", default="3650d",
                    help="Lookback window for adaptive queries. Default 3650d "
                         "to include arbitrarily old timestamps.")
    ap.add_argument("--skip-ingest", action="store_true",
                    help="Skip the (re-)ingest step; assume both indexes already populated.")
    ap.add_argument("--verbose", "-v", action="store_true",
                    help="On a DIFF, print the full eager/adaptive responses.")
    args = ap.parse_args()

    spec = resolve_dataset(args.dataset, args.log_name)
    schema = discover_schema(spec.path)

    # Equivalence is only meaningful when the adaptive perspective groups
    # events the same way the eager backend does — i.e. by case.  Any
    # other perspective (e.g. resource) makes the two backends answer
    # different questions, not the same question on different paths.
    grouping_keys = ["trace_id"]

    print(f"Dataset:       {spec.path}")
    print(f"log_name:      {spec.log_name}")
    print(f"activities:    {schema.activities}")
    print(f"grouping_keys: {grouping_keys}  (fixed — equivalence only "
          f"makes sense per case)")
    print(f"lookback:      {args.lookback}")

    health_check()

    if not args.skip_ingest:
        print("\nIngesting (adaptive + eager) ...")
        ingest_adaptive(spec.log_name, spec.path, CONFIG)
        ingest_eager(spec.log_name, spec.path, CONFIG)

    queries = build_probe_queries(schema.activities, schema.attribute_values)

    print("\nProbe queries:")
    for q in queries:
        print(f"  {q['id']}: {q['pattern']!r}")

    print("\nRunning equivalence check:")
    diffs: list[tuple[str, str, list[str], list[str], dict]] = []
    for q in queries:
        eager_t, adapt_t, raw = run_pair(
            spec.log_name, q["pattern"], grouping_keys, args.lookback,
        )
        if eager_t == adapt_t:
            print(f"  OK   {q['id']}  {q['pattern']!r:50s}  -> "
                  f"{len(eager_t)} traces")
        else:
            diffs.append((q["id"], q["pattern"], eager_t, adapt_t, raw))
            only_e = sorted(set(eager_t) - set(adapt_t))
            only_a = sorted(set(adapt_t) - set(eager_t))
            print(f"  DIFF {q['id']}  {q['pattern']!r:50s}")
            print(f"       eager    : {len(eager_t)} traces  {eager_t[:5]}")
            print(f"       adaptive : {len(adapt_t)} traces  {adapt_t[:5]}")
            if only_e:
                print(f"       only in eager    : {only_e}")
            if only_a:
                print(f"       only in adaptive : {only_a}")
            if args.verbose:
                print("       eager response:")
                print("         " + json.dumps(raw["eager"], indent=2).replace("\n", "\n         "))
                print("       adaptive response:")
                print("         " + json.dumps(raw["adaptive"], indent=2).replace("\n", "\n         "))

    print()
    if diffs:
        print(f"FAIL — {len(diffs)} of {len(queries)} queries diverged.")
        return 1
    print(f"PASS — all {len(queries)} queries returned identical trace sets.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
