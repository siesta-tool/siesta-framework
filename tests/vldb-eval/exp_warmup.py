"""
tests/eval/exp_warmup.py

Experiment 6.3.1 — Warm-up curve for adaptive perspective indexing.

Goal
----
Show that query latency drops as the adaptive system observes demand
and materialises frequently-queried pairs under each perspective.

Design
------
A single query stream, run once.  The stream contains:

  - HOT pairs: high-coverage pairs that appear reps_per_hot_pair times
    in the stream (default 4).  By their 3rd appearance the cost gate
    fires and the pair is promoted to PERSISTENT in the background.
    Their 4th appearance reads from the materialised Delta table.

  - COLD pairs: low-coverage pairs that appear once.  They stay at
    ABSENT or TRANSIENT throughout, serving as an in-stream control
    that shows the system does NOT blindly promote everything.

All patterns are 2-activity ("A B") — skip-CEP eligible, no CEP
blowup risk.  Patterns are drawn from observed pair co-occurrences
under each perspective (via the /pair_coverage endpoint), so every
query returns at least one result.

Perspectives are filtered to event-level attributes with at least
`min_perspective_cardinality` groups (see eval_common.discover_schema).

A sleep sentinel is inserted after the round that crosses the
min_query_count threshold, giving the background materialisation
worker time to finish before the final round reads from Delta.

The resulting plot is:
  x-axis = query position in the stream (seq)
  y-axis = query latency (seconds)
  color  = HOT / COLD
  shape  = pair_status at query time (ABSENT / TRANSIENT / PERSISTENT)

Expected curve: HOT queries start at ~15-25s (lazy scan), drop to
~3s (LRU/TRANSIENT), then drop further to ~1-2s (Delta/PERSISTENT).
COLD queries stay at ~15-25s throughout.

Output
------
results/6_3_1_warmup.jsonl

Each line:
  {"event": "query", "system": "adaptive", "seq": N,
   "qid": "H1", "bucket": "HOT", "shared_pair": "A->B",
   "hot_rep": 2, "latency_s": 3.21, "total": 113,
   "pair_status_after": {"A->B": "TRANSIENT"}, ...}
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tests.eval.eval_common import (
    CONFIG_DIR,
    Recorder, health_check,
    ingest_eager,
    timed_query,
    resolve_dataset,
)
from tests.eval.workload import build_workloads, build_shared_structure_workload


CONFIG = CONFIG_DIR / "adaptive_index.config.json"

# Retention overrides.  min_query_count=3 means the system needs to see
# a pair queried 3 times before promotion fires.  This is the real
# adaptive decision logic — not a bypass.
RETENTION_OVERRIDES = {
    "min_query_count":   3,
    "half_life_seconds": 3600.0,
    "hysteresis":        0.0,
}


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_stream(
    rec: Recorder,
    workload: list[dict],
    *,
    retention_overrides: dict,
) -> None:
    """
    Run the query stream once, recording per-query latency.

    Handles __sleep__ sentinels by pausing — these are not real queries
    and are not recorded in the JSONL.
    """
    real_count = sum(1 for q in workload if q["id"] != "__sleep__")
    print(f"\n── query stream: {real_count} queries ──")

    seq = 0
    for q in workload:
        # Sleep sentinel — pause for background materialisation.
        if q.get("id") == "__sleep__":
            secs = q.get("sleep_seconds", 120)
            print(f"\n  [sleep] waiting {secs}s for background "
                  f"materialisation to complete ...")
            time.sleep(secs)
            print(f"  [sleep] resuming.\n")
            continue

        try:
            body, latency = timed_query(
                log_name=q["log_name"],
                pattern=q["pattern"],
                grouping_keys=q["grouping_keys"],
                retention_overrides=retention_overrides,
            )
            statuses = body.get("pair_status_after") or {}
            summary  = ",".join(f"{k}={v}" for k, v in statuses.items())
            tag      = (q.get("tags") or [None])[0]

            rec.emit(
                "query",
                system="adaptive",
                seq=seq,
                qid=q["id"],
                pattern=q["pattern"],
                grouping_keys=q["grouping_keys"],
                bucket=tag,
                shared_pair=q.get("shared_pair"),
                hot_rep=q.get("hot_rep"),
                group_coverage=q.get("group_coverage"),
                has_constraints="[" in q["pattern"],
                latency_s=latency,
                total=body.get("total", 0),
                perspective=body.get("perspective"),
                pair_status_after=statuses,
            )
            print(f"  seq={seq:3d} {q['id']:<6s} [{tag or '?':<4s}] "
                  f"rep={q.get('hot_rep', '?')}  "
                  f"{q['pattern']:30s} -> {latency:.2f}s  "
                  f"total={body.get('total')} [{summary}]")
        except Exception as exc:
            rec.emit(
                "query_error",
                seq=seq, qid=q["id"], error=str(exc),
            )
            print(f"  seq={seq:3d} {q['id']:<6s} ERROR: {exc}")

        seq += 1


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Experiment 6.3.1 — Warm-up curve for adaptive "
                    "perspective indexing.",
    )
    ap.add_argument("--dataset",  default=None,
                    help="Path to the dataset file (.xes or .csv).")
    ap.add_argument("--log-name", default=None,
                    help="Log name used as the storage key.")
    ap.add_argument("--out-suffix", default="warmup",
                    help="JSONL filename suffix (default: warmup).")
    ap.add_argument("--skip-ingest", action="store_true",
                    help="Skip the eager ingest.  Use when the dataset "
                         "is already in MinIO from a previous run.")
    ap.add_argument("--n-hot", type=int, default=5,
                    help="Number of hot pairs per perspective (default: 5).")
    ap.add_argument("--n-cold", type=int, default=8,
                    help="Number of cold pairs per perspective (default: 8).")
    ap.add_argument("--hot-reps", type=int, default=4,
                    help="Times each hot pair appears in the stream (default: 4).")
    ap.add_argument("--sleep", type=int, default=120,
                    help="Seconds to sleep after the promotion round (default: 120).")
    args = ap.parse_args()

    # ── dataset / context ────────────────────────────────────────────────
    spec      = resolve_dataset(args.dataset, args.log_name)
    workloads = build_workloads(args.dataset, args.log_name)

    print(f"Dataset : {spec.path} (log_name={spec.log_name})")
    print(f"  activities   = {workloads.context.activities}")
    print(f"  perspectives = {workloads.context.perspectives}")
    print(f"  retention    = {RETENTION_OVERRIDES}")

    # ── health check ─────────────────────────────────────────────────────
    health_check()

    # ── ingest ───────────────────────────────────────────────────────────
    if args.skip_ingest:
        print("\nSkipping ingest (--skip-ingest).")
    else:
        print("\nIngesting (eager, clear_existing=True) ...")
        ingest_eager(spec.log_name, spec.path, CONFIG, clear_existing=True)
        print("Ingest complete.")
    time.sleep(2)

    # ── workload ─────────────────────────────────────────────────────────
    print("\nFetching pair coverage and building query stream ...")
    stream = build_shared_structure_workload(
        workloads.context,
        n_hot_pairs=args.n_hot,
        n_cold_pairs=args.n_cold,
        reps_per_hot_pair=args.hot_reps,
        sleep_seconds=args.sleep,
    )

    if not stream:
        print("ERROR: no queries generated.  Check perspective cardinality "
              "and pair coverage.")
        sys.exit(1)

    real_count  = sum(1 for q in stream if q["id"] != "__sleep__")
    sleep_count = sum(1 for q in stream if q["id"] == "__sleep__")
    print(f"  {real_count} queries + {sleep_count} sleep sentinel(s)")

    # ── recorder ─────────────────────────────────────────────────────────
    out_name = f"6_3_1_{args.out_suffix}.jsonl"
    rec = Recorder("6.3.1", out_name)
    rec.emit(
        "dataset",
        path=str(spec.path),
        log_name=spec.log_name,
        activities=workloads.context.activities,
        perspectives=workloads.context.perspectives,
        retention_overrides=RETENTION_OVERRIDES,
    )

    # ── run ──────────────────────────────────────────────────────────────
    run_stream(rec, stream, retention_overrides=RETENTION_OVERRIDES)

    print(f"\nResults written to {rec.path}")


if __name__ == "__main__":
    main()