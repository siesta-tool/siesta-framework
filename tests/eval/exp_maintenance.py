"""
tests/eval/exp_maintenance.py

Experiment 6.3.2 — Index maintenance cost vs workload coverage.

Goal
----
Demonstrate that adaptive maintenance cost tracks the observed
workload footprint, not the full schema.  Compared against an eager
baseline that materialises every (perspective, A, B) combination:

  - Skewed workload : adaptive maintains only the hot pair set ->
                      maintenance cost << eager.
  - Uniform workload: adaptive eventually promotes all pairs ->
                      adaptive cost converges toward eager.

Design
------
The log is split into N disjoint batches (trace_sample by default).
Each batch contains genuine new events — different trace IDs with
their original timestamps — so LastChecked watermarks advance and
incremental maintenance fires correctly.

Batch sequence (shared across both workloads):
    batch 0  : full bootstrap ingest (overwrite_data=True)
    batch 1…N: genuine incremental ingests (overwrite_data=False)

Between consecutive batch ingests a slice of the query workload is
executed against the adaptive system to drive retention promotions.
The eager baseline sees the same batch sequence but no queries — it
always maintains the full schema regardless.

The eager baseline is run only once per batch sequence (not once per
workload label) because its maintenance cost is workload-independent.
The two workload runs produce different adaptive cost curves; the
single eager curve is the shared upper bound for both.

Output
------
results/6_3_2_maintenance.jsonl

    {"event": "batch_maintenance", "system": "adaptive",
     "workload": "skewed", "batch": 2,
     "maintenance_s": 0.41, "events_in_batch": 6241}

    {"event": "batch_maintenance", "system": "eager",
     "workload": "baseline", "batch": 2,
     "maintenance_s": 1.83, "events_in_batch": 6241}

    {"event": "footprint", "workload": "skewed",
     "queried_combos": 6, "schema_combos": 32, "ratio": 0.19}
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tests.eval.eval_common import (
    CONFIG_DIR, RESULTS_DIR,
    Recorder, health_check,
    ingest_adaptive, ingest_eager,
    timed_query,
    perspective_pair_set, extract_required_pairs,
    resolve_dataset,
)
from tests.eval.workload import build_workloads
from tests.eval.batch_splitter import split_log


N_BATCHES   = 5
SPLIT_MODE  = "trace_sample"
CONFIG      = CONFIG_DIR / "adaptive_index.config.json"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def queries_per_batch(workload: list[dict], batch_idx: int,
                      n_batches: int) -> list[dict]:
    """Slice workload into n_batches chunks; return the chunk for batch_idx."""
    chunk = max(1, len(workload) // n_batches)
    return workload[batch_idx * chunk : (batch_idx + 1) * chunk]


def compute_footprint(workload: list[dict]) -> dict:
    triples = perspective_pair_set(workload)
    activities: set[str] = set()
    perspectives: set[tuple] = set()
    for q in workload:
        perspectives.add(tuple(sorted(q.get("grouping_keys", []))))
        for a, b in extract_required_pairs(q["pattern"]):
            activities.add(a); activities.add(b)
    schema_combos  = len(perspectives) * len(activities) * len(activities)
    queried_combos = len(triples)
    return {
        "perspectives":   len(perspectives),
        "activities":     len(activities),
        "queried_combos": queried_combos,
        "schema_combos":  schema_combos,
        "ratio": queried_combos / schema_combos if schema_combos else 0.0,
    }


def _count_events(path: Path) -> int:
    with path.open() as f:
        return max(0, sum(1 for _ in f) - 1)


# ---------------------------------------------------------------------------
# Runners
# ---------------------------------------------------------------------------

def run_eager_baseline(
    rec: Recorder,
    log_name: str,
    batch_paths: list[Path],
) -> None:
    """
    Run the eager indexer once per batch.  Recorded as workload='baseline'
    because eager cost is independent of the query workload — one curve
    serves as the upper-bound reference for both skewed and uniform plots.
    """
    print("\n── Eager baseline ──")

    body = ingest_eager(log_name, batch_paths[0], CONFIG)
    n0 = _count_events(batch_paths[0])
    rec.emit("ingest_complete", system="eager", workload="baseline",
             batch=0, time_s=body.get("time"), events_in_batch=n0)
    print(f"  batch 0 (bootstrap): {n0} events")

    for batch_idx, batch_path in enumerate(batch_paths[1:], start=1):
        t0    = time.perf_counter()
        body  = ingest_eager(log_name, batch_path, CONFIG)
        maint = time.perf_counter() - t0
        n     = _count_events(batch_path)
        rec.emit("batch_maintenance",
                 system="eager", workload="baseline",
                 batch=batch_idx, maintenance_s=maint,
                 events_in_batch=n, reported_time=body.get("time"))
        print(f"  batch {batch_idx}: {n} events, maintenance={maint:.3f}s")


def run_adaptive_workload(
    rec: Recorder,
    log_name: str,
    batch_paths: list[Path],
    workload_label: str,
    workload: list[dict],
) -> None:
    """
    Run the adaptive indexer across all batches, interleaving a query
    slice between consecutive ingests to drive retention promotions.

    Each run starts with a clean bootstrap (overwrite_data=True on
    batch 0) so the two workload runs do not share catalog state.
    """
    n_batches = len(batch_paths)
    print(f"\n── Adaptive / '{workload_label}' ──")

    # Bootstrap
    body = ingest_adaptive(log_name, batch_paths[0], CONFIG, overrides={
        "overwrite_data": True,
    })
    n0 = _count_events(batch_paths[0])
    rec.emit("ingest_complete", system="adaptive", workload=workload_label,
             batch=0, time_s=body.get("time"), events_in_batch=n0)
    print(f"  batch 0 (bootstrap): {n0} events, {body.get('time', '?'):.2f}s")

    for batch_idx, batch_path in enumerate(batch_paths[1:], start=1):
        # Query slice between batch (batch_idx-1) and batch (batch_idx).
        # Use batch_idx-1 as the slice index so slice 0 runs after the
        # bootstrap and before the first incremental batch.
        slice_ = queries_per_batch(workload, batch_idx - 1, n_batches - 1)
        n_queries_run = 0
        for q in slice_:
            try:
                timed_query(log_name=q["log_name"], pattern=q["pattern"],
                            grouping_keys=q["grouping_keys"])
                n_queries_run += 1
            except Exception as exc:
                rec.emit("query_error", system="adaptive",
                         workload=workload_label, batch=batch_idx,
                         qid=q["id"], error=str(exc))

        # Incremental ingest
        t0    = time.perf_counter()
        body  = ingest_adaptive(log_name, batch_path, CONFIG, overrides={
            "overwrite_data": False,
        })
        maint = time.perf_counter() - t0
        n     = _count_events(batch_path)
        rec.emit("batch_maintenance",
                 system="adaptive", workload=workload_label,
                 batch=batch_idx, maintenance_s=maint,
                 events_in_batch=n, reported_time=body.get("time"),
                 queries_run=n_queries_run)
        print(f"  batch {batch_idx}: {n} events, "
              f"queries={n_queries_run}, maintenance={maint:.3f}s")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Experiment 6.3.2 — Index maintenance cost vs workload coverage.",
    )
    ap.add_argument("--dataset",  default=None,
                    help="Path to dataset (CSV or XES).")
    ap.add_argument("--log-name", default=None)
    ap.add_argument("--n-batches", type=int, default=N_BATCHES,
                    help=f"Number of ingest batches (default {N_BATCHES}).")
    ap.add_argument("--split-mode",
                    choices=["trace_sample", "temporal", "synthetic"],
                    default=SPLIT_MODE)
    ap.add_argument("--batch-dir", default=None, type=Path,
                    help="Reuse pre-split batches from this directory "
                         "(batch_0.csv … batch_N-1.csv).  If provided, "
                         "splitting is skipped.")
    args = ap.parse_args()

    spec      = resolve_dataset(args.dataset, args.log_name)
    workloads = build_workloads(args.dataset, args.log_name)
    gk        = (workloads.context.perspectives[0][0]
                 if workloads.context.perspectives else None)

    print(f"Dataset:      {spec.path}  (log_name={spec.log_name})")
    print(f"Activities:   {workloads.context.activities}")
    print(f"Perspectives: {workloads.context.perspectives}")
    print(f"Grouping key: {gk}")

    # ── Prepare batches ────────────────────────────────────────────────────
    if args.batch_dir and args.batch_dir.exists():
        batch_paths = sorted(args.batch_dir.glob("batch_*.csv"))
        if len(batch_paths) < 2:
            ap.error(f"--batch-dir needs at least 2 batch files.")
        print(f"\nReusing {len(batch_paths)} pre-split batches from {args.batch_dir}")
    else:
        batch_dir = RESULTS_DIR / "batches" / spec.log_name
        print(f"\nSplitting into {args.n_batches} batches ({args.split_mode}) ...")
        batch_paths = split_log(
            src=spec.path,
            n_batches=args.n_batches,
            mode=args.split_mode,
            output_dir=batch_dir,
            grouping_key=gk,
        )

    n_batches = len(batch_paths)
    print(f"Batches: {n_batches}  "
          f"(sizes: {[_count_events(p) for p in batch_paths]} events)")

    health_check()
    rec = Recorder("6.3.2", "6_3_2_maintenance.jsonl")
    rec.emit("dataset", path=str(spec.path), log_name=spec.log_name,
             activities=workloads.context.activities,
             perspectives=workloads.context.perspectives,
             n_batches=n_batches, split_mode=args.split_mode)

    # ── Eager baseline — run once, shared reference for both workloads ─────
    run_eager_baseline(rec, spec.log_name, batch_paths)

    # ── Adaptive runs — one per workload, each with a clean bootstrap ──────
    for workload_label, workload in [
        ("skewed",  workloads.skewed),
        ("uniform", workloads.uniform),
    ]:
        fp = compute_footprint(workload)
        rec.emit("footprint", workload=workload_label, **fp)
        print(f"\n[{workload_label}] footprint: "
              f"{fp['queried_combos']}/{fp['schema_combos']} ({fp['ratio']:.0%})")

        run_adaptive_workload(rec, spec.log_name, batch_paths,
                              workload_label, workload)

    print(f"\nResults written to {rec.path}")


if __name__ == "__main__":
    main()