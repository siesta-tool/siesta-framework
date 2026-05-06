"""
tests/eval/exp_6_3_2_maintenance.py

Experiment 6.3.2 — Index maintenance cost vs workload coverage.

Goal
----
Demonstrate that adaptive maintenance cost tracks the observed
workload footprint, not the full schema.  Compared against an eager
baseline that materialises every (perspective, A, B) combination,
the adaptive system should:

  - Match the eager baseline when the workload is uniform.
  - Significantly undercut the eager baseline when the workload is skewed.

Design
------
Two runs, identical except for the query workload:

  Run A (skewed)  : 80% of queries hit a small hot pair set
  Run B (uniform) : queries spread evenly across all combinations

For each run we measure:

  1. Total maintenance cost per ingest batch.  Multiple batches are
     simulated by re-ingesting the same dataset N times — between
     batches we run a slice of the workload so the catalog evolves.

  2. Workload footprint: |perspective × pair combinations actually
     queried| vs |all combinations the eager system maintains|.

Dataset selection
-----------------
The dataset and log_name are resolved via eval_common.resolve_dataset
(EVAL_DATASET / EVAL_LOG_NAME or --dataset / --log-name CLI flags).
Workloads are derived from the observed activities and perspectives.

Output
------
results/6_3_2_maintenance.jsonl with two record types:

    {"event": "batch_maintenance", "workload": "skewed", "batch": 3,
     "system": "adaptive", "maintenance_s": 0.412, ...}

    {"event": "footprint", "workload": "skewed",
     "queried_combos": 6, "schema_combos": 32, "ratio": 0.19, ...}
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tests.eval.eval_common import (
    CONFIG_DIR,
    Recorder, health_check,
    ingest_adaptive, ingest_eager,
    timed_query,
    perspective_pair_set, extract_required_pairs,
    resolve_dataset,
)
from tests.eval.workload import build_workloads


N_BATCHES = 5     # number of simulated ingest cycles
CONFIG    = CONFIG_DIR / "adaptive_index.config.json"


def queries_per_batch(workload: list[dict], batch: int, n_batches: int) -> list[dict]:
    """Slice the workload into n_batches roughly-equal chunks."""
    chunk = max(1, len(workload) // n_batches)
    return workload[batch * chunk : (batch + 1) * chunk]


def compute_footprint(workload: list[dict]) -> dict:
    """Return the workload footprint as |queried| / |schema|."""
    triples = perspective_pair_set(workload)

    activities: set[str] = set()
    perspectives: set[tuple[str, ...]] = set()
    for q in workload:
        perspectives.add(tuple(sorted(q.get("grouping_keys", []))))
        for a, b in extract_required_pairs(q["pattern"]):
            activities.add(a)
            activities.add(b)

    schema_combos = len(perspectives) * len(activities) * len(activities)
    queried_combos = len(triples)
    return {
        "perspectives":   len(perspectives),
        "activities":     len(activities),
        "queried_combos": queried_combos,
        "schema_combos":  schema_combos,
        "ratio":          queried_combos / schema_combos if schema_combos else 0.0,
    }


def run_workload_against_adaptive(
    rec: Recorder,
    log_name: str,
    dataset_path: Path,
    workload_label: str,
    workload: list[dict],
) -> None:
    """Simulate N batches: ingest, run a slice of workload, measure maintenance."""
    print(f"\n── Adaptive run on '{workload_label}' workload ──")

    body = ingest_adaptive(log_name, dataset_path, CONFIG)
    rec.emit("ingest_complete",
             system="adaptive", workload=workload_label,
             batch=0, time_s=body.get("time"))

    for batch in range(N_BATCHES):
        slice_ = queries_per_batch(workload, batch, N_BATCHES)
        for q in slice_:
            try:
                timed_query(
                    log_name=q["log_name"],
                    pattern=q["pattern"],
                    grouping_keys=q["grouping_keys"],
                )
            except Exception as exc:
                rec.emit("query_error",
                         system="adaptive", workload=workload_label,
                         batch=batch, qid=q["id"], error=str(exc))
                continue

        t0 = time.perf_counter()
        body = ingest_adaptive(log_name, dataset_path, CONFIG, overrides={
            "overwrite_data": False,
        })
        ingest_s = time.perf_counter() - t0
        rec.emit("batch_maintenance",
                 system="adaptive",
                 workload=workload_label,
                 batch=batch + 1,
                 maintenance_s=ingest_s,
                 reported_time=body.get("time"))
        print(f"  batch {batch+1}: queries={len(slice_)} "
              f"maintenance={ingest_s:.3f}s")


def run_workload_against_eager(
    rec: Recorder,
    log_name: str,
    dataset_path: Path,
    workload_label: str,
) -> None:
    """Eager baseline — re-ingests N times; queries are not relevant."""
    print(f"\n── Eager baseline on '{workload_label}' workload ──")

    body = ingest_eager(log_name, dataset_path, CONFIG)
    rec.emit("ingest_complete",
             system="eager", workload=workload_label, batch=0,
             time_s=body.get("time"))

    for batch in range(N_BATCHES):
        t0 = time.perf_counter()
        body = ingest_eager(log_name, dataset_path, CONFIG)
        ingest_s = time.perf_counter() - t0
        rec.emit("batch_maintenance",
                 system="eager",
                 workload=workload_label,
                 batch=batch + 1,
                 maintenance_s=ingest_s,
                 reported_time=body.get("time"))
        print(f"  batch {batch+1}: maintenance={ingest_s:.3f}s")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Experiment 6.3.2 — Index maintenance cost vs workload coverage.",
    )
    ap.add_argument("--dataset", default=None)
    ap.add_argument("--log-name", default=None)
    args = ap.parse_args()

    spec = resolve_dataset(args.dataset, args.log_name)
    workloads = build_workloads(args.dataset, args.log_name)

    print(f"Dataset: {spec.path} (log_name={spec.log_name})")
    print(f"  activities = {workloads.context.activities}")

    health_check()
    rec = Recorder("6.3.2", "6_3_2_maintenance.jsonl")
    rec.emit("dataset", path=str(spec.path), log_name=spec.log_name,
             activities=workloads.context.activities,
             perspectives=workloads.context.perspectives)

    for workload_label, workload in [
        ("skewed",  workloads.skewed),
        ("uniform", workloads.uniform),
    ]:
        fp = compute_footprint(workload)
        rec.emit("footprint", workload=workload_label, **fp)
        print(f"\n[{workload_label}] footprint: "
              f"{fp['queried_combos']}/{fp['schema_combos']} "
              f"({fp['ratio']:.0%})")

        run_workload_against_adaptive(rec, spec.log_name, spec.path,
                                      workload_label, workload)
        run_workload_against_eager(rec, spec.log_name, spec.path,
                                   workload_label)

    print(f"\nResults written to {rec.path}")


if __name__ == "__main__":
    main()
