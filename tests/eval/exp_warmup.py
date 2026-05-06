"""
tests/eval/exp_6_3_1_warmup.py

Experiment 6.3.1 — Query performance under adaptive indexing.

Goal
----
Show how query latency evolves as the adaptive system warms up from
cold (everything at L0, ABSENT) through to warm (frequent pairs at
L3 PERSISTENT).  This produces the "warm-up curve" figure: latency
drops as the adaptive lifecycle progresses, and the eager baseline is
a flat line above the converged adaptive latency.

Design
------
For each of two query categories — structural and attribute-aware —
we run a workload of derived queries against a freshly-ingested log.
Each query's wall-clock time is recorded along with its category and
sequence number.  Categories run as separate passes so warm-up curves
do not interfere with each other.

The eager baseline is measured by running the same queries through
the eager indexer's query endpoint.

Dataset selection
-----------------
The dataset and log_name are resolved via eval_common.resolve_dataset
(EVAL_DATASET / EVAL_LOG_NAME env vars or --dataset/--log-name CLI
arguments).  Workloads are derived from the dataset's observed
activities and attributes — there is no hardcoded schema.

Output
------
results/6_3_1_warmup.jsonl with one record per query.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

# Allow running as a script from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tests.eval.eval_common import (
    CONFIG_DIR,
    Recorder, health_check,
    ingest_adaptive, ingest_eager,
    timed_query, detect_eager,
    resolve_dataset,
)
from tests.eval.workload import build_workloads


REPS_PER_CATEGORY = 3   # number of full passes over the workload to amplify warm-up signal
CONFIG = CONFIG_DIR / "adaptive_index.config.json"


def run_category(
    rec: Recorder,
    category: str,
    workload: list[dict],
    reps: int,
) -> None:
    """Run `workload` × `reps` times, recording per-query latency."""
    print(f"\n── {category} workload — {len(workload)} queries × {reps} reps ──")

    seq = 0
    for rep in range(reps):
        for q in workload:
            try:
                body, latency = timed_query(
                    log_name=q["log_name"],
                    pattern=q["pattern"],
                    grouping_keys=q["grouping_keys"],
                )
                rec.emit(
                    "query",
                    system="adaptive",
                    category=category,
                    rep=rep,
                    seq=seq,
                    qid=q["id"],
                    pattern=q["pattern"],
                    grouping_keys=q["grouping_keys"],
                    has_constraints="[" in q["pattern"],
                    latency_s=latency,
                    total=body.get("total", 0),
                    perspective=body.get("perspective"),
                )
                print(f"  [adaptive] seq={seq:3d} {q['id']:4s} "
                      f"{q['pattern']:30s} -> {latency:.3f}s, total={body.get('total')}")
            except Exception as exc:
                rec.emit(
                    "query_error",
                    system="adaptive",
                    category=category,
                    rep=rep,
                    seq=seq,
                    qid=q["id"],
                    error=str(exc),
                )
                print(f"  [adaptive] seq={seq:3d} {q['id']:4s} ERROR: {exc}")
            seq += 1


def run_eager_baseline(rec: Recorder, category: str, workload: list[dict]) -> None:
    """Run each query once through the eager endpoint as a reference."""
    print(f"\n── {category} eager baseline ──")
    for q in workload:
        t0 = time.perf_counter()
        try:
            body = detect_eager(log_name=q["log_name"], pattern=q["pattern"])
            latency = time.perf_counter() - t0
            rec.emit(
                "query",
                system="eager",
                category=category,
                rep=0,
                qid=q["id"],
                pattern=q["pattern"],
                has_constraints="[" in q["pattern"],
                latency_s=latency,
                total=body.get("total", 0),
            )
            print(f"  [eager]    {q['id']:4s} {q['pattern']:30s} -> {latency:.3f}s")
        except Exception as exc:
            rec.emit(
                "query_error",
                system="eager",
                category=category,
                qid=q["id"],
                error=str(exc),
            )
            print(f"  [eager]    {q['id']:4s} ERROR: {exc}")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Experiment 6.3.1 — Query performance under adaptive indexing.",
    )
    ap.add_argument("--dataset", default=None,
                    help="Path to dataset (CSV or XES). Defaults to "
                         "EVAL_DATASET env var, then datasets/test.xes.")
    ap.add_argument("--log-name", default=None,
                    help="Log name (default: EVAL_LOG_NAME env var, then 'test').")
    ap.add_argument("--reps", type=int, default=REPS_PER_CATEGORY,
                    help="Passes per workload to amplify the warm-up signal.")
    args = ap.parse_args()

    spec = resolve_dataset(args.dataset, args.log_name)
    workloads = build_workloads(args.dataset, args.log_name)

    print(f"Dataset: {spec.path} (log_name={spec.log_name})")
    print(f"  activities = {workloads.context.activities}")
    print(f"  perspectives = {workloads.context.perspectives}")

    health_check()
    rec = Recorder("6.3.1", "6_3_1_warmup.jsonl")
    rec.emit("dataset", path=str(spec.path), log_name=spec.log_name,
             activities=workloads.context.activities,
             perspectives=workloads.context.perspectives)

    print("Ingesting (adaptive + eager) ...")
    ingest_adaptive(spec.log_name, spec.path, CONFIG)
    ingest_eager(spec.log_name, spec.path, CONFIG)
    rec.emit("ingest_complete", log_name=spec.log_name)
    time.sleep(2)

    run_category(rec, "structural", workloads.structural, args.reps)
    run_eager_baseline(rec, "structural", workloads.structural)

    run_category(rec, "attribute_aware", workloads.attribute_aware, args.reps)
    run_eager_baseline(rec, "attribute_aware", workloads.attribute_aware)

    print(f"\nResults written to {rec.path}")


if __name__ == "__main__":
    main()
