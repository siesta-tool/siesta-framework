"""
tests/eval/exp_6_3_3_retention.py

Experiment 6.3.3 — Retention policy sensitivity.

Two ablations:

(a) HALF-LIFE
    For each value of half_life_seconds, run the same workload with a
    deliberate demand gap in the middle.  Record query latency over
    the entire sequence.  After the gap, latency reveals whether the
    affected pairs were demoted (rebuild cost) or preserved through
    decay.

(b) HYSTERESIS
    For each value of hysteresis ε, run a workload with periodic
    demand shifts.  Record promotion/demotion transitions visible in
    the API logs (or, equivalently, infer from latency spikes).

Recording transitions
---------------------
The current API does not directly expose promote/demote events to the
client.  We approximate by watching latency curves: a TRANSIENT->ABSENT
demotion produces a measurable rebuild on next query.  This script
emits raw latency curves; transition counts are derived offline.

Dataset selection
-----------------
The dataset and log_name are resolved via eval_common.resolve_dataset.
The demand-shift workload is generated from the dataset's observed
activities and perspectives — there is no hardcoded schema.
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
    ingest_adaptive, timed_query,
    discover_schema, resolve_dataset,
)
from tests.eval.query_generator import gen_demand_shift


CONFIG = CONFIG_DIR / "adaptive_index.config.json"

HALF_LIFE_VALUES   = [60, 300, 3600, 43200]   # 1m, 5m, 1h, 12h
HYSTERESIS_VALUES  = [0.0, 0.10, 0.15, 0.30]
N_QUERIES_PER_RUN  = 40
GAP_AFTER          = 15      # query index after which a "cooling" gap is inserted
GAP_DURATION_S     = 30      # seconds of artificial idle


def _build_shift_workload(
    log_name: str,
    activities: list[str],
    perspectives: list[list[str]],
    seed: int,
    shift_at: float,
) -> list[dict]:
    return gen_demand_shift(
        activities=activities,
        perspectives=perspectives,
        n_queries=N_QUERIES_PER_RUN,
        shift_at=shift_at,
        seed=seed,
        log_name=log_name,
    )


def run_half_life_sweep(
    rec: Recorder,
    log_name: str,
    dataset_path: Path,
    activities: list[str],
    perspectives: list[list[str]],
) -> None:
    print("\n══ Half-life sweep ══")

    workload = _build_shift_workload(log_name, activities, perspectives,
                                     seed=42, shift_at=0.5)

    for hl in HALF_LIFE_VALUES:
        print(f"\n  half_life_seconds = {hl}")

        ingest_adaptive(log_name, dataset_path, CONFIG, overrides={
            "overwrite_data": True,
            "half_life_seconds": hl,
        })
        rec.emit("ingest_complete", ablation="half_life", half_life=hl)

        # Phase 1: queries 0..GAP_AFTER, drives some pairs to PERSISTENT.
        for seq, q in enumerate(workload[:GAP_AFTER]):
            try:
                body, latency = timed_query(
                    log_name=q["log_name"],
                    pattern=q["pattern"],
                    grouping_keys=q["grouping_keys"],
                )
                rec.emit("query",
                         ablation="half_life", half_life=hl,
                         phase="pre_gap", seq=seq, qid=q["id"],
                         pattern=q["pattern"],
                         latency_s=latency, total=body.get("total", 0))
            except Exception as exc:
                rec.emit("query_error",
                         ablation="half_life", half_life=hl, seq=seq,
                         error=str(exc))

        print(f"    [gap of {GAP_DURATION_S}s]")
        time.sleep(GAP_DURATION_S)
        rec.emit("demand_gap_end",
                 ablation="half_life", half_life=hl, duration_s=GAP_DURATION_S)

        # Phase 2: queries GAP_AFTER..end.  Latency here reveals whether
        # pairs were demoted during the gap.
        for seq, q in enumerate(workload[GAP_AFTER:], start=GAP_AFTER):
            try:
                body, latency = timed_query(
                    log_name=q["log_name"],
                    pattern=q["pattern"],
                    grouping_keys=q["grouping_keys"],
                )
                rec.emit("query",
                         ablation="half_life", half_life=hl,
                         phase="post_gap", seq=seq, qid=q["id"],
                         pattern=q["pattern"],
                         latency_s=latency, total=body.get("total", 0))
            except Exception as exc:
                rec.emit("query_error",
                         ablation="half_life", half_life=hl, seq=seq,
                         error=str(exc))


def run_hysteresis_sweep(
    rec: Recorder,
    log_name: str,
    dataset_path: Path,
    activities: list[str],
    perspectives: list[list[str]],
) -> None:
    print("\n══ Hysteresis sweep ══")

    workload = _build_shift_workload(log_name, activities, perspectives,
                                     seed=43, shift_at=0.4)

    for eps in HYSTERESIS_VALUES:
        print(f"\n  hysteresis = {eps}")

        ingest_adaptive(log_name, dataset_path, CONFIG, overrides={
            "overwrite_data": True,
            "retention_hysteresis": eps,
        })
        rec.emit("ingest_complete", ablation="hysteresis", hysteresis=eps)

        for seq, q in enumerate(workload):
            try:
                body, latency = timed_query(
                    log_name=q["log_name"],
                    pattern=q["pattern"],
                    grouping_keys=q["grouping_keys"],
                )
                rec.emit("query",
                         ablation="hysteresis", hysteresis=eps,
                         seq=seq, qid=q["id"],
                         pattern=q["pattern"],
                         phase=("phase1" if "phase1" in q.get("tags", []) else "phase2"),
                         latency_s=latency, total=body.get("total", 0))
            except Exception as exc:
                rec.emit("query_error",
                         ablation="hysteresis", hysteresis=eps, seq=seq,
                         error=str(exc))


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Experiment 6.3.3 — Retention policy sensitivity.",
    )
    ap.add_argument("--dataset", default=None)
    ap.add_argument("--log-name", default=None)
    args = ap.parse_args()

    spec = resolve_dataset(args.dataset, args.log_name)
    schema = discover_schema(spec.path)
    activities = schema.activities[:6]
    perspectives = (
        [[k] for k in schema.perspective_keys[:1]]
        or [[]]
    )
    if len(activities) < 2:
        raise SystemExit(
            f"Dataset has too few activities for the demand-shift workload: {activities}"
        )

    print(f"Dataset: {spec.path} (log_name={spec.log_name})")
    print(f"  activities = {activities}")
    print(f"  perspectives = {perspectives}")

    health_check()
    rec = Recorder("6.3.3", "6_3_3_retention.jsonl")
    rec.emit("dataset", path=str(spec.path), log_name=spec.log_name,
             activities=activities, perspectives=perspectives)

    run_half_life_sweep(rec, spec.log_name, spec.path, activities, perspectives)
    run_hysteresis_sweep(rec, spec.log_name, spec.path, activities, perspectives)

    print(f"\nResults written to {rec.path}")
    print("Note: transition counts for the hysteresis figure must be derived")
    print("from the API logs (grep for 'promoting' / 'demoting'), or inferred")
    print("from latency spikes in the JSONL output.")


if __name__ == "__main__":
    main()
