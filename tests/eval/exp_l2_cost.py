"""
tests/eval/exp_l2_cost.py

Experiment 6.3.5 — Positional annotation (L2) cost.

Goal
----
Justify the L1/L2 split: positional annotation is a workload-dependent
optimisation, not a correctness requirement.  Demonstrate that:

  - L2 promotion is only triggered when queries reference pos.
  - The extra per-batch maintenance cost of L2 over L1 is bounded.
  - Queries that do not reference pos pay no L2 overhead — they stay
    at L1 and sort pseudo-sequences by timestamp.

Design
------
Three scenarios, each run as separate ingest + query cycles:

  Scenario l1_only
      All queries are structural with no pos references.  The
      perspective must stay at L1; no L2 promotion should fire.
      Per-batch maintenance cost = L1 baseline.

  Scenario mixed
      Half the queries reference pos (via SeQL variable binding on
      the `pos` attribute, e.g. ``A[pos=$1] B[pos=$1+1]``).  L2 must
      promote once pos queries cross min_query_count.  Maintenance cost
      rises by m_pos per batch from that point.

  Scenario pos_heavy
      All queries reference pos.  L2 must promote on the first ingest
      cycle after min_query_count is reached.

For each scenario we record per-batch maintenance time and per-query
latency, distinguishing queries by whether they reference pos.

SeQL pos syntax
---------------
Positional constraints are expressed by treating `pos` as an attribute
in the bracket notation::

    A[pos=$1] B[pos=$1+1]    # B occurs exactly one position after A
    A[pos=$1] B[pos=$1]      # same position (degenerate, usually empty)

The adaptive query module detects pos references via the check
``"pos=" in pattern``, which fires on these patterns.

Dataset selection
-----------------
The dataset and log_name are resolved via eval_common.resolve_dataset.
Perspectives are derived from the dataset's string attributes.

Output
------
results/6_3_5_l2_cost.jsonl with two record types:

    {"event": "batch_maintenance", "scenario": "mixed",
     "batch": 2, "maintenance_s": 0.41}

    {"event": "query", "scenario": "mixed", "seq": 5,
     "references_pos": true, "latency_s": 0.18, "total": 2}

Verification in API logs
------------------------
  l1_only   : no 'L1->L2' promotion log lines should appear
  mixed     : 'L1->L2' should appear on the second or third batch
  pos_heavy : 'L1->L2' should appear on the first batch
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


CONFIG  = CONFIG_DIR / "adaptive_index.config.json"
N_BATCHES = 4


# ---------------------------------------------------------------------------
# Query builders
# ---------------------------------------------------------------------------

def _q(qid: str, pattern: str, log_name: str, gkeys: list[str]) -> dict:
    return {
        "id":             qid,
        "log_name":       log_name,
        "pattern":        pattern,
        "grouping_keys":  gkeys,
        "references_pos": "pos=" in pattern,
        "category":       "attribute_aware" if "[" in pattern else "structural",
    }


def build_scenario_queries(
    activities: list[str],
    perspectives: list[list[str]],
    log_name: str,
    scenario: str,
) -> list[dict]:
    """
    Build the query list for one scenario.

    `scenario` is one of:
      "l1_only"   — no pos references
      "mixed"     — alternating pos / non-pos
      "pos_heavy" — all pos references

    Patterns use the first two activities in the dataset.
    If fewer than two activities are available the scenario is skipped.
    """
    if len(activities) < 2:
        return []

    gk = perspectives[0] if perspectives else []
    a, b = activities[0], activities[1]

    # Structural pairs to use for non-pos queries.
    structural_patterns = [f"{a} {b}"]
    if len(activities) >= 3:
        structural_patterns.append(f"{a} {activities[2]}")
        structural_patterns.append(f"{b} {activities[2]}")

    # Pos-constrained patterns: A is immediately followed by B in
    # position order (B.pos = A.pos + 1).
    pos_patterns = [f"{a}[pos=$1] {b}[pos=$1+1]"]
    if len(activities) >= 3:
        c = activities[2]
        pos_patterns.append(f"{a}[pos=$1] {c}[pos=$1+1]")
        pos_patterns.append(f"{b}[pos=$1] {c}[pos=$1+1]")

    queries: list[dict] = []
    n = 10   # number of queries per scenario
    for i in range(n):
        if scenario == "l1_only":
            pat = structural_patterns[i % len(structural_patterns)]
        elif scenario == "pos_heavy":
            pat = pos_patterns[i % len(pos_patterns)]
        else:  # mixed
            if i % 2 == 0:
                pat = pos_patterns[i // 2 % len(pos_patterns)]
            else:
                pat = structural_patterns[i // 2 % len(structural_patterns)]
        queries.append(_q(f"{scenario[0].upper()}{i}", pat, log_name, gk))

    return queries


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def queries_per_batch(
    workload: list[dict], batch: int, n_batches: int
) -> list[dict]:
    chunk = max(1, len(workload) // n_batches)
    return workload[batch * chunk : (batch + 1) * chunk]


def run_scenario(
    rec: Recorder,
    scenario: str,
    workload: list[dict],
    log_name: str,
    dataset_path: Path,
) -> None:
    if not workload:
        print(f"\n  [SKIP] Scenario {scenario}: empty workload.")
        return

    pos_count  = sum(1 for q in workload if q["references_pos"])
    non_pos    = len(workload) - pos_count
    print(f"\n── Scenario {scenario}: "
          f"{len(workload)} queries ({pos_count} pos / {non_pos} non-pos) "
          f"× {N_BATCHES} batches ──")

    ingest_adaptive(log_name, dataset_path, CONFIG, overrides={
        "overwrite_data": True,
    })
    rec.emit("ingest_complete", scenario=scenario, batch=0)

    for batch in range(N_BATCHES):
        # Run a slice of the workload before triggering maintenance.
        slice_ = queries_per_batch(workload, batch, N_BATCHES)
        for seq, q in enumerate(slice_):
            try:
                body, latency = timed_query(
                    log_name=q["log_name"],
                    pattern=q["pattern"],
                    grouping_keys=q["grouping_keys"],
                )
                rec.emit("query",
                         scenario=scenario,
                         batch=batch,
                         seq=seq,
                         qid=q["id"],
                         pattern=q["pattern"],
                         references_pos=q["references_pos"],
                         latency_s=latency,
                         total=body.get("total", 0))
                print(f"  seq={seq:2d} {q['id']:5s} "
                      f"{'[pos]' if q['references_pos'] else '     '} "
                      f"{q['pattern']:35s} -> {latency:.3f}s")
            except Exception as exc:
                rec.emit("query_error",
                         scenario=scenario, batch=batch, seq=seq,
                         qid=q["id"], error=str(exc))
                print(f"  seq={seq:2d} {q['id']:5s} ERROR: {exc}")

        # Re-ingest to trigger incremental maintenance.
        t0 = time.perf_counter()
        body = ingest_adaptive(log_name, dataset_path, CONFIG, overrides={
            "overwrite_data": False,
        })
        maint_s = time.perf_counter() - t0
        rec.emit("batch_maintenance",
                 scenario=scenario,
                 batch=batch + 1,
                 maintenance_s=maint_s,
                 reported_time=body.get("time"))
        print(f"  → batch {batch+1} maintenance: {maint_s:.3f}s")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Experiment 6.3.5 — Positional annotation (L2) cost.",
    )
    ap.add_argument("--dataset", default=None,
                    help="Path to dataset (CSV or XES). Defaults to "
                         "EVAL_DATASET env var, then datasets/test.xes.")
    ap.add_argument("--log-name", default=None,
                    help="Log name. Defaults to EVAL_LOG_NAME env var, then 'test'.")
    args = ap.parse_args()

    spec    = resolve_dataset(args.dataset, args.log_name)
    schema  = discover_schema(spec.path)

    activities = schema.activities[:4]
    perspectives = (
        [[k] for k in schema.perspective_keys[:1]] or [[]]
    )

    if len(activities) < 2:
        raise SystemExit(
            f"Dataset has too few activities for the L2 experiment: {activities}"
        )

    print(f"Dataset:      {spec.path}  (log_name={spec.log_name})")
    print(f"Activities:   {activities}")
    print(f"Perspectives: {perspectives}")

    health_check()
    rec = Recorder("6.3.5", "6_3_5_l2_cost.jsonl")
    rec.emit("dataset", path=str(spec.path), log_name=spec.log_name,
             activities=activities, perspectives=perspectives)

    for scenario in ("l1_only", "mixed", "pos_heavy"):
        workload = build_scenario_queries(
            activities, perspectives, spec.log_name, scenario
        )
        run_scenario(rec, scenario, workload, spec.log_name, spec.path)

    print(f"\nResults written to {rec.path}")
    print("\nVerify in API logs:")
    print("  l1_only   : no 'L1->L2' promotion log lines")
    print("  mixed     : 'L1->L2' appears on batch 2 or 3")
    print("  pos_heavy : 'L1->L2' appears on batch 1")


if __name__ == "__main__":
    main()
