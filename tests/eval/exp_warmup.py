"""
tests/eval/exp_warmup.py

Experiment 6.3.1 — Query performance under adaptive indexing.

Goal
----
Show how query latency evolves as the adaptive system warms up from
cold (everything at L0, ABSENT) through to warm (frequent pairs at
L3 PERSISTENT).  Three reps are not enough to observe PERSISTENT under
the default `min_query_count = 3`: promotion fires at the END of rep
2 with no subsequent rep to read from L3.  This experiment therefore
sets `min_query_count = 1` so promotion fires after rep 0, and rep 1
already reads from the materialised per-pair PairsIndex on Delta.

Lifecycle reached under these settings (per pair, per perspective):
    rep 0  -> ABSENT       (cold lazy scan + sync promotion to PERSISTENT)
    rep 1  -> PERSISTENT   (Delta read)
    rep 2…N -> PERSISTENT  (Delta read, should be stable)

If you want the canonical "transient hit, then persistent" curve,
keep `min_query_count = 2` and use 4 reps:
    rep 0  -> ABSENT -> TRANSIENT
    rep 1  -> TRANSIENT -> PERSISTENT
    rep 2…N -> PERSISTENT

Eager comparison
----------------
The eager system answers detection by case (trace_id).  The adaptive
system answers by the perspective grouping.  To compare like-for-like we
post-process the eager output: each detected trace contributes its
perspective value, and we count distinct perspective values.  This is
the "eager_grouped" curve and is what the paper should compare against
the adaptive curve.

Output
------
results/6_3_1_warmup.jsonl with three record types:
  {"event": "query", "system": "adaptive", ..., "pair_status_after": {...}}
  {"event": "query", "system": "eager"}
  {"event": "query", "system": "eager_grouped", ...}
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


REPS_PER_CATEGORY = 5
CONFIG = CONFIG_DIR / "adaptive_index.config.json"

# Retention overrides for the warm-up benchmark.  These force the system
# to promote pairs after a single query, so reps 1+ observe the L3 phase
# and the warm-up curve is fully visible within the run.
WARMUP_RETENTION_OVERRIDES = {
    "min_query_count":     1,
    "half_life_seconds":   3600.0,
    "hysteresis":          0.15,
}


def run_category(
    rec: Recorder,
    category: str,
    workload: list[dict],
    reps: int,
) -> None:
    """Run `workload` × `reps` times, recording per-query latency."""
    print(f"\n── adaptive {category} — {len(workload)} queries × {reps} reps ──")

    seq = 0
    for rep in range(reps):
        for q in workload:
            try:
                body, latency = timed_query(
                    log_name=q["log_name"],
                    pattern=q["pattern"],
                    grouping_keys=q["grouping_keys"],
                    retention_overrides=WARMUP_RETENTION_OVERRIDES,
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
                    pair_status_after=body.get("pair_status_after", {}),
                )
                statuses = body.get("pair_status_after") or {}
                # Compact one-line status summary for the console.
                summary = ",".join(f"{k}={v[0]}" for k, v in statuses.items())
                print(f"  [adaptive] seq={seq:3d} rep={rep} {q['id']:4s} "
                      f"{q['pattern']:30s} -> {latency:.3f}s "
                      f"total={body.get('total')} [{summary}]")
            except Exception as exc:
                rec.emit(
                    "query_error",
                    system="adaptive", category=category,
                    rep=rep, seq=seq, qid=q["id"], error=str(exc),
                )
                print(f"  [adaptive] seq={seq:3d} {q['id']:4s} ERROR: {exc}")
            seq += 1


def run_eager_reference(
    rec: Recorder,
    category: str,
    workload: list[dict],
) -> None:
    """
    Run each query once through the case-centric eager detector.

    This is a REFERENCE point only — the eager system answers a different
    question (which traces match) under a different grouping (trace_id).
    Including it on the same plot as the adaptive curves requires a clear
    legend caveat in the paper.

    The reason to keep it: it is the latency floor any system relying on a
    pre-built case-centric pairs_index could achieve.  Showing adaptive
    warm converging toward this latency floor (despite answering a more
    expressive question) is a strong positive result.

    Note: there is NO separate 'eager_perspective' baseline.  The fully-
    materialised L3 PERSISTENT state of the adaptive system IS the eager
    perspective baseline — both read from a pre-built per-perspective
    PairsIndex Delta table.  Reps 1+ of the adaptive warm run already
    measure this.  Running it again separately would be redundant.
    """
    print(f"\n── eager reference {category} ──")
    for q in workload:
        t0 = time.perf_counter()
        try:
            body = detect_eager(log_name=q["log_name"], pattern=q["pattern"])
            latency = time.perf_counter() - t0
            rec.emit(
                "query",
                system="eager_trace", category=category, rep=0, qid=q["id"],
                pattern=q["pattern"],
                has_constraints="[" in q["pattern"],
                latency_s=latency,
                total=body.get("total", 0),
            )
            print(f"  [eager_trace] {q['id']:4s} {q['pattern']:30s} -> "
                  f"{latency:.3f}s total={body.get('total')}")
        except Exception as exc:
            rec.emit("query_error", system="eager_trace", category=category,
                     qid=q["id"], error=str(exc))


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Experiment 6.3.1 — Query performance under adaptive indexing.",
    )
    ap.add_argument("--dataset",  default=None)
    ap.add_argument("--log-name", default=None)
    ap.add_argument("--reps", type=int, default=None,
                    help="Passes per workload.  Defaults to 1 for the cold "
                         "pass (rep 0 is the only informative measurement) "
                         "and 5 for the warm pass (rep 0 cold + reps 1-4 "
                         "verify the L3 plateau).  Pass an explicit value "
                         "to override either default.")
    ap.add_argument("--no-promote-on-first", action="store_true",
                    help="Disable the min_query_count=1 override so promotion "
                         "uses the default min_query_count=3.  Use this for "
                         "the COLD pass: rep 0 measures the lazy-scan path "
                         "without synchronous promotion.  Subsequent reps "
                         "produce essentially the same measurement and are "
                         "skipped by default — use --reps to override.")
    ap.add_argument("--out-suffix", default=None,
                    help="Override the JSONL filename suffix.  Defaults to "
                         "'cold' if --no-promote-on-first, else 'warm'.")
    args = ap.parse_args()

    # Resolve --reps default based on pass mode.
    if args.reps is None:
        args.reps = 1 if args.no_promote_on_first else 5

    spec = resolve_dataset(args.dataset, args.log_name)
    workloads = build_workloads(args.dataset, args.log_name)

    print(f"Dataset: {spec.path} (log_name={spec.log_name})")
    print(f"  activities = {workloads.context.activities}")
    print(f"  perspectives = {workloads.context.perspectives}")

    if args.no_promote_on_first:
        global WARMUP_RETENTION_OVERRIDES
        WARMUP_RETENTION_OVERRIDES = {
            "min_query_count":   3,
            "half_life_seconds": 3600.0,
            "hysteresis":        0.15,
        }
        suffix = args.out_suffix or "cold"
        print("  PASS = cold  (defaults: min_query_count=3, no early promotion)")
    else:
        suffix = args.out_suffix or "warm"
        print(f"  PASS = warm  (overrides: {WARMUP_RETENTION_OVERRIDES})")

    health_check()
    out_name = f"6_3_1_warmup_{suffix}.jsonl"
    rec = Recorder("6.3.1", out_name)
    rec.emit("dataset", path=str(spec.path), log_name=spec.log_name,
             activities=workloads.context.activities,
             perspectives=workloads.context.perspectives,
             retention_overrides=WARMUP_RETENTION_OVERRIDES,
             pass_mode=suffix)

    print("\nIngesting (adaptive + eager) ...")
    ingest_adaptive(spec.log_name, spec.path, CONFIG,
                    overrides=WARMUP_RETENTION_OVERRIDES)
    ingest_eager(spec.log_name, spec.path, CONFIG)
    rec.emit("ingest_complete", log_name=spec.log_name)
    time.sleep(2)

    run_category(rec, "structural",      workloads.structural,      args.reps)
    run_eager_reference(rec, "structural",      workloads.structural)

    # run_category(rec, "attribute_aware", workloads.attribute_aware, args.reps)
    # run_eager_reference(rec, "attribute_aware", workloads.attribute_aware)

    print(f"\nResults written to {rec.path}")


if __name__ == "__main__":
    main()