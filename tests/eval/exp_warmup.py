"""
tests/eval/exp_warmup.py

Experiment 6.3.1 — Query performance under adaptive indexing.

Goal
----
Show how query latency evolves as the adaptive system warms up from
cold (everything at L0, ABSENT) through to warm (frequent pairs at
L3 PERSISTENT).

Two passes
----------
COLD pass  (--no-promote-on-first):
    min_query_count=3.  rep 0 measures the lazy-scan path against an
    empty catalog.  No promotion fires during the measured rep.
    Default --reps 1: only rep 0 is informative for the cold curve.
    Eager ingest and eager reference are skipped — they are not needed
    for the cold baseline and would waste time.

WARM pass  (default):
    min_query_count=1 plus the eager-promotion bypass in
    retention.py::should_persist_pair.  Promotion fires at the end of
    rep 0, so rep 1 reads from the materialised per-perspective
    PairsIndex on Delta.  Default --reps 5: rep 0 captures the
    lazy-scan + sync-build cost (marked plot_exclude=True), reps 1-4
    verify the L3 plateau.

The intended plot combines both files:
  - rep 0 from the cold file  (pure lazy scan, no build on hot path)
  - reps 1-4 from the warm file (pure Delta reads)
  rep 0 of the warm file carries plot_exclude=True because its latency
  includes the synchronous build_pair_persistent cost and is therefore
  not representative of either the cold or the warm steady state.

Workload
--------
With --result-bearing the workload is built from observed pair
co-occurrences in the eager sequence table (built during warm ingest).
Pairs are stratified into DENSE / SPARSE / SINGLETON buckets so the
warmup curve covers the full spectrum of result density.  Perspectives
with fewer than min_perspective_cardinality groups are skipped.

Without --result-bearing the synthetic all-pairs structural workload
is used (all ordered activity pairs, all perspectives, no cardinality
filter).  This is useful for smoke tests and small datasets.

Eager comparison
----------------
The eager detector answers "which traces match" under trace_id grouping.
It is run once per query as a reference floor (system="eager_trace").
It is NOT a like-for-like baseline — the adaptive system answers a
more expressive question (which perspective groups match).  The paper
should plot it with a clear legend caveat.

Output
------
results/6_3_1_warmup_cold.jsonl  (cold pass)
results/6_3_1_warmup_warm.jsonl  (warm pass)

Each line is a JSON record:
  {"event": "query", "system": "adaptive", ...,
   "pair_status_after": {...}, "plot_exclude": bool}
  {"event": "query", "system": "eager_trace", ...}
"""

from __future__ import annotations

import argparse
import json
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


CONFIG = CONFIG_DIR / "adaptive_index.config.json"

# Retention overrides for the warm pass.  min_query_count=1 triggers the
# eager-promotion bypass in retention.py so rep 0 promotes every touched
# pair to PERSISTENT and reps 1+ read from Delta.
WARMUP_RETENTION_OVERRIDES = {
    "min_query_count":   1,
    "half_life_seconds": 3600.0,
    "hysteresis":        0.15,
}

COLD_RETENTION_OVERRIDES = {
    "min_query_count":   3,
    "half_life_seconds": 3600.0,
    "hysteresis":        0.15,
}


# ---------------------------------------------------------------------------
# Query runners
# ---------------------------------------------------------------------------

def run_category(
    rec: Recorder,
    category: str,
    workload: list[dict],
    reps: int,
    *,
    pass_mode: str,
    retention_overrides: dict,
) -> None:
    """
    Run `workload` × `reps` times, recording per-query latency.

    Each record includes `plot_exclude=True` for rep 0 of the warm
    pass, because that rep's latency includes the synchronous
    build_pair_persistent cost and should not be plotted alongside
    either the cold baseline or the warm steady-state readings.
    """
    print(f"\n── adaptive {category} — {len(workload)} queries × {reps} reps ──")

    seq = 0
    for rep in range(reps):
        for q in workload:
            try:
                body, latency = timed_query(
                    log_name=q["log_name"],
                    pattern=q["pattern"],
                    grouping_keys=q["grouping_keys"],
                    retention_overrides=retention_overrides,
                )
                statuses = body.get("pair_status_after") or {}
                summary  = ",".join(f"{k}={v}" for k, v in statuses.items())
                rec.emit(
                    "query",
                    system="adaptive",
                    category=category,
                    rep=rep,
                    seq=seq,
                    qid=q["id"],
                    pattern=q["pattern"],
                    grouping_keys=q["grouping_keys"],
                    bucket=q.get("tags", [None])[0],
                    group_coverage=q.get("group_coverage"),
                    has_constraints="[" in q["pattern"],
                    latency_s=latency,
                    total=body.get("total", 0),
                    perspective=body.get("perspective"),
                    pair_status_after=statuses,
                    plot_exclude=pass_mode == "warm" and rep == 0,
                )
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

    Reference point only — answers a different question (which traces
    match) under a different grouping (trace_id).  See module docstring.
    """
    print(f"\n── eager reference {category} ──")
    for q in workload:
        t0 = time.perf_counter()
        try:
            body    = detect_eager(log_name=q["log_name"], pattern=q["pattern"])
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
            print(f"  [eager_trace] {q['id']:4s} ERROR: {exc}")


# ---------------------------------------------------------------------------
# Workload helpers
# ---------------------------------------------------------------------------

def _load_workload_from_jsonl(path: Path) -> list[dict]:
    """
    Reconstruct the structural workload from an existing warm JSONL so
    the cold pass uses the exact same query set without re-running pair
    coverage (which requires the eager sequence table).
    """
    queries: list[dict] = []
    seen_ids: set[str] = set()
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            if (
                r.get("event") == "query"
                and r.get("system") == "adaptive"
                and r.get("rep") == 0
                and r.get("qid") not in seen_ids
            ):
                seen_ids.add(r["qid"])
                queries.append({
                    "id":             r["qid"],
                    "log_name":       r.get("log_name", ""),
                    "pattern":        r["pattern"],
                    "grouping_keys":  r["grouping_keys"],
                    "category":       r.get("category", "structural"),
                    "tags":           [r["bucket"]] if r.get("bucket") else [],
                    "group_coverage": r.get("group_coverage"),
                })
    return queries


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Experiment 6.3.1 — Query performance under adaptive indexing.",
    )
    ap.add_argument("--dataset",  default=None,
                    help="Path to the dataset file (.xes or .csv).")
    ap.add_argument("--log-name", default=None,
                    help="Log name used as the storage key.")
    ap.add_argument("--reps", type=int, default=None,
                    help="Passes per workload.  Defaults to 1 for the cold "
                         "pass and 5 for the warm pass.")
    ap.add_argument("--no-promote-on-first", action="store_true",
                    help="COLD pass mode: use min_query_count=3 so no "
                         "promotion fires during the measured rep.  "
                         "Skips eager ingest and eager reference.")
    ap.add_argument("--out-suffix", default=None,
                    help="Override the JSONL filename suffix.  Defaults to "
                         "'cold' or 'warm' based on pass mode.")
    ap.add_argument("--result-bearing", action="store_true",
                    help="Build the workload from observed pair co-occurrences "
                         "(DENSE/SPARSE/SINGLETON buckets) instead of the "
                         "synthetic all-pairs structural workload.  Requires "
                         "the eager sequence table, so only usable on the "
                         "warm pass.  Ignored on the cold pass when "
                         "--queries-from is supplied.")
    ap.add_argument("--queries-from", default=None, metavar="JSONL",
                    help="Load the query set from an existing warm-pass JSONL "
                         "instead of rebuilding it.  Use this for the cold "
                         "pass so both passes share the exact same queries.")
    args = ap.parse_args()

    # ── resolve pass mode ────────────────────────────────────────────────
    is_cold = args.no_promote_on_first

    if args.reps is None:
        args.reps = 1 if is_cold else 5

    if is_cold:
        retention_overrides = dict(COLD_RETENTION_OVERRIDES)
        suffix   = args.out_suffix or "cold"
        pass_mode = "cold"
    else:
        retention_overrides = dict(WARMUP_RETENTION_OVERRIDES)
        suffix   = args.out_suffix or "warm"
        pass_mode = "warm"

    # ── dataset / context ────────────────────────────────────────────────
    spec      = resolve_dataset(args.dataset, args.log_name)
    workloads = build_workloads(args.dataset, args.log_name)

    print(f"Dataset : {spec.path} (log_name={spec.log_name})")
    print(f"  activities   = {workloads.context.activities}")
    print(f"  perspectives = {workloads.context.perspectives}")
    print(f"  PASS         = {pass_mode}  (overrides: {retention_overrides})")

    # ── health + recorder ────────────────────────────────────────────────
    health_check()
    out_name = f"6_3_1_warmup_{suffix}.jsonl"
    rec = Recorder("6.3.1", out_name)
    rec.emit(
        "dataset",
        path=str(spec.path),
        log_name=spec.log_name,
        activities=workloads.context.activities,
        perspectives=workloads.context.perspectives,
        retention_overrides=retention_overrides,
        pass_mode=pass_mode,
    )

    # ── ingest ───────────────────────────────────────────────────────────
    print("\nIngesting (adaptive) ...")
    ingest_adaptive(spec.log_name, spec.path, CONFIG,
                    overrides=retention_overrides)
    if not is_cold:
        print("Ingesting (eager) ...")
        ingest_eager(spec.log_name, spec.path, CONFIG)
    rec.emit("ingest_complete", log_name=spec.log_name)
    time.sleep(2)

    # ── workload construction ─────────────────────────────────────────────
    # Done AFTER ingest so the eager sequence table exists for pair_coverage.
    if args.queries_from:
        # Reuse queries from an existing warm JSONL (typical cold-pass usage).
        queries_path = Path(args.queries_from)
        if not queries_path.exists():
            ap.error(f"--queries-from: file not found: {queries_path}")
        print(f"\nLoading workload from {queries_path} ...")
        structural = _load_workload_from_jsonl(queries_path)
        print(f"  loaded {len(structural)} queries")
    elif args.result_bearing and not is_cold:
        from tests.eval.workload import build_result_bearing_workload
        print("\nFetching pair coverage and building result-bearing workload ...")
        structural = build_result_bearing_workload(workloads.context)
        print(f"  built {len(structural)} queries")
    elif args.result_bearing and is_cold:
        ap.error(
            "--result-bearing requires the eager sequence table which is only "
            "built during the warm pass.  Run the warm pass first, then use "
            "--queries-from results/6_3_1_warmup_warm.jsonl for the cold pass."
        )
    else:
        structural = workloads.structural

    print(f"\n  structural workload : {len(structural)} queries "
          f"× {args.reps} reps = {len(structural) * args.reps} total")

    # ── run ───────────────────────────────────────────────────────────────
    run_category(
        rec, "structural", structural,
        reps=args.reps,
        pass_mode=pass_mode,
        retention_overrides=retention_overrides,
    )
    if not is_cold:
        run_eager_reference(rec, "structural", structural)

    print(f"\nResults written to {rec.path}")


if __name__ == "__main__":
    main()