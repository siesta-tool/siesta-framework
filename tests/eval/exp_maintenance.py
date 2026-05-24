"""
tests/eval/exp_maintenance.py

Experiment — Adaptive maintenance cost vs query demand.

Goal
----
Show that adaptive incremental maintenance tracks actual query demand
rather than the full index schema.  Two workloads are evaluated:

  skewed  : 80 % of queries (configurable via --hot-ratio) hit a small
            hot perspective-pair set, 20 % hit cold combinations.
            Adaptive maintains only the hot set → cost << eager baseline.
  uniform : queries spread evenly across all combinations → adaptive
            eventually promotes the full schema → cost converges to eager.

Only simple 2-activity patterns ("A B") are used; Kleene/regex operators
are excluded.  Both workloads vary across multiple perspectives and pairs.

How the workloads differ
------------------------
build_skewed picks the 2 highest-coverage pairs under each perspective
as "hot" (concentrating 80 % of queries on them) and a handful of other
pairs as "cold" (remaining 20 %).  Hot pairs cross the min_query_count
threshold quickly and are promoted to L3 (persistent); the adaptive
indexer then only pays incremental maintenance for those hot pairs.
Cold pairs stay at ABSENT, incurring zero maintenance cost.

build_uniform cycles round-robin across every (perspective, pair)
combination with equal weight.  All pairs gradually accumulate enough
query touches to be promoted to L3, so by the last batch the adaptive
system is maintaining the complete schema — its cost converges toward
the eager baseline.

The footprint ratio (queried_combos / schema_combos) makes this
concrete: skewed ≈ 0.15–0.25, uniform ≈ 1.0 after warm-up.

Setup
-----
The log is partitioned into N disjoint batches (default 5) using
stratified trace sampling on the primary grouping attribute so every
perspective-group value is represented in every batch.  Original trace
identifiers and timestamps are preserved.

Batch sequence
--------------
  batch 0   — bootstrap ingest (clear_existing=True for both systems)
  batch 1…N — for adaptive: execute workload slice first to update
              retention counters and promote frequent pairs to L3, then
              ingest and record wall-clock maintenance time.
              For eager baseline: ingest only (no queries).

Running
-------
Single dataset:
    python -m tests.eval.exp_maintenance \\
        --dataset /mnt/datasets/bpic_2017.xes --log-name bpic_2017

All datasets in a directory (one experiment per file):
    python -m tests.eval.exp_maintenance --datasets-dir /mnt/datasets

Key options:
    --n-batches  N         number of ingest batches      (default 5)
    --n-queries  N         queries per workload           (default 50)
    --hot-ratio  F         fraction of hot queries        (default 0.8)
    --max-perspectives N   perspectives per dataset       (default 4)
    --batch-dir  DIR       reuse pre-split batches
    --split-mode MODE      trace_sample|temporal|synthetic

Output
------
One JSONL file per dataset under tests/eval/results/:

    maintenance_<log_name>.jsonl

Record types:

    dataset        — log path, activities, perspectives, split params
    schema         — total co-occurring (perspective, pair) combos
    ingest_complete — bootstrap time for batch 0
    footprint       — queried_combos / schema_combos per workload
    batch_maintenance — wall-clock maintenance time per batch
    query_error     — any query that raised an exception

Example records:
    {"event": "batch_maintenance", "system": "adaptive",
     "workload": "skewed", "batch": 2, "log_name": "bpic_2017",
     "maintenance_s": 0.41, "events_in_batch": 6241}

    {"event": "footprint", "workload": "skewed", "log_name": "bpic_2017",
     "queried_combos": 6, "schema_combos": 32, "ratio": 0.19}
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tests.eval.eval_common import (
    CONFIG_DIR, RESULTS_DIR,
    Recorder, health_check,
    ingest_adaptive, ingest_eager,
    timed_query,
    perspective_pair_set,
    resolve_dataset,
)
from tests.eval.workload import (
    build_workloads, fetch_pair_coverage,
    build_skewed, build_uniform,
)
from tests.eval.batch_splitter import split_log


N_BATCHES         = 5
SPLIT_MODE        = "trace_sample"
ADAPTIVE_CONFIG   = CONFIG_DIR / "adaptive_index.config.json"
EAGER_CONFIG      = CONFIG_DIR / "index.config.json"

# Retention overrides: each query touch counts immediately toward
# promotion so the experiment converges within 5 batches.
RETENTION_OVERRIDES = {"min_query_count": 1, "half_life_seconds": 300}

_REGEX_OP = re.compile(r"[*+?]")

# Extensions recognised as event-log datasets.
_LOG_EXTS = {".csv", ".xes"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def is_simple_pattern(pattern: str) -> bool:
    return not _REGEX_OP.search(pattern)


def filter_simple(workload: list[dict]) -> list[dict]:
    """Discard queries with Kleene/regex operators."""
    return [q for q in workload if is_simple_pattern(q["pattern"])]


def queries_per_batch(workload: list[dict], batch_idx: int,
                      n_batches: int) -> list[dict]:
    """Return the workload slice for `batch_idx` (0-indexed)."""
    chunk = max(1, len(workload) // n_batches)
    return workload[batch_idx * chunk : (batch_idx + 1) * chunk]


def compute_footprint(workload: list[dict], schema_combos: int) -> dict:
    triples = perspective_pair_set(workload)
    queried = len(triples)
    return {
        "queried_combos": queried,
        "schema_combos":  schema_combos,
        "ratio": queried / schema_combos if schema_combos else 0.0,
    }


def measure_schema_combos(log_name: str,
                           perspectives: list[list[str]]) -> int:
    """Sum co-occurring (perspective, ordered-pair) combos from pair_coverage."""
    total = 0
    for gk in perspectives:
        try:
            cov = fetch_pair_coverage(log_name, gk)
            total += len(cov.get("pairs", []))
        except Exception as exc:
            print(f"  [footprint] pair_coverage failed for {gk}: {exc}")
    return total


def _count_events(path: Path) -> int:
    with path.open() as f:
        return max(0, sum(1 for _ in f) - 1)


# ---------------------------------------------------------------------------
# Eager baseline
# ---------------------------------------------------------------------------

def run_eager_baseline(rec: Recorder, log_name: str,
                       batch_paths: list[Path]) -> None:
    print("\n── Eager baseline ──")

    t0   = time.perf_counter()
    body = ingest_eager(log_name, batch_paths[0], EAGER_CONFIG,
                        clear_existing=True)
    n0   = _count_events(batch_paths[0])
    rec.emit("ingest_complete", system="eager", workload="baseline",
             log_name=log_name, batch=0,
             time_s=time.perf_counter() - t0, events_in_batch=n0)
    print(f"  batch 0 (bootstrap): {n0} events")

    for batch_idx, batch_path in enumerate(batch_paths[1:], start=1):
        t0    = time.perf_counter()
        body  = ingest_eager(log_name, batch_path, EAGER_CONFIG)
        maint = time.perf_counter() - t0
        n     = _count_events(batch_path)
        rec.emit("batch_maintenance",
                 system="eager", workload="baseline", log_name=log_name,
                 batch=batch_idx, maintenance_s=maint,
                 events_in_batch=n, reported_time=body.get("time"))
        print(f"  batch {batch_idx}: {n} events, maintenance={maint:.3f}s")


# ---------------------------------------------------------------------------
# Adaptive run
# ---------------------------------------------------------------------------

def run_adaptive_workload(rec: Recorder, log_name: str,
                          batch_paths: list[Path],
                          workload_label: str, workload: list[dict],
                          schema_combos: int,
                          perspectives: list[list[str]]) -> None:
    n_batches = len(batch_paths)
    print(f"\n── Adaptive / '{workload_label}' ──")

    t0   = time.perf_counter()
    ingest_adaptive(
        log_name, batch_paths[0], ADAPTIVE_CONFIG,
        overrides={"perspectives": [{"grouping_keys": gk}
                                    for gk in perspectives]},
        clear_existing=True,
    )
    t_boot = time.perf_counter() - t0
    n0 = _count_events(batch_paths[0])
    rec.emit("ingest_complete", system="adaptive", workload=workload_label,
             log_name=log_name, batch=0, time_s=t_boot, events_in_batch=n0)
    print(f"  batch 0 (bootstrap): {n0} events, {t_boot:.2f}s")

    fp = compute_footprint(workload, schema_combos)
    rec.emit("footprint", workload=workload_label, log_name=log_name, **fp)
    print(f"  footprint: {fp['queried_combos']}/{fp['schema_combos']} "
          f"({fp['ratio']:.0%})")

    for batch_idx, batch_path in enumerate(batch_paths[1:], start=1):
        slice_ = queries_per_batch(workload, batch_idx - 1, n_batches - 1)
        n_queries_run = 0
        for q in slice_:
            try:
                timed_query(
                    log_name=q["log_name"],
                    pattern=q["pattern"],
                    grouping_keys=q["grouping_keys"],
                    retention_overrides=RETENTION_OVERRIDES,
                )
                n_queries_run += 1
            except Exception as exc:
                rec.emit("query_error", system="adaptive",
                         workload=workload_label, log_name=log_name,
                         batch=batch_idx, qid=q["id"], error=str(exc))

        t0    = time.perf_counter()
        body  = ingest_adaptive(log_name, batch_path, ADAPTIVE_CONFIG)
        maint = time.perf_counter() - t0
        n     = _count_events(batch_path)
        rec.emit("batch_maintenance",
                 system="adaptive", workload=workload_label, log_name=log_name,
                 batch=batch_idx, maintenance_s=maint,
                 events_in_batch=n, reported_time=body.get("time"),
                 queries_run=n_queries_run)
        print(f"  batch {batch_idx}: {n} events, "
              f"queries={n_queries_run}, maintenance={maint:.3f}s")


# ---------------------------------------------------------------------------
# Per-dataset entry point
# ---------------------------------------------------------------------------

def run_dataset(
    dataset_path: Path,
    log_name: str,
    *,
    n_batches: int,
    split_mode: str,
    n_queries: int,
    hot_ratio: float,
    max_perspectives: int,
    batch_dir: Path | None,
) -> None:
    """Run the full maintenance experiment for one dataset."""
    print(f"\n{'='*60}")
    print(f"Dataset: {dataset_path}  log_name={log_name}")
    print(f"{'='*60}")

    # Use build_workloads only for schema discovery + context; then build
    # skewed/uniform directly so hot_ratio is forwarded correctly.
    workloads = build_workloads(
        str(dataset_path), log_name,
        max_perspectives=max_perspectives,
    )
    ctx          = workloads.context
    perspectives = ctx.perspectives

    primary_gk = perspectives[0][0] if perspectives and perspectives[0] else None

    print(f"Activities:   {ctx.activities}")
    print(f"Perspectives: {perspectives}")
    print(f"Splitting on: {primary_gk!r}")

    skewed_wl  = filter_simple(build_skewed(ctx,  n_queries=n_queries, hot_ratio=hot_ratio))
    uniform_wl = filter_simple(build_uniform(ctx, n_queries=n_queries))
    print(f"Workload sizes: skewed={len(skewed_wl)}, uniform={len(uniform_wl)}")
    if not skewed_wl or not uniform_wl:
        print(f"  SKIP {log_name}: no simple-pattern queries after filter.")
        return

    # ── Batches ────────────────────────────────────────────────────────────
    if batch_dir and batch_dir.exists():
        batch_paths = sorted(batch_dir.glob("batch_*.csv"))
        if len(batch_paths) < 2:
            print(f"  SKIP {log_name}: --batch-dir has fewer than 2 files.")
            return
        print(f"Reusing {len(batch_paths)} pre-split batches from {batch_dir}")
    else:
        out_dir = RESULTS_DIR / "batches" / log_name
        print(f"Splitting into {n_batches} batches ({split_mode}) ...")
        batch_paths = split_log(
            src=dataset_path,
            n_batches=n_batches,
            mode=split_mode,
            output_dir=out_dir,
            grouping_key=primary_gk,
        )

    print(f"Batches: {len(batch_paths)}  "
          f"(sizes: {[_count_events(p) for p in batch_paths]} events)")

    # ── Record ─────────────────────────────────────────────────────────────
    rec = Recorder("maintenance", f"maintenance_{log_name}.jsonl")
    rec.emit("dataset",
             path=str(dataset_path), log_name=log_name,
             activities=ctx.activities, perspectives=perspectives,
             n_batches=len(batch_paths), split_mode=split_mode,
             n_queries=n_queries, hot_ratio=hot_ratio)

    # ── Eager baseline ─────────────────────────────────────────────────────
    run_eager_baseline(rec, log_name, batch_paths)

    # ── Measure complete schema via pair_coverage (one shared bootstrap) ───
    print("\n── Measuring complete schema (pair_coverage) ──")
    ingest_adaptive(
        log_name, batch_paths[0], ADAPTIVE_CONFIG,
        overrides={"perspectives": [{"grouping_keys": gk}
                                    for gk in perspectives]},
        clear_existing=True,
    )
    schema_combos = measure_schema_combos(log_name, perspectives)
    print(f"  schema_combos = {schema_combos}")
    rec.emit("schema", log_name=log_name,
             schema_combos=schema_combos, perspectives=perspectives)

    # ── Adaptive runs ──────────────────────────────────────────────────────
    for label, wl in [("skewed", skewed_wl), ("uniform", uniform_wl)]:
        run_adaptive_workload(rec, log_name, batch_paths,
                              label, wl, schema_combos, perspectives)

    print(f"\nResults → {rec.path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Adaptive maintenance cost vs query demand.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # ── Dataset selection ──────────────────────────────────────────────────
    src = ap.add_mutually_exclusive_group()
    src.add_argument("--dataset", default=None,
                     help="Path to a single dataset (CSV or XES).")
    src.add_argument("--datasets-dir", default=None, type=Path,
                     help="Directory; every *.csv and *.xes file inside is "
                          "treated as a separate dataset and run independently.")

    ap.add_argument("--log-name", default=None,
                    help="log_name override (single-dataset mode only).")

    # ── Batching ──────────────────────────────────────────────────────────
    ap.add_argument("--n-batches", type=int, default=N_BATCHES,
                    help=f"Number of ingest batches (default {N_BATCHES}).")
    ap.add_argument("--split-mode",
                    choices=["trace_sample", "temporal", "synthetic"],
                    default=SPLIT_MODE,
                    help="How to split the log into batches.")
    ap.add_argument("--batch-dir", default=None, type=Path,
                    help="Reuse pre-split batches from this directory "
                         "(batch_0.csv … batch_N-1.csv).  "
                         "Ignored when --datasets-dir is set.")

    # ── Workload ──────────────────────────────────────────────────────────
    ap.add_argument("--n-queries", type=int, default=50,
                    help="Total queries per workload (skewed and uniform).")
    ap.add_argument("--hot-ratio", type=float, default=0.8,
                    help="Fraction of skewed queries targeting the hot set "
                         "(default 0.8).  The remaining (1−hot_ratio) target "
                         "cold combinations.")
    ap.add_argument("--max-perspectives", type=int, default=4,
                    help="Maximum number of grouping perspectives discovered "
                         "per dataset (default 4).")

    args = ap.parse_args()

    health_check()

    # ── Collect dataset specs ──────────────────────────────────────────────
    if args.datasets_dir:
        datasets_dir = Path(args.datasets_dir)
        if not datasets_dir.is_dir():
            ap.error(f"--datasets-dir {datasets_dir} is not a directory.")
        specs = [
            (p, p.stem)
            for p in sorted(datasets_dir.iterdir())
            if p.suffix.lower() in _LOG_EXTS
        ]
        if not specs:
            ap.error(f"No CSV/XES files found in {datasets_dir}.")
        print(f"Found {len(specs)} dataset(s) in {datasets_dir}:")
        for p, name in specs:
            print(f"  {name:30s}  {p}")
    else:
        spec = resolve_dataset(args.dataset, args.log_name)
        specs = [(spec.path, spec.log_name)]

    # ── Run ───────────────────────────────────────────────────────────────
    for dataset_path, log_name in specs:
        try:
            run_dataset(
                dataset_path=dataset_path,
                log_name=log_name,
                n_batches=args.n_batches,
                split_mode=args.split_mode,
                n_queries=args.n_queries,
                hot_ratio=args.hot_ratio,
                max_perspectives=args.max_perspectives,
                batch_dir=args.batch_dir if not args.datasets_dir else None,
            )
        except Exception as exc:
            print(f"\n[ERROR] {log_name}: {exc}")
            import traceback
            traceback.print_exc()
            print("Continuing with next dataset …")


if __name__ == "__main__":
    main()
