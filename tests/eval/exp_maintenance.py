"""
tests/eval/exp_maintenance.py

Experiment — Adaptive maintenance cost vs query demand.

Goal
----
Show that adaptive incremental maintenance tracks actual query demand
rather than the full index schema.  Two workloads are evaluated:

  skewed  : 80 % of queries (configurable via --hot-ratio) hit a small
            hot perspective-pair set (top-2 pairs by group coverage per
            perspective); 20 % hit cold pairs.
            Adaptive maintains only the hot set → cost << eager baseline.

  uniform : queries spread evenly across all co-occurring pairs →
            adaptive eventually promotes the full schema → cost converges
            to the eager baseline.

Only simple 2-activity patterns ("A B") are used; Kleene/regex operators
are excluded.  Workloads are built from pair_coverage results so they
contain only pairs that actually co-occur in the data.

Eager baseline
--------------
The eager baseline is NOT the SIESTA per-trace indexer.  It is an
*exhaustive adaptive* run: the adaptive indexer bootstrapped with all
discovered perspectives, then ALL real (perspective, pair) combinations
force-promoted to L3 by querying each pair N_FORCE times before the
measurement batches start.  This ensures the baseline pays the full
per-perspective maintenance cost on every subsequent batch, serving as
the upper-bound reference.  After force-promotion a configurable sleep
(--promotion-sleep, default 120 s) is inserted so the asynchronous
background promotion workers finish before batch 1 is ingested.

Pair-coverage correctness
-------------------------
Workload pairs and the footprint denominator are both derived from the
/pair_coverage endpoint AFTER batch 0 is bootstrapped.  This guarantees:
  - No non-existent pairs in the workload (empty scans, zero savings).
  - Footprint ratio is well-defined: queried_combos ≤ schema_combos.
  - The experiment mirrors the warm-up test's pair-selection criterion.

Batch sequence
--------------
  batch 0   — bootstrap (clear_existing=True for both systems)
  batch 1…N — adaptive: workload slice first, then ingest + time.
              Exhaustive baseline: ingest only (no queries).

Running
-------
Single dataset:
    python -m tests.eval.exp_maintenance \\
        --dataset /mnt/datasets/bpic_2017.xes --log-name bpic_2017

All datasets in a directory:
    python -m tests.eval.exp_maintenance --datasets-dir /mnt/datasets

Key options:
    --n-batches       N    ingest batches                  [5]
    --n-queries       N    queries per workload            [50]
    --hot-ratio       F    hot-query fraction in skewed    [0.8]
    --n-hot-pairs     N    hot pairs per perspective       [2]
    --promotion-sleep S    seconds to wait after force-
                          promoting all pairs (eager)      [120]
    --max-perspectives N   perspectives per dataset        [4]

Output
------
tests/eval/results/maintenance_<log_name>.jsonl  (one file per dataset)

Record types:

  dataset            path, activities, perspectives, workload params
  schema             total real (perspective, pair) combos from pair_coverage
  pair_coverage      per-perspective coverage data
  ingest_complete    bootstrap time (batch 0)
  footprint          queried_combos / schema_combos per workload
  batch_maintenance  wall-clock maintenance time per batch
  query_error        any query that raised an exception
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
    ingest_adaptive,
    timed_query, detect_adaptive,
    perspective_pair_set,
    resolve_dataset,
    quote_label,
)
from tests.eval.workload import build_workloads, fetch_pair_coverage
from tests.eval.batch_splitter import split_log


N_BATCHES         = 5
SPLIT_MODE        = "trace_sample"
ADAPTIVE_CONFIG   = CONFIG_DIR / "adaptive_index.config.json"

# Retention overrides applied to ALL queries (workload + force-promote).
# min_query_count=1 so a pair is eligible for L3 after the very first
# query touch; half_life_seconds=300 keeps the decay fast.
RETENTION_OVERRIDES = {"min_query_count": 1, "half_life_seconds": 300}

# Number of queries needed to promote a pair from ABSENT to PERSISTENT
# with default hysteresis=0.15:
#   Q1  ABSENT → TRANSIENT  (lazy scan, savings=0)
#   Q2  TRANSIENT            LRU hit, savings = build_cost; 1*C > 1.15*C? NO
#   Q3  TRANSIENT            LRU hit, savings +=C; 2*C > 1.15*C? YES → promoted
N_FORCE_QUERIES   = 3

_REGEX_OP  = re.compile(r"[*+?]")
_LOG_EXTS  = {".csv", ".xes"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pat2(a: str, b: str) -> str:
    return f"{quote_label(a)} {quote_label(b)}"


def is_simple_pattern(pattern: str) -> bool:
    return not _REGEX_OP.search(pattern)


def queries_per_batch(workload: list[dict], batch_idx: int,
                      n_batches: int) -> list[dict]:
    chunk = max(1, len(workload) // n_batches)
    return workload[batch_idx * chunk : (batch_idx + 1) * chunk]


def _count_events(path: Path) -> int:
    with path.open() as f:
        return max(0, sum(1 for _ in f) - 1)


# ---------------------------------------------------------------------------
# Pair-coverage helpers
# ---------------------------------------------------------------------------

def fetch_all_coverage(
    log_name: str,
    perspectives: list[list[str]],
) -> dict[tuple, list[dict]]:
    """
    Call /pair_coverage for each perspective.

    Returns a dict mapping perspective-key-tuple to the list of
    co-occurring pair dicts ({"source": A, "target": B, "groups": N})
    sorted by groups descending.
    """
    result: dict[tuple, list[dict]] = {}
    for gk in perspectives:
        key = tuple(gk)
        try:
            cov = fetch_pair_coverage(log_name, gk)
            pairs = cov.get("pairs", [])
            result[key] = sorted(pairs, key=lambda p: -p["groups"])
        except Exception as exc:
            print(f"  [coverage] pair_coverage failed for {gk}: {exc}")
            result[key] = []
    return result


def schema_combos_from_coverage(coverage: dict[tuple, list[dict]]) -> int:
    return sum(len(pairs) for pairs in coverage.values())


# ---------------------------------------------------------------------------
# Data-driven workload builders
# ---------------------------------------------------------------------------

def _q(qid, pattern, *, log_name, gkeys, tag):
    return {
        "id":            qid,
        "log_name":      log_name,
        "pattern":       pattern,
        "grouping_keys": list(gkeys),
        "tags":          [tag],
    }


def build_maintenance_skewed(
    coverage: dict[tuple, list[dict]],
    log_name: str,
    n_queries: int,
    hot_ratio: float,
    n_hot_pairs: int,
) -> list[dict]:
    """
    Skewed workload from real co-occurring pairs only.

    For each perspective:
      hot  = top n_hot_pairs by group coverage (most co-occurrences).
      cold = remaining pairs.
    hot_ratio of all queries go to hot, (1-hot_ratio) to cold.
    Hot pairs cross the L3 promotion threshold quickly; cold ones don't.
    """
    n_hot  = int(n_queries * hot_ratio)
    n_cold = n_queries - n_hot

    hot_templates: list[tuple[str, tuple]] = []
    cold_templates: list[tuple[str, tuple]] = []

    for gk_tuple, pairs in coverage.items():
        if not pairs:
            continue
        hot_p  = pairs[:n_hot_pairs]
        cold_p = pairs[n_hot_pairs:]
        for p in hot_p:
            hot_templates.append((_pat2(p["source"], p["target"]), gk_tuple))
        for p in cold_p:
            cold_templates.append((_pat2(p["source"], p["target"]), gk_tuple))

    if not hot_templates:
        return []
    if not cold_templates:
        cold_templates = hot_templates[-1:]

    queries: list[dict] = []
    for i in range(n_hot):
        pat, gk = hot_templates[i % len(hot_templates)]
        queries.append(_q(f"H{i}", pat, log_name=log_name, gkeys=gk, tag="hot"))
    for i in range(n_cold):
        pat, gk = cold_templates[i % len(cold_templates)]
        queries.append(_q(f"C{i}", pat, log_name=log_name, gkeys=gk, tag="cold"))

    # Interleave so cold queries don't bunch at the end.
    hot_qs  = [q for q in queries if "hot"  in q["tags"]]
    cold_qs = [q for q in queries if "cold" in q["tags"]]
    n = len(queries)
    interleaved: list[dict] = []
    hi = ci = 0
    for k in range(n):
        if ci < len(cold_qs) and (k * len(cold_qs)) // n > ci - 1:
            interleaved.append(cold_qs[ci]); ci += 1
        elif hi < len(hot_qs):
            interleaved.append(hot_qs[hi]); hi += 1
        elif ci < len(cold_qs):
            interleaved.append(cold_qs[ci]); ci += 1
    return interleaved


def build_maintenance_uniform(
    coverage: dict[tuple, list[dict]],
    log_name: str,
    n_queries: int,
) -> list[dict]:
    """
    Uniform workload from real co-occurring pairs only.

    All (perspective, pair) combinations queried round-robin.
    """
    templates = [
        (_pat2(p["source"], p["target"]), gk_tuple)
        for gk_tuple, pairs in coverage.items()
        for p in pairs
    ]
    if not templates:
        return []
    queries = []
    for i in range(n_queries):
        pat, gk = templates[i % len(templates)]
        queries.append(_q(f"U{i}", pat, log_name=log_name, gkeys=gk, tag="uniform"))
    return queries


def compute_footprint(workload: list[dict], schema_combos: int) -> dict:
    triples = perspective_pair_set(workload)
    queried = len(triples)
    return {
        "queried_combos": queried,
        "schema_combos":  schema_combos,
        "ratio": queried / schema_combos if schema_combos else 0.0,
    }


# ---------------------------------------------------------------------------
# Exhaustive adaptive baseline (replaces SIESTA ingest_eager)
# ---------------------------------------------------------------------------

def force_promote_all_pairs(
    log_name: str,
    coverage: dict[tuple, list[dict]],
    n_queries: int,
) -> int:
    """
    Run each real (perspective, pair) combination `n_queries` times to
    drive all pairs from ABSENT through TRANSIENT to PERSISTENT.

    Returns the total number of queries executed.
    """
    total = 0
    for gk_tuple, pairs in coverage.items():
        gk = list(gk_tuple)
        for pair in pairs:
            pattern = _pat2(pair["source"], pair["target"])
            for _ in range(n_queries):
                try:
                    detect_adaptive(log_name, pattern, gk,
                                    retention_overrides=RETENTION_OVERRIDES)
                    total += 1
                except Exception as exc:
                    print(f"  [force-promote] {gk_tuple} ({pair['source']},"
                          f"{pair['target']}): {exc}")
    return total


def run_exhaustive_adaptive_baseline(
    rec: Recorder,
    log_name: str,
    batch_paths: list[Path],
    perspectives: list[list[str]],
    coverage: dict[tuple, list[dict]],
    promotion_sleep_s: int,
) -> None:
    """
    Bootstrap the adaptive index and promote ALL real perspective-pair
    combinations to L3 (persistent), then run incremental batches without
    queries.  This is the workload-independent upper-bound baseline.

    The maintenance cost in each batch equals the cost of maintaining
    the COMPLETE perspective-pair schema — what an exhaustive eager
    system would pay unconditionally.
    """
    print("\n── Exhaustive adaptive baseline ──")

    t0 = time.perf_counter()
    ingest_adaptive(
        log_name, batch_paths[0], ADAPTIVE_CONFIG,
        overrides={"perspectives": [{"grouping_keys": gk}
                                    for gk in perspectives]},
        clear_existing=True,
    )
    n0 = _count_events(batch_paths[0])
    rec.emit("ingest_complete", system="eager", workload="baseline",
             log_name=log_name, batch=0,
             time_s=time.perf_counter() - t0, events_in_batch=n0)
    print(f"  batch 0 (bootstrap): {n0} events")

    # Force-promote every real pair to L3.
    schema_size = schema_combos_from_coverage(coverage)
    print(f"  Force-promoting {schema_size} (perspective, pair) combos "
          f"({N_FORCE_QUERIES} queries each) …")
    t0   = time.perf_counter()
    done = force_promote_all_pairs(log_name, coverage, N_FORCE_QUERIES)
    print(f"  {done} queries in {time.perf_counter() - t0:.1f}s")

    # Wait for background promotion workers to finish building L3 tables.
    if promotion_sleep_s > 0:
        print(f"  Sleeping {promotion_sleep_s}s for async promotions …")
        time.sleep(promotion_sleep_s)

    for batch_idx, batch_path in enumerate(batch_paths[1:], start=1):
        t0    = time.perf_counter()
        body  = ingest_adaptive(log_name, batch_path, ADAPTIVE_CONFIG)
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

def run_adaptive_workload(
    rec: Recorder,
    log_name: str,
    batch_paths: list[Path],
    workload_label: str,
    workload: list[dict],
    schema_combos: int,
    perspectives: list[list[str]],
) -> None:
    """
    Bootstrap and run the adaptive indexer, interleaving a workload slice
    before each incremental batch to drive retention promotions.
    """
    n_batches = len(batch_paths)
    print(f"\n── Adaptive / '{workload_label}' ──")

    t0 = time.perf_counter()
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
    n_hot_pairs: int,
    max_perspectives: int,
    promotion_sleep_s: int,
    batch_dir: Path | None,
) -> None:
    print(f"\n{'='*60}")
    print(f"Dataset: {dataset_path}  log_name={log_name}")
    print(f"{'='*60}")

    workloads    = build_workloads(str(dataset_path), log_name,
                                   max_perspectives=max_perspectives)
    ctx          = workloads.context
    perspectives = ctx.perspectives
    primary_gk   = perspectives[0][0] if perspectives and perspectives[0] else None

    print(f"Activities:   {ctx.activities}")
    print(f"Perspectives: {perspectives}")
    print(f"Splitting on: {primary_gk!r}")

    # ── Batches ────────────────────────────────────────────────────────────
    if batch_dir and batch_dir.exists():
        batch_paths = sorted(batch_dir.glob("batch_*.csv"))
        if len(batch_paths) < 2:
            print(f"  SKIP {log_name}: --batch-dir has fewer than 2 files."); return
        print(f"Reusing {len(batch_paths)} pre-split batches from {batch_dir}")
    else:
        out_dir = RESULTS_DIR / "batches" / log_name
        print(f"Splitting into {n_batches} batches ({split_mode}) …")
        batch_paths = split_log(src=dataset_path, n_batches=n_batches,
                                mode=split_mode, output_dir=out_dir,
                                grouping_key=primary_gk)

    print(f"Batches: {len(batch_paths)}  "
          f"(sizes: {[_count_events(p) for p in batch_paths]} events)")

    rec = Recorder("maintenance", f"maintenance_{log_name}.jsonl")
    rec.emit("dataset", path=str(dataset_path), log_name=log_name,
             activities=ctx.activities, perspectives=perspectives,
             n_batches=len(batch_paths), split_mode=split_mode,
             n_queries=n_queries, hot_ratio=hot_ratio,
             n_hot_pairs=n_hot_pairs)

    # ── Shared bootstrap to discover real pairs via pair_coverage ──────────
    # This run is for schema/workload discovery only.  Each workload run
    # will re-bootstrap cleanly with clear_existing=True.
    print("\n── Shared bootstrap (pair_coverage discovery) ──")
    ingest_adaptive(
        log_name, batch_paths[0], ADAPTIVE_CONFIG,
        overrides={"perspectives": [{"grouping_keys": gk}
                                    for gk in perspectives]},
        clear_existing=True,
    )
    coverage      = fetch_all_coverage(log_name, perspectives)
    schema_combos = schema_combos_from_coverage(coverage)
    print(f"  Real (perspective, pair) combos: {schema_combos}")

    for gk_tuple, pairs in coverage.items():
        rec.emit("pair_coverage", log_name=log_name,
                 perspective=list(gk_tuple), n_pairs=len(pairs),
                 pairs=[{"source": p["source"], "target": p["target"],
                         "groups": p["groups"]} for p in pairs])

    rec.emit("schema", log_name=log_name,
             schema_combos=schema_combos, perspectives=perspectives)

    if schema_combos == 0:
        print(f"  SKIP {log_name}: no co-occurring pairs found in coverage."); return

    # ── Build data-driven workloads (only real pairs) ──────────────────────
    skewed_wl  = build_maintenance_skewed(coverage, log_name, n_queries,
                                          hot_ratio, n_hot_pairs)
    uniform_wl = build_maintenance_uniform(coverage, log_name, n_queries)

    # Safety-filter (should be a no-op since we build from pair names, but
    # activity labels with special chars could still contain regex chars).
    skewed_wl  = [q for q in skewed_wl  if is_simple_pattern(q["pattern"])]
    uniform_wl = [q for q in uniform_wl if is_simple_pattern(q["pattern"])]

    if not skewed_wl or not uniform_wl:
        print(f"  SKIP {log_name}: empty workload after filter."); return

    fp_skewed  = compute_footprint(skewed_wl,  schema_combos)
    fp_uniform = compute_footprint(uniform_wl, schema_combos)
    print(f"  Skewed  footprint: {fp_skewed['queried_combos']}/{schema_combos} "
          f"({fp_skewed['ratio']:.0%})")
    print(f"  Uniform footprint: {fp_uniform['queried_combos']}/{schema_combos} "
          f"({fp_uniform['ratio']:.0%})")

    # ── Exhaustive adaptive baseline ───────────────────────────────────────
    run_exhaustive_adaptive_baseline(
        rec, log_name, batch_paths,
        perspectives, coverage, promotion_sleep_s,
    )

    # ── Adaptive runs ──────────────────────────────────────────────────────
    for label, wl in [("skewed", skewed_wl), ("uniform", uniform_wl)]:
        run_adaptive_workload(rec, log_name, batch_paths, label, wl,
                              schema_combos, perspectives)

    print(f"\nResults → {rec.path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Adaptive maintenance cost vs query demand.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    src = ap.add_mutually_exclusive_group()
    src.add_argument("--dataset",      default=None)
    src.add_argument("--datasets-dir", default=None, type=Path,
                     help="Run on every *.csv / *.xes file in this directory.")

    ap.add_argument("--log-name",         default=None)
    ap.add_argument("--n-batches",        type=int,   default=N_BATCHES)
    ap.add_argument("--split-mode",
                    choices=["trace_sample", "temporal", "synthetic"],
                    default=SPLIT_MODE)
    ap.add_argument("--batch-dir",        default=None, type=Path,
                    help="Reuse pre-split batches (single-dataset mode only).")
    ap.add_argument("--n-queries",        type=int,   default=50,
                    help="Total queries per workload (default 50).")
    ap.add_argument("--hot-ratio",        type=float, default=0.8,
                    help="Fraction of skewed queries targeting the hot set (default 0.8).")
    ap.add_argument("--n-hot-pairs",      type=int,   default=2,
                    help="Number of hot pairs per perspective (default 2).")
    ap.add_argument("--max-perspectives", type=int,   default=4)
    ap.add_argument("--promotion-sleep",  type=int,   default=120,
                    help="Seconds to wait for async L3 promotions to finish "
                         "before running baseline batches (default 120).")

    args = ap.parse_args()

    health_check()

    if args.datasets_dir:
        d = Path(args.datasets_dir)
        if not d.is_dir():
            ap.error(f"{d} is not a directory.")
        specs = [(p, p.stem)
                 for p in sorted(d.iterdir())
                 if p.suffix.lower() in _LOG_EXTS]
        if not specs:
            ap.error(f"No CSV/XES files in {d}.")
        print(f"Found {len(specs)} dataset(s) in {d}")
    else:
        spec = resolve_dataset(args.dataset, args.log_name)
        specs = [(spec.path, spec.log_name)]

    for dataset_path, log_name in specs:
        try:
            run_dataset(
                dataset_path=dataset_path,
                log_name=log_name,
                n_batches=args.n_batches,
                split_mode=args.split_mode,
                n_queries=args.n_queries,
                hot_ratio=args.hot_ratio,
                n_hot_pairs=args.n_hot_pairs,
                max_perspectives=args.max_perspectives,
                promotion_sleep_s=args.promotion_sleep,
                batch_dir=args.batch_dir if not args.datasets_dir else None,
            )
        except Exception as exc:
            print(f"\n[ERROR] {log_name}: {exc}")
            import traceback
            traceback.print_exc()
            print("Continuing …")


if __name__ == "__main__":
    main()
