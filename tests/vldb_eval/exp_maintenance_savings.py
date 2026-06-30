"""
tests/eval/exp_maintenance_savings.py

Experiment — Eager vs Adaptive incremental maintenance cost.

Goal
----
Show that the naive eager approach of pre-building indices for every
perspective is increasingly expensive as the number of perspectives
grows, while the adaptive system only maintains what the workload
demands.

Core comparison
---------------
The eager indexer (SIESTA) builds the full PairsIndex for ALL A²
activity pairs on every batch ingest, but it only supports a single
grouping key (trace_id column) per log.  To simulate eager indexing
across P perspectives, we ingest the same batch P+1 times: once for
the original case-centric perspective and once for each attribute
perspective (resource, department, region, …), each under a separate
log_name with field_mappings.csv.trace_id remapped to the perspective
attribute.  The eager total is the sum.

The adaptive indexer handles all perspectives in a single ingest call,
maintaining only the pairs that the workload has promoted to PERSISTENT.
Under a skewed workload, this is a small subset.

Protocol
--------
For each batch k ∈ {1, …, N}:

  1. Issue a workload slice (adaptive only) to drive promotions.
  2. Eager:    ingest batch_k under each perspective log_name, sum times.
     Adaptive: ingest batch_k once via the adaptive endpoint.
  3. Record both times.

The experiment produces a per-batch bar chart (eager stacked by
perspective vs adaptive single bar) and a cumulative time comparison.

The independent variable is batch index; the dependent variable is
wall-clock maintenance time.  The story is: eager maintenance grows
linearly with the number of perspectives, while adaptive maintenance
stays bounded by the workload footprint.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tests.vldb_eval.eval_common import (
    API_BASE, API_TIMEOUT_S, EAGER_INDEXER,
    CONFIG_DIR, RESULTS_DIR,
    Recorder, health_check,
    ingest_adaptive, ingest_eager,
    timed_query, detect_adaptive,
    perspective_pair_set,
    resolve_dataset,
    quote_label,
    _guess_mime,
)
from tests.vldb_eval.workload import build_workloads, fetch_pair_coverage
from tests.vldb_eval.batch_splitter import split_log

import requests
from urllib.parse import urljoin

# ── Defaults ──────────────────────────────────────────────────────────────
N_BATCHES          = 5
SPLIT_MODE         = "trace_sample"
ADAPTIVE_CONFIG    = CONFIG_DIR / "adaptive_index.config.json"

RETENTION_OVERRIDES = {"min_query_count": 1, "half_life_seconds": 300}
N_FORCE_QUERIES    = 3

_REGEX_OP  = re.compile(r"[*+?]")
_LOG_EXTS  = {".csv", ".xes"}


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

def _pat2(a: str, b: str) -> str:
    return f"{quote_label(a)} {quote_label(b)}"


def is_simple_pattern(pattern: str) -> bool:
    return not _REGEX_OP.search(pattern)


def _count_events(path: Path) -> int:
    with path.open() as f:
        return max(0, sum(1 for _ in f) - 1)


# ═══════════════════════════════════════════════════════════════════════════
# Eager ingest with field_mappings override
# ═══════════════════════════════════════════════════════════════════════════

def ingest_eager_as_perspective(
    log_name: str,
    dataset_path: Path,
    config_path: Path,
    perspective_key: str | None = None,
    clear_existing: bool = False,
) -> dict:
    """
    Call the eager indexer.  When perspective_key is provided, override
    field_mappings so that trace_id maps to that attribute column.  This
    makes the eager indexer build its full PairsIndex grouped by that
    attribute instead of the original trace_id.

    When perspective_key is None, use the default (case-centric) mapping.
    """
    config = json.loads(config_path.read_text())
    config["log_name"] = log_name
    config["clear_existing"] = clear_existing

    if perspective_key is not None:
        # Override the trace_id mapping for CSV format.
        fmt = dataset_path.suffix.lstrip(".").lower()
        if fmt == "xes":
            fmt = "xes"
        elif fmt in ("csv", "tsv"):
            fmt = "csv"
        else:
            fmt = "csv"

        if "field_mappings" not in config:
            config["field_mappings"] = {}
        if fmt not in config["field_mappings"]:
            config["field_mappings"][fmt] = {}
        config["field_mappings"][fmt]["trace_id"] = perspective_key

    with dataset_path.open("rb") as fp:
        r = requests.post(
            urljoin(API_BASE, f"/{EAGER_INDEXER}/run"),
            files={"log_file": (dataset_path.name, fp, _guess_mime(dataset_path))},
            data={"index_config": json.dumps(config)},
            timeout=API_TIMEOUT_S,
        )
    r.raise_for_status()
    return r.json()


# ═══════════════════════════════════════════════════════════════════════════
# Pair-coverage & workload helpers
# ═══════════════════════════════════════════════════════════════════════════

def fetch_all_coverage(log_name, perspectives, activities=None):
    result = {}
    for gk in perspectives:
        key = tuple(gk)
        try:
            cov = fetch_pair_coverage(log_name, gk, activities=activities)
            pairs = cov.get("pairs", [])
            result[key] = sorted(pairs, key=lambda p: -p["groups"])
        except Exception as exc:
            print(f"  [coverage] pair_coverage failed for {gk}: {exc}")
            result[key] = []
    return result


def schema_combos_from_coverage(coverage):
    return sum(len(pairs) for pairs in coverage.values())


def _q(qid, pattern, *, log_name, gkeys, tag):
    return {"id": qid, "log_name": log_name, "pattern": pattern,
            "grouping_keys": list(gkeys), "tags": [tag]}


def build_skewed_workload(coverage, log_name, n_queries, hot_ratio, n_hot_pairs):
    n_hot  = int(n_queries * hot_ratio)
    n_cold = n_queries - n_hot
    hot_templates, cold_templates = [], []
    for gk_tuple, pairs in coverage.items():
        if not pairs:
            continue
        for p in pairs[:n_hot_pairs]:
            hot_templates.append((_pat2(p["source"], p["target"]), gk_tuple))
        for p in pairs[n_hot_pairs:]:
            cold_templates.append((_pat2(p["source"], p["target"]), gk_tuple))
    if not hot_templates and not cold_templates:
        return []
    if not hot_templates:
        hot_templates = cold_templates[:1]
    if not cold_templates:
        cold_templates = hot_templates[-1:]
    queries = []
    for i in range(n_hot):
        pat, gk = hot_templates[i % len(hot_templates)]
        queries.append(_q(f"H{i}", pat, log_name=log_name, gkeys=gk, tag="hot"))
    for i in range(n_cold):
        pat, gk = cold_templates[i % len(cold_templates)]
        queries.append(_q(f"C{i}", pat, log_name=log_name, gkeys=gk, tag="cold"))
    return queries


def compute_footprint(workload, schema_combos):
    triples = perspective_pair_set(workload)
    queried = len(triples)
    return {"queried_combos": queried, "schema_combos": schema_combos,
            "ratio": queried / schema_combos if schema_combos else 0.0}


# ═══════════════════════════════════════════════════════════════════════════
# Adaptive workload promotion
# ═══════════════════════════════════════════════════════════════════════════

def promote_pairs_via_queries(log_name, workload, n_rounds=3):
    seen, unique = set(), []
    for q in workload:
        key = (q["pattern"], tuple(q["grouping_keys"]))
        if key not in seen:
            seen.add(key)
            unique.append(q)
    total = 0
    for _ in range(n_rounds):
        for q in unique:
            try:
                detect_adaptive(log_name, q["pattern"], q["grouping_keys"],
                                retention_overrides=RETENTION_OVERRIDES)
                total += 1
            except:
                pass
    return total


def queries_per_batch(workload, batch_idx, n_batches):
    chunk = max(1, len(workload) // n_batches)
    return workload[batch_idx * chunk : (batch_idx + 1) * chunk]


# ═══════════════════════════════════════════════════════════════════════════
# Per-dataset entry point
# ═══════════════════════════════════════════════════════════════════════════

def run_dataset(
    dataset_path, log_name, *,
    n_batches, split_mode, n_queries, n_hot_pairs,
    max_perspectives, promotion_sleep_s, batch_dir,
):
    print(f"\n{'='*64}")
    print(f"  Maintenance savings — {dataset_path}")
    print(f"{'='*64}")

    # ── Schema discovery ──────────────────────────────────────────────
    workloads = build_workloads(str(dataset_path), log_name,
                                max_perspectives=max_perspectives)
    ctx          = workloads.context
    perspectives = ctx.perspectives    # e.g. [["resource"], ["department"], ["region"]]
    activities   = ctx.activities
    primary_gk   = perspectives[0][0] if perspectives and perspectives[0] else None

    print(f"Activities:     {activities}")
    print(f"Perspectives:   {perspectives}")
    print(f"N perspectives: {len(perspectives)}")

    # ── Batches ───────────────────────────────────────────────────────
    if batch_dir and batch_dir.exists():
        batch_paths = sorted(batch_dir.glob("batch_*.csv"))
    else:
        out_dir = RESULTS_DIR / "batches" / log_name
        batch_paths = split_log(src=dataset_path, n_batches=n_batches,
                                mode=split_mode, output_dir=out_dir,
                                grouping_key=primary_gk)

    print(f"Batches: {len(batch_paths)}")
    print(f"Events per batch: {[_count_events(p) for p in batch_paths]}")

    rec = Recorder("maintenance_savings",
                   f"maintenance_savings_{log_name}.jsonl")

    # ── Bootstrap adaptive to discover pair coverage ──────────────────
    print("\n── Bootstrap + pair-coverage discovery ──")
    ingest_adaptive(
        log_name, batch_paths[0], ADAPTIVE_CONFIG,
        overrides={"perspectives": [{"grouping_keys": gk}
                                    for gk in perspectives]},
        clear_existing=True,
    )
    coverage = fetch_all_coverage(log_name, perspectives, activities=activities)
    schema_combos = schema_combos_from_coverage(coverage)
    print(f"  Schema combos: {schema_combos}")
    for gk_tuple, pairs in coverage.items():
        print(f"  {list(gk_tuple)}: {len(pairs)} pairs")
        rec.emit("pair_coverage", log_name=log_name,
                 perspective=list(gk_tuple), n_pairs=len(pairs),
                 pairs=[{"source": p["source"], "target": p["target"],
                         "groups": p["groups"]} for p in pairs])

    # ── Build a skewed workload for the adaptive system ───────────────
    # Auto-compute n_queries so every pair gets touched at σ=0.
    import math
    min_queries = schema_combos * N_FORCE_QUERIES
    effective_n_queries = max(n_queries, min_queries)
    if effective_n_queries != n_queries:
        print(f"  Auto-adjusted n_queries: {n_queries} → {effective_n_queries}")
    n_queries = effective_n_queries

    # Use a skewed workload (hot_ratio=0.8): 80% hot, 20% cold.
    workload = build_skewed_workload(coverage, log_name, n_queries,
                                     hot_ratio=0.8, n_hot_pairs=n_hot_pairs)
    workload = [q for q in workload if is_simple_pattern(q["pattern"])]
    fp = compute_footprint(workload, schema_combos)
    print(f"  Workload: {len(workload)} queries, "
          f"footprint={fp['queried_combos']}/{schema_combos} ({fp['ratio']:.0%})")

    # ── Construct perspective log names for eager ─────────────────────
    # log_name is used as an S3/local path prefix, so perspective keys
    # containing colons (XES convention: "org:resource") or other
    # path-unsafe characters must be sanitised before embedding.
    def _safe_label(k: str) -> str:
        return re.sub(r"[^a-zA-Z0-9_-]", "_", k)

    eager_perspectives = []
    # Original case-centric (default trace_id mapping)
    eager_perspectives.append({
        "label":           "case",
        "log_name":        f"{log_name}__case",
        "perspective_key": None,
    })
    # One per discovered perspective
    for gk in perspectives:
        key   = gk[0]             # e.g. "org:resource" (original for field_mappings)
        label = _safe_label(key)  # e.g. "org_resource" (safe for log_name/path)
        eager_perspectives.append({
            "label":           label,
            "log_name":        f"{log_name}__{label}",
            "perspective_key": key,
        })

    print(f"\n  Eager will ingest under {len(eager_perspectives)} log names:")
    for ep in eager_perspectives:
        print(f"    {ep['log_name']} (trace_id → {ep['perspective_key'] or 'original'})")

    rec.emit("dataset",
             path=str(dataset_path), log_name=log_name,
             activities=activities, perspectives=perspectives,
             n_batches=len(batch_paths), split_mode=split_mode,
             n_queries=n_queries, n_hot_pairs=n_hot_pairs,
             schema_combos=schema_combos,
             n_eager_perspectives=len(eager_perspectives),
             protocol="eager_sum_vs_adaptive")

    # ══════════════════════════════════════════════════════════════════
    # Bootstrap phase (batch 0)
    # ══════════════════════════════════════════════════════════════════
    print("\n── Bootstrap (batch 0) ──")

    # Eager: bootstrap each perspective log
    for ep in eager_perspectives:
        t0 = time.perf_counter()
        ingest_eager_as_perspective(
            ep["log_name"], batch_paths[0], ADAPTIVE_CONFIG,
            perspective_key=ep["perspective_key"],
            clear_existing=True,
        )
        bt = time.perf_counter() - t0
        print(f"  eager/{ep['label']}: bootstrap {bt:.2f}s")

    # Adaptive: bootstrap + initial workload promotion
    t0 = time.perf_counter()
    ingest_adaptive(
        log_name, batch_paths[0], ADAPTIVE_CONFIG,
        overrides={"perspectives": [{"grouping_keys": gk}
                                    for gk in perspectives]},
        clear_existing=True,
    )
    bt = time.perf_counter() - t0
    print(f"  adaptive: bootstrap {bt:.2f}s")

    # Drive promotions with first workload slice
    print("  adaptive: promoting via workload queries …")
    t0 = time.perf_counter()
    done = promote_pairs_via_queries(log_name, workload, n_rounds=N_FORCE_QUERIES)
    print(f"  {done} queries in {time.perf_counter()-t0:.1f}s")

    if promotion_sleep_s > 0:
        print(f"  waiting {promotion_sleep_s}s for async promotions …")
        time.sleep(promotion_sleep_s)

    # Stabilisation: ingest batch 1 once (not measured) to drain any
    # leftover async build_pair_persistent jobs from the promotion phase.
    print("  adaptive: stabilisation ingest (not measured) …", end="", flush=True)
    t0 = time.perf_counter()
    ingest_adaptive(log_name, batch_paths[1], ADAPTIVE_CONFIG)
    stab_t = time.perf_counter() - t0
    print(f" {stab_t:.2f}s (discarded)")
    rec.emit("stabilisation", log_name=log_name, stabilisation_s=stab_t)

    # Also bootstrap the eager perspective logs with batch 1
    # so both systems have ingested the same data before measurement.
    for ep in eager_perspectives:
        ingest_eager_as_perspective(
            ep["log_name"], batch_paths[1], ADAPTIVE_CONFIG,
            perspective_key=ep["perspective_key"],
        )

    # ══════════════════════════════════════════════════════════════════
    # Measurement (batches 2 … N-1, pure ingest, no queries)
    # ══════════════════════════════════════════════════════════════════
    eager_cumulative   = 0.0
    adaptive_cumulative = 0.0
    measurement_paths = batch_paths[2:]  # skip bootstrap (0) and stabilisation (1)
    n_measurement = len(measurement_paths)

    if n_measurement == 0:
        print("  WARNING: no measurement batches left. Increase --n-batches.")
        return

    print(f"\n── Measurement ({n_measurement} batches, no queries) ──")
    print(f"  {'batch':>5s}  {'eager_total':>12s}  {'adaptive':>10s}  ", end="")
    for ep in eager_perspectives:
        print(f"  {ep['label']:>10s}", end="")
    print()
    print(f"  {'─'*5}  {'─'*12}  {'─'*10}", end="")
    for _ in eager_perspectives:
        print(f"  {'─'*10}", end="")
    print()

    for batch_idx, batch_path in enumerate(measurement_paths, start=1):
        n_events = _count_events(batch_path)

        # ── Adaptive: pure ingest, no queries ─────────────────────────
        t0 = time.perf_counter()
        body_a = ingest_adaptive(log_name, batch_path, ADAPTIVE_CONFIG)
        t_adaptive = time.perf_counter() - t0
        adaptive_cumulative += t_adaptive

        # ── Eager: ingest under each perspective log ──────────────────
        eager_per_perspective = {}
        eager_batch_total = 0.0
        for ep in eager_perspectives:
            t0 = time.perf_counter()
            ingest_eager_as_perspective(
                ep["log_name"], batch_path, ADAPTIVE_CONFIG,
                perspective_key=ep["perspective_key"],
            )
            t_ep = time.perf_counter() - t0
            eager_per_perspective[ep["label"]] = t_ep
            eager_batch_total += t_ep

        eager_cumulative += eager_batch_total

        # Record
        rec.emit("batch_maintenance",
                 log_name=log_name, batch=batch_idx,
                 events_in_batch=n_events,
                 eager_total_s=eager_batch_total,
                 eager_per_perspective=eager_per_perspective,
                 adaptive_s=t_adaptive,
                 adaptive_reported_time=body_a.get("time"),
                 eager_cumulative_s=eager_cumulative,
                 adaptive_cumulative_s=adaptive_cumulative)

        # Print
        print(f"  {batch_idx:5d}  {eager_batch_total:12.3f}  "
              f"{t_adaptive:10.3f}", end="")
        for ep in eager_perspectives:
            print(f"  {eager_per_perspective[ep['label']]:10.3f}", end="")
        print()

    # ── Summary ───────────────────────────────────────────────────────
    savings = 1.0 - (adaptive_cumulative / eager_cumulative) if eager_cumulative > 0 else None
    sr = f"{savings:.1%}" if savings is not None else "N/A"

    rec.emit("summary",
             log_name=log_name,
             eager_cumulative_s=eager_cumulative,
             adaptive_cumulative_s=adaptive_cumulative,
             savings_ratio=savings,
             n_perspectives=len(eager_perspectives),
             n_batches=n_measurement,
             schema_combos=schema_combos,
             footprint=fp)

    print(f"\n{'─'*64}")
    print(f"  SUMMARY — {log_name}")
    print(f"{'─'*64}")
    print(f"  Perspectives:         {len(eager_perspectives)} "
          f"(case + {len(perspectives)} attribute)")
    print(f"  Schema combos:        {schema_combos}")
    print(f"  Workload footprint:   {fp['queried_combos']}/{schema_combos} "
          f"({fp['ratio']:.0%})")
    print(f"  Eager cumulative:     {eager_cumulative:.3f}s")
    print(f"  Adaptive cumulative:  {adaptive_cumulative:.3f}s")
    print(f"  Savings:              {sr}")
    print(f"\nResults → {rec.path}")


# ═══════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(
        description="Eager (all perspectives) vs Adaptive maintenance cost.",
    )

    src = ap.add_mutually_exclusive_group()
    src.add_argument("--dataset",      default=None)
    src.add_argument("--datasets-dir", default=None, type=Path)

    ap.add_argument("--log-name",         default=None)
    ap.add_argument("--n-batches",        type=int,   default=N_BATCHES)
    ap.add_argument("--split-mode",
                    choices=["trace_sample", "temporal", "synthetic"],
                    default=SPLIT_MODE)
    ap.add_argument("--batch-dir",        default=None, type=Path)
    ap.add_argument("--n-queries",        type=int,   default=50)
    ap.add_argument("--n-hot-pairs",      type=int,   default=2)
    ap.add_argument("--max-perspectives", type=int,   default=4)
    ap.add_argument("--promotion-sleep",  type=int,   default=120)

    args = ap.parse_args()
    health_check()

    if args.datasets_dir:
        d = Path(args.datasets_dir)
        specs = [(p, p.stem) for p in sorted(d.iterdir())
                 if p.suffix.lower() in _LOG_EXTS]
    else:
        spec = resolve_dataset(args.dataset, args.log_name)
        specs = [(spec.path, spec.log_name)]

    for dataset_path, log_name in specs:
        try:
            run_dataset(
                dataset_path=dataset_path, log_name=log_name,
                n_batches=args.n_batches, split_mode=args.split_mode,
                n_queries=args.n_queries, n_hot_pairs=args.n_hot_pairs,
                max_perspectives=args.max_perspectives,
                promotion_sleep_s=args.promotion_sleep,
                batch_dir=args.batch_dir if not args.datasets_dir else None,
            )
        except Exception as exc:
            print(f"\n[ERROR] {log_name}: {exc}")
            import traceback; traceback.print_exc()


if __name__ == "__main__":
    main()