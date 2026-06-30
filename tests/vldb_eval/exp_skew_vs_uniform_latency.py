"""
tests/eval/exp_skew_vs_uniform_latency.py

Experiment 6.3.2 — Query-latency convergence: skewed vs. uniform workloads.

Goal
----
Show that aggregate query execution time converges to near-optimal levels
at different speeds under the two canonical workload distributions, and
that the adaptive index faithfully tracks demand: pairs are promoted in
proportion to query frequency.

Design
------
A single static log is ingested via the adaptive indexer.  Two workloads
are built from the /pair_coverage endpoint so that every query targets a
pair that genuinely co-occurs in the data.

  skewed  : per round, each of the n_hot_pairs top-coverage pairs is
            queried REPS_PER_HOT times (≈ 4×); the remaining cold pairs
            are each queried once.  Hot queries account for roughly
            HOT_RATIO (default 0.80) of all queries per round.
            Hot pairs accumulate min_query_count (default 3) within the
            first round and are promoted to PERSISTENT during the
            post-round sleep; cold pairs rarely reach the threshold.

  uniform : per round, each discovered pair is queried exactly once.
            Query mass is spread uniformly: every pair crosses
            min_query_count at round min_query_count (= 3), so the
            full schema is promoted simultaneously after round 3.

Each workload is run for N_ROUNDS rounds against the same static log.
The adaptive catalog is cleared (ingest_adaptive clear_existing=True)
before each workload so the two runs start from identical empty state.
A fixed sleep of BETWEEN_ROUND_SLEEP seconds follows every round to let
any background pair-materialisation workers complete before the next
round reads from Delta tables.

Key measurements per round
  - Mean and median query wall-clock latency (s)
  - Fraction of queries answered from PERSISTENT / TRANSIENT / ABSENT tier

Expected result
---------------
Round 1   Both workloads: all pairs ABSENT, full lazy scans (~20 s avg).

Round 2   Skewed:  hot pairs PERSISTENT after round-1 sleep.
                   ~80 % of queries hit Delta (~1.5 s).
                   Mean ≈ 0.80×1.5 + 0.20×20 ≈ 5.2 s.
          Uniform: all pairs TRANSIENT after round-1 (LRU cache, ~3 s).
                   Mean ≈ 3 s.

Round 3   Skewed:  unchanged (~5.2 s).  Cold pairs still ABSENT.
          Uniform: all pairs still TRANSIENT (~3 s).

Round 4+  Skewed:  unchanged (~5.2 s).  Cold pairs never promoted.
          Uniform: all pairs PERSISTENT after round-3 sleep (~1.5 s).
                   Mean ≈ 1.5 s.

The curves cross between rounds 1 and 2: the skewed workload obtains
the initial benefit of promotion faster (because demand is concentrated),
but the uniform workload ultimately achieves lower mean latency (because
every pair is eventually promoted).  This confirms that the adaptive
index trades off convergence speed for breadth of materialisation in
direct proportion to the query distribution.

Comparison with existing experiments
-------------------------------------
exp_warmup        (6.3.1): per-pair learning curve in a single query
                  stream; x-axis = repetition count for that pair.
                  Does NOT show aggregate workload latency per round,
                  and does NOT contrast two distinct workload regimes.

exp_maintenance   : measures batch *ingest* cost (maintenance_s) under
                  skewed vs uniform footprints.  Does NOT measure query
                  execution time.

This experiment fills the gap: aggregate *query* latency across repeated
execution of a whole workload, comparing two query distributions.

Output
------
results/6_3_2_skew_uniform_<log_name>.jsonl

Record types
  dataset        path, activities, perspectives, workload params
  schema         schema_combos, workload summary
  round_start    workload, round_num
  query          workload, round_num, seq, qid, pattern, grouping_keys,
                 tag, shared_pair, latency_s, total, tier,
                 pair_status_after
  round_summary  workload, round_num, n_queries, mean_latency_s,
                 median_latency_s, frac_persistent, frac_transient,
                 frac_absent, round_wall_s
  query_error    workload, round_num, seq, qid, error

Running
-------
Single dataset:
    python -m tests.vldb_eval.exp_skew_vs_uniform_latency \\
        --dataset /mnt/datasets/bpic_2017.xes --log-name bpic_2017

All datasets in a directory:
    python -m tests.vldb_eval.exp_skew_vs_uniform_latency \\
        --datasets-dir /mnt/datasets

Key CLI options:
    --n-rounds          N    rounds per workload              [6]
    --n-hot-pairs       N    hot pairs per perspective        [3]
    --hot-ratio         F    target hot-query fraction        [0.80]
    --max-perspectives  N    perspectives per dataset         [4]
    --sleep             S    seconds to sleep after each round [90]
"""

from __future__ import annotations

import argparse
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tests.vldb_eval.eval_common import (
    CONFIG_DIR, RESULTS_DIR,
    Recorder, health_check,
    ingest_adaptive,
    timed_query,
    resolve_dataset,
    quote_label,
)
from tests.vldb_eval.workload import (
    build_workloads,
    fetch_pair_coverage,
)


# ---------------------------------------------------------------------------
# Defaults  (all overridable via CLI)
# ---------------------------------------------------------------------------

N_ROUNDS                = 6    # rounds per workload
N_HOT_PAIRS             = 3    # hot pairs per perspective (skewed only)
MAX_PAIRS_PER_PERSP     = 25   # cap on pairs sampled per perspective
# Cold pairs = MAX_PAIRS_PER_PERSP - N_HOT_PAIRS = 22 per perspective.
# With 4 perspectives: 4 × (3×4 + 22) = 136 skewed queries/round,
#                      4 × 25 = 100 uniform queries/round.
# Round 1 wall time at ~20s/query: ~45 min (skewed) / ~33 min (uniform).
HOT_RATIO            = 0.80    # kept for CLI compat; not used in reps calc
BETWEEN_ROUND_SLEEP  = 90      # seconds — wait for async materialisation
MIN_PERSP_CARD       = 5       # skip perspectives with fewer groups
MAX_PERSPECTIVES     = 4
ADAPTIVE_CONFIG      = CONFIG_DIR / "adaptive_index.config.json"

# Retention knobs — same as Experiment 6.3.1 (warm-up).
# min_query_count=3: a pair must be seen 3 times before promotion fires.
# half_life_seconds=3600: negligible decay over the ~10 min experiment.
RETENTION_OVERRIDES = {
    "min_query_count":   3,
    "half_life_seconds": 3600.0,
    "hysteresis":        0.0,
}

_LOG_EXTS = {".csv", ".xes"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pat2(a: str, b: str) -> str:
    return f"{quote_label(a)} {quote_label(b)}"


def _q(qid: str, pattern: str, *, log_name: str, gk: list[str], tag: str,
       shared_pair: str = "") -> dict:
    return {
        "id":            qid,
        "log_name":      log_name,
        "pattern":       pattern,
        "grouping_keys": list(gk),
        "tags":          [tag],
        "shared_pair":   shared_pair,
    }


def _tier(statuses: dict) -> str:
    """
    Return the highest lifecycle tier among all pairs in the response.

    A query that needed even one ABSENT pair paid the full lazy-scan cost,
    so we label it ABSENT unless *all* pairs are at TRANSIENT or higher.
    Similarly, TRANSIENT dominates ABSENT.
    """
    vals = list(statuses.values()) if statuses else []
    if "PERSISTENT" in vals and "ABSENT" not in vals and "TRANSIENT" not in vals:
        return "PERSISTENT"
    if "TRANSIENT" in vals and "ABSENT" not in vals:
        return "TRANSIENT"
    if not vals:
        return "ABSENT"
    # Mixed or all-ABSENT.
    if all(v == "ABSENT" for v in vals):
        return "ABSENT"
    if any(v == "PERSISTENT" for v in vals):
        return "PERSISTENT"
    return "TRANSIENT"


# ---------------------------------------------------------------------------
# Workload builders (data-driven from pair_coverage)
# ---------------------------------------------------------------------------

def build_skewed_round(
    coverage: dict[tuple, list[dict]],
    log_name: str,
    *,
    n_hot_pairs: int,
    max_pairs_per_persp: int,
) -> list[dict]:
    """
    Build the query set for one round of the skewed workload.

    For each perspective (capped at max_pairs_per_persp pairs total):
      - Hot pairs : the top n_hot_pairs by group coverage, each repeated
        REPS_HOT times.  REPS_HOT = min_query_count + 1 (= 4 by default)
        guarantees promotion in round 1 while keeping the round size small.
      - Cold pairs: the remaining (max_pairs_per_persp - n_hot_pairs) pairs,
        queried exactly once per round.  They never accumulate enough queries
        to cross the promotion threshold.

    Skewness is expressed by the pair split (n_hot vs n_cold), not by
    inflating reps_hot — that formula blows up when pair coverage is large.
    """
    min_reps = int(RETENTION_OVERRIDES["min_query_count"])
    reps_hot = min_reps + 1   # = 4: cross threshold in round 1, minimal overhead

    queries: list[dict] = []
    ctr = [0]

    for gk_tuple, pairs in coverage.items():
        if not pairs:
            continue
        gk = list(gk_tuple)

        # Cap the total pairs considered for this perspective.
        pairs = pairs[:max_pairs_per_persp]

        n_hot  = min(n_hot_pairs, len(pairs))
        hot_ps = pairs[:n_hot]
        cold_ps = pairs[n_hot:]   # len = max_pairs_per_persp - n_hot (at most)

        for pair in hot_ps:
            for _ in range(reps_hot):
                ctr[0] += 1
                queries.append(_q(
                    f"H{ctr[0]}", _pat2(pair["source"], pair["target"]),
                    log_name=log_name, gk=gk, tag="HOT",
                    shared_pair=f"{pair['source']}->{pair['target']}",
                ))

        for pair in cold_ps:
            ctr[0] += 1
            queries.append(_q(
                f"C{ctr[0]}", _pat2(pair["source"], pair["target"]),
                log_name=log_name, gk=gk, tag="COLD",
                shared_pair=f"{pair['source']}->{pair['target']}",
            ))

    # Interleave hot and cold so cold queries are spread through the round,
    # not bunched at the end (matches the style of exp_warmup).
    hot_qs  = [q for q in queries if q["tags"] == ["HOT"]]
    cold_qs = [q for q in queries if q["tags"] == ["COLD"]]
    n_total = len(queries)
    interleaved: list[dict] = []
    hi = ci = 0
    for k in range(n_total):
        if ci < len(cold_qs) and (k * len(cold_qs)) // n_total > ci - 1:
            interleaved.append(cold_qs[ci]); ci += 1
        elif hi < len(hot_qs):
            interleaved.append(hot_qs[hi]); hi += 1
        elif ci < len(cold_qs):
            interleaved.append(cold_qs[ci]); ci += 1
    return interleaved


def build_uniform_round(
    coverage: dict[tuple, list[dict]],
    log_name: str,
    *,
    max_pairs_per_persp: int,
) -> list[dict]:
    """
    Build the query set for one round of the uniform workload.

    Each perspective is capped at max_pairs_per_persp pairs (sampled from
    both ends of the coverage-sorted list to preserve the high/low density
    mix).  Each selected pair is queried exactly once per round.

    After exactly min_query_count rounds every sampled pair will have
    accumulated enough queries to trigger promotion — the full sampled
    schema is promoted simultaneously, unlike the skewed workload where
    cold pairs are never promoted.
    """
    queries: list[dict] = []
    ctr = [0]

    for gk_tuple, pairs in coverage.items():
        if not pairs:
            continue
        gk = list(gk_tuple)

        # Stratified sample: take the top half (high-coverage, dense) and
        # bottom half (low-coverage, sparse) so the workload spans the full
        # result-density range rather than being biased toward popular pairs.
        if len(pairs) <= max_pairs_per_persp:
            sampled = pairs
        else:
            half = max_pairs_per_persp // 2
            sampled = pairs[:half] + pairs[-(max_pairs_per_persp - half):]

        for pair in sampled:
            ctr[0] += 1
            queries.append(_q(
                f"U{ctr[0]}", _pat2(pair["source"], pair["target"]),
                log_name=log_name, gk=gk, tag="UNIFORM",
                shared_pair=f"{pair['source']}->{pair['target']}",
            ))

    return queries


# ---------------------------------------------------------------------------
# Round runner
# ---------------------------------------------------------------------------

def run_round(
    rec: Recorder,
    queries: list[dict],
    *,
    workload: str,
    round_num: int,
) -> dict:
    """
    Execute one round of `queries` and emit per-query and per-round records.

    Returns the round summary dict (same fields as the round_summary record).
    """
    latencies: list[float] = []
    tiers:     list[str]   = []
    t_round   = time.perf_counter()

    print(f"  round {round_num:2d}: {len(queries)} queries ...", flush=True)

    for seq, q in enumerate(queries):
        try:
            body, latency = timed_query(
                log_name=q["log_name"],
                pattern=q["pattern"],
                grouping_keys=q["grouping_keys"],
                retention_overrides=RETENTION_OVERRIDES,
            )
            statuses = body.get("pair_status_after") or {}
            tier     = _tier(statuses)
            latencies.append(latency)
            tiers.append(tier)

            rec.emit(
                "query",
                workload=workload,
                round_num=round_num,
                seq=seq,
                qid=q["id"],
                pattern=q["pattern"],
                grouping_keys=q["grouping_keys"],
                tag=q["tags"][0],
                shared_pair=q.get("shared_pair", ""),
                latency_s=latency,
                total=body.get("total", 0),
                tier=tier,
                pair_status_after=statuses,
            )

        except Exception as exc:
            rec.emit(
                "query_error",
                workload=workload,
                round_num=round_num,
                seq=seq,
                qid=q["id"],
                error=str(exc),
            )
            print(f"    seq={seq:3d} {q['id']} ERROR: {exc}", flush=True)

    round_wall = time.perf_counter() - t_round
    n = len(latencies)

    if n == 0:
        return {}

    mean_lat   = sum(latencies) / n
    median_lat = statistics.median(latencies)
    n_pers  = tiers.count("PERSISTENT")
    n_trans = tiers.count("TRANSIENT")
    n_abs   = n - n_pers - n_trans

    summary = {
        "workload":         workload,
        "round_num":        round_num,
        "n_queries":        n,
        "mean_latency_s":   mean_lat,
        "median_latency_s": median_lat,
        "frac_persistent":  n_pers  / n,
        "frac_transient":   n_trans / n,
        "frac_absent":      n_abs   / n,
        "round_wall_s":     round_wall,
    }
    rec.emit("round_summary", **summary)

    print(
        f"         mean={mean_lat:.2f}s  median={median_lat:.2f}s  "
        f"PERSISTENT={n_pers/n:.0%}  TRANSIENT={n_trans/n:.0%}  "
        f"ABSENT={n_abs/n:.0%}  wall={round_wall:.1f}s",
        flush=True,
    )
    return summary


# ---------------------------------------------------------------------------
# Full workload runner  (N_ROUNDS rounds from a clean slate)
# ---------------------------------------------------------------------------

def run_workload(
    rec: Recorder,
    queries_per_round: list[dict],
    *,
    workload: str,
    log_name: str,
    dataset_path: Path,
    perspectives: list[list[str]],
    n_rounds: int,
    between_round_sleep: int,
) -> None:
    """
    Reset the adaptive catalog with a fresh ingest, then run n_rounds rounds.

    The re-ingest ensures the two workloads do not share any accumulated
    pair lifecycle state from the previous run.
    """
    print(f"\n{'─'*60}")
    print(
        f"Workload: {workload}  "
        f"({len(queries_per_round)} queries/round × {n_rounds} rounds)",
        flush=True,
    )
    print(f"Re-ingesting (clear_existing=True) …", flush=True)

    t0 = time.perf_counter()
    ingest_adaptive(
        log_name, dataset_path, ADAPTIVE_CONFIG,
        overrides={"perspectives": [{"grouping_keys": gk} for gk in perspectives]},
        clear_existing=True,
    )
    print(f"  ingest complete in {time.perf_counter()-t0:.1f}s.", flush=True)
    time.sleep(2)

    for round_num in range(1, n_rounds + 1):
        rec.emit("round_start", workload=workload, round_num=round_num)
        run_round(rec, queries_per_round, workload=workload, round_num=round_num)

        if round_num < n_rounds:
            print(
                f"  [sleep {between_round_sleep}s — awaiting background "
                f"materialisation …]",
                flush=True,
            )
            time.sleep(between_round_sleep)


# ---------------------------------------------------------------------------
# Per-dataset entry point
# ---------------------------------------------------------------------------

def run_dataset(
    dataset_path: Path,
    log_name: str,
    *,
    n_rounds: int,
    n_hot_pairs: int,
    hot_ratio: float,
    max_perspectives: int,
    max_pairs_per_persp: int,
    between_round_sleep: int,
) -> None:
    print(f"\n{'='*60}")
    print(f"Dataset  : {dataset_path}")
    print(f"log_name : {log_name}")
    print(f"{'='*60}", flush=True)

    workloads_obj = build_workloads(
        str(dataset_path), log_name, max_perspectives=max_perspectives
    )
    ctx          = workloads_obj.context
    perspectives = ctx.perspectives

    print(f"Activities   : {ctx.activities}")
    print(f"Perspectives : {perspectives}", flush=True)

    rec = Recorder("6.3.2", f"6_3_2_skew_uniform_{log_name}.jsonl")
    rec.emit(
        "dataset",
        path=str(dataset_path),
        log_name=log_name,
        activities=ctx.activities,
        perspectives=perspectives,
        n_rounds=n_rounds,
        n_hot_pairs=n_hot_pairs,
        hot_ratio=hot_ratio,
        retention_overrides=RETENTION_OVERRIDES,
    )

    # ── Bootstrap once to discover real co-occurring pairs ─────────────────
    # This initial ingest populates the SequenceTable and registers
    # perspectives so /pair_coverage can report genuine pair co-occurrences.
    # Each subsequent workload run re-ingests with clear_existing=True,
    # resetting the pair lifecycle catalog to an empty state.
    print("\n── Bootstrap for pair-coverage discovery ──", flush=True)
    ingest_adaptive(
        log_name, dataset_path, ADAPTIVE_CONFIG,
        overrides={"perspectives": [{"grouping_keys": gk} for gk in perspectives]},
        clear_existing=True,
    )

    coverage: dict[tuple, list[dict]] = {}
    for gk in perspectives:
        key = tuple(gk)
        try:
            cov   = fetch_pair_coverage(log_name, gk, activities=ctx.activities)
            gc    = cov.get("group_count", 0)
            pairs = sorted(cov.get("pairs", []), key=lambda p: -p["groups"])
            if gc >= MIN_PERSP_CARD and pairs:
                coverage[key] = pairs
                print(f"  {gk}: {gc} groups, {len(pairs)} co-occurring pairs")
            else:
                print(f"  {gk}: skipped (groups={gc}, pairs={len(pairs)})")
        except Exception as exc:
            print(f"  {gk}: ERROR — {exc}")

    if not coverage:
        print("  SKIP: no valid perspectives found after filtering.")
        return

    n_unique_pairs = sum(len(p) for p in coverage.values())
    print(f"\nTotal unique (perspective, pair) combos : {n_unique_pairs}")

    # ── Build per-round query sets ─────────────────────────────────────────
    skewed_qs  = build_skewed_round(
        coverage, log_name,
        n_hot_pairs=n_hot_pairs,
        max_pairs_per_persp=max_pairs_per_persp,
    )
    uniform_qs = build_uniform_round(
        coverage, log_name,
        max_pairs_per_persp=max_pairs_per_persp,
    )

    if not skewed_qs or not uniform_qs:
        print("  SKIP: empty workload — cannot proceed.")
        return

    # Summarise hot-pair repetitions for transparency.
    hot_reps: dict[str, int] = {}
    for q in skewed_qs:
        if q["tags"] == ["HOT"]:
            k = q["shared_pair"]
            hot_reps[k] = hot_reps.get(k, 0) + 1

    hot_counts = sorted(set(hot_reps.values()))
    n_cold_total = sum(1 for q in skewed_qs if q["tags"] == ["COLD"])
    n_hot_total  = sum(1 for q in skewed_qs if q["tags"] == ["HOT"])

    min_qc = int(RETENTION_OVERRIDES["min_query_count"])
    promotes_at = {
        pair_id: (
            f"round {max(1, -(-min_qc // reps))}"   # ceiling division
            if reps >= 1 else "never"
        )
        for pair_id, reps in hot_reps.items()
    }
    promotes_at_uniform = f"round {min_qc}"  # 1 query per pair per round

    print(
        f"\nSkewed  workload : {len(skewed_qs)} queries/round "
        f"({n_hot_total} hot [{hot_counts} reps/pair], {n_cold_total} cold)",
        flush=True,
    )
    for pid, rnd in promotes_at.items():
        print(f"  hot pair {pid!r:40s} → PERSISTENT at {rnd}")

    print(
        f"\nUniform workload : {len(uniform_qs)} queries/round "
        f"(1 per pair, PERSISTENT at {promotes_at_uniform})",
        flush=True,
    )

    rec.emit(
        "schema",
        log_name=log_name,
        n_unique_pairs=n_unique_pairs,
        skewed_n_per_round=len(skewed_qs),
        uniform_n_per_round=len(uniform_qs),
        hot_reps_per_pair=hot_reps,
        expected_skewed_promote_round=max(
            (max(1, -(-min_qc // reps)) for reps in hot_reps.values()),
            default=1,
        ),
        expected_uniform_promote_round=min_qc,
    )

    # ── Run both workloads ─────────────────────────────────────────────────
    for workload_label, qs in [("skewed", skewed_qs), ("uniform", uniform_qs)]:
        run_workload(
            rec, qs,
            workload=workload_label,
            log_name=log_name,
            dataset_path=dataset_path,
            perspectives=perspectives,
            n_rounds=n_rounds,
            between_round_sleep=between_round_sleep,
        )

    print(f"\nResults written to {rec.path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Experiment 6.3.2 — Skewed vs. uniform query-latency "
            "convergence under adaptive indexing."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    src = ap.add_mutually_exclusive_group()
    src.add_argument(
        "--dataset", default=None,
        help="Path to a single dataset (.csv or .xes).",
    )
    src.add_argument(
        "--datasets-dir", default=None, type=Path,
        help="Run on every .csv / .xes file in this directory.",
    )

    ap.add_argument("--log-name",         default=None)
    ap.add_argument("--n-rounds",         type=int,   default=N_ROUNDS,
                    help=f"Query rounds per workload (default {N_ROUNDS}).")
    ap.add_argument("--n-hot-pairs",      type=int,   default=N_HOT_PAIRS,
                    help=f"Hot pairs per perspective in the skewed workload "
                         f"(default {N_HOT_PAIRS}).")
    ap.add_argument("--hot-ratio",        type=float, default=HOT_RATIO,
                    help=f"Target fraction of hot queries per round "
                         f"(default {HOT_RATIO}).")
    ap.add_argument("--max-perspectives", type=int,   default=MAX_PERSPECTIVES,
                    help=f"Max perspectives per dataset (default {MAX_PERSPECTIVES}).")
    ap.add_argument("--max-pairs",        type=int,   default=MAX_PAIRS_PER_PERSP,
                    help=f"Max pairs sampled per perspective (default "
                         f"{MAX_PAIRS_PER_PERSP}). Controls round size: "
                         f"uniform = perspectives × max-pairs queries/round; "
                         f"skewed = perspectives × (n_hot×4 + (max-pairs−n_hot)) queries/round.")
    ap.add_argument("--sleep",            type=int,   default=BETWEEN_ROUND_SLEEP,
                    help=f"Seconds to sleep after each round for background "
                         f"materialisation (default {BETWEEN_ROUND_SLEEP}).")

    args = ap.parse_args()
    health_check()

    if args.datasets_dir:
        d = Path(args.datasets_dir)
        if not d.is_dir():
            ap.error(f"{d} is not a directory.")
        specs = [
            (p, p.stem)
            for p in sorted(d.iterdir())
            if p.suffix.lower() in _LOG_EXTS
        ]
        if not specs:
            ap.error(f"No CSV/XES files found in {d}.")
        print(f"Found {len(specs)} dataset(s) in {d}", flush=True)
    else:
        spec  = resolve_dataset(args.dataset, args.log_name)
        specs = [(spec.path, spec.log_name)]

    for dataset_path, log_name in specs:
        try:
            run_dataset(
                dataset_path=dataset_path,
                log_name=log_name,
                n_rounds=args.n_rounds,
                n_hot_pairs=args.n_hot_pairs,
                hot_ratio=args.hot_ratio,
                max_perspectives=args.max_perspectives,
                max_pairs_per_persp=args.max_pairs,
                between_round_sleep=args.sleep,
            )
        except Exception as exc:
            import traceback
            print(f"\n[ERROR] {log_name}: {exc}")
            traceback.print_exc()
            print("Continuing …")


if __name__ == "__main__":
    main()