"""
tests/vldb_eval/exp_skew_vs_uniform_latency.py — Exp 2: is the transient
tier effective, and how does workload skew affect convergence? (Fig. 3b)

Two workloads of ``--n-queries`` structural queries under the case
perspective, each from a clean slate (fresh ingest, empty catalog / LRU):

  skewed   each query hits the hot set (the ``--n-hot`` highest-coverage
           pairs) with probability ``--hot-ratio`` (0.8), otherwise a
           uniformly drawn cold pair
  uniform  each query is a uniformly drawn pair of the whole pair space

The x-axis is the global query position.  Both workloads are then replayed
against original (eager) SIESTA with its case index (all pairs from the
start), the baseline the adaptive system has to match.  With
the adaptive ingest, the promotion time of every query (``promotion_s``) and
the eager ingest, this also gives the cumulative cost (break-even) of the two
approaches.

Output: results/exp2_skew_uniform/<dataset>.jsonl (``query`` records with
system = adaptive | eager and workload = skewed | uniform; ``ingest``
records for both systems).

Usage:
    python -m tests.vldb_eval.exp_skew_vs_uniform_latency --datasets bpic2017
"""

from __future__ import annotations

import argparse

from tests.vldb_eval.eval_common import ResultWriter, query_eager, restart_api
from tests.vldb_eval.indexing_common import (
    eager_log, ingest_adaptive_fresh, ingest_eager_full, comparison_perspective,
    run_adaptive_query, run_eager_query, warm_up,
)
from tests.vldb_eval.suite_data import ALL_DATASETS, prepare
from tests.vldb_eval.workload import Query, hot_set, pair_coverage, save, skewed, uniform

EXPERIMENT = "exp2_skew_uniform"


def run(name: str, n_queries: int, n_hot: int, hot_ratio: float, seed: int) -> None:
    prep = prepare(name)
    persp = comparison_perspective(prep)  # case: compared with eager SIESTA
    label, gk = persp["label"], persp["grouping_keys"]
    cov = pair_coverage(prep, label)
    workloads = {
        "skewed": skewed(cov, label, gk, n_queries, n_hot, hot_ratio, seed),
        "uniform": uniform(cov, label, gk, n_queries, seed),
    }
    w = ResultWriter(EXPERIMENT, name)
    for wl, qs in workloads.items():
        save(qs, w.path.with_name(f"{name}.{wl}.workload.jsonl"))
    w.emit("setup", perspective=label, grouping_keys=gk, group_count=cov["group_count"],
           pair_space=len(cov["pairs"]), n_queries=n_queries, n_hot=n_hot,
           hot_ratio=hot_ratio, seed=seed,
           hot_pairs=[f"{a}->{b}" for a, b in hot_set(cov, n_hot)],
           realised_hot_share={wl: sum(q.hot for q in qs) / len(qs) for wl, qs in workloads.items()})

    # Every measured workload, of either system, starts from a restarted API
    # plus one discarded query, so no workload inherits another's warm caches.
    log = name
    for wl, qs in workloads.items():
        restart_api()
        ingest_adaptive_fresh(w, log, prep.full_csv, workload=wl)
        warm_up(prep, log)
        for q in qs:
            rec = run_adaptive_query(w, log, q, workload=wl)
            print(f"  [{name}/{wl}] {q.seq:3d} {'H' if q.hot else 'c'} {rec['time']:7.2f}s {list(rec['pair_sources'].values())}")

    # Baseline: the same workloads on eager SIESTA, all pairs indexed.
    ingest_eager_full(w, prep, label)
    a, b = cov["pairs"][-1]["source"], cov["pairs"][-1]["target"]
    for wl, qs in workloads.items():
        restart_api()
        query_eager(eager_log(prep, label), Query(-1, label, gk, a, b, hot=False).pattern)  # warm-up
        for q in qs:
            rec = run_eager_query(w, prep, q, workload=wl)
            print(f"  [{name}/{wl}/eager] {q.seq:3d} {rec['time']:7.2f}s")
    w.done()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--datasets", nargs="+", default=ALL_DATASETS)
    ap.add_argument("--n-queries", type=int, default=30)
    ap.add_argument("--n-hot", type=int, default=3)
    ap.add_argument("--hot-ratio", type=float, default=0.8)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()
    for name in args.datasets:
        if args.resume and ResultWriter.is_complete(EXPERIMENT, name):
            print(f"[{name}] complete, skipping")
            continue
        run(name, args.n_queries, args.n_hot, args.hot_ratio, args.seed)


if __name__ == "__main__":
    main()
