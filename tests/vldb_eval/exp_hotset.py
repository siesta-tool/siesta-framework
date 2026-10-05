"""
tests/vldb_eval/exp_hotset.py — Exp 3: effect of hot-set size on retention
convergence (Fig. 4; BPIC 2017 by default).

Workloads under the perspective under test, each from a clean slate:
skewed with n_hot in ``--n-hot`` (default 3, 10, 30; hot ratio 0.8) and
uniform over the whole pair space.  Each has ``--rounds`` x
``--round-size`` queries.

(a) latency vs query position, and the materialisation position: the first
    query from which every hot pair is served from its Delta table.
(b) after each round (pending promotions drained), from the catalog:
      skewed-hot      fraction of the hot set that is PERSISTENT
      skewed-cold     fraction of the cold pairs queried so far that is PERSISTENT
      skewed-overall  fraction of all pairs queried so far that is PERSISTENT
      uniform         fraction of the pairs queried so far that is PERSISTENT

Output: results/exp3_hotset/<dataset>.jsonl (``query`` and ``round``
records; workload = hot_<n> | uniform).

Usage:
    python -m tests.vldb_eval.exp_hotset --datasets bpic2017
"""

from __future__ import annotations

import argparse

from tests.vldb_eval.eval_common import ResultWriter, pair_statuses, restart_api
from tests.vldb_eval.indexing_common import (
    ingest_adaptive_fresh, perspective_under_test, run_adaptive_query, warm_up,
)
from tests.vldb_eval.suite_data import prepare
from tests.vldb_eval.workload import hot_set, pair_coverage, save, skewed, uniform

EXPERIMENT = "exp3_hotset"


def _frac(pairs: set[str], statuses: dict[str, str]) -> float | None:
    if not pairs:
        return None
    return sum(statuses.get(p) == "PERSISTENT" for p in pairs) / len(pairs)


def run(name: str, n_hots: list[int], rounds: int, round_size: int,
        hot_ratio: float, seed: int) -> None:
    prep = prepare(name)
    persp = perspective_under_test(prep)
    label, gk = persp["label"], persp["grouping_keys"]
    cov = pair_coverage(prep, label)
    n = rounds * round_size
    workloads = {f"hot_{h}": (h, skewed(cov, label, gk, n, h, hot_ratio, seed)) for h in n_hots}
    workloads["uniform"] = (0, uniform(cov, label, gk, n, seed))

    w = ResultWriter(EXPERIMENT, name)
    for wl, (_, qs) in workloads.items():
        save(qs, w.path.with_name(f"{name}.{wl}.workload.jsonl"))
    w.emit("setup", perspective=label, grouping_keys=gk, group_count=cov["group_count"],
           pair_space=len(cov["pairs"]), rounds=rounds, round_size=round_size,
           hot_ratio=hot_ratio, seed=seed, n_hot=n_hots)

    restart_api()
    log = name
    for wl, (h, qs) in workloads.items():
        hot = {f"{a}->{b}" for a, b in hot_set(cov, h)} if h else set()
        ingest_adaptive_fresh(w, log, prep.full_csv, workload=wl)
        warm_up(prep, log)
        touched: set[str] = set()
        for r in range(rounds):
            for q in qs[r * round_size:(r + 1) * round_size]:
                rec = run_adaptive_query(w, log, q, workload=wl, round=r + 1)
                touched.add(q.pair)
                print(f"  [{wl}] r{r + 1} {q.seq:3d} {'H' if q.hot else 'c'} {rec['time']:7.2f}s "
                      f"{list(rec['pair_sources'].values())}")
            statuses = pair_statuses(log, gk)
            cold = touched - hot
            w.emit(
                "round", workload=wl, round=r + 1, n_hot=h,
                queries_so_far=(r + 1) * round_size,
                n_touched=len(touched),
                n_persistent=sum(s == "PERSISTENT" for s in statuses.values()),
                frac_hot=_frac(hot, statuses),
                frac_cold_touched=_frac(cold, statuses) if h else None,
                frac_touched=_frac(touched, statuses),
            )
    w.done()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--datasets", nargs="+", default=["bpic2017"])
    ap.add_argument("--n-hot", nargs="+", type=int, default=[3, 10, 30])
    ap.add_argument("--rounds", type=int, default=10)
    ap.add_argument("--round-size", type=int, default=15)
    ap.add_argument("--hot-ratio", type=float, default=0.8)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()
    for name in args.datasets:
        if args.resume and ResultWriter.is_complete(EXPERIMENT, name):
            print(f"[{name}] complete, skipping")
            continue
        run(name, args.n_hot, args.rounds, args.round_size, args.hot_ratio, args.seed)


if __name__ == "__main__":
    main()
