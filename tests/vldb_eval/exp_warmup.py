"""
tests/vldb_eval/exp_warmup.py — Exp 1: warm-up under a static log (Fig. 3a).

Each dataset is ingested once; no further batches arrive.  A fixed
structural workload W (the n highest-coverage pairs of the perspective under
test) is executed R times against an initially empty index for that
perspective.  Repetition 1 pays the cold path (on-the-fly grouping and pair
extraction from the shared SequenceTable); with min_query_count = 3 a pair
is served from the LRU in repetitions 2-3, is promoted after its third
query, and is read from its persisted Delta table from repetition 4 on.
Expected pair sources per repetition: SCAN, LRU, LRU, DELTA, DELTA.

Perspective: the case perspective, so the baseline is original (eager)
SIESTA in its native setting: all pairs indexed from the start, grouped by
trace id.
It answers the same queries, so both systems return the same groups and
support (the ``parity`` record checks this), and it is run for the same R
repetitions, so repetition k of one system is compared with repetition k of
the other.  One discarded query (a pair outside W) precedes each system's
measured repetitions.

Output: results/exp1_warmup/<dataset>.jsonl with ``query`` records
(system = adaptive | eager, rep, pair, time, support, ...; adaptive also has
timings, pair_sources, promotion_s), a ``catalog`` record after every
adaptive repetition and a ``parity`` record.

Usage:
    python -m tests.vldb_eval.exp_warmup --datasets bpic2011 bpic2017
"""

from __future__ import annotations

import argparse

from tests.vldb_eval.eval_common import ResultWriter, eval_drain, pair_statuses, query_eager, restart_api
from tests.vldb_eval.indexing_common import (
    eager_log, ingest_adaptive_fresh, ingest_eager_full, comparison_perspective,
    run_adaptive_query, run_eager_query, warm_up,
)
from tests.vldb_eval.suite_data import ALL_DATASETS, prepare
from tests.vldb_eval.workload import Query, fixed, pair_coverage, save

EXPERIMENT = "exp1_warmup"


def run(name: str, n_pairs: int, reps: int) -> None:
    prep = prepare(name)
    persp = comparison_perspective(prep)  # case: compared with eager SIESTA
    label, gk = persp["label"], persp["grouping_keys"]
    cov = pair_coverage(prep, label)
    work = fixed(cov, label, gk, n_pairs)
    w = ResultWriter(EXPERIMENT, name)
    save(work, w.path.with_suffix(".workload.jsonl"))
    w.emit("setup", perspective=label, grouping_keys=gk, group_count=cov["group_count"],
           pair_space=len(cov["pairs"]), n_pairs=n_pairs, reps=reps,
           pairs=[q.pair for q in work])
    print(f"[{name}] perspective={label} groups={cov['group_count']} pairs={[q.pair for q in work]}")

    restart_api()
    log = name
    ingest_adaptive_fresh(w, log, prep.full_csv)
    warm_up(prep, log)

    for rep in range(1, reps + 1):
        for q in work:
            rec = run_adaptive_query(w, log, q, rep=rep)
            print(f"  rep{rep} {q.pair:<50} {rec['time']:7.2f}s {rec['pair_sources']}")
        eval_drain(log)
        w.emit("catalog", rep=rep, statuses=pair_statuses(log, gk))

    # Baseline: eager SIESTA on the same perspective, all pairs indexed.
    ingest_eager_full(w, prep, label)
    restart_api()  # same starting state as the adaptive repetitions
    src, tgt = cov["pairs"][-1]["source"], cov["pairs"][-1]["target"]
    query_eager(eager_log(prep, label), Query(-1, label, gk, src, tgt, hot=False).pattern)  # warm-up
    for rep in range(1, reps + 1):
        for q in work:
            rec = run_eager_query(w, prep, q, rep=rep)
            print(f"  eager rep{rep} {q.pair:<44} {rec['time']:7.2f}s")

    # Parity: both systems answer the same query, so support must agree.
    sup = {}
    for sysname in ("adaptive", "eager"):
        for r in w.read():
            if r["event"] == "query" and r.get("system") == sysname and r.get("rep") == reps:
                sup.setdefault(r["pair"], {})[sysname] = r.get("support")
    mismatched = {k: v for k, v in sup.items()
                  if v.get("adaptive") is None or v.get("eager") is None
                  or abs(v["adaptive"] - v["eager"]) > 1e-9}
    w.emit("parity", pairs=len(sup), mismatched=mismatched)
    if mismatched:
        print(f"  WARNING support mismatch: {mismatched}")
    w.done()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--datasets", nargs="+", default=ALL_DATASETS)
    ap.add_argument("--n-pairs", type=int, default=5)
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--resume", action="store_true", help="skip datasets with a complete result file")
    args = ap.parse_args()
    for name in args.datasets:
        if args.resume and ResultWriter.is_complete(EXPERIMENT, name):
            print(f"[{name}] complete, skipping")
            continue
        run(name, args.n_pairs, args.reps)


if __name__ == "__main__":
    main()
