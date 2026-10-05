"""
tests/vldb_eval/run_indexing_suite.py — run the indexing experiments.

    Exp 1  exp_warmup                     warm-up under a static log
    Exp 2  exp_skew_vs_uniform_latency    skewed vs uniform workload
    Exp 3  exp_hotset                     hot-set size (BPIC 2017)
    Exp 4  exp_maintenance_savings        eager vs adaptive ingest cost
    Exp 5  (plotted from Exp 4)           BPIC 2017 figure + all-dataset table

Every experiment writes results/<experiment>/<dataset>.jsonl; ``--resume``
skips (experiment, dataset) pairs whose file is complete.  ``--quick`` runs
a small smoke configuration on the synthetic log.  Plot with
``python -m tests.vldb_eval.plot_indexing``.

Usage:
    python -m tests.vldb_eval.run_indexing_suite --exp 1 2 3 4 --resume
    python -m tests.vldb_eval.run_indexing_suite --quick
"""

from __future__ import annotations

import argparse
import sys
import time
from types import SimpleNamespace

from tests.vldb_eval import exp_hotset, exp_maintenance_savings, exp_skew_vs_uniform_latency, exp_warmup
from tests.vldb_eval.eval_common import ResultWriter, health_check
from tests.vldb_eval.suite_data import ALL_DATASETS, prepare

# Largest dataset last: its eager baseline dominates the running time.
DEFAULT_ORDER = ["synthetic", "bpic2015", "bpic2011", "bpic2012", "bpic2017", "bpic2018"]
# Exp 4 repeats full-size ingests per configuration (24 eager ingests for
# P = 4, plus the pinned and all-pairs runs); BPIC 2018 is left out by default
# because of its running time.
EXP4_DEFAULT = [d for d in DEFAULT_ORDER if d != "bpic2018"]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--exp", nargs="+", type=int, default=[1, 2, 3, 4])
    ap.add_argument("--datasets", nargs="+", default=DEFAULT_ORDER, choices=ALL_DATASETS)
    ap.add_argument("--exp3-datasets", nargs="+", default=["bpic2017"])
    ap.add_argument("--exp4-datasets", nargs="+", default=EXP4_DEFAULT, choices=ALL_DATASETS)
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--quick", action="store_true", help="small smoke run on the synthetic log")
    args = ap.parse_args()

    health_check()
    if args.quick:
        from tests.vldb_eval import eval_common
        # Keep smoke results apart so --resume never mistakes them for a real run.
        eval_common.RESULTS_DIR = eval_common.RESULTS_DIR / "quick"
        args.datasets = ["synthetic"]
        args.exp3_datasets = ["synthetic"]
        args.exp4_datasets = ["synthetic"]
    for name in dict.fromkeys(args.datasets + (args.exp3_datasets if 3 in args.exp else [])
                              + (args.exp4_datasets if 4 in args.exp else [])):
        prepare(name)

    def todo(exp_mod, name):
        if args.resume and ResultWriter.is_complete(exp_mod.EXPERIMENT, name):
            print(f"== {exp_mod.EXPERIMENT} / {name}: complete, skipping")
            return False
        print(f"== {exp_mod.EXPERIMENT} / {name}  ({time.strftime('%H:%M:%S')})", flush=True)
        return True

    q = args.quick
    for name in args.datasets:
        if 1 in args.exp and todo(exp_warmup, name):
            exp_warmup.run(name, n_pairs=2 if q else 5, reps=5)
        if 2 in args.exp and todo(exp_skew_vs_uniform_latency, name):
            exp_skew_vs_uniform_latency.run(name, n_queries=8 if q else 30, n_hot=2 if q else 3,
                                            hot_ratio=0.8, seed=42)
    if 3 in args.exp:
        for name in args.exp3_datasets:
            if todo(exp_hotset, name):
                exp_hotset.run(name, n_hots=[2, 4] if q else [3, 10, 30],
                               rounds=3 if q else 10, round_size=5 if q else 15,
                               hot_ratio=0.8, seed=42)
    if 4 in args.exp:
        for name in args.exp4_datasets:
            if todo(exp_maintenance_savings, name):
                exp_maintenance_savings.run(name, SimpleNamespace(
                    configs=["eager", "adaptive", "sweep", "allpairs"],
                    n_hot=2 if q else 3, hot_ratio=0.8,
                    slice=3 if q else 10, promo_cap=40 if q else 150,
                    sweep=[4, 8] if q else [10, 30, 100],
                    allpairs_cap=3000, seed=42, keep_logs=False,
                ))
    print("done", time.strftime("%H:%M:%S"))


if __name__ == "__main__":
    sys.exit(main())
