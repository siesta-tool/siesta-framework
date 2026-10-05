"""
tests/vldb_eval/exp_sensitivity.py — Section 2, Exp 3: sensitivity of the
retention policy (BPIC 2017, evolving log B0..B5).

Every run starts from a fresh ingest of B0; then, for b = 1..5, a query
phase runs and batch B_b is ingested (maintenance + retention: demotions
happen at ingest time).  Counters decay with a short half-life
(``--half-life``, default 300 s), so a quiet phase lets them fall.

(a) eps    ε ∈ {0, 0.05, 0.15, 0.3, 0.5}, bursty workload: phases alternate
           burst (``--burst`` queries, 90 % on the hot set) and quiet
           (``--quiet`` queries, 10 % hot).  Thrash = pairs that are
           promoted, demoted and promoted again; plus total cost and latency.
(b) cost   cost_scale ∈ {0.25, 0.5, 1, 2, 4} (estimated build/maintenance
           costs under- / over-estimated), ε = 0.15, same bursty workload.
           Decisions are compared with cost_scale = 1.
(c) drift  the hot perspective and its hot set change every phase
           (case -> Action -> lifecycle:transition -> EventOrigin -> case),
           each phase a burst.  Re-adaptation lag (queries until the new hot
           set is served from Delta) and wasted builds (pairs persisted, then
           demoted after few further queries) per episode.

Queries are length-2 structural pair queries (as in the indexing suite).
Every query waits for its promotion (promotion_s, excluded from latency);
a catalog snapshot is taken after every phase and every ingest.

Output: results/exp_sensitivity/bpic2017.<sweep>_<value>.jsonl

Usage:
    python -m tests.vldb_eval.exp_sensitivity --sweeps eps cost drift
"""

from __future__ import annotations

import argparse
import random

from tests.vldb_eval.eval_common import (
    RETENTION, ResultWriter, eval_catalog, eval_drain, eval_reset, ingest, query_adaptive, restart_api,
)
from tests.vldb_eval.suite_data import prepare
from tests.vldb_eval.workload import Query, pair_coverage

EXPERIMENT = "exp_sensitivity"
DATASET = "bpic2017"
EPS = [0.0, 0.05, 0.15, 0.3, 0.5]
COST = [0.25, 0.5, 1.0, 2.0, 4.0]
DRIFT_ORDER = ["case", "Action", "lifecycle:transition", "EventOrigin", "case"]


def _phase_queries(rng, cov, label, keys, n, hot_share, n_hot, seq0):
    pairs = [(p["source"], p["target"]) for p in cov["pairs"]]
    hot, cold = pairs[:n_hot], pairs[n_hot:]
    out = []
    for i in range(n):
        is_hot = not cold or rng.random() < hot_share
        a, b = rng.choice(hot if is_hot else cold)
        out.append(Query(seq0 + i, label, keys, a, b, hot=is_hot))
    return out


def schedule(prep, sweep: str, args) -> list[tuple[str, list[Query]]]:
    """[(phase name, queries)] for the 5 phases before B1..B5; seeded, so all
    values of a sweep replay the same workload."""
    rng = random.Random(f"{args.seed}:{sweep}")
    persps = {p["label"]: p["grouping_keys"] for p in prep.perspective_list()}
    covs = {}
    phases, seq = [], 0
    for k in range(5):
        if sweep == "drift":
            label = DRIFT_ORDER[k]
            n, share, name = args.burst, 0.9, f"drift:{label}"
        else:
            label = "case"
            burst = k % 2 == 0
            n, share, name = (args.burst, 0.9, "burst") if burst else (args.quiet, 0.1, "quiet")
        cov = covs.setdefault(label, pair_coverage(prep, label))
        qs = _phase_queries(rng, cov, label, persps[label], n, share, args.n_hot, seq)
        seq += len(qs)
        phases.append((name, qs))
    return phases


def run_one(prep, sweep: str, value: float, args) -> None:
    retention = {**RETENTION, "half_life_seconds": args.half_life}
    if sweep == "eps":
        retention["hysteresis"] = value
    elif sweep == "cost":
        retention["cost_scale"] = value
    w = ResultWriter(EXPERIMENT, DATASET, suffix=f".{sweep}_{value:g}")
    w.emit("setup", sweep=sweep, value=value, retention=retention, burst=args.burst,
           quiet=args.quiet, n_hot=args.n_hot, seed=args.seed)
    log = f"{DATASET}_sens"
    restart_api()
    batches = prep.batches
    r = ingest("adaptive", log, batches[0], clear_existing=True, extra=retention)
    eval_reset(log)
    w.emit("ingest", batch=0, time=r["time"], timings=r.get("timings"))
    for k, (name, qs) in enumerate(schedule(prep, sweep, args)):
        for q in qs:
            resp = query_adaptive(log, q.pattern, q.grouping_keys, wait_promotion=True, retention=retention)
            w.emit("query", phase=k, phase_name=name, seq=q.seq, perspective=q.perspective,
                   pair=q.pair, hot=q.hot, time=resp.get("time"), promotion_s=resp.get("promotion_s"),
                   pair_sources=resp.get("pair_sources"), status_before=resp.get("pair_status_before"),
                   status_after=resp.get("pair_status_after"))
        eval_drain(log)
        w.emit("catalog", after="phase", phase=k, catalog=eval_catalog(log))
        r = ingest("adaptive", log, batches[k + 1], extra=retention)
        w.emit("ingest", batch=k + 1, time=r["time"], timings=r.get("timings"))
        eval_drain(log)
        w.emit("catalog", after="ingest", phase=k, batch=k + 1, catalog=eval_catalog(log))
        print(f"  [{sweep}={value:g}] phase {k} ({name}) done, B{k + 1} {r['time']:.1f}s")
    w.done()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sweeps", nargs="+", default=["eps", "cost", "drift"], choices=["eps", "cost", "drift"])
    ap.add_argument("--half-life", type=float, default=300.0)
    ap.add_argument("--burst", type=int, default=30)
    ap.add_argument("--quiet", type=int, default=6)
    ap.add_argument("--n-hot", type=int, default=5)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--values", nargs="*", type=float, help="override the sweep values")
    args = ap.parse_args()
    prep = prepare(DATASET)
    for sweep in args.sweeps:
        values = args.values or {"eps": EPS, "cost": COST, "drift": [RETENTION["hysteresis"]]}[sweep]
        for v in values:
            if args.values is None and sweep == "cost" and v == 1.0 and "eps" in args.sweeps:
                continue  # identical to eps = 0.15
            run_one(prep, sweep, v, args)


if __name__ == "__main__":
    main()
