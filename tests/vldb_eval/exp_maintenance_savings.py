"""
tests/vldb_eval/exp_maintenance_savings.py — Exp 4 and 5: ingest cost that
adaptivity avoids, per batch, under a multiperspective query workload.

The log is split into B0 (bootstrap, not measured) + B1..B5 (measured), a
stratified trace sample (suite_data).  P is the case perspective plus the top
attribute perspectives.  Configurations, each on its own log:

  eager       original SIESTA, one independent ingest per perspective per
              batch (the perspective's attribute used as trace_id), each
              building the full PairsIndex of all pairs.
              T_eager(b) = sum over P of those ingests.
  adaptive    one adaptive ingest per batch; the pairs it maintains are the
              ones the query workload promoted.
  pinned_<n>  adaptive, with the n highest-coverage pairs of every
              perspective force-persisted after B0 (pinned, never demoted)
              and no queries: maintenance cost as a function of the number
              of maintained pairs.
  allpairs    adaptive with every co-occurring pair of every perspective
              pinned: the counterfactual that maintains what eager maintains,
              in the adaptive engine.  Run when every perspective has at most
              ``--allpairs-cap`` co-occurring pairs; otherwise T_allpairs is
              extrapolated from the pinned sweep (plot_indexing does this).

Saving ratio R(b) = T_eager(b) / T_adaptive(b), saving = 1 - 1/R, and
    R = F_persp * F_pair,  F_persp = T_eager / T_allpairs,
                           F_pair  = T_allpairs / T_adaptive.
F_persp: P independent ingests, each rebuilding the shared structures, vs one
shared base with P per-perspective pair indices.  F_pair: maintaining the
workload's hot pairs instead of all pairs; depends on the hot-set size.

Workload: one multiperspective stream (a uniformly chosen perspective per
query, then 80 % hot / 20 % cold within it; ``--n-hot`` hot pairs per
perspective).  After B0 its first ``--promo-cap`` queries are the promotion
phase (adaptive stops early once every hot pair is PERSISTENT); after each
measured batch the next ``--slice`` queries run against eager and adaptive
on the adaptive log, so retention stays live (eager maintains everything
regardless of queries and runs none).

Output: results/exp4_maintenance/<dataset>.jsonl with ``ingest`` records
(config, batch, perspective, time, timings), ``query`` records and
``catalog`` records.  Exp 5 (BPIC 2017 figure, all-dataset table) is
plotted from these files.

Usage:
    python -m tests.vldb_eval.exp_maintenance_savings --datasets bpic2017
"""

from __future__ import annotations

import argparse

from tests.vldb_eval.eval_common import (
    ResultWriter, delete_log, eval_catalog, eval_force_persist, eval_reset,
    ingest, restart_api,
)
from tests.vldb_eval.indexing_common import (
    eager_log, run_adaptive_query, warm_up,
)
from tests.vldb_eval.suite_data import ALL_DATASETS, CASE, N_BATCHES, prepare, safe_name
from tests.vldb_eval.workload import hot_set, multiperspective, pair_coverage, save

EXPERIMENT = "exp4_maintenance"
MEASURED = range(1, N_BATCHES)


def _check_maintained(resp: dict, config: str, batch: int) -> None:
    """
    A measured ingest of a log with persisted pairs must report maintenance
    for their perspectives; an empty report means the pairs were silently
    left stale (the indexer lost the catalog) and the timing is meaningless.
    """
    maint = (resp.get("timings") or {}).get("maintenance") or {}
    n_pairs = sum(v.get("n_pairs", 0) for v in maint.values())
    errors = {p: v["error"] for p, v in maint.items() if "error" in v}
    if n_pairs == 0 or errors:
        raise RuntimeError(
            f"{config} B{batch}: no pair maintenance reported "
            f"(maintenance={maint}); refusing to record a meaningless timing"
        )


def _catalog_summary(log: str, perspectives: list[dict], hot: dict[str, set]) -> dict:
    out = {}
    for p in perspectives:
        snap = eval_catalog(log, p["grouping_keys"])["perspectives"]
        pairs = next(iter(snap.values()))["pairs"] if snap else {}
        persistent = {k for k, v in pairs.items() if v["status"] == "PERSISTENT"}
        out[p["label"]] = {
            "n_persistent": len(persistent),
            "hot_persistent": len(persistent & hot.get(p["label"], set())),
            "cold_persistent": len(persistent - hot.get(p["label"], set())),
            "n_known": len(pairs),
            "level": next(iter(snap.values()))["level"] if snap else None,
        }
    return out


def run_eager(w, prep, perspectives, slices) -> None:
    restart_api()
    for b in range(N_BATCHES):
        total = 0.0
        for p in perspectives:
            label = p["label"]
            path = prep.eager_batches(label)[b]
            resp = ingest("eager", eager_log(prep, label), path, clear_existing=(b == 0),
                          trace_id_column=None if label == CASE else p["attribute"])
            total += resp["time"]
            w.emit("ingest", config="eager", batch=b, perspective=label, file=path.name,
                   time=resp["time"], wall_s=resp["wall_s"], timings=resp.get("timings"))
            print(f"  eager   B{b} {label:<22} {resp['time']:8.1f}s")
        w.emit("batch", config="eager", batch=b, time=total)
        # No query slices on eager: its detection validates every group in
        # full, which takes minutes per query on coarse perspectives, and
        # the eager index does not depend on the workload anyway.


def run_adaptive(w, prep, perspectives, hot, promo, slices) -> None:
    restart_api()
    log = f"{prep.name}__adaptive"
    resp = ingest("adaptive", log, prep.batches[0], clear_existing=True)
    eval_reset(log)
    w.emit("ingest", config="adaptive", batch=0, time=resp["time"], wall_s=resp["wall_s"],
           timings=resp.get("timings"))
    w.emit("batch", config="adaptive", batch=0, time=resp["time"])
    warm_up(prep, log)

    # Promotion phase: replay the workload until every hot pair is persisted.
    status = {label: {} for label in hot}
    used = 0
    for q in promo:
        rec = run_adaptive_query(w, log, q, config="adaptive", batch=0, phase="promotion")
        status[q.perspective].update(rec.get("status_after") or {})
        used += 1
        if all(status[l].get(k) == "PERSISTENT" for l, ks in hot.items() for k in ks):
            break
    w.emit("promotion", queries=used, cap=len(promo),
           complete=all(status[l].get(k) == "PERSISTENT" for l, ks in hot.items() for k in ks),
           catalog=_catalog_summary(log, perspectives, hot))
    print(f"  adaptive promotion phase: {used} queries")

    for b in MEASURED:
        resp = ingest("adaptive", log, prep.batches[b])
        _check_maintained(resp, "adaptive", b)
        w.emit("ingest", config="adaptive", batch=b, time=resp["time"], wall_s=resp["wall_s"],
               timings=resp.get("timings"))
        w.emit("batch", config="adaptive", batch=b, time=resp["time"])
        print(f"  adaptive B{b} {resp['time']:8.1f}s")
        for q in slices[b]:
            run_adaptive_query(w, log, q, config="adaptive", batch=b, phase="batch")
        w.emit("catalog", config="adaptive", batch=b, summary=_catalog_summary(log, perspectives, hot))
    return log


def run_pinned(w, prep, perspectives, config: str, pairs_for) -> str:
    """Adaptive ingest with a fixed set of pinned pairs per perspective, no queries."""
    restart_api()
    log = f"{prep.name}__{config}"
    resp = ingest("adaptive", log, prep.batches[0], clear_existing=True)
    eval_reset(log)
    w.emit("ingest", config=config, batch=0, time=resp["time"], wall_s=resp["wall_s"],
           timings=resp.get("timings"))
    w.emit("batch", config=config, batch=0, time=resp["time"])
    n_pinned = 0
    for p in perspectives:
        fp = eval_force_persist(log, p["grouping_keys"], pairs_for(p["label"]))
        n_pinned += fp["requested"]
        w.emit("force_persist", config=config, perspective=p["label"],
               requested=fp["requested"], built=fp["built"], time=fp["time"])
    for b in MEASURED:
        resp = ingest("adaptive", log, prep.batches[b])
        _check_maintained(resp, config, b)
        w.emit("ingest", config=config, batch=b, time=resp["time"], wall_s=resp["wall_s"],
               timings=resp.get("timings"), n_pinned=n_pinned)
        w.emit("batch", config=config, batch=b, time=resp["time"], n_pinned=n_pinned)
        print(f"  {config:<12} B{b} {resp['time']:8.1f}s  ({n_pinned} pinned pairs)")
    return log


def run(name: str, args) -> None:
    prep = prepare(name)
    perspectives = prep.perspective_list()
    covs = {p["label"]: pair_coverage(prep, p["label"]) for p in perspectives}
    keys = {p["label"]: p["grouping_keys"] for p in perspectives}
    hot = {l: {f"{a}->{b}" for a, b in hot_set(c, args.n_hot)} for l, c in covs.items()}

    stream = multiperspective(covs, keys, args.promo_cap + args.slice * len(MEASURED),
                              args.n_hot, args.hot_ratio, args.seed)
    promo = stream[:args.promo_cap]
    slices = {b: stream[args.promo_cap + (i * args.slice): args.promo_cap + ((i + 1) * args.slice)]
              for i, b in enumerate(MEASURED)}

    n_acts = prep.perspectives["n_activities"]
    # Upper bound of the pairs "all" persists: co-occurring cross pairs plus
    # one self-pair per activity.
    all_pairs = {l: len(c["pairs"]) + n_acts for l, c in covs.items()}
    allpairs_feasible = max(all_pairs.values()) <= args.allpairs_cap

    keep = None
    if getattr(args, "reuse_eager", False) and "eager" not in args.configs:
        # Partial rerun: keep the eager measurements already on file.
        keep = lambda r: r.get("config") == "eager" and r["event"] in ("ingest", "batch", "query")  # noqa: E731
    w = ResultWriter(EXPERIMENT, name, keep=keep)
    save(stream, w.path.with_suffix(".workload.jsonl"))
    w.emit("setup", perspectives=[p["label"] for p in perspectives], P=len(perspectives),
           n_activities=n_acts, n_hot=args.n_hot, hot_ratio=args.hot_ratio, seed=args.seed,
           hot_set_total=sum(len(v) for v in hot.values()),
           hot_pairs={l: sorted(v) for l, v in hot.items()},
           pair_space={l: len(c["pairs"]) for l, c in covs.items()},
           all_pairs_upper_bound=all_pairs, allpairs_measured=allpairs_feasible,
           sweep=args.sweep, slice=args.slice, promo_cap=args.promo_cap,
           batch_events=[sum(1 for _ in open(p)) - 1 for p in prep.batches])

    logs = []
    configs = args.configs
    if "eager" in configs:
        run_eager(w, prep, perspectives, slices)
        logs += [eager_log(prep, p["label"]) for p in perspectives]
    if "adaptive" in configs:
        logs.append(run_adaptive(w, prep, perspectives, hot, promo, slices))
    if "sweep" in configs:
        for n in args.sweep:
            logs.append(run_pinned(
                w, prep, perspectives, f"pinned_{n}",
                lambda label, n=n: [list(p) for p in hot_set(covs[label], n)],
            ))
    if "allpairs" in configs and allpairs_feasible:
        logs.append(run_pinned(w, prep, perspectives, "allpairs", lambda label: "all"))
    w.done()
    if not args.keep_logs:
        for log in logs:
            delete_log(log)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--datasets", nargs="+", default=ALL_DATASETS)
    ap.add_argument("--configs", nargs="+", default=["eager", "adaptive", "sweep", "allpairs"],
                    choices=["eager", "adaptive", "sweep", "allpairs"])
    ap.add_argument("--n-hot", type=int, default=3, help="hot pairs per perspective")
    ap.add_argument("--hot-ratio", type=float, default=0.8)
    ap.add_argument("--slice", type=int, default=10, help="queries after each measured batch")
    ap.add_argument("--promo-cap", type=int, default=150)
    ap.add_argument("--sweep", nargs="+", type=int, default=[10, 30, 100],
                    help="pinned pairs per perspective")
    ap.add_argument("--allpairs-cap", type=int, default=3000,
                    help="max co-occurring pairs per perspective for a measured all-pairs run")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--keep-logs", action="store_true")
    ap.add_argument("--reuse-eager", action="store_true",
                    help="with --configs lacking eager: keep the eager records already on file")
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()
    for name in args.datasets:
        if args.resume and ResultWriter.is_complete(EXPERIMENT, name):
            print(f"[{name}] complete, skipping")
            continue
        run(name, args)


if __name__ == "__main__":
    main()
