"""
tests/vldb_eval/scaleout/exp_scaleout_persist.py — Exp 2 counterfactual: Fig 1
case queries with adaptive pairs forced PERSISTENT.

In the main sweep every adaptive pair stayed TRANSIENT (the retention demand
gate never promoted it), so each query shipped its pair rows through the driver.
This re-runs only the adaptive case queries of Fig 1 with part of the pairs
persisted up front, to show how much of the gap to eager is the transient tier:

  size 3:  100% persistent -- every pair the workload needs
  size 2:   50% persistent -- the pairs of the first half of the case queries

A query on pair (A, B) needs (A, B); its attribute variant A[pred] B also needs
the self-pair (A, A).  Pairs are persisted once through eval_force_persist
(pinned, so retention cannot demote them; the catalog is in Delta, so they
survive the API restart of every resize).  No re-indexing: the adaptive indexes
of the main sweep are reused.  Auto-promotion is switched off for these queries
(min_query_count huge), so the pairs left out stay TRANSIENT as in the sweep.

Output: results/exp_scaleout/adaptivep<pct>_r<size>_c<cores>.jsonl

Usage:
    python -m tests.vldb_eval.scaleout.exp_scaleout_persist --resume
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from tests.vldb_eval.eval_common import (  # noqa: E402
    RETENTION, eval_force_persist, pair_statuses, run_meta,
)
from tests.vldb_eval.scaleout import cluster, workload_scaleout  # noqa: E402
from tests.vldb_eval.scaleout.exp_scaleout import (  # noqa: E402
    CASE, MAX_WORKER_CORES, OUT_DIR, _run_queries,
)

# size -> fraction of the case queries whose pairs are persisted
VARIANTS = {3: 1.0, 2: 0.5}
GROUPING = ["trace_id"]
# Same retention as the sweep, but no auto-promotion: unforced pairs stay TRANSIENT.
NO_PROMOTION = {**RETENTION, "min_query_count": 10**9}


def needed_pairs(q: dict) -> list[tuple[str, str]]:
    """Pairs the structural and attribute query of one workload pair read."""
    a, b = q["source"], q["target"]
    return [(a, b), (a, a)]


def cell_path(pct: int, size: int, cores: int) -> Path:
    return OUT_DIR / f"adaptivep{pct}_r{size}_c{cores}.jsonl"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cores", nargs="+", type=int, default=[3, 6, 9, 12, 15, 18])
    ap.add_argument("--query-limit", type=int, default=2)
    ap.add_argument("--rounds", type=int, default=2)
    ap.add_argument("--query-timeout", type=float, default=120.0)
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()

    qs = workload_scaleout.load()["perspectives"][CASE]["queries"][:args.query_limit]
    plan = {}
    for size, frac in VARIANTS.items():
        forced = qs[:math.ceil(len(qs) * frac)]
        pairs = sorted({p for q in forced for p in needed_pairs(q)})
        plan[size] = {"pct": round(frac * 100), "pairs": pairs}

    # One-off: persist the chosen pairs of each size's existing adaptive index.
    cluster.wait_ready()
    for size, p in plan.items():
        log = f"so_adaptive_r{size}"
        r = eval_force_persist(log, GROUPING, pairs=[list(x) for x in p["pairs"]])
        st = pair_statuses(log, GROUPING)
        print(f"[r{size}] persisted {p['pct']}%: built {r.get('built')} of "
              f"{r.get('requested')} pairs in {r.get('time', 0):.1f}s; "
              f"statuses={ {f'{a}->{b}': st.get(f'{a}->{b}') for a, b in p['pairs']} }")

    for cores in args.cores:
        expected = min(cores, MAX_WORKER_CORES)
        if args.resume and all(cell_path(p["pct"], s, cores).exists() for s, p in plan.items()):
            print(f"== cores_max={cores}: done, skipping ==")
            continue
        print(f"== resize cluster to cores_max={cores} ==")
        cluster.resize(cores)
        cluster.wait_ready(expected_cores=expected)
        for size, p in plan.items():
            out = cell_path(p["pct"], size, cores)
            if args.resume and out.exists():
                print(f"  skip (resume) {out.name}")
                continue
            log = f"so_adaptive_r{size}"
            cluster.ensure_idle()
            st = pair_statuses(log, GROUPING)
            forced = {f"{a}->{b}": st.get(f"{a}->{b}") for a, b in p["pairs"]}
            if any(v != "PERSISTENT" for v in forced.values()):
                print(f"  warning: forced pairs not all PERSISTENT: {forced}")
            ex = cluster.driver_executors() or {}
            print(f"  [adaptive/persist{p['pct']}] r{size} (cores={cores}, "
                  f"executors={ex.get('executors')}) …")
            struct = [(q["structural"], q["groups_struct"] * size) for q in qs]
            attr = [(q["attribute"], q["groups_attr"] * size) for q in qs]
            rows = (_run_queries("adaptive", log, CASE, GROUPING, struct, "structural",
                                 args.rounds, args.query_timeout, retention=NO_PROMOTION)
                    + _run_queries("adaptive", log, CASE, GROUPING, attr, "attribute",
                                   args.rounds, args.query_timeout, retention=NO_PROMOTION))
            variant = f"persist{p['pct']}"
            for r in rows:
                r.update({"part": "case", "variant": variant})
            with out.open("w") as f:
                f.write(json.dumps({
                    "type": "meta", "part": "case", "variant": variant, "system": "adaptive",
                    "size": size, "cores": cores, "persist_fraction": p["pct"] / 100,
                    "persisted_pairs": [f"{a}->{b}" for a, b in p["pairs"]],
                    "persisted_status": forced, "executors": ex.get("executors"),
                    "executor_cores": ex.get("cores"), "query_limit": args.query_limit,
                    "rounds": args.rounds, "retention": NO_PROMOTION, **run_meta(),
                }) + "\n")
                for r in rows:
                    f.write(json.dumps(r, default=str) + "\n")
            print(f"    wrote {out.name}")

    print("done. Plot with: python -m tests.vldb_eval.scaleout.plot_scaleout")


if __name__ == "__main__":
    main()
