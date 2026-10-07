"""
tests/vldb_eval/scaleout/exp_scaleout.py — Experiment 2: SCALE-OUT.

Index and query a replicated BPIC2017 on the running Docker Swarm, sweeping the
number of Spark executor cores (the scale-out axis) and the replication factor
(data size).  Produces the data behind two figures:

  Fig 1  eager (case-centric) vs adaptive INDEXING time, and QUERY latency with
         and without attribute predicates — on the case perspective.
  Fig 2  distributed multiperspective ADAPTIVE querying under the Action and
         org:resource perspectives, structural and attribute-predicate queries.

Queries come from a fixed, non-empty workload (scaleout/workload_scaleout.py)
shared by every cell; each query row records the server's matched_groups next
to the expected count from r1.

Shuffle time for the group-colocation step is captured per index build from the
Spark driver REST metrics (scaleout/shuffle.py) and from the indexer's own
per-stage timings.

Each (system, size, cores) cell writes one JSONL under
results/exp_scaleout/; ``--resume`` skips completed cells.

Usage:
    # one-time: build the datasets
    python -m tests.vldb_eval.scaleout.replicate_bpic2017 --factors 1 2 3
    # smoke (1x, 3 cores) end to end:
    python -m tests.vldb_eval.scaleout.exp_scaleout --quick
    # full sweep (resumable):
    python -m tests.vldb_eval.scaleout.exp_scaleout \
        --sizes 1 2 3 --cores 3 6 9 12 15 18 --resume
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import requests

_REPO = Path(__file__).resolve().parents[3]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from tests.vldb_eval.eval_common import (
    RESULTS_DIR, LOOKBACK,
    discover_schema, ingest, query_adaptive, query_eager, run_meta,
)
from tests.vldb_eval.scaleout import cluster, shuffle, workload_scaleout

DATA_DIR = Path("/srv/datasets/scaleout")
OUT_DIR = RESULTS_DIR / "exp_scaleout"
CASE = "case"
MAX_WORKER_CORES = cluster.N_WORKERS * cluster.WORKER_CORES  # 18

# Queries come from a fixed workload (scaleout/workload_scaleout.py), built
# once from the r1 log and reused for every (size, cores) cell: per perspective,
# the same activity pairs as a structural query `A B` and as an attribute query
# `A[attr="v"] B` whose predicate is non-empty and selective.


# ---------------------------------------------------------------------------
# Per-cell runs
# ---------------------------------------------------------------------------

def _time_index(system: str, log_name: str, csv: Path) -> dict:
    """Index `csv` under `system`, capturing build time, stage timings, shuffle."""
    before = shuffle.snapshot()
    resp = ingest(system, log_name, csv, clear_existing=True,
                  extra={"lookback": LOOKBACK})
    after = shuffle.snapshot()
    return {
        "index_time_s": float(resp.get("time", 0.0)),
        "wall_s": float(resp.get("wall_s", 0.0)),
        "timings": resp.get("timings", {}),
        "shuffle": shuffle.diff(before, after),
    }


def _run_queries(system: str, log_name: str, perspective: str,
                 grouping_keys: list[str], queries: list[tuple[str, int]], kind: str,
                 rounds: int, query_timeout: float,
                 retention: dict | None = None) -> list[dict]:
    """Run (pattern, expected_groups) queries `rounds` times each.

    `retention` overrides the adaptive retention parameters sent with each
    query (default: eval_common.RETENTION)."""
    rows: list[dict] = []
    for rnd in range(rounds):
        for i, (pat, expected) in enumerate(queries):
            t0 = time.perf_counter()
            try:
                if system == "adaptive":
                    r = query_adaptive(log_name, pat, grouping_keys,
                                       retention=retention, timeout=query_timeout)
                else:
                    r = query_eager(log_name, pat, timeout=query_timeout)
            except requests.exceptions.Timeout:
                # Censor: this query exceeds the cap (e.g. a coarse perspective
                # validating huge groups on few executors).  Record the cap so
                # the figure still shows the point without blocking the sweep,
                # and cancel it on the server so the next query starts clean.
                wall = time.perf_counter() - t0
                killed = cluster.ensure_idle()
                print(f"      query TIMEOUT>{query_timeout}s [{kind}/{perspective}] "
                      f"{pat!r} (killed {killed} jobs)")
                rows.append({
                    "type": "query", "system": system, "perspective": perspective,
                    "kind": kind, "round": rnd, "qi": i, "pattern": pat,
                    "latency_s": float(query_timeout), "wall_s": wall,
                    "matched_groups": None, "expected_groups": expected,
                    "group_count": None, "timeout": True, "killed_jobs": killed,
                })
                continue
            except Exception as e:
                print(f"      query failed [{kind}/{perspective}] {pat!r}: {e}")
                cluster.ensure_idle()
                continue
            # Adaptive reports matched_groups, eager reports total; keep a real 0.
            mg = r.get("matched_groups")
            if mg is None:
                mg = r.get("total")
            if mg == 0:
                print(f"      warning: 0 matched groups [{kind}/{perspective}] {pat!r}")
            rows.append({
                "type": "query", "system": system, "perspective": perspective,
                "kind": kind, "round": rnd, "qi": i, "pattern": pat,
                "latency_s": float(r.get("time", r.get("wall_s", 0.0))),
                "wall_s": float(r.get("wall_s", 0.0)),
                "matched_groups": mg, "expected_groups": expected,
                "group_count": r.get("group_count"), "timeout": False,
            })
    return rows


def cell_path(part: str, system: str, size: int, cores: int) -> Path:
    # Part "mp" re-indexes adaptive and runs only the attribute perspectives.
    name = f"{system}_r{size}_c{cores}" if part == "case" else f"adaptivemp_r{size}_c{cores}"
    return OUT_DIR / f"{name}.jsonl"


def run_cell(system: str, size: int, cores: int, *, workload: dict, rounds: int,
             query_timeout: float, out: Path, query_limit: int | None = None,
             part: str = "case") -> None:
    """One (system, size, cores) cell of one part.

    part "case" (Fig 1): index and query the case perspective; the index row is
    the one plotted.  part "mp" (Fig 2): adaptive only, re-indexed so its
    adaptive state starts clean, then queried under the attribute perspectives;
    that index row is stored as "reindex" and not plotted.
    """
    csv = DATA_DIR / f"bpic2017_r{size}.csv"
    if not csv.exists():
        raise FileNotFoundError(f"missing dataset {csv}; run replicate_bpic2017 first")
    log_name = f"so_{system}_r{size}"
    records: list[dict] = []

    print(f"  [{system}/{part}] index r{size} (cores={cores}) …")
    # Start from an idle driver: no leftover queries or background pair
    # promotions of the previous cell may overlap this index or its shuffle diff.
    leftover = cluster.ensure_idle()
    if leftover:
        print(f"    (cancelled {leftover} leftover jobs before indexing)")
    idx = _time_index(system, log_name, csv)
    # Record the executors the driver actually held, so the x-axis value of
    # every cell is verified rather than assumed.
    ex = cluster.driver_executors() or {}
    idx.update({"type": "index" if part == "case" else "reindex", "part": part,
                "system": system, "size": size, "cores": cores,
                "log_name": log_name, "executors": ex.get("executors"),
                "executor_cores": ex.get("cores"), "driver_app": ex.get("app")})
    if ex.get("cores") != min(cores, MAX_WORKER_CORES):
        print(f"    warning: driver held {ex.get('cores')} cores, expected {cores}")
    print(f"    index_time={idx['index_time_s']:.1f}s "
          f"shuffle_s={idx['shuffle'].get('shuffle_seconds')} "
          f"shuffle_bytes={idx['shuffle'].get('shuffle_bytes')} "
          f"executors={ex.get('executors')}")
    records.append(idx)

    # Part "case": the case perspective only (eager is trace_id-grouped, so this
    # is all it can answer).  Part "mp": the attribute perspectives.
    if part == "case":
        persps = [CASE]
    else:
        persps = [p for p in workload["perspectives"] if p != CASE]
    for persp in persps:
        spec = workload["perspectives"][persp]
        qs = spec["queries"][:query_limit]
        gkeys = [spec["group_column"]]
        # Expected counts are for r1; a case group is a trace, so replicas
        # multiply them, while attribute-perspective groups just grow.
        scale = size if persp == CASE else 1
        struct = [(q["structural"], q["groups_struct"] * scale) for q in qs]
        attr = [(q["attribute"], q["groups_attr"] * scale) for q in qs]
        for rows in (_run_queries(system, log_name, persp, gkeys, struct,
                                  "structural", rounds, query_timeout),
                     _run_queries(system, log_name, persp, gkeys, attr,
                                  "attribute", rounds, query_timeout)):
            for r in rows:
                r["part"] = part
            records += rows
        print(f"    {persp}: {len(struct)} structural, {len(attr)} attribute")

    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w") as f:
        f.write(json.dumps({"type": "meta", "part": part, "system": system,
                            "size": size, "cores": cores, "query_limit": query_limit,
                            "rounds": rounds, **run_meta()}) + "\n")
        for r in records:
            f.write(json.dumps(r, default=str) + "\n")
    print(f"    wrote {out}")


# ---------------------------------------------------------------------------
# Sweep driver
# ---------------------------------------------------------------------------

def main() -> None:
    global DATA_DIR
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sizes", nargs="+", type=int, default=[1, 2, 3])
    ap.add_argument("--cores", nargs="+", type=int, default=[3, 6, 9, 12, 15, 18])
    ap.add_argument("--systems", nargs="+", default=["eager", "adaptive"],
                    choices=["eager", "adaptive"])
    ap.add_argument("--perspective-keys", default="Action,org:resource",
                    help="comma-separated grouping perspectives to query under "
                         "(in addition to case). Empty -> top-N discovered.")
    ap.add_argument("--perspectives", type=int, default=4,
                    help="if --perspective-keys is empty, how many discovered to use")
    ap.add_argument("--n-queries", type=int, default=4,
                    help="activity pairs per perspective in the fixed workload")
    ap.add_argument("--query-limit", type=int, default=None,
                    help="run only the first N pairs per perspective of the workload "
                         "(the workload is ordered, so every cell runs the same queries)")
    ap.add_argument("--rounds", type=int, default=2)
    ap.add_argument("--parts", nargs="+", default=["case", "mp"], choices=["case", "mp"],
                    help="run each part's whole grid in this order: case = Fig 1 "
                         "(eager + adaptive, case perspective), mp = Fig 2 "
                         "(adaptive, attribute perspectives)")
    ap.add_argument("--query-timeout", type=float, default=120.0,
                    help="per-query cap (s); slower queries are censored at this value")
    ap.add_argument("--data-dir", type=Path, default=DATA_DIR)
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--no-cluster", action="store_true",
                    help="assume the stack is already deployed at the right size")
    ap.add_argument("--quick", action="store_true",
                    help="smoke: sizes=[1], cores=[3], 2 queries per kind, rounds=1")
    args = ap.parse_args()

    DATA_DIR = args.data_dir

    query_limit = args.query_limit
    if args.quick:
        args.sizes, args.cores, args.rounds = [1], [3], 1
        query_limit = query_limit or 2

    base_csv = DATA_DIR / "bpic2017_r1.csv"
    persps = [p.strip() for p in args.perspective_keys.split(",") if p.strip()]
    if not persps:
        persps = list(discover_schema(base_csv).perspective_keys)[:args.perspectives]
    workload = workload_scaleout.ensure(persps, args.n_queries, csv=base_csv)
    print(f"workload: {workload_scaleout.WORKLOAD_JSON} "
          f"perspectives={list(workload['perspectives'])}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    def done(path: Path) -> bool:
        return args.resume and path.exists() and path.stat().st_size > 0

    for part in args.parts:
        systems = args.systems if part == "case" else [s for s in args.systems if s == "adaptive"]
        print(f"######## part {part}: systems={systems} ########")
        for cores in args.cores:
            expected = min(cores, MAX_WORKER_CORES)
            if all(done(cell_path(part, s, z, cores)) for z in args.sizes for s in systems):
                print(f"== [{part}] cores_max={cores}: all cells done, skipping ==")
                continue
            if not args.no_cluster:
                print(f"== [{part}] resize cluster to cores_max={cores} ==")
                try:
                    cluster.resize(cores)
                    # Blocks until the *new* driver holds `expected` executor cores.
                    cluster.wait_ready(expected_cores=expected)
                except Exception as e:
                    print(f"  !! resize to {cores} failed, skipping this point: {e}")
                    continue
            for size in args.sizes:
                print(f"-- size r{size} --")
                for system in systems:
                    out = cell_path(part, system, size, cores)
                    if done(out):
                        print(f"  skip (resume) {out.name}")
                        continue
                    try:
                        run_cell(system, size, cores, workload=workload,
                                 rounds=args.rounds, query_timeout=args.query_timeout,
                                 out=out, query_limit=query_limit, part=part)
                    except Exception as e:
                        # Keep the overnight sweep alive: log the failed cell and
                        # move on (it can be retried later with --resume).  If the
                        # API went away, wait for it before the next cell so one
                        # outage does not fail every remaining cell.
                        print(f"  !! cell failed [{system}/{part} r{size} c{cores}]: {e}")
                        if not args.no_cluster:
                            try:
                                cluster.wait_ready(expected_cores=expected)
                            except Exception as e2:
                                print(f"  !! cluster not ready after failure: {e2}")

    print("sweep complete. Plot with: python -m tests.vldb_eval.scaleout.plot_scaleout")


if __name__ == "__main__":
    main()
