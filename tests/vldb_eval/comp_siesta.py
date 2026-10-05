"""
tests/vldb_eval/comp_siesta.py — adaptive SIESTA on the Exp 4 pattern queries.

Same queries and result format as the competitor adapters (see
pattern_common).  Per (dataset, perspective), on a freshly ingested log:

  cold  every query from an empty pair state: the LRU is dropped
        (eval_reset) before each query and promotion is disabled
        (min_query_count = 10^9), so every pair is extracted by a scan
  lru   the query's pairs served from the LRU (2nd execution)
  warm  the query's pairs persisted (Delta): the query is re-run with
        wait_promotion until its pairs are PERSISTENT (min_query_count = 3),
        then measured

``time_s`` is SIESTA's server-side latency; background promotion work is
excluded (reported as promotion_s).  A query exceeding the timeout is
abandoned and the API restarted (Spark keeps running a job whose HTTP
request timed out).

Output: results/exp4_patterns/siesta/<ds>.<perspective>.jsonl

Usage:
    python -m tests.vldb_eval.comp_siesta --datasets bpic2015 --perspectives case
"""

from __future__ import annotations

import argparse
import json
import time

import requests

from tests.vldb_eval.eval_common import (
    RETENTION, RESULTS_DIR, eval_catalog, eval_drain, eval_reset, ingest, query_adaptive, restart_api, run_meta,
)
from tests.vldb_eval.pattern_common import (
    PERSPECTIVES, SKIP_AFTER, TIMEOUT_S, SkipAfter, load_queries, safe, skipped_record,
)
from tests.vldb_eval.suite_data import prepare

OUT_DIR = RESULTS_DIR / "exp4_patterns" / "siesta"
NO_PROMOTION = {**RETENTION, "min_query_count": 10**9}


def _run(log: str, q: dict, retention: dict, wait: bool = False) -> dict:
    try:
        r = query_adaptive(log, q["siesta_pattern"], q["grouping_keys"],
                           wait_promotion=wait, retention=retention,
                           timeout=TIMEOUT_S + 60)
    except requests.exceptions.Timeout:
        restart_api()
        return {"timed_out": True}
    except requests.exceptions.RequestException as exc:
        restart_api()
        return {"error": str(exc)[:300]}
    if r.get("code", 200) != 200:
        return {"error": str(r)[:300]}
    return r


def _record(f, q: dict, variant: str, r: dict, rep: int = 0) -> dict:
    timed_out = bool(r.get("timed_out")) or (r.get("time", 0) > TIMEOUT_S)
    n = None if timed_out or "error" in r else r.get("matched_groups")
    rec = {
        "system": "siesta", "variant": variant, "qid": q["qid"], "dataset": q["dataset"],
        "perspective": q["perspective"], "length": q["length"], "kind": q["kind"], "rep": rep,
        "time_s": r.get("time"), "wall_s": r.get("wall_s"), "matched_groups": n,
        "truth": q["truth"]["matched_groups"], "parity": n == q["truth"]["matched_groups"] if n is not None else None,
        "timed_out": timed_out, "error": r.get("error"),
        "phases": r.get("timings"), "pair_sources": r.get("pair_sources"),
        "promotion_s": r.get("promotion_s"),
    }
    f.write(json.dumps(rec) + "\n")
    f.flush()
    return rec


def run(ds: str, perspective: str, setup: bool, limit: int | None, qids: set | None,
        skip_after: int = SKIP_AFTER) -> None:
    qs = load_queries(ds, perspective)
    if qids:
        qs = [q for q in qs if q["qid"] in qids]
    if limit:
        qs = qs[:limit]
    log = ds
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUT_DIR / f"{ds}.{safe(perspective)}.jsonl"
    with path.open("w") as f:
        restart_api()
        t0 = time.perf_counter()
        ing = ingest("adaptive", log, prepare(ds).full_csv, clear_existing=True) if setup else {}
        f.write(json.dumps({"event": "setup", "system": "siesta", "dataset": ds, "perspective": perspective,
                            "ingest_s": ing.get("time"), "wall_s": time.perf_counter() - t0,
                            "retention": RETENTION, "meta": run_meta()}) + "\n")
        _run(log, qs[-1], NO_PROMOTION)  # warm-up (JVM, perspective L1), discarded

        # cold: empty pair state per query, nothing promoted
        cold_timeout = set()
        cutoff = SkipAfter(skip_after)
        for q in qs:
            if cutoff.skip(q):
                cold_timeout.add(q["qid"])
                f.write(json.dumps(skipped_record("siesta", q, variant="cold")) + "\n")
                f.flush()
                print(f"  [{ds}/{perspective}] cold {q['qid']} skipped ({skip_after} consecutive timeouts)")
                continue
            eval_reset(log)
            eval_catalog(log)  # reload the catalog outside the measured query
            rec = _record(f, q, "cold", _run(log, q, NO_PROMOTION))
            cutoff.record(q, rec["timed_out"])
            if rec["timed_out"]:
                cold_timeout.add(q["qid"])
            print(f"  [{ds}/{perspective}] cold {q['qid']} {rec['time_s']} parity={rec['parity']}")

        # lru then warm (persisted), per query.  A query whose cold run timed
        # out is not re-run (its first execution of this phase is that same
        # scan); its lru / warm records are marked skipped.
        eval_reset(log)
        for q in qs:
            if q["qid"] in cold_timeout:
                for variant in ("lru", "warm"):
                    _record(f, q, variant, {"timed_out": True, "error": "skipped: cold run timed out"})
                print(f"  [{ds}/{perspective}] skip {q['qid']} (cold run timed out)")
                continue
            _run(log, q, RETENTION)                                    # 1st: scan
            rec = _record(f, q, "lru", _run(log, q, RETENTION))        # 2nd: LRU
            for _ in range(3):                                        # promote
                r = _run(log, q, RETENTION, wait=True)
                after = (r.get("pair_status_after") or {}).values()
                if after and all(s == "PERSISTENT" for s in after):
                    break
            eval_drain(log)
            rec = _record(f, q, "warm", _run(log, q, RETENTION))
            print(f"  [{ds}/{perspective}] warm {q['qid']} {rec['time_s']} parity={rec['parity']} "
                  f"{sorted(set((rec['pair_sources'] or {}).values()))}")
        f.write(json.dumps({"event": "done"}) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--datasets", nargs="+", default=["bpic2011", "bpic2012", "bpic2015", "bpic2017", "bpic2018"])
    ap.add_argument("--perspectives", nargs="*", default=None)
    ap.add_argument("--no-setup", action="store_true")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--qids", nargs="*")
    ap.add_argument("--skip-after", type=int, default=SKIP_AFTER,
                    help="consecutive cold timeouts of a kind before the rest of that kind is skipped (0: off)")
    args = ap.parse_args()
    for ds in args.datasets:
        persps = args.perspectives if args.perspectives is not None else ["case"] + PERSPECTIVES.get(ds, [])
        for persp in persps:
            run(ds, persp, not args.no_setup, args.limit, set(args.qids) if args.qids else None,
                args.skip_after)


if __name__ == "__main__":
    main()
