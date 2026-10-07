"""
tests/vldb_eval/comp_siesta.py — adaptive SIESTA on the Exp 4 pattern queries.

Same queries and result format as the competitor adapters (see
pattern_common).  Per (dataset, perspective), on a freshly ingested log,
SIESTA is measured WARM only, like the competitors' pre-built indexes: for
each query, one unmeasured run with forced promotion (min_query_count = 1,
which bypasses the cost gate) builds and persists the query's pairs, then
the query is run again and measured (variant ``warm``).  Pairs persisted
for earlier queries stay persisted, as in a pre-built index.  Build /
promotion cost is not part of this experiment.

``time_s`` is SIESTA's server-side latency.  A query exceeding the timeout
(in the promoting or the measured run) is recorded as timed out and the API
restarted (Spark keeps running a job whose HTTP request timed out).

Output: results/exp4_patterns/siesta/<ds>.<perspective>.jsonl

Usage:
    python -m tests.vldb_eval.comp_siesta --datasets bpic2015 --perspectives case
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import requests

from tests.vldb_eval.eval_common import (
    RETENTION, RESULTS_DIR, eval_drain, ingest, query_adaptive, restart_api, run_meta,
)
from tests.vldb_eval.pattern_common import (
    PERSPECTIVES, SKIP_AFTER, TIMEOUT_S, SkipAfter, load_queries, safe, skipped_record,
)
from tests.vldb_eval.suite_data import prepare

OUT_DIR = RESULTS_DIR / "exp4_patterns" / "siesta"
NO_PROMOTION = {**RETENTION, "min_query_count": 10**9}
FORCE_PROMOTION = {**RETENTION, "min_query_count": 1}
QUERY_TIMEOUT_S = TIMEOUT_S  # --timeout


def _run(log: str, q: dict, retention: dict, wait: bool = False) -> dict:
    try:
        r = query_adaptive(log, q["siesta_pattern"], q["grouping_keys"],
                           wait_promotion=wait, retention=retention,
                           timeout=QUERY_TIMEOUT_S + 60)
    except requests.exceptions.Timeout:
        restart_api()
        return {"timed_out": True}
    except requests.exceptions.RequestException as exc:
        restart_api()
        return {"error": str(exc)[:300]}
    if r.get("code", 200) != 200:
        return {"error": str(r)[:300]}
    return r


def _record(f, q: dict, variant: str, r: dict, rep: int = 0, **extra) -> dict:
    timed_out = bool(r.get("timed_out")) or ((r.get("time") or 0) > QUERY_TIMEOUT_S)
    n = None if timed_out or "error" in r else r.get("matched_groups")
    rec = {
        "system": "siesta", "variant": variant, "qid": q["qid"], "dataset": q["dataset"],
        "perspective": q["perspective"], "length": q["length"], "kind": q["kind"], "rep": rep,
        "time_s": r.get("time"), "wall_s": r.get("wall_s"), "matched_groups": n,
        "truth": q["truth"]["matched_groups"], "parity": n == q["truth"]["matched_groups"] if n is not None else None,
        "timed_out": timed_out, "error": r.get("error"),
        "phases": r.get("timings"), "pair_sources": r.get("pair_sources"),
        "promotion_s": r.get("promotion_s"), **extra,
    }
    f.write(json.dumps(rec) + "\n")
    f.flush()
    return rec


def run(ds: str, perspective: str, setup: bool, limit: int | None, qids: set | None,
        skip_after: int = SKIP_AFTER, out: str | None = None) -> None:
    qs = load_queries(ds, perspective)
    if qids:
        qs = [q for q in qs if q["qid"] in qids]
    if limit:
        qs = qs[:limit]
    log = ds
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = Path(out) if out else OUT_DIR / f"{ds}.{safe(perspective)}.jsonl"
    with path.open("w") as f:
        restart_api()
        t0 = time.perf_counter()
        ing = ingest("adaptive", log, prepare(ds).full_csv, clear_existing=True) if setup else {}
        f.write(json.dumps({"event": "setup", "system": "siesta", "dataset": ds, "perspective": perspective,
                            "ingest_s": ing.get("time"), "wall_s": time.perf_counter() - t0,
                            "retention": RETENTION, "meta": run_meta()}) + "\n")
        _run(log, qs[-1], NO_PROMOTION)  # warm-up (JVM, perspective L1), discarded

        def attempt(q: dict) -> tuple[dict, bool]:
            """Forced promotion (unmeasured), then the measured warm run."""
            persisted, r = False, {}
            for _ in range(2):
                r = _run(log, q, FORCE_PROMOTION, wait=True)
                if r.get("timed_out") or "error" in r:
                    return {**r, "stage": "promotion"}, False
                after = (r.get("pair_status_after") or {}).values()
                if after and all(st == "PERSISTENT" for st in after):
                    persisted = True
                    break
            eval_drain(log)
            return _run(log, q, FORCE_PROMOTION), persisted

        cutoff = SkipAfter(skip_after)
        for q in qs:
            if cutoff.skip(q):
                f.write(json.dumps(skipped_record("siesta", q, variant="warm")) + "\n")
                f.flush()
                print(f"  [{ds}/{perspective}] warm {q['qid']} skipped ({skip_after} consecutive timeouts)")
                continue
            r, persisted = attempt(q)
            retried = False
            if "error" in r and not r.get("timed_out"):
                # The driver died (e.g. its container's memory limit) and _run
                # restarted the API: warm the new JVM up, then retry once.
                _run(log, qs[-1], NO_PROMOTION)
                r, persisted = attempt(q)
                retried = True
            rec = _record(f, q, "warm", r, all_persisted=persisted, retried=retried, stage=r.get("stage"))
            cutoff.record(q, rec["timed_out"])
            print(f"  [{ds}/{perspective}] warm {q['qid']} {rec['time_s']} parity={rec['parity']} "
                  f"persisted={persisted}{' retried' if retried else ''} "
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
                    help="consecutive timeouts of a kind before the rest of that kind is skipped (0: off)")
    ap.add_argument("--timeout", type=float, default=TIMEOUT_S, help="per-query timeout (s)")
    ap.add_argument("--out", default=None, help="output path (single dataset/perspective only)")
    args = ap.parse_args()
    global QUERY_TIMEOUT_S
    QUERY_TIMEOUT_S = args.timeout
    for ds in args.datasets:
        persps = args.perspectives if args.perspectives is not None else ["case"] + PERSPECTIVES.get(ds, [])
        for persp in persps:
            run(ds, persp, not args.no_setup, args.limit, set(args.qids) if args.qids else None,
                args.skip_after, args.out)


if __name__ == "__main__":
    main()
