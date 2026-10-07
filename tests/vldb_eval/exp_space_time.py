"""
tests/vldb_eval/exp_space_time.py — Section 2, Exp 1: space-time trade-off of
adaptive vs eager SIESTA (BPIC 2012, 2015, 2017).

Part 1  case perspective.  Eager indexes every pair up front; adaptive
        persists only the pairs a skewed workload keeps querying.  Eager's
        pairs-index bytes are split by how often the workload touches each
        pair (never / 1-2 / 3+), and both systems' storage and query latency
        are recorded per workload round: storage vs latency.
Part 2  all perspectives of P (case + top-3, as in the indexing suite).
        Eager keeps one full index per perspective; adaptive one shared
        sequence table, per-perspective overlays and the persisted hot
        pairs, under the multiperspective workload.  Eager queries under the
        coarse attribute perspectives validate whole huge groups (minutes),
        so eager latency there is sampled (``--eager-sample`` queries per
        perspective, ``--timeout`` each).
Part 3  attribute embedding vs join-back.  Eager (case) and adaptive (case,
        plus the BPIC 2017 attribute perspectives) are indexed twice: with
        attribute maps in the pair rows (default) and without them
        (embed_pair_attributes = False, attributes joined back from the
        Activity index / perspective sequence at query time).  Pair-table
        size and the latency of structural and attribute-aware queries
        (a sample of the Exp 4 queries, lengths 8-15) for both.

Storage is the live size of each Delta table (storage_common), i.e. after
VACUUM; the LRU is in memory and reported as entries.

Output: results/exp_space_time/<dataset>.jsonl

Usage:
    python -m tests.vldb_eval.exp_space_time --datasets bpic2015 --parts 1 2 3
"""

from __future__ import annotations

import argparse
import collections
import time

import requests

from tests.vldb_eval.eval_common import (
    RETENTION, ResultWriter, eval_catalog, eval_drain, eval_reset, ingest, query_adaptive, query_eager, restart_api,
)
from tests.vldb_eval.indexing_common import eager_log
from tests.vldb_eval.pattern_common import PERSPECTIVES, load_queries
from tests.vldb_eval.storage_common import pair_row_counts, summarise, table_sizes
from tests.vldb_eval.suite_data import CASE, prepare
from tests.vldb_eval.workload import multiperspective, pair_coverage, save, skewed

EXPERIMENT = "exp_space_time"
N_QUERIES = 50
ROUND = 10
N_HOT = 10
N_HOT_PERSP = 5
TIMEOUT_S = 700.0


def _sizes(w, log: str, **extra) -> dict:
    sizes = table_sizes(log)
    rec = w.emit("storage", log=log, summary=summarise(sizes),
                 tables={k: v["bytes"] for k, v in sizes.items()}, **extra)
    return rec["summary"]


def _with_retry(call) -> dict:
    """
    One query; on a timeout the API is restarted and the query recorded as
    timed out.  When the driver dies (HTTP 500 / dropped connection, e.g.
    its container's memory limit), the API is restarted and the query
    retried once (``retried: true``; its latency includes the new JVM's
    first query).
    """
    for attempt in range(2):
        try:
            r = call()
            if attempt:
                r["retried"] = True
            return r
        except requests.exceptions.Timeout:
            restart_api()
            return {"timed_out": True, "time": None}
        except requests.exceptions.RequestException as exc:
            restart_api()
            err = str(exc)[:300]
    return {"error": err, "time": None, "retried": True}


def _q_adaptive(log, pattern, keys, **kw) -> dict:
    return _with_retry(lambda: query_adaptive(log, pattern, keys, timeout=TIMEOUT_S + 60, **kw))


def _q_eager(log, pattern, **kw) -> dict:
    return _with_retry(lambda: query_eager(log, pattern, timeout=TIMEOUT_S, **kw))


def _ingest_eager(w, prep, label: str, embed: bool = True, suffix: str = "") -> str:
    attr = next(p["attribute"] for p in prep.perspective_list() if p["label"] == label)
    log = eager_log(prep, label) + suffix
    r = ingest("eager", log, prep.eager_full(label), clear_existing=True, trace_id_column=attr,
               extra={"embed_pair_attributes": embed})
    w.emit("ingest", system="eager", perspective=label, log=log, embed=embed,
           time=r["time"], timings=r.get("timings"))
    return log


def _ingest_adaptive(w, prep, log: str, embed: bool = True) -> str:
    r = ingest("adaptive", log, prep.full_csv, clear_existing=True,
               extra={"embed_pair_attributes": embed})
    eval_reset(log)
    w.emit("ingest", system="adaptive", log=log, embed=embed, time=r["time"], timings=r.get("timings"))
    return log


# ---------------------------------------------------------------------------
# Part 1 / 2: workload rounds
# ---------------------------------------------------------------------------

def _rounds(w, part: str, log: str, qs, eager_logs: dict, eager_sample: dict | None) -> None:
    """Adaptive: run the workload, storage after every round.  Eager: replay."""
    for r0 in range(0, len(qs), ROUND):
        for q in qs[r0:r0 + ROUND]:
            resp = _q_adaptive(log, q.pattern, q.grouping_keys, wait_promotion=True)
            w.emit("query", part=part, system="adaptive", round=r0 // ROUND, seq=q.seq,
                   perspective=q.perspective, pair=q.pair, hot=q.hot, time=resp.get("time"),
                   timed_out=resp.get("timed_out", False), matched_groups=resp.get("matched_groups"),
                   pair_sources=resp.get("pair_sources"), promotion_s=resp.get("promotion_s"),
                   retried=resp.get("retried", False), error=resp.get("error"))
        eval_drain(log)
        cat = eval_catalog(log)
        w.emit("catalog", part=part, round=r0 // ROUND, catalog=cat)
        _sizes(w, log, part=part, system="adaptive", round=r0 // ROUND)
    restart_api()
    seen = collections.Counter()
    for q in qs:
        if eager_sample is not None and q.perspective != CASE:
            seen[q.perspective] += 1
            if seen[q.perspective] > eager_sample.get(q.perspective, 0):
                continue
        resp = _q_eager(eager_logs[q.perspective], q.pattern)
        w.emit("query", part=part, system="eager", round=q.seq // ROUND, seq=q.seq,
               perspective=q.perspective, pair=q.pair, hot=q.hot, time=resp.get("time"),
               timed_out=resp.get("timed_out", False), support=resp.get("support"),
               matched_groups=resp.get("total"), retried=resp.get("retried", False), error=resp.get("error"))


def part1(w, prep) -> None:
    label, keys = CASE, ["trace_id"]
    cov = pair_coverage(prep, label)
    qs = skewed(cov, label, keys, N_QUERIES, N_HOT, 0.8, seed=42)
    save(qs, w.path.with_name(f"{prep.name}.part1.workload.jsonl"))
    restart_api()
    elog = _ingest_eager(w, prep, label)
    _sizes(w, elog, part="1", system="eager")
    rows = pair_row_counts(elog)
    touches = collections.Counter((q.source, q.target) for q in qs)
    w.emit("eager_pairs", part="1", log=elog,
           pairs=[{"pair": f"{a}->{b}", "rows": n, "touches": touches.get((a, b), 0)}
                  for (a, b), n in rows.items()])
    log = _ingest_adaptive(w, prep, prep.name)
    _sizes(w, log, part="1", system="adaptive", round=-1)
    _rounds(w, "1", log, qs, {CASE: elog}, None)


def part2(w, prep, eager_sample: int) -> None:
    persps = prep.perspective_list()
    labels = [p["label"] for p in persps]
    keys = {p["label"]: p["grouping_keys"] for p in persps}
    covs = {l: pair_coverage(prep, l) for l in labels}
    qs = multiperspective(covs, keys, N_QUERIES, N_HOT_PERSP, 0.8, seed=42)
    save(qs, w.path.with_name(f"{prep.name}.part2.workload.jsonl"))
    restart_api()
    elogs = {}
    for l in labels:
        elogs[l] = _ingest_eager(w, prep, l) if l != CASE else eager_log(prep, CASE)
        if l == CASE and not table_sizes(elogs[l]):
            elogs[l] = _ingest_eager(w, prep, l)
        _sizes(w, elogs[l], part="2", system="eager", perspective=l)
    log = _ingest_adaptive(w, prep, prep.name)
    _sizes(w, log, part="2", system="adaptive", round=-1)
    _rounds(w, "2", log, qs, elogs, {l: eager_sample for l in labels})


# ---------------------------------------------------------------------------
# Part 3: embedded attributes vs join-back
# ---------------------------------------------------------------------------

def _sample(ds: str, perspective: str, per: int, max_len: int = 15) -> list[dict]:
    qs = [q for q in load_queries(ds, perspective) if q["length"] <= max_len]
    out, seen = [], collections.Counter()
    for q in qs:
        k = (q["length"], q["kind"])
        if seen[k] < per:
            seen[k] += 1
            out.append(q)
    return out


FORCE_PROMOTION = {**RETENTION, "min_query_count": 1}


def part3(w, prep, per: int, max_len: int, systems=("eager", "adaptive"), case_only: bool = False) -> None:
    ds = prep.name
    restart_api()
    case_qs = _sample(ds, CASE, per, max_len)
    for embed in (True, False):
        variant = "embedded" if embed else "join"
        extra = {} if embed else {"pair_attributes": "join"}
        # eager, case: each query is run once unmeasured right before its
        # measured run, so eager, like adaptive (after its promotion runs),
        # is measured warm.
        if "eager" in systems:
            elog = _ingest_eager(w, prep, CASE, embed=embed, suffix="" if embed else "__join")
            _sizes(w, elog, part="3", system="eager", variant=variant)
        for q in (case_qs if "eager" in systems else []):
            _q_eager(elog, q["siesta_pattern"], extra=extra)
            r = _q_eager(elog, q["siesta_pattern"], extra=extra)
            w.emit("query", part="3", system="eager", variant=variant, qid=q["qid"], length=q["length"],
                   kind=q["kind"], time=r.get("time"), timed_out=r.get("timed_out", False),
                   matched_groups=r.get("total"), truth=q["truth"]["matched_groups"],
                   retried=r.get("retried", False), error=r.get("error"))
        # adaptive: warm (pairs persisted), case + attribute perspectives
        if "adaptive" not in systems:
            continue
        alog = _ingest_adaptive(w, prep, f"{ds}__{variant}", embed=embed)
        persps = [CASE] + ([] if case_only else PERSPECTIVES.get(ds, []))
        for persp in persps:
            qs = case_qs if persp == CASE else _sample(ds, persp, max(1, per // 2), max_len)
            for q in qs:
                # forced promotion (unmeasured), then the measured warm run
                for _ in range(2):
                    r = _q_adaptive(alog, q["siesta_pattern"], q["grouping_keys"], wait_promotion=True,
                                    retention=FORCE_PROMOTION, extra=extra)
                    after = (r.get("pair_status_after") or {}).values()
                    if r.get("timed_out") or (after and all(v == "PERSISTENT" for v in after)):
                        break
                eval_drain(alog)
                r = _q_adaptive(alog, q["siesta_pattern"], q["grouping_keys"],
                                retention=FORCE_PROMOTION, extra=extra)
                w.emit("query", part="3", system="adaptive", variant=variant, perspective=persp,
                       qid=q["qid"], length=q["length"], kind=q["kind"], time=r.get("time"),
                       timed_out=r.get("timed_out", False), matched_groups=r.get("matched_groups"),
                       truth=q["truth"]["matched_groups"], pair_sources=r.get("pair_sources"),
                       phases=r.get("timings"), retried=r.get("retried", False), error=r.get("error"))
        _sizes(w, alog, part="3", system="adaptive", variant=variant)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--datasets", nargs="+", default=["bpic2015", "bpic2012", "bpic2017"])
    ap.add_argument("--parts", nargs="+", default=["1", "2", "3"])
    ap.add_argument("--eager-sample", type=int, default=2,
                    help="eager queries per attribute perspective in part 2")
    ap.add_argument("--per", type=int, default=1, help="part 3: queries per (length, kind)")
    ap.add_argument("--p3-max-length", type=int, default=11, help="part 3: longest pattern")
    ap.add_argument("--suffix", default="", help="result file suffix (e.g. .part3 for a part-3-only run)")
    ap.add_argument("--p3-systems", nargs="+", default=["eager", "adaptive"], choices=["eager", "adaptive"])
    ap.add_argument("--p3-case-only", action="store_true", help="part 3: case perspective only")
    args = ap.parse_args()
    for ds in args.datasets:
        prep = prepare(ds)
        w = ResultWriter(EXPERIMENT, ds, suffix=args.suffix)
        w.emit("setup", perspectives=[p["label"] for p in prep.perspective_list()],
               n_queries=N_QUERIES, round=ROUND, n_hot=N_HOT, n_hot_persp=N_HOT_PERSP)
        t0 = time.time()
        if "1" in args.parts:
            part1(w, prep)
        if "2" in args.parts:
            part2(w, prep, args.eager_sample)
        if "3" in args.parts:
            w.emit("part3_setup", per=args.per, max_length=args.p3_max_length, promotion="forced (min_query_count=1)",
                   systems=args.p3_systems, eager_warm="one unmeasured run before each measured run")
            part3(w, prep, args.per, args.p3_max_length, tuple(args.p3_systems), args.p3_case_only)
        w.done(elapsed_s=time.time() - t0)


if __name__ == "__main__":
    main()
