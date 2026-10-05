"""
tests/vldb_eval/indexing_common.py

Pieces shared by the indexing experiments (Exp 1-5): the perspective under
test, log naming, and recording a query or an ingest.
"""

from __future__ import annotations

from tests.vldb_eval.eval_common import (
    ResultWriter,
    eval_drain,
    eval_reset,
    ingest,
    query_adaptive,
    query_eager,
)
from tests.vldb_eval.suite_data import CASE, Prepared, safe_name
from tests.vldb_eval.workload import Query, pair_coverage


def perspective_under_test(prep: Prepared) -> dict:
    """
    The attribute perspective of P with the most groups: the one where
    per-perspective work is largest.  Used by Exp 1-3 for every dataset.
    """
    attrs = [p for p in prep.perspective_list() if p["label"] != CASE]
    return max(attrs, key=lambda p: (pair_coverage(prep, p["label"])["group_count"], p["label"]))


def comparison_perspective(prep: Prepared) -> dict:
    """
    The case perspective: the one original SIESTA indexes natively (all pairs,
    grouped by trace id), so adaptive and eager answer the same queries over
    normal-sized groups.  Used by Exp 1-2, which compare the two systems'
    query latency.  (Under coarse attribute perspectives original SIESTA's
    detection validates each huge group in full: minutes per query.)
    """
    return next(p for p in prep.perspective_list() if p["label"] == CASE)


def eager_log(prep: Prepared, label: str) -> str:
    """Log name of the eager (original SIESTA) index of one perspective."""
    return f"{prep.name}__eager__{safe_name(label)}"


def record_query(w: ResultWriter, q: Query, resp: dict, **extra) -> dict:
    return w.emit(
        "query",
        seq=q.seq, perspective=q.perspective, pair=q.pair, hot=q.hot,
        pattern=q.pattern,
        time=resp["time"], wall_s=resp["wall_s"],
        timings=resp.get("timings", {}),
        pair_sources=resp.get("pair_sources", {}),
        status_before=resp.get("pair_status_before", {}),
        status_after=resp.get("pair_status_after"),
        promotion_s=resp.get("promotion_s"),
        support=resp.get("support"),
        matched_groups=resp.get("matched_groups"),
        group_count=resp.get("group_count"),
        **extra,
    )


def run_adaptive_query(w: ResultWriter, log: str, q: Query, **extra) -> dict:
    """
    One adaptive query that waits for the promotion it triggers, so the next
    query does not share Spark with a background pair build.  The waiting is
    not part of the recorded server latency.
    """
    resp = query_adaptive(log, q.pattern, q.grouping_keys, wait_promotion=True)
    return record_query(w, q, resp, system="adaptive", **extra)


def ingest_eager_full(w: ResultWriter, prep: Prepared, label: str, **extra) -> dict:
    """
    Eager (original SIESTA) index of one perspective over the whole log, all
    pairs indexed up front: the baseline that answers the same queries as the
    adaptive system under that perspective.
    """
    attr = next(p["attribute"] for p in prep.perspective_list() if p["label"] == label)
    log = eager_log(prep, label)
    resp = ingest("eager", log, prep.eager_full(label), clear_existing=True, trace_id_column=attr)
    w.emit("ingest", system="eager", perspective=label, log=log, time=resp["time"],
           wall_s=resp["wall_s"], timings=resp.get("timings"), **extra)
    return resp


def run_eager_query(w: ResultWriter, prep: Prepared, q: Query, **extra) -> dict:
    resp = query_eager(eager_log(prep, q.perspective), q.pattern)
    return w.emit(
        "query", seq=q.seq, perspective=q.perspective, pair=q.pair, hot=q.hot,
        pattern=q.pattern, time=resp["time"], wall_s=resp["wall_s"],
        support=resp.get("support"), system="eager", **extra,
    )


def ingest_adaptive_fresh(w: ResultWriter, log: str, path, **extra) -> dict:
    """Clean-slate adaptive ingest (deletes the log and resets memory state)."""
    resp = ingest("adaptive", log, path, clear_existing=True)
    eval_reset(log)
    w.emit("ingest", system="adaptive", log=log, file=str(path.name),
           time=resp["time"], wall_s=resp["wall_s"], timings=resp.get("timings"), **extra)
    return resp


def warm_up(prep: Prepared, log: str) -> None:
    """
    One discarded query, so JVM / Python-worker start-up is not charged to
    the first measured query.  The log's in-memory adaptive state is reset
    afterwards.  It uses the least-covered pair of the first attribute
    perspective (case only if the log has none): not the case perspective
    Exp 1-2 measure, and never a hot pair (its counter survives the reset in
    the catalog table).
    """
    persp = next((p for p in prep.perspective_list() if p["label"] != CASE),
                 prep.perspective_list()[0])
    cov = pair_coverage(prep, persp["label"])
    a, b = cov["pairs"][-1]["source"], cov["pairs"][-1]["target"]
    q = Query(-1, persp["label"], persp["grouping_keys"], a, b, hot=False)
    query_adaptive(log, q.pattern, q.grouping_keys, wait_promotion=True)
    eval_drain(log)
    eval_reset(log)
