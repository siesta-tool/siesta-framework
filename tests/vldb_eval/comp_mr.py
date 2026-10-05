"""
tests/vldb_eval/comp_mr.py — Apache Flink SQL MATCH_RECOGNIZE competitor for
the pattern-detection experiment (Exp 4: case perspective of every dataset,
plus the attribute perspectives of BPIC 2017; see pattern_common).

Engine: Flink 1.20 behind the SQL Gateway (``docker-compose-flink.yml``:
JobManager, one TaskManager with 4 slots and 6 GB process memory,
parallelism 4, gateway on :8083).

Why STREAMING mode over a bounded source, not batch mode.  In Flink 1.20
batch MATCH_RECOGNIZE is processing-time: ``BatchExecMatch.isProcTime()``
returns true, and the planner generates an event comparator only when ORDER
BY has two or more keys.  With ``ORDER BY rt`` the CEP operator therefore
consumes each partition in the order the batch key-sort delivers it, which
is not stable within a key; with ``ORDER BY rt, gpos`` it sorts only within
1 ms processing-time buckets.  Either way the row order inside a partition
is arbitrary: on bpic2015 the 2-event pattern 01_HOOFD_011 -> 01_HOOFD_015
returned 387-405 groups instead of 794, and longer patterns returned 0.  In
streaming mode the same query is event-time: rows are buffered per key by
their rowtime and fed to the NFA in rowtime order once the watermark passes
them.  The watermark is ``rt - INTERVAL '1' DAY`` (gpos < 86.4e6 is
asserted), so it passes no row before the end-of-input watermark, no row is
ever late, and every partition is matched in exact group order regardless of
arrival order.  The source is bounded (filesystem connector, no monitoring),
so the job finishes and the gateway sends EOS.

Data.  For every (dataset, perspective) the canonical events are put in the
perspective's group order (``pattern_common.group_order``) and written as
header-less CSV parts under ``results/data/<ds>/mr/<perspective>/``, which the
compose file bind-mounts read-only at ``/opt/flink/siesta_data`` in all Flink
containers.  Columns: ``grp`` (group key), ``gpos`` (BIGINT, 0-based position
in the group), ``activity``, ``ts`` (BIGINT, Unix seconds) and one STRING
column ``a_<attr>`` per attribute used by that perspective's queries; values
are the exact canonical strings, nulls are written as ``\\N`` (the CSV
``null-literal``).  The time attribute is ``rt AS TO_TIMESTAMP_LTZ(gpos, 3)``:
gpos is unique within a group, so ``PARTITION BY grp ORDER BY rt`` is exactly
the group order, while raw timestamps may tie.  The setup scan checks row,
group and per-attribute non-null counts against pandas.

Query (same semantics as ``TruthIndex``)::

    SELECT COUNT(DISTINCT grp) AS n FROM ev_<ds>_<persp>
    MATCH_RECOGNIZE (
      PARTITION BY grp ORDER BY rt
      MEASURES E1.gpos AS p1
      ONE ROW PER MATCH
      AFTER MATCH SKIP PAST LAST ROW
      PATTERN (E1 G1*? E2 G2*? ... EL)
      DEFINE E1 AS E1.activity = '...' [AND E1.a_x = 'v' ...],
             Ei AS Ei.activity = '...' [AND Ei.a_x = Ej.a_x | Ei.a_x <> Ej.a_x ...],
             ...
    ) AS T

``Gi`` are not DEFINEd, i.e. TRUE: any event may lie between two pattern
events.  A binding compares with the row bound to ``Ej`` in the same partial
match; a NULL on either side makes the comparison UNKNOWN, so a null never
satisfies a binding (or a literal).  Flink's NFA keeps every partial match
alive (the reluctant ``*?`` only orders the branches, it does not prune
them; a greedy ``*`` with a TRUE gap never leaves the gap and finds
nothing), so the first match of a group is found whenever one exists,
whatever the bindings require (but see the CEP defect below).  The skip strategy only prunes after a match,
and only the existence of one match per group is counted; ``SKIP PAST LAST
ROW`` is therefore exact and emits the fewest rows (``--skip next`` gives
``SKIP TO NEXT ROW``).  Only the count travels to the client: mini-batch
aggregation folds the per-group updates, so the changelog of the global
COUNT has one or a few rows; the last one is the count.

Variants (``variant`` field; non-default variants write
``<ds>.<persp>.gaps_<g>.skip_<s>.jsonl``):

* ``--gaps any`` (default): every gap is TRUE, as above.  Exact, but the
  NFA forks a run at every candidate row of every pattern event, so the
  number of live partial matches grows combinatorially with repeated
  activities (bpic2012 / bpic2017: many queries exceed the timeout).
* ``--gaps next``: ``Gi AS (<condition of E(i+1)>) IS NOT TRUE`` unless a
  later binding refers to E(i+1): a run binds E(i+1) to the first row that
  satisfies it and forks only where a later binding depends on the choice.
  Same counts (the earliest admissible row is optimal when nothing refers to
  it; TruthIndex uses the same argument), linear work per run.

Known engine defect (Flink 1.20 CEP), affects both variants: when a binding
refers to an earlier variable (``Ei.a <> Ej.a``) and runs that started at
different rows share buffered events, the condition can be evaluated against
the ``Ej`` row of another run, so a valid match is missed.  Minimal repro in
bpic2017 ``Application_1157633683`` restricted to gpos 23 and 34..41::

    PATTERN (A G0*? W0 H0*? W1 H1*? W2 H2*? B GB*? C)
    DEFINE A AS activity = 'A_Incomplete',
           W0/W1/W2 AS activity = 'W_Call incomplete files',
           B AS activity = 'W_Validate application' AND B.org <> A.org,
           C AS activity = 'A_Validating'

returns nothing although A=34 (User_109), W=35,36,37, B=39 (User_133), C=41
matches; without row 23 (A=23, User_133) the match is found, with two W's
instead of three too.  Such queries show ``parity: false`` (under-count).

A query whose partial matches exhaust the TaskManager heap makes it stop
answering heartbeats; the job then fails, the record gets ``error`` and
``phases.failure = "taskmanager_lost"``; the TaskManager container restarts
(``restart: unless-stopped``) and the driver waits for free slots
(``phases.recovery_s``, not part of ``time_s``) before the next query.

Timing.  ``time_s`` is the wall time from the statement POST until EOS of
the count's changelog.  ``phases`` (from the JobManager's job timestamps):
``plan_s`` (POST -> job initialising: parse/optimise/submit), ``run_s`` (job
initialising -> finished), ``fetch_s`` (job finished -> EOS received).  After
TIMEOUT_S the operation is cancelled and closed and the job is cancelled
through the JobManager REST API (``timed_out: true``,
``matched_groups: null``).

Usage:
    docker compose -f tests/vldb_eval/docker-compose-flink.yml up -d
    python -m tests.vldb_eval.comp_mr --datasets bpic2015
    python -m tests.vldb_eval.comp_mr --datasets bpic2017 --perspectives case org:resource --per-length 1
    python -m tests.vldb_eval.comp_mr --datasets bpic2012 --gaps next
    docker compose -f tests/vldb_eval/docker-compose-flink.yml stop
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import statistics
import sys
import time
from pathlib import Path

import numpy as np
import requests

from tests.vldb_eval.eval_common import RESULTS_DIR, run_meta
from tests.vldb_eval.pattern_common import (
    DATA_DIR, PERSPECTIVES, SKIP_AFTER, TIMEOUT_S, SkipAfter, attribute_columns, group_order,
    load_events, load_queries, safe, skipped_record,
)

GATEWAY = "http://localhost:8083"
JOBMANAGER = "http://localhost:8081"
COMPOSE = Path(__file__).resolve().parent / "docker-compose-flink.yml"
CONTAINER_DATA = "/opt/flink/siesta_data"     # bind mount of results/data
OUT_DIR = RESULTS_DIR / "exp4_patterns" / "mr"
NULL_LITERAL = "\\N"
N_PARTS = 4                                    # CSV parts = source parallelism
WATERMARK_DELAY_MS = 86_400_000              # > max gpos: no row is ever late
FLINK_CONFIG = {
    "flink_version": None,                     # filled from /v1/info
    "execution.runtime-mode": "streaming (bounded source, event time)",
    "state.backend": "hashmap (default)",
    "taskmanager.memory.managed.fraction": 0.1,
    "parallelism.default": 4,
    "taskmanagers": 1,
    "taskmanager.numberOfTaskSlots": 4,
    "taskmanager.memory.process.size": "6g",
    "jobmanager.memory.process.size": "1600m",
    "restart-strategy.type": "none",
    "image": "flink:1.20-java17",
    "host": "12 cores, 22 GB RAM",
}
SESSION_SET = {
    "execution.runtime-mode": "streaming",
    "parallelism.default": "4",
    "table.exec.mini-batch.enabled": "true",
    "table.exec.mini-batch.allow-latency": "5 s",
    "table.exec.mini-batch.size": "1000000",
}


# ---------------------------------------------------------------------------
# REST clients
# ---------------------------------------------------------------------------

class GatewayError(RuntimeError):
    pass


class Gateway:
    def __init__(self, base: str = GATEWAY):
        self.base = base.rstrip("/")
        self.http = requests.Session()

    def info(self) -> dict:
        return self.http.get(f"{self.base}/v1/info", timeout=10).json()

    def open_session(self, props: dict | None = None) -> str:
        r = self.http.post(f"{self.base}/v1/sessions", json={"properties": props or {}}, timeout=30)
        r.raise_for_status()
        return r.json()["sessionHandle"]

    def close_session(self, s: str) -> None:
        try:
            self.http.delete(f"{self.base}/v1/sessions/{s}", timeout=30)
        except requests.RequestException:
            pass

    def submit(self, s: str, sql: str) -> str:
        r = self.http.post(f"{self.base}/v1/sessions/{s}/statements", json={"statement": sql}, timeout=60)
        if r.status_code != 200:
            raise GatewayError(_errors(r))
        return r.json()["operationHandle"]

    def fetch(self, s: str, op: str, token: int) -> dict:
        r = self.http.get(f"{self.base}/v1/sessions/{s}/operations/{op}/result/{token}",
                          params={"rowFormat": "JSON"}, timeout=60)
        if r.status_code != 200:
            raise GatewayError(_errors(r))
        return r.json()

    def cancel(self, s: str, op: str) -> None:
        for method, path in (("POST", "cancel"), ("DELETE", "close")):
            try:
                self.http.request(method, f"{self.base}/v1/sessions/{s}/operations/{op}/{path}", timeout=30)
            except requests.RequestException:
                pass

    def close(self, s: str, op: str) -> None:
        try:
            self.http.delete(f"{self.base}/v1/sessions/{s}/operations/{op}/close", timeout=30)
        except requests.RequestException:
            pass

    def run(self, s: str, sql: str, timeout_s: float = 600.0) -> list[list]:
        """Execute a statement and return all its rows (no timing)."""
        op = self.submit(s, sql)
        rows, token, t0 = [], 0, time.perf_counter()
        try:
            while True:
                d = self.fetch(s, op, token)
                # keep the insert/update-after rows of a (streaming) changelog
                rows += [x["fields"] for x in (d.get("results") or {}).get("data", [])
                         if x.get("kind", "INSERT") in ("INSERT", "UPDATE_AFTER")]
                if d.get("resultType") == "EOS" or not d.get("nextResultUri"):
                    return rows
                if d.get("resultType") == "NOT_READY":
                    time.sleep(0.05)
                token = int(d["nextResultUri"].split("?")[0].rstrip("/").split("/")[-1])
                if time.perf_counter() - t0 > timeout_s:
                    raise TimeoutError(sql[:200])
        finally:
            self.close(s, op)


def _errors(r: requests.Response) -> str:
    try:
        errs = r.json().get("errors") or []
    except ValueError:
        return f"HTTP {r.status_code}: {r.text[:2000]}"
    # the gateway returns [summary, full stack trace]; keep the root cause
    text = "\n".join(errs)
    causes = re.findall(r"Caused by: ([^\n]+)", text)
    head = errs[0] if errs else f"HTTP {r.status_code}"
    return head + (" | root cause: " + causes[-1] if causes else "")


class JobManager:
    def __init__(self, base: str = JOBMANAGER):
        self.base = base.rstrip("/")
        self.http = requests.Session()

    def jobs(self) -> list[dict]:
        try:
            return self.http.get(f"{self.base}/jobs/overview", timeout=10).json().get("jobs", [])
        except (requests.RequestException, ValueError):
            return []

    def job(self, jid: str) -> dict | None:
        try:
            r = self.http.get(f"{self.base}/jobs/{jid}", timeout=10)
            return r.json() if r.status_code == 200 else None
        except (requests.RequestException, ValueError):
            return None

    def cancel(self, jid: str) -> None:
        try:
            self.http.patch(f"{self.base}/jobs/{jid}", params={"mode": "cancel"}, timeout=10)
        except requests.RequestException:
            pass

    def wait_idle(self, wait_s: float = 600.0) -> float:
        """Wait until no job runs and every slot is free (e.g. after a TaskManager restart)."""
        t0 = time.perf_counter()
        while time.perf_counter() - t0 < wait_s:
            try:
                ov = self.http.get(f"{self.base}/overview", timeout=10).json()
                if (ov.get("jobs-running", 1) == 0 and ov.get("slots-total", 0) > 0
                        and ov.get("slots-available") == ov.get("slots-total")):
                    break
            except (requests.RequestException, ValueError):
                pass
            time.sleep(2.0)
        return round(time.perf_counter() - t0, 3)

    def find(self, name: str) -> list[dict]:
        return [j for j in self.jobs() if j.get("name") == name]

    def cancel_named(self, name: str, wait_s: float = 120.0, appear_s: float = 5.0) -> list[str]:
        """Cancel every unfinished job called ``name`` and wait until they are gone."""
        done = {"FINISHED", "CANCELED", "FAILED"}
        ids = []
        t0 = time.perf_counter()
        while time.perf_counter() - t0 < wait_s:
            live = [j for j in self.find(name) if j.get("state") not in done]
            if not live:
                # the gateway may still be planning/submitting: give the job
                # a few seconds to appear before concluding there is none
                if ids or time.perf_counter() - t0 > appear_s:
                    break
                time.sleep(0.5)
                continue
            for j in live:
                if j["jid"] not in ids:
                    ids.append(j["jid"])
                self.cancel(j["jid"])
            time.sleep(1.0)
        return ids


def wait_flink(gw: Gateway, jm: JobManager, wait_s: float = 180.0) -> dict:
    t0 = time.perf_counter()
    while True:
        try:
            info = gw.info()
            ov = jm.http.get(f"{jm.base}/overview", timeout=5).json()
            if ov.get("slots-total", 0) >= 1:
                return {"gateway": info, "overview": ov}
        except (requests.RequestException, ValueError):
            pass
        if time.perf_counter() - t0 > wait_s:
            raise RuntimeError(f"Flink not reachable at {GATEWAY} / {JOBMANAGER}; "
                               f"run: docker compose -f {COMPOSE} up -d")
        time.sleep(2.0)


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

def table_name(ds: str, persp: str) -> str:
    return "ev_" + re.sub(r"[^A-Za-z0-9_]", "_", f"{ds}_{persp}").lower()


def sanitize_attrs(attrs: list[str]) -> dict[str, str]:
    """attr -> SQL column ``a_<attr with non [A-Za-z0-9_] -> '_'>`` (unique, lower-cased)."""
    out, used = {}, set()
    for a in attrs:
        base = "a_" + re.sub(r"[^A-Za-z0-9_]", "_", a).lower()
        name, k = base, 2
        while name in used:
            name, k = f"{base}_{k}", k + 1
        used.add(name)
        out[a] = name
    return out


def query_attrs(qs: list[dict]) -> list[str]:
    seen = []
    for q in qs:
        for e in q["events"]:
            for p in e["preds"]:
                if p["attr"] not in seen:
                    seen.append(p["attr"])
    return seen


def data_dir(ds: str, persp: str) -> Path:
    return DATA_DIR / ds / "mr" / safe(persp)


def prepare_data(ds: str, persp: str, attrs: list[str]) -> dict:
    """Write the perspective's group-ordered events as CSV parts; return facts about it."""
    t0 = time.perf_counter()
    df = load_events(ds)
    t_load = time.perf_counter() - t0
    present = [a for a in attrs if a in attribute_columns(df)]
    g = group_order(df, persp)
    assert int(g["gpos"].max()) < WATERMARK_DELAY_MS, "gpos exceeds the watermark delay"
    out = g[["group", "gpos", "activity", "ts"]].copy()
    out["group"] = out["group"].astype(str)
    cmap = sanitize_attrs(present)
    for a in present:
        col = g[a].astype(object).where(g[a].notna(), None)
        bad = col.map(lambda v: isinstance(v, str) and (v == NULL_LITERAL or bool(re.search(r"[\r\n]", v))))
        if bad.any():
            raise ValueError(f"{ds}/{a}: values clash with the CSV encoding")
        out[cmap[a]] = col
    d = data_dir(ds, persp)
    if d.exists():
        shutil.rmtree(d)
    d.mkdir(parents=True)
    # Split at group boundaries into N_PARTS files of similar size, so the
    # source reads in parallel; MATCH_RECOGNIZE re-partitions by grp anyway.
    starts = np.flatnonzero(np.r_[True, out["group"].to_numpy()[1:] != out["group"].to_numpy()[:-1]])
    cuts = [0]
    for k in range(1, N_PARTS):
        i = int(np.searchsorted(starts, k * len(out) / N_PARTS))
        c = int(starts[min(i, len(starts) - 1)])
        if c > cuts[-1]:
            cuts.append(c)
    cuts.append(len(out))
    for k in range(len(cuts) - 1):
        out.iloc[cuts[k]:cuts[k + 1]].to_csv(d / f"part-{k}.csv", header=False, index=False,
                                             na_rep=NULL_LITERAL, lineterminator="\n")
    for p in d.iterdir():
        p.chmod(0o644)
    d.chmod(0o755)
    d.parent.chmod(0o755)
    return {
        "rows": int(len(out)), "groups": int(out["group"].nunique()),
        "attr_columns": cmap, "missing_attrs": [a for a in attrs if a not in present],
        "nonnull": {cmap[a]: int(out[cmap[a]].notna().sum()) for a in present},
        "parts": len(cuts) - 1, "host_dir": str(d),
        "bytes": int(sum(p.stat().st_size for p in d.iterdir())),
        "load_s": round(t_load, 3), "prep_s": round(time.perf_counter() - t0, 3),
    }


def container_dir(ds: str, persp: str) -> str:
    return f"{CONTAINER_DATA}/{data_dir(ds, persp).relative_to(DATA_DIR).as_posix()}"


def ddl(ds: str, persp: str, cmap: dict[str, str]) -> str:
    cols = ["  grp STRING", "  gpos BIGINT", "  activity STRING", "  ts BIGINT"]
    cols += [f"  `{c}` STRING" for c in cmap.values()]
    cols += ["  rt AS TO_TIMESTAMP_LTZ(gpos, 3)",
             "  WATERMARK FOR rt AS rt - INTERVAL '1' DAY"]
    return (f"CREATE TEMPORARY TABLE {table_name(ds, persp)} (\n" + ",\n".join(cols) + "\n) WITH (\n"
            f"  'connector' = 'filesystem',\n"
            f"  'path' = 'file://{container_dir(ds, persp)}',\n"
            f"  'format' = 'csv',\n"
            f"  'csv.null-literal' = '{NULL_LITERAL}',\n"
            f"  'csv.ignore-parse-errors' = 'false'\n)")


# ---------------------------------------------------------------------------
# Query translation
# ---------------------------------------------------------------------------

def sql_str(v: str) -> str:
    return "'" + v.replace("'", "''") + "'"


def event_cond(q: dict, i: int, cmap: dict[str, str], var: str) -> str:
    """DEFINE condition of pattern event ``i`` (1-based) on the row of variable ``var``."""
    e = q["events"][i - 1]
    conds = [f"{var}.activity = {sql_str(e['activity'])}"]
    for p in e["preds"]:
        col = cmap.get(p["attr"])
        op = {"=": "=", "!=": "<>"}[p["op"]]
        if col is None:          # attribute absent from the log: always null
            conds.append("FALSE")
        elif "ref" in p:
            j = int(p["ref"])
            assert 1 <= j < i, (q["qid"], p)
            conds.append(f"{var}.`{col}` {op} E{j}.`{col}`")
        else:
            conds.append(f"{var}.`{col}` {op} {sql_str(p['value'])}")
    return " AND ".join(conds)


def mr_sql(q: dict, table: str, cmap: dict[str, str], skip: str = "past",
           gaps: str = "any", measure_rows: bool = False) -> str:
    """
    ``gaps="any"``: every gap ``Gi`` is TRUE (skip-till-any-match; the NFA
    forks a run at every candidate of every pattern event).
    ``gaps="next"``: ``Gi`` is ``(<condition of E(i+1)>) IS NOT TRUE`` unless a
    later binding refers to E(i+1), so a run binds E(i+1) to the first row
    that satisfies it (skip-till-next-match) and forks only at the events
    whose row choice matters to a later binding.  Same counts: when no later
    event refers to E(i+1), its earliest admissible row is optimal (the
    same argument TruthIndex uses to stop at the first candidate).
    """
    events = q["events"]
    L = len(events)
    needed = {int(p["ref"]) for e in events for p in e["preds"] if "ref" in p}
    pattern = " ".join(f"E{i} G{i}*?" if i < L else f"E{i}" for i in range(1, L + 1))
    defines = [f"    E{i} AS " + event_cond(q, i, cmap, f"E{i}") for i in range(1, L + 1)]
    if gaps == "next":
        defines += [f"    G{i} AS ({event_cond(q, i + 1, cmap, f'G{i}')}) IS NOT TRUE"
                    for i in range(1, L) if (i + 1) not in needed]
    elif gaps != "any":
        raise ValueError(gaps)
    skip_sql = {"past": "SKIP PAST LAST ROW", "next": "SKIP TO NEXT ROW"}[skip]
    select = "grp, p1" if measure_rows else "COUNT(DISTINCT grp) AS n"
    return (f"SELECT {select} FROM {table}\n"
            f"MATCH_RECOGNIZE (\n"
            f"  PARTITION BY grp ORDER BY rt\n"
            f"  MEASURES E1.gpos AS p1\n"
            f"  ONE ROW PER MATCH\n"
            f"  AFTER MATCH {skip_sql}\n"
            f"  PATTERN ({pattern})\n"
            f"  DEFINE\n" + ",\n".join(defines) + "\n) AS T")


# ---------------------------------------------------------------------------
# Timed execution
# ---------------------------------------------------------------------------

def timed_count(gw: Gateway, jm: JobManager, s: str, sql: str, name: str,
                timeout_s: float = TIMEOUT_S) -> dict:
    """Run a COUNT query; wall time from the POST until the changelog's EOS (final count)."""
    jm.wait_idle()
    gw.run(s, f"SET 'pipeline.name' = {sql_str(name)}")
    wall0 = time.time()
    t0 = time.perf_counter()
    res = {"time_s": None, "matched_groups": None, "timed_out": False, "error": None, "phases": {}}
    op = None
    try:
        op = gw.submit(s, sql)
        t_sub = time.perf_counter()
        token, sleep, value, jid, n_rows = 0, 0.005, None, None, 0
        while True:
            left = timeout_s - (time.perf_counter() - t0)
            if left <= 0:
                raise TimeoutError
            d = gw.fetch(s, op, token)
            jid = jid or d.get("jobID")
            for row in (d.get("results") or {}).get("data", []):
                n_rows += 1
                if row.get("kind", "INSERT") in ("INSERT", "UPDATE_AFTER"):
                    value = row["fields"][0]
                else:                       # UPDATE_BEFORE / DELETE
                    value = None
            rt = d.get("resultType")
            if rt == "EOS" or not d.get("nextResultUri"):
                break
            nxt = int(d["nextResultUri"].split("?")[0].rstrip("/").split("/")[-1])
            if rt == "NOT_READY" or nxt == token:
                time.sleep(min(sleep, max(left, 0.0)))
                sleep = min(sleep * 1.5, 0.1)
            else:
                sleep = 0.005
            token = nxt
        t_end = time.perf_counter()
        res["time_s"] = round(t_end - t0, 4)
        res["matched_groups"] = int(value) if value is not None else 0
        res["phases"] = {"submit_post_s": round(t_sub - t0, 4), "changelog_rows": n_rows}
        res["_jid"], res["_wall"] = jid, (wall0, wall0 + (t_end - t0))
    except TimeoutError:
        res["time_s"] = round(time.perf_counter() - t0, 4)
        res["timed_out"] = True
        if op:
            gw.cancel(s, op)
            op = None
        res["phases"]["cancelled_jobs"] = jm.cancel_named(name, wait_s=300)
        res["phases"]["recovery_s"] = jm.wait_idle()     # not part of time_s
    except GatewayError as e:
        res["time_s"] = round(time.perf_counter() - t0, 4)
        res["error"] = str(e)[:4000]
        if re.search(r"[Hh]eartbeat of TaskManager|NoResourceAvailable|TaskManager.*(lost|disconnect)", res["error"]):
            # the TaskManager stopped responding (GC thrashing / killed): a resource
            # failure of the engine, not a query error
            res["phases"]["failure"] = "taskmanager_lost"
        jm.cancel_named(name, wait_s=30)
        res["phases"]["recovery_s"] = jm.wait_idle()
    finally:
        if op:
            gw.close(s, op)
    jid = res.pop("_jid", None)
    wall = res.pop("_wall", None)
    if wall and not res["timed_out"]:
        res["phases"].update(job_phases(jm, jid, name, wall))
    return res


def job_phases(jm: JobManager, jid: str | None, name: str, wall: tuple[float, float]) -> dict:
    """plan/run/fetch split from the JobManager's job timestamps (ms since epoch)."""
    if not jid:
        js = sorted(jm.find(name), key=lambda j: j.get("start-time", 0))
        jid = js[-1]["jid"] if js else None
    if not jid:
        return {}
    for _ in range(20):
        j = jm.job(jid)
        if j and j.get("state") in ("FINISHED", "CANCELED", "FAILED"):
            break
        time.sleep(0.1)
    if not j:
        return {"job_id": jid}
    ts = j.get("timestamps") or {}
    created = ts.get("INITIALIZING") or j.get("start-time") or ts.get("CREATED")
    finished = ts.get("FINISHED") or j.get("end-time")
    out = {"job_id": jid, "job_state": j.get("state")}
    if created:
        out["plan_s"] = round(created / 1000.0 - wall[0], 4)
    if created and finished and finished > 0:
        out["run_s"] = round((finished - created) / 1000.0, 4)
        out["fetch_s"] = round(wall[1] - finished / 1000.0, 4)
    return out


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def open_session(gw: Gateway) -> str:
    s = gw.open_session(SESSION_SET)
    for k, v in SESSION_SET.items():
        gw.run(s, f"SET {sql_str(k)} = {sql_str(v)}")
    return s


def register(gw: Gateway, s: str, ds: str, persp: str, cmap: dict[str, str]) -> dict:
    t0 = time.perf_counter()
    sql = ddl(ds, persp, cmap)
    gw.run(s, f"DROP TEMPORARY TABLE IF EXISTS {table_name(ds, persp)}")
    gw.run(s, sql)
    t_reg = time.perf_counter() - t0
    # verification scan: row/group counts and non-null counts per attribute
    tb = table_name(ds, persp)
    sel = ["COUNT(*)", "COUNT(DISTINCT grp)", "MIN(gpos)", "MAX(gpos)"] + [f"COUNT(`{c}`)" for c in cmap.values()]
    t1 = time.perf_counter()
    row = gw.run(s, f"SELECT {', '.join(sel)} FROM {tb}")[-1]   # final changelog row
    t_ver = time.perf_counter() - t1
    return {"table": tb, "ddl": sql, "register_s": round(t_reg, 3), "verify_s": round(t_ver, 3),
            "flink_rows": int(row[0]), "flink_groups": int(row[1]),
            "flink_gpos_range": [int(row[2]), int(row[3])],
            "flink_nonnull": {c: int(v) for c, v in zip(cmap.values(), row[4:])}}


def variant(args) -> str:
    return f"gaps={args.gaps},skip={args.skip}"


def select_queries(qs: list[dict], args) -> list[dict]:
    if args.qids:
        want = set(args.qids)
        qs = [q for q in qs if q["qid"] in want or any(q["qid"].startswith(w) for w in want)]
    if args.per_length:
        kept, cnt = [], {}
        for q in qs:
            k = (q["length"], q["kind"])
            if cnt.get(k, 0) < args.per_length:
                kept.append(q)
                cnt[k] = cnt.get(k, 0) + 1
        qs = kept
    if args.limit:
        qs = qs[:args.limit]
    return qs


def run_perspective(gw: Gateway, jm: JobManager, ds: str, persp: str, args, flink: dict) -> None:
    all_qs = load_queries(ds, persp)
    qs = select_queries(all_qs, args)
    attrs = query_attrs(all_qs)
    cmap = sanitize_attrs([a for a in attrs])
    tb = table_name(ds, persp)
    d = data_dir(ds, persp)
    if args.setup or not d.exists():
        print(f"[{ds}/{persp}] writing source CSV ...", flush=True)
        prep = prepare_data(ds, persp, attrs)
    else:
        prep = {"host_dir": str(d), "reused": True}
        meta = d / "_prep.json"
        if meta.exists():
            prep = {**json.loads(meta.read_text()), "reused": True}
    if not prep.get("reused"):
        (d / "_prep.json").write_text(json.dumps(prep))
    cmap = prep.get("attr_columns", cmap)
    s = open_session(gw)
    try:
        reg = register(gw, s, ds, persp, cmap)
        if "rows" in prep:
            assert reg["flink_rows"] == prep["rows"], (reg, prep)
            assert reg["flink_groups"] == prep["groups"], (reg, prep)
            assert reg["flink_nonnull"] == prep["nonnull"], (reg, prep)
        print(f"[{ds}/{persp}] table {tb}: rows={reg['flink_rows']} groups={reg['flink_groups']} "
              f"prep_s={prep.get('prep_s')} register_s={reg['register_s']} verify_s={reg['verify_s']}",
              flush=True)
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        suffix = "" if (args.gaps, args.skip) == ("any", "past") else "." + safe(variant(args).replace(",", "."))
        out = Path(args.out) if args.out else OUT_DIR / f"{ds}.{safe(persp)}{suffix}.jsonl"
        with out.open("w") as f:
            f.write(json.dumps({
                "type": "setup", "system": "mr", "dataset": ds, "perspective": persp,
                "data": prep, "table": reg, "flink": flink, "session": SESSION_SET,
                "variant": variant(args), "skip": args.skip, "gaps": args.gaps, "timeout_s": args.timeout, "reps": args.reps,
                "sql_example": mr_sql(qs[0], tb, cmap, args.skip, args.gaps) if qs else None,
                "meta": run_meta()}) + "\n")
            f.flush()
            if qs:   # warm-up, discarded: the first query cut to its first 2 events
                wq = {**qs[0], "events": [{**e, "preds": [p for p in e["preds"] if "ref" not in p or p["ref"] <= 1]}
                                          for e in qs[0]["events"][:2]]}
                w = timed_count(gw, jm, s, mr_sql(wq, tb, cmap, args.skip, args.gaps), f"warmup.{ds}.{persp}",
                                timeout_s=min(args.timeout, 120.0))
                print(f"[{ds}/{persp}] warm-up: {w['time_s']}s n={w['matched_groups']} "
                      f"{w['error'] or ''}", flush=True)
            n_ok = n_run = n_to = 0
            times: dict[int, list[float]] = {}
            cutoff = SkipAfter(args.skip_after)
            for q in qs:
                if cutoff.skip(q):
                    f.write(json.dumps(skipped_record("mr", q, variant=variant(args))) + "\n")
                    f.flush()
                    print(f"  {q['qid']} skipped ({args.skip_after} consecutive timeouts)", flush=True)
                    continue
                sql = mr_sql(q, tb, cmap, args.skip, args.gaps)
                for rep in range(args.reps):
                    res = timed_count(gw, jm, s, sql, f"{q['qid']}.r{rep}", timeout_s=args.timeout)
                    truth = q["truth"]
                    parity = res["matched_groups"] == truth["matched_groups"]
                    rec = {"system": "mr", "variant": variant(args), "qid": q["qid"], "dataset": ds, "perspective": persp,
                           "length": q["length"], "kind": q["kind"], "rep": rep,
                           "time_s": res["time_s"], "matched_groups": res["matched_groups"],
                           "truth": truth, "parity": parity, "timed_out": res["timed_out"],
                           "error": res["error"], "phases": res["phases"]}
                    f.write(json.dumps(rec) + "\n")
                    f.flush()
                    if rep == 0:
                        cutoff.record(q, res["timed_out"])
                    n_run += 1
                    n_ok += parity
                    n_to += res["timed_out"]
                    if not res["timed_out"] and res["error"] is None:
                        times.setdefault(q["length"], []).append(res["time_s"])
                    ph = res["phases"]
                    print(f"  {q['qid']} rep={rep} t={res['time_s']}s run={ph.get('run_s')} "
                          f"got={res['matched_groups']} truth={truth['matched_groups']} "
                          f"{'OK' if parity else 'MISMATCH'}{' TIMEOUT' if res['timed_out'] else ''}"
                          f"{' ERROR ' + res['error'][:300] if res['error'] else ''}", flush=True)
        print(f"[{ds}/{persp}] parity {n_ok}/{n_run} timeouts={n_to} -> {out}")
        for L in sorted(times):
            print(f"  L={L:2d} median={statistics.median(times[L]):.3f}s max={max(times[L]):.3f}s "
                  f"n={len(times[L])}")
    finally:
        gw.close_session(s)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--datasets", nargs="+", default=["bpic2011", "bpic2012", "bpic2015", "bpic2017", "bpic2018"])
    ap.add_argument("--perspectives", nargs="*", default=None, help="default: case plus PERSPECTIVES[ds]")
    ap.add_argument("--setup", action=argparse.BooleanOptionalAction, default=True,
                    help="(re)write the source CSV (default) or reuse it with --no-setup")
    ap.add_argument("--limit", type=int, default=None, help="only the first N queries")
    ap.add_argument("--per-length", type=int, default=None,
                    help="only the first N queries per (length, kind)")
    ap.add_argument("--reps", type=int, default=1)
    ap.add_argument("--qids", nargs="*", default=None, help="qids (or qid prefixes) to run")
    ap.add_argument("--skip", choices=["past", "next"], default="past",
                    help="AFTER MATCH SKIP PAST LAST ROW (default) or SKIP TO NEXT ROW")
    ap.add_argument("--gaps", choices=["any", "next"], default="any",
                    help="gap variables: TRUE everywhere (default) or skip-till-next-match "
                         "where no later binding needs the choice (see mr_sql)")
    ap.add_argument("--timeout", type=float, default=TIMEOUT_S)
    ap.add_argument("--skip-after", type=int, default=SKIP_AFTER,
                    help="consecutive timeouts of a kind before the rest of that kind is skipped (0: off)")
    ap.add_argument("--out", default=None, help="output path (single dataset/perspective only)")
    args = ap.parse_args()
    gw, jm = Gateway(), JobManager()
    st = wait_flink(gw, jm)
    flink = {**FLINK_CONFIG, "flink_version": st["gateway"].get("version"),
             "slots_total": st["overview"].get("slots-total"),
             "taskmanagers": st["overview"].get("taskmanagers")}
    for ds in args.datasets:
        persps = args.perspectives if args.perspectives is not None else ["case"] + PERSPECTIVES.get(ds, [])
        for persp in persps:
            run_perspective(gw, jm, ds, persp, args, flink)


if __name__ == "__main__":
    sys.exit(main())
