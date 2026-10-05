"""
tests/vldb_eval/comp_lpg.py — LPG competitor (KuzuDB, embedded) for the
pattern-detection experiments (Exp 4 / Exp 1), see ``pattern_common``.

Graph model: the event knowledge graph (EKG) of Esser & Fahland
(``graphdb-eventlogs/csv_to_eventgraph_kuzudb``), built from the canonical
events with exact values:

* ``Event``  node: idx (INT64 PK, row of the canonical file), trace_id,
  position, activity, ts, and every attribute column as STRING (NULL when
  absent).
* ``Entity`` node: uid (PK, ``<type>:<id>``), type, id.  Type ``case`` (one
  entity per trace) for every dataset; for bpic2017 also one type per
  attribute perspective in ``PERSPECTIVES`` (id = attribute value, only
  events with a non-null value are correlated).
* ``CORR`` rel Event→Entity with ``type`` and ``gpos`` — the event's
  position within the entity in the benchmark's group order
  (``pattern_common.group_order``), so ordering within an entity is
  exactly the benchmark's (raw timestamps tie).
* ``DF``   rel Event→Event with ``type`` and ``id``: directly-follows per
  entity, inferred as in the EKG scripts (events of each entity ordered,
  consecutive pairs copied in).  Part of the model; not used by queries.

Query (default plan ``anchor``): one Cypher query per pattern, a
shared-entity join with a join-order hint::

    MATCH (n:Entity), (e1:Event)-[c1:CORR]->(n), ..., (eL:Event)-[cL:CORR]->(n)
    WHERE n.type = $t AND e1.activity = $a1 AND ... AND ci.gpos < cj.gpos (all i < j)
      AND <literal preds e_i.attr = $v>  AND <bindings e_i.attr = / <> e_j.attr>
    HINT (((e3 JOIN c3) JOIN n) JOIN (c7 JOIN e7)) JOIN ...
    RETURN count(DISTINCT n.uid)

NULL never satisfies a predicate (Cypher's three-valued logic), strictly
increasing gpos makes the pattern events distinct and ordered.  The join
enumerates every embedding of the pattern in a group, which explodes with
repeated activities and large groups; ``--plan stepwise`` is an exact
single-query alternative that keeps only the earliest position per
binding state (see ``translate_stepwise``).

Setup: ``setup_<ds>.json`` (load times, rows, db size) next to the
database; it is the first record of every result file.

Usage::

    python -m tests.vldb_eval.comp_lpg --datasets bpic2015 --setup
    python -m tests.vldb_eval.comp_lpg --datasets bpic2017 --perspectives case org:resource --no-setup --limit 8
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import kuzu

from tests.vldb_eval import pattern_common as pc

LPG_DIR = pc.RESULTS_DIR / "exp4_patterns" / "lpg"
DEFAULT_BUFFER_GB = 8
DEFAULT_THREADS = 12


def db_path(ds: str) -> Path:
    return LPG_DIR / f"db_{ds}"


def out_path(ds: str, perspective: str, plan: str = "anchor") -> Path:
    suffix = "" if plan == "anchor" else f".{plan}"
    return LPG_DIR / f"{ds}.{pc.safe(perspective)}{suffix}.jsonl"


def q(name: str) -> str:
    """Backtick-quoted Cypher identifier (attribute keys contain ':')."""
    return "`" + name.replace("`", "``") + "`"


def du_bytes(p: Path) -> int:
    out = subprocess.run(["du", "-sb", str(p)], capture_output=True, text=True, check=True).stdout
    return int(out.split()[0])


def open_db(ds: str, buffer_gb: float, threads: int, read_only: bool = False) -> tuple[kuzu.Database, kuzu.Connection]:
    db = kuzu.Database(str(db_path(ds)), buffer_pool_size=int(buffer_gb * 1024 ** 3),
                       max_num_threads=threads, read_only=read_only)
    conn = kuzu.Connection(db, num_threads=threads)
    return db, conn


def entity_types(ds: str) -> list[str]:
    return ["case"] + pc.PERSPECTIVES.get(ds, [])


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

def _str_array(s: pd.Series) -> pa.Array:
    vals = s.to_numpy(dtype=object, na_value=None)
    return pa.array([v if isinstance(v, str) else (None if v is None else str(v)) for v in vals],
                    type=pa.string())


def build(ds: str, buffer_gb: float, threads: int) -> dict:
    """(Re)build the EKG of ``ds``; returns the setup record (also saved as setup.json)."""
    path = db_path(ds)
    for p in (path, path.with_name(path.name + ".wal")):   # Kuzu 0.11: single file (+ WAL)
        if p.is_dir():
            shutil.rmtree(p)
        elif p.exists():
            p.unlink()
    stage = LPG_DIR / f"stage_{ds}"
    shutil.rmtree(stage, ignore_errors=True)
    stage.mkdir(parents=True)
    LPG_DIR.mkdir(parents=True, exist_ok=True)

    rec: dict = {"system": "lpg", "record": "setup", "dataset": ds, "engine": f"kuzu {kuzu.__version__}",
                 "config": {"buffer_pool_gb": buffer_gb, "max_threads": threads, "storage": "on-disk",
                            "db_path": str(path)},
                 "entity_types": entity_types(ds), "prep_s": {}, "load_s": {}, "rows": {}}

    t0 = time.perf_counter()
    df = pc.load_events(ds)
    attrs = pc.attribute_columns(df)
    df = df.reset_index(drop=True)
    df["idx"] = np.arange(len(df), dtype=np.int64)
    ev = pa.table({"idx": pa.array(df["idx"].to_numpy(), type=pa.int64()),
                   "trace_id": _str_array(df["trace_id"]),
                   "position": pa.array(df["position"].to_numpy(np.int64), type=pa.int64()),
                   "activity": _str_array(df["activity"]),
                   "ts": pa.array(df["ts"].to_numpy(np.int64), type=pa.int64()),
                   **{a: _str_array(df[a]) for a in attrs}})
    pq.write_table(ev, stage / "event.parquet")
    rec["prep_s"]["Event"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    ent_parts, corr_parts = [], []
    for et in entity_types(ds):
        g = pc.group_order(df, et)
        gid = g["group"].astype(str)
        uid = et + ":" + gid
        ent = pd.DataFrame({"uid": uid, "type": et, "id": gid}).drop_duplicates("uid")
        ent_parts.append(ent)
        corr_parts.append(pd.DataFrame({"from": g["idx"].to_numpy(np.int64), "to": uid.to_numpy(),
                                        "type": et, "gpos": g["gpos"].to_numpy(np.int64)}))
        rec["rows"][f"CORR[{et}]"] = len(g)
        rec["rows"][f"Entity[{et}]"] = len(ent)
    ent = pd.concat(ent_parts, ignore_index=True)
    corr = pd.concat(corr_parts, ignore_index=True)
    pq.write_table(pa.table({"uid": _str_array(ent["uid"]), "type": _str_array(ent["type"]),
                             "id": _str_array(ent["id"])}), stage / "entity.parquet")
    pq.write_table(pa.table({"from": pa.array(corr["from"].to_numpy(), type=pa.int64()),
                             "to": _str_array(corr["to"]), "type": _str_array(corr["type"]),
                             "gpos": pa.array(corr["gpos"].to_numpy(), type=pa.int64())}),
                   stage / "corr.parquet")
    rec["prep_s"]["Entity+CORR"] = time.perf_counter() - t0
    del ent, corr, ent_parts, corr_parts

    db, conn = open_db(ds, buffer_gb, threads)
    ddl = ", ".join(["idx INT64", "trace_id STRING", "position INT64", "activity STRING", "ts INT64"]
                    + [f"{q(a)} STRING" for a in attrs] + ["PRIMARY KEY(idx)"])
    conn.execute(f"CREATE NODE TABLE Event ({ddl})")
    conn.execute("CREATE NODE TABLE Entity (uid STRING, type STRING, id STRING, PRIMARY KEY(uid))")
    conn.execute("CREATE REL TABLE CORR (FROM Event TO Entity, type STRING, gpos INT64)")
    for table, f in [("Event", "event"), ("Entity", "entity"), ("CORR", "corr")]:
        t0 = time.perf_counter()
        conn.execute(f"COPY {table} FROM '{stage / (f + '.parquet')}'")
        rec["load_s"][table] = time.perf_counter() - t0
    rec["rows"]["Event"] = conn.execute("MATCH (e:Event) RETURN count(e)").get_next()[0]
    rec["rows"]["Entity"] = conn.execute("MATCH (n:Entity) RETURN count(n)").get_next()[0]
    rec["rows"]["CORR"] = conn.execute("MATCH ()-[c:CORR]->() RETURN count(c)").get_next()[0]

    # DF inference as in graphdb-eventlogs/infer_df_edges.createDirectlyFollowsFast:
    # query the events of every entity in order, pair consecutive ones, COPY.
    t0 = time.perf_counter()
    conn.execute("CREATE REL TABLE DF (FROM Event TO Event, type STRING, id STRING)")
    df_rows = 0
    for et in entity_types(ds):
        res = conn.execute("MATCH (n:Entity)<-[c:CORR]-(e:Event) WHERE n.type = $t "
                           "RETURN n.uid AS uid, n.id AS id, e.idx AS src ORDER BY n.uid, c.gpos",
                           {"t": et})
        cs = res.get_as_arrow().to_pandas()
        same = cs["uid"].to_numpy()[1:] == cs["uid"].to_numpy()[:-1]
        src = cs["src"].to_numpy()[:-1][same]
        tgt = cs["src"].to_numpy()[1:][same]
        ids = cs["id"].to_numpy(dtype=object)[:-1][same]
        tab = pa.table({"from": pa.array(src, type=pa.int64()), "to": pa.array(tgt, type=pa.int64()),
                        "type": pa.array([et] * len(src), type=pa.string()),
                        "id": pa.array(list(ids), type=pa.string())})
        f = stage / f"df_{pc.safe(et)}.parquet"
        pq.write_table(tab, f)
        conn.execute(f"COPY DF FROM '{f}'")
        rec["rows"][f"DF[{et}]"] = len(src)
        df_rows += len(src)
    rec["load_s"]["DF"] = time.perf_counter() - t0
    rec["rows"]["DF"] = df_rows

    conn.execute("CHECKPOINT")
    conn.close()
    db.close()
    shutil.rmtree(stage, ignore_errors=True)
    rec["db_bytes"] = du_bytes(path)
    rec["load_s"]["total_graph"] = sum(v for k, v in rec["load_s"].items() if k != "total_graph")
    rec["attributes"] = attrs
    (LPG_DIR / f"setup_{ds}.json").write_text(json.dumps(rec, indent=1))
    return rec


def load_setup(ds: str) -> dict:
    p = LPG_DIR / f"setup_{ds}.json"
    rec = json.loads(p.read_text()) if p.exists() else {"system": "lpg", "record": "setup", "dataset": ds}
    rec["reused"] = True
    if db_path(ds).exists():
        rec["db_bytes"] = du_bytes(db_path(ds))
    return rec


# ---------------------------------------------------------------------------
# Query translation
# ---------------------------------------------------------------------------

PLANS = ["anchor", "entity", "plain", "stepwise"]
DEFAULT_PLAN = "anchor"


def est_card(e: dict, act_freq: dict[str, int]) -> float:
    """Estimated candidate events of a pattern event: activity frequency, /10 per literal predicate."""
    lit = sum(1 for p in e["preds"] if "ref" not in p and p["op"] == "=")
    return act_freq.get(e["activity"], 0) / (10.0 ** lit)


def translate(query: dict, attrs: set[str], order: str = "anchor",
              act_freq: dict[str, int] | None = None) -> tuple[str, dict]:
    """
    Cypher text and parameters for a pattern query.

    ``order`` sets the join order through Kuzu's ``HINT`` clause (without a
    hint, Kuzu's planner picks bushy hash-join plans for these 9–16-way
    joins that exhaust the buffer pool even on bpic2015):

    * ``anchor`` (default) — a left-deep chain of hash joins on the shared
      entity: start from the pattern event with the fewest candidate events
      (activity frequency under the perspective, /10 per literal equality
      predicate) and its entity, then join, in ascending estimated
      cardinality, each other pattern event as a build side
      ``(c_i JOIN e_i)`` (events of that activity with their CORR edges).
      gpos order is stated for *all* pairs i < j (implied by transitivity
      anyway) so each join is filtered against everything bound so far.
    * ``entity`` — start from the entities of the perspective's type and
      extend to e1, e2, ... in pattern order (adjacent gpos constraints);
      3–8x slower than ``anchor`` on bpic2017.
    * ``plain``  — no hint, adjacent constraints (Kuzu's own plan).
    """
    persp = query["perspective"]
    evs = query["events"]
    L = len(evs)
    params: dict = {"t": persp}
    where = ["n.type = $t"]
    for i, e in enumerate(evs, 1):
        params[f"a{i}"] = e["activity"]
        where.append(f"e{i}.activity = $a{i}")
    if order == "anchor":
        where += [f"c{i}.gpos < c{j}.gpos" for i in range(1, L + 1) for j in range(i + 1, L + 1)]
    else:
        where += [f"c{i}.gpos < c{i + 1}.gpos" for i in range(1, L)]
    k = 0
    for i, e in enumerate(evs, 1):
        for p in e["preds"]:
            a = p["attr"]
            op = "=" if p["op"] == "=" else "<>"
            if a not in attrs:          # unknown attribute: NULL everywhere, never satisfied
                where.append("false")
            elif "ref" in p:            # binding: NULL on either side never satisfies
                where.append(f"e{i}.{q(a)} {op} e{p['ref']}.{q(a)}")
            else:
                k += 1
                params[f"v{k}"] = p["value"]
                where.append(f"e{i}.{q(a)} {op} $v{k}")
    pats = ["(n:Entity)"] + [f"(e{i}:Event)-[c{i}:CORR]->(n)" for i in range(1, L + 1)]
    hint = None
    if order == "anchor":
        seq = sorted(range(1, L + 1), key=lambda i: (est_card(evs[i - 1], act_freq or {}), i))
        hint = f"((e{seq[0]} JOIN c{seq[0]}) JOIN n)"
        for i in seq[1:]:
            hint = f"({hint} JOIN (c{i} JOIN e{i}))"
    elif order == "entity":
        hint = "n"
        for i in range(1, L + 1):
            hint = f"(({hint} JOIN c{i}) JOIN e{i})"
    text = ("MATCH " + ", ".join(pats) + "\nWHERE " + "\n  AND ".join(where)
            + (f"\nHINT {hint}" if hint else "")
            + "\nRETURN count(DISTINCT n.uid) AS matched")
    return text, params


def translate_stepwise(query: dict, attrs: set[str]) -> tuple[str, dict]:
    """
    Alternative (not the default competitor): one Cypher query that walks
    the pattern event by event with ``WITH`` pipelines, keeping per entity
    and per binding state only the earliest gpos::

        MATCH (n:Entity)<-[c1:CORR]-(e1:Event) WHERE n.type = $t AND e1.activity = $a1 ...
        WITH n, e1.`attr` AS b1_0, min(c1.gpos) AS p1
        MATCH (n)<-[c2:CORR]-(e2:Event) WHERE e2.activity = $a2 AND c2.gpos > p1 AND e2.`attr` = b1_0 ...
        WITH n, ..., min(c2.gpos) AS p2
        ...
        RETURN count(DISTINCT n.uid)

    For a fixed state (entity, values referenced by later events) the
    earliest position dominates, so this is exact (it is the memoised
    search of ``pattern_common.TruthIndex`` written in Cypher).
    """
    persp = query["perspective"]
    evs = query["events"]
    L = len(evs)
    params: dict = {"t": persp}
    # binding variables: (ref j, attr) -> name, live from step j up to its last use
    last_use: dict[tuple[int, str], int] = {}
    for i, e in enumerate(evs, 1):
        for p in e["preds"]:
            if "ref" in p:
                key = (p["ref"], p["attr"])
                last_use[key] = max(last_use.get(key, 0), i)
    names = {key: f"b{key[0]}_{n}" for n, key in enumerate(sorted(last_use))}
    parts = []
    k = 0
    for i, e in enumerate(evs, 1):
        params[f"a{i}"] = e["activity"]
        head = "(n:Entity)" if i == 1 else "(n)"
        where = (["n.type = $t"] if i == 1 else []) + [f"e{i}.activity = $a{i}"]
        if i > 1:
            where.append(f"c{i}.gpos > p{i - 1}")
        for p in e["preds"]:
            a = p["attr"]
            op = "=" if p["op"] == "=" else "<>"
            if a not in attrs:
                where.append("false")
            elif "ref" in p:
                where.append(f"e{i}.{q(a)} {op} {names[(p['ref'], a)]}")
            else:
                k += 1
                params[f"v{k}"] = p["value"]
                where.append(f"e{i}.{q(a)} {op} $v{k}")
        parts.append(f"MATCH {head}<-[c{i}:CORR]-(e{i}:Event)\nWHERE " + " AND ".join(where))
        if i < L:
            keep = ["n"]
            for key, nm in sorted(names.items(), key=lambda kv: kv[1]):
                j, a = key
                if j < i and last_use[key] > i:
                    keep.append(nm)                       # carried over
                elif j == i and a in attrs:
                    keep.append(f"e{i}.{q(a)} AS {nm}")   # bound here
                elif j == i:
                    keep.append(f"NULL AS {nm}")
            parts.append("WITH " + ", ".join(keep) + f", min(c{i}.gpos) AS p{i}")
    parts.append("RETURN count(DISTINCT n.uid) AS matched")
    return "\n".join(parts), params


def translate_plan(query: dict, attrs: set[str], plan: str, act_freq: dict[str, int] | None) -> tuple[str, dict]:
    if plan == "stepwise":
        return translate_stepwise(query, attrs)
    return translate(query, attrs, plan, act_freq)


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

def run_query(conn: kuzu.Connection, text: str, params: dict) -> tuple[float, int | None, bool, str | None]:
    t0 = time.perf_counter()
    try:
        res = conn.execute(text, params)
        n = int(res.get_next()[0])
        return time.perf_counter() - t0, n, False, None
    except RuntimeError as ex:
        dt = time.perf_counter() - t0
        msg = str(ex)
        if "interrupt" in msg.lower() or "timeout" in msg.lower():
            return dt, None, True, None
        return dt, None, False, msg


def activity_freq(conn: kuzu.Connection, persp: str) -> dict[str, int]:
    res = conn.execute("MATCH (n:Entity)<-[:CORR]-(e:Event) WHERE n.type = $t "
                       "RETURN e.activity, count(*)", {"t": persp})
    out = {}
    while res.has_next():
        a, c = res.get_next()
        out[a] = int(c)
    return out


def run(ds: str, persp: str, conn: kuzu.Connection, setup_rec: dict, args) -> None:
    if args.queries:  # testing: explicit query file (records of this ds/perspective)
        with open(args.queries) as fq:
            qs = [json.loads(l) for l in fq if l.strip()]
        qs = [x for x in qs if x["dataset"] == ds and x["perspective"] == persp]
    else:
        qs = pc.load_queries(ds, persp)
    if args.qids:
        qs = [x for x in qs if x["qid"] in set(args.qids)]
    if args.limit:
        qs = pick_limit(qs, args.limit)
    attrs = set(conn._get_node_property_names("Event"))
    freq = activity_freq(conn, persp)
    out = Path(args.out) if args.out else out_path(ds, persp, args.plan)
    out.parent.mkdir(parents=True, exist_ok=True)
    conn.set_query_timeout(int(args.timeout * 1000))
    with out.open("w") as f:
        rec = dict(setup_rec)
        rec.update({"perspective": persp, "plan": args.plan, "timeout_s": args.timeout})
        f.write(json.dumps(rec) + "\n")
        f.flush()
        if qs:  # warm-up: one discarded query
            text, params = translate_plan(qs[0], attrs, args.plan, freq)
            dt, n, to, err = run_query(conn, text, params)
            print(f"[{ds}/{persp}] warm-up {qs[0]['qid']}: {dt:.3f}s n={n} timeout={to} err={err}", flush=True)
        ok = tot = 0
        streak: dict[str, int] = {}       # consecutive timeouts per kind (rep 0)
        skipped: set[str] = set()
        for rep in range(args.reps):
            for x in qs:
                if x["qid"] in skipped or (rep == 0 and args.skip_after and streak.get(x["kind"], 0) >= args.skip_after):
                    skipped.add(x["qid"])
                    r = {"system": "lpg", "variant": args.plan, "qid": x["qid"], "dataset": ds,
                         "perspective": persp, "length": x["length"], "kind": x["kind"], "rep": rep,
                         "time_s": None, "matched_groups": None, "truth": x["truth"]["matched_groups"],
                         "parity": None, "timed_out": True, "skipped": True, "error": None}
                    f.write(json.dumps(r) + "\n")
                    print(f"[{ds}/{persp}] {x['qid']} rep={rep} skipped (--skip-after {args.skip_after})", flush=True)
                    tot += 1
                    continue
                text, params = translate_plan(x, attrs, args.plan, freq)
                dt, n, to, err = run_query(conn, text, params)
                if rep == 0:
                    streak[x["kind"]] = streak.get(x["kind"], 0) + 1 if to else 0
                truth = x["truth"]["matched_groups"]
                parity = None if n is None else (n == truth)
                tot += 1
                ok += bool(parity)
                r = {"system": "lpg", "variant": args.plan, "qid": x["qid"], "dataset": ds, "perspective": persp,
                     "length": x["length"], "kind": x["kind"], "rep": rep, "time_s": dt,
                     "matched_groups": n, "truth": truth, "parity": parity,
                     "timed_out": to, "skipped": False, "error": err}
                f.write(json.dumps(r) + "\n")
                f.flush()
                print(f"[{ds}/{persp}] {x['qid']} rep={rep} {dt:8.3f}s n={n} truth={truth}"
                      f"{'' if parity else '  <-- MISMATCH' if parity is False else ''}"
                      f"{'  TIMEOUT' if to else ''}{'  ERR ' + err if err else ''}", flush=True)
        print(f"[{ds}/{persp}] parity {ok}/{tot} -> {out}", flush=True)


def pick_limit(qs: list[dict], n: int) -> list[dict]:
    """``n`` queries spread over lengths and kinds (round robin over (length, kind))."""
    buckets: dict = {}
    for x in qs:
        buckets.setdefault((x["length"], x["kind"]), []).append(x)
    keys = sorted(buckets)
    out = []
    r = 0
    while len(out) < n and any(r < len(buckets[k]) for k in keys):
        for k in keys:
            if r < len(buckets[k]) and len(out) < n:
                out.append(buckets[k][r])
        r += 1
    return sorted(out, key=lambda x: (x["length"], x["kind"], x["qid"]))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--datasets", nargs="+", default=["bpic2011", "bpic2012", "bpic2015", "bpic2017", "bpic2018"])
    ap.add_argument("--perspectives", nargs="*", default=None, help="default: case plus PERSPECTIVES[ds]")
    ap.add_argument("--setup", dest="setup", action="store_true", default=None,
                    help="(re)build the database (default: build only if missing)")
    ap.add_argument("--no-setup", dest="setup", action="store_false", help="reuse the existing database")
    ap.add_argument("--limit", type=int, default=0, help="run N queries spread over lengths/kinds")
    ap.add_argument("--reps", type=int, default=1)
    ap.add_argument("--qids", nargs="*", default=None)
    ap.add_argument("--queries", default=None, help="query JSONL to use instead of the standard file (testing)")
    ap.add_argument("--out", default=None, help="output JSONL instead of the standard path (testing)")
    ap.add_argument("--timeout", type=float, default=pc.TIMEOUT_S)
    ap.add_argument("--skip-after", type=int, default=0,
                    help="after N consecutive timeouts of a kind (queries run by increasing length), record "
                         "the remaining queries of that kind as timed out without running them "
                         "(skipped: true, time_s: null); 0 = run everything")
    ap.add_argument("--plan", choices=PLANS, default=DEFAULT_PLAN,
                    help="join order hint of the shared-entity join (anchor|entity|plain, see translate) "
                         "or the stepwise alternative (see translate_stepwise); non-default plans "
                         "write <ds>.<perspective>.<plan>.jsonl")
    ap.add_argument("--buffer-gb", type=float, default=DEFAULT_BUFFER_GB)
    ap.add_argument("--threads", type=int, default=DEFAULT_THREADS)
    args = ap.parse_args()

    for ds in args.datasets:
        persps = args.perspectives if args.perspectives is not None else entity_types(ds)
        if args.setup or (args.setup is None and not db_path(ds).exists()):
            print(f"[{ds}] building {db_path(ds)}", flush=True)
            setup_rec = build(ds, args.buffer_gb, args.threads)
            setup_rec["reused"] = False
            print(f"[{ds}] setup {json.dumps(setup_rec)}", flush=True)
        else:
            if not db_path(ds).exists():
                sys.exit(f"{db_path(ds)} missing; run with --setup")
            setup_rec = load_setup(ds)
        db, conn = open_db(ds, args.buffer_gb, args.threads, read_only=True)
        for persp in persps:
            if not args.queries and not pc.query_path(ds, persp).exists():
                print(f"[{ds}/{persp}] no query file {pc.query_path(ds, persp)}", file=sys.stderr)
                continue
            run(ds, persp, conn, setup_rec, args)
        conn.close()
        db.close()


if __name__ == "__main__":
    main()
