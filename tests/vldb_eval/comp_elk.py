"""
tests/vldb_eval/comp_elk.py — Elasticsearch competitor for the
pattern-detection experiment (Exp 4, case perspective).

Model: one document per trace in ``<ds>_traces``::

    {"trace_id": keyword,
     "events": nested [{activity: keyword, position: integer, ts: long,
                        a_<attr>: keyword, ...}]}

Attribute keys are sanitized to ``a_<key with non [A-Za-z0-9_] -> '_'>``
(suffixed ``_2``, ``_3``... on collisions); the mapping table is stored in
the index ``_meta`` and in the ``setup`` record.  Null attribute values are
omitted from the event object.

Detection of a query (see pattern_common):

1. Server-side candidate pruning: a bool ``filter`` with one ``nested``
   query per pattern event (activity term + one term per literal ``=``
   predicate + an ``exists`` on every attribute taking part in a binding,
   on both the binding event and the referenced event, since a null never
   satisfies a binding).  Candidates are paged with a point-in-time and
   ``search_after`` (sorted by trace_id); ``_source`` is restricted to the
   fields the client-side check needs.
2. Client-side validation: per candidate trace, events ordered by position,
   backtracking search for an ordered subsequence (with gaps) satisfying
   all predicates, memoised on the binding state — same semantics as
   ``TruthIndex``.

``time_s`` is the wall time of both steps; ``TIMEOUT_S`` is checked
between pages and every 256 validated traces, and bounds each HTTP call.

Usage:
    python -m tests.vldb_eval.comp_elk --datasets bpic2015 --setup
    python -m tests.vldb_eval.comp_elk --datasets bpic2017 --no-setup --limit 16 --reps 3
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import subprocess
import sys
import time
from pathlib import Path

import requests

from tests.vldb_eval.eval_common import RESULTS_DIR, run_meta
from tests.vldb_eval.pattern_common import (
    TIMEOUT_S, attribute_columns, load_events, load_queries,
    SKIP_AFTER, SkipAfter, skipped_record,
)

ES = "http://localhost:9200"
CONTAINER = "siesta_elk"
OUT_DIR = RESULTS_DIR / "exp4_patterns" / "elk"
PAGE_SIZE = 1000
BULK_DOCS = 500
CHECK_EVERY = 256

_session = requests.Session()


class Timeout(Exception):
    pass


# ---------------------------------------------------------------------------
# Elasticsearch plumbing
# ---------------------------------------------------------------------------

def es(method: str, path: str, body=None, timeout: float = 120.0, ok=(200,), **kw):
    data = None
    headers = {}
    if body is not None:
        if isinstance(body, (bytes, str)):
            data = body
            headers["Content-Type"] = "application/x-ndjson"
        else:
            data = json.dumps(body)
            headers["Content-Type"] = "application/json"
    r = _session.request(method, f"{ES}{path}", data=data, headers=headers, timeout=timeout, **kw)
    if r.status_code not in ok:
        raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text[:500]}")
    return r.json() if r.content else None


def ensure_es(wait_s: float = 180.0) -> dict:
    """Start the siesta_elk container if ES is not reachable; return node info."""
    def up():
        try:
            return _session.get(f"{ES}/_cluster/health", timeout=3).json()["status"] in ("green", "yellow")
        except Exception:
            return False

    if not up():
        print(f"ES not reachable, starting container {CONTAINER} ...", flush=True)
        subprocess.run(["docker", "start", CONTAINER], check=False, capture_output=True)
        t0 = time.time()
        while not up():
            if time.time() - t0 > wait_s:
                raise RuntimeError("Elasticsearch did not come up")
            time.sleep(2)
    info = es("GET", "/")
    jvm = es("GET", "/_nodes/jvm?filter_path=**.heap_max_in_bytes")
    heap = [n["jvm"]["mem"]["heap_max_in_bytes"] for n in jvm["nodes"].values()]
    return {"es_version": info["version"]["number"], "heap_max_bytes": heap[0] if heap else None}


def index_name(ds: str) -> str:
    return f"{ds.lower()}_traces"


def sanitize_attrs(attrs: list[str]) -> dict[str, str]:
    out, used = {}, set()
    for a in attrs:
        base = "a_" + re.sub(r"[^A-Za-z0-9_]", "_", a)
        name, k = base, 2
        while name in used:
            name, k = f"{base}_{k}", k + 1
        used.add(name)
        out[a] = name
    return out


# ---------------------------------------------------------------------------
# Setup (ingest)
# ---------------------------------------------------------------------------

def setup(ds: str) -> dict:
    idx = index_name(ds)
    t_load = time.perf_counter()
    df = load_events(ds)
    attrs = attribute_columns(df)
    fmap = sanitize_attrs(attrs)
    load_s = time.perf_counter() - t_load

    es("DELETE", f"/{idx}", ok=(200, 404))
    props = {"activity": {"type": "keyword"}, "position": {"type": "integer"}, "ts": {"type": "long"}}
    for a in attrs:
        props[fmap[a]] = {"type": "keyword"}
    es("PUT", f"/{idx}", {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": "-1",
                     "index.mapping.nested_objects.limit": 1_000_000},
        "mappings": {
            "dynamic": "strict",
            "_meta": {"attr_fields": fmap, "dataset": ds},
            "properties": {
                "trace_id": {"type": "keyword"},
                "events": {"type": "nested", "properties": props},
            },
        },
    })

    t0 = time.perf_counter()
    df = df.sort_values(["trace_id", "position"], kind="stable")
    tids = df["trace_id"].to_numpy()
    cols = {
        "activity": df["activity"].to_numpy(),
        "position": df["position"].to_numpy(),
        "ts": df["ts"].to_numpy(),
    }
    acols = [(fmap[a], df[a].to_numpy(dtype=object)) for a in attrs]
    n = len(df)
    lines: list[str] = []
    ndocs = 0
    bulk_s = 0.0

    def flush():
        nonlocal lines, bulk_s
        if not lines:
            return
        tb = time.perf_counter()
        r = es("POST", "/_bulk", "\n".join(lines) + "\n", timeout=600)
        bulk_s += time.perf_counter() - tb
        if r.get("errors"):
            bad = next(it for it in r["items"] if it["index"].get("error"))
            raise RuntimeError(f"bulk error: {bad}")
        lines = []

    i = 0
    while i < n:
        j = i
        tid = tids[i]
        while j < n and tids[j] == tid:
            j += 1
        evs = []
        for r in range(i, j):
            e = {"activity": cols["activity"][r], "position": int(cols["position"][r]), "ts": int(cols["ts"][r])}
            for f, arr in acols:
                v = arr[r]
                if isinstance(v, str):
                    e[f] = v
            evs.append(e)
        lines.append(json.dumps({"index": {"_index": idx, "_id": str(tid)}}))
        lines.append(json.dumps({"trace_id": str(tid), "events": evs}))
        ndocs += 1
        if ndocs % BULK_DOCS == 0:
            flush()
        i = j
    flush()
    t_bulk = time.perf_counter() - t0
    tr = time.perf_counter()
    es("POST", f"/{idx}/_refresh", timeout=600)
    t_refresh = time.perf_counter() - tr
    tf = time.perf_counter()
    es("POST", f"/{idx}/_forcemerge?max_num_segments=1", timeout=3600)
    t_merge = time.perf_counter() - tf
    es("POST", f"/{idx}/_flush?wait_if_ongoing=true", timeout=600)
    es("POST", f"/{idx}/_refresh", timeout=600)
    ingest_s = time.perf_counter() - t0
    segs = es("GET", f"/{idx}/_segments")["indices"][idx]["shards"]["0"][0]["num_search_segments"]
    st = es("GET", f"/{idx}/_stats/store,docs")["indices"][idx]["primaries"]
    count = es("GET", f"/{idx}/_count")["count"]
    assert count == ndocs, (count, ndocs)
    return {
        "index": idx, "traces": ndocs, "events": n,
        "lucene_docs": st["docs"]["count"],          # includes nested event docs
        "size_bytes": st["store"]["size_in_bytes"], "segments": segs,
        "ingest_s": round(ingest_s, 3),
        "phases": {"load_canonical_s": round(load_s, 3), "bulk_s": round(t_bulk, 3),
                   "bulk_http_s": round(bulk_s, 3), "refresh_s": round(t_refresh, 3),
                   "forcemerge_s": round(t_merge, 3)},
        "attr_fields": fmap, "page_size": PAGE_SIZE, "bulk_docs": BULK_DOCS,
    }


def reuse(ds: str) -> dict:
    idx = index_name(ds)
    m = es("GET", f"/{idx}/_mapping")[idx]["mappings"]
    st = es("GET", f"/{idx}/_stats/store,docs")["indices"][idx]["primaries"]
    return {"index": idx, "reused": True, "traces": es("GET", f"/{idx}/_count")["count"],
            "lucene_docs": st["docs"]["count"], "size_bytes": st["store"]["size_in_bytes"],
            "attr_fields": m["_meta"]["attr_fields"], "page_size": PAGE_SIZE}


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------

def build_filter(events: list[dict], fmap: dict[str, str]) -> list[dict]:
    """One nested query per pattern event (server-side pruning)."""
    exists = [set() for _ in events]
    for i, e in enumerate(events):
        for p in e["preds"]:
            if "ref" in p:
                exists[i].add(p["attr"])
                exists[p["ref"] - 1].add(p["attr"])
    out = []
    for i, e in enumerate(events):
        must = [{"term": {"events.activity": e["activity"]}}]
        for p in e["preds"]:
            if "ref" not in p and p["op"] == "=":
                f = fmap.get(p["attr"], "__no_such_attr__")
                must.append({"term": {f"events.{f}": p["value"]}})
        for a in sorted(exists[i]):
            f = fmap.get(a, "__no_such_attr__")
            must.append({"exists": {"field": f"events.{f}"}})
        out.append({"nested": {"path": "events", "query": {"bool": {"filter": must}}}})
    return out


class Matcher:
    """Exact pattern_common semantics on one group's events (ordered by position)."""

    def __init__(self, events: list[dict], fmap: dict[str, str]):
        self.L = L = len(events)
        self.acts = [e["activity"] for e in events]
        # literals are re-checked client-side (the server filter is per event, not per match)
        self.lit_preds = [[(fmap.get(p["attr"], "__no_such_attr__"), p["op"], p["value"])
                           for p in e["preds"] if "ref" not in p] for e in events]
        self.bind = [[(fmap.get(p["attr"], "__no_such_attr__"), p["op"] == "=", p["ref"] - 1)
                      for p in e["preds"] if "ref" in p] for e in events]
        self.needed = [set() for _ in range(L)]
        for bp in self.bind:
            for f, _, j in bp:
                self.needed[j].add(f)
        live = [set() for _ in range(L + 1)]
        for i in range(L - 1, -1, -1):
            live[i] = set(live[i + 1])
            for f, _, j in self.bind[i]:
                live[i].add((j, f))
        self.live = [sorted(k for k in live[i] if k[0] < i) for i in range(L + 1)]
        fields = {"activity", "position"}
        for lp in self.lit_preds:
            fields |= {f for f, _, _ in lp}
        for bp in self.bind:
            fields |= {f for f, _, _ in bp}
        self.source = ["trace_id"] + [f"events.{f}" for f in sorted(fields)]

    def match(self, evs: list[dict]) -> bool:
        by_act: dict[str, list[dict]] = {}
        for e in sorted(evs, key=lambda e: e["position"]):
            by_act.setdefault(e.get("activity"), []).append(e)
        L = self.L
        cand = []
        for i in range(L):
            a, lp = self.acts[i], self.lit_preds[i]
            if not lp:
                c = by_act.get(a)
                if not c:
                    return False
                cand.append(c)
                continue
            c = []
            for e in by_act.get(a, ()):
                ok = True
                for f, op, v in lp:
                    x = e.get(f)
                    if op == "=":
                        if x != v:
                            ok = False
                            break
                    elif x is None or x == v:   # literal != (not generated)
                        ok = False
                        break
                if ok:
                    c.append(e)
            if not c:
                return False
            cand.append(c)
        bind, needed, live = self.bind, self.needed, self.live
        failed = set()

        def dfs(i: int, after: int, bound: dict) -> bool:
            if i == L:
                return True
            state = (i, after, tuple(bound.get(k) for k in live[i]))
            if state in failed:
                return False
            for e in cand[i]:
                pos = e["position"]
                if pos <= after:
                    continue
                ok = True
                for f, eq, j in bind[i]:
                    rv = bound.get((j, f))
                    v = e.get(f)
                    if rv is None or v is None or (v == rv) != eq:
                        ok = False
                        break
                if not ok:
                    continue
                nb = bound
                if needed[i]:
                    nb = dict(bound)
                    for f in needed[i]:
                        nb[(i, f)] = e.get(f)
                if dfs(i + 1, pos, nb):
                    return True
                if not needed[i]:
                    break   # nothing later refers to this event: earliest candidate is optimal
            failed.add(state)
            return False

        return dfs(0, -1, {})


def detect(ds: str, q: dict, fmap: dict[str, str], timeout_s: float = TIMEOUT_S) -> dict:
    idx = index_name(ds)
    t0 = time.perf_counter()
    deadline = t0 + timeout_s
    matcher = Matcher(q["events"], fmap)
    query = {"bool": {"filter": build_filter(q["events"], fmap)}}
    search_s = validate_s = 0.0
    candidates = matched = pages = 0
    pit = None
    timed_out = False

    def remaining():
        r = deadline - time.perf_counter()
        if r <= 0:
            raise Timeout
        return r

    try:
        pit = es("POST", f"/{idx}/_pit?keep_alive=2m", timeout=remaining())["id"]
        after = None
        while True:
            body = {"size": PAGE_SIZE, "query": query, "_source": matcher.source,
                    "pit": {"id": pit, "keep_alive": "2m"}, "sort": [{"trace_id": "asc"}],
                    "track_total_hits": False}
            if after is not None:
                body["search_after"] = after
            ts = time.perf_counter()
            try:
                r = es("POST", "/_search", body, timeout=remaining() + 1.0)
            except requests.Timeout:
                raise Timeout
            search_s += time.perf_counter() - ts
            pit = r.get("pit_id", pit)
            hits = r["hits"]["hits"]
            pages += 1
            if not hits:
                break
            tv = time.perf_counter()
            for k, h in enumerate(hits):
                if k % CHECK_EVERY == 0:
                    remaining()
                candidates += 1
                if matcher.match(h["_source"].get("events", [])):
                    matched += 1
            validate_s += time.perf_counter() - tv
            if len(hits) < PAGE_SIZE:
                break
            after = hits[-1]["sort"]
        remaining()
    except Timeout:
        timed_out = True
    finally:
        if pit is not None:
            try:
                es("DELETE", "/_pit", {"id": pit}, timeout=10, ok=(200, 404))
            except Exception:
                pass
    time_s = time.perf_counter() - t0
    return {"time_s": round(time_s, 6), "matched_groups": None if timed_out else matched,
            "timed_out": timed_out,
            "phases": {"search_s": round(search_s, 6), "validate_s": round(validate_s, 6),
                       "candidates": candidates, "pages": pages}}


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def run_dataset(ds: str, args) -> None:
    node = ensure_es()
    try:
        qs = load_queries(ds, "case")
    except FileNotFoundError:
        print(f"[{ds}] no query file: setup only", flush=True)
        qs = []
    if args.qids:
        want = set(args.qids)
        qs = [q for q in qs if q["qid"] in want or any(q["qid"].startswith(w) for w in want)]
    if args.limit is not None:
        qs = qs[:args.limit]
    if args.setup:
        print(f"[{ds}] ingesting ...", flush=True)
        st = setup(ds)
    else:
        st = reuse(ds)
    print(f"[{ds}] setup: traces={st['traces']} size={st['size_bytes'] / 2**20:.1f} MiB "
          f"ingest_s={st.get('ingest_s')}", flush=True)
    fmap = st["attr_fields"]

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = Path(args.out) if args.out else OUT_DIR / f"{ds}.case.jsonl"
    with out.open("w") as f:
        f.write(json.dumps({"type": "setup", "system": "elk", "dataset": ds, "perspective": "case",
                            **node, **st, "timeout_s": args.timeout, "reps": args.reps,
                            "clear_cache": args.clear_cache, "meta": run_meta()}) + "\n")
        f.flush()
        if qs:   # warm-up, discarded
            w = detect(ds, qs[0], fmap, args.timeout)
            print(f"[{ds}] warm-up {qs[0]['qid']}: {w['time_s']:.3f}s", flush=True)
        n_ok = n_run = 0
        times: dict[int, list[float]] = {}
        cutoff = SkipAfter(args.skip_after)
        for q in qs:
            if cutoff.skip(q):
                f.write(json.dumps(skipped_record("elk", q)) + "\n")
                f.flush()
                print(f"  {q['qid']} skipped ({args.skip_after} consecutive timeouts)", flush=True)
                continue
            for rep in range(args.reps):
                if args.clear_cache:
                    es("POST", f"/{index_name(ds)}/_cache/clear")
                err = None
                try:
                    res = detect(ds, q, fmap, args.timeout)
                except Exception as e:   # noqa: BLE001
                    res = {"time_s": None, "matched_groups": None, "timed_out": False, "phases": {}}
                    err = f"{type(e).__name__}: {e}"
                truth = q["truth"]
                parity = res["matched_groups"] == truth["matched_groups"]
                rec = {"system": "elk", "qid": q["qid"], "dataset": ds, "perspective": q["perspective"],
                       "length": q["length"], "kind": q["kind"], "rep": rep,
                       "time_s": res["time_s"], "matched_groups": res["matched_groups"],
                       "truth": truth, "parity": parity, "timed_out": res["timed_out"],
                       "error": err, "phases": res["phases"]}
                f.write(json.dumps(rec) + "\n")
                f.flush()
                if rep == 0:
                    cutoff.record(q, res["timed_out"])
                n_run += 1
                n_ok += parity
                if res["time_s"] is not None:
                    times.setdefault(q["length"], []).append(res["time_s"])
                print(f"  {q['qid']} rep={rep} t={res['time_s']}s got={res['matched_groups']} "
                      f"truth={truth['matched_groups']} cand={res['phases'].get('candidates')} "
                      f"{'OK' if parity else 'MISMATCH'}{' TIMEOUT' if res['timed_out'] else ''}"
                      f"{' ' + err if err else ''}", flush=True)
    print(f"[{ds}] parity {n_ok}/{n_run} -> {out}")
    for L in sorted(times):
        print(f"  L={L:2d} median={statistics.median(times[L]):.3f}s max={max(times[L]):.3f}s n={len(times[L])}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--datasets", nargs="+", default=["bpic2011", "bpic2012", "bpic2015", "bpic2017", "bpic2018"])
    ap.add_argument("--setup", action=argparse.BooleanOptionalAction, default=True,
                    help="(re)ingest the index (default) or reuse it with --no-setup")
    ap.add_argument("--limit", type=int, default=None, help="only the first N queries")
    ap.add_argument("--reps", type=int, default=1)
    ap.add_argument("--qids", nargs="*", default=None, help="qids (or qid prefixes) to run")
    ap.add_argument("--clear-cache", action="store_true",
                    help="clear the index caches before every measured run")
    ap.add_argument("--timeout", type=float, default=TIMEOUT_S, help="per-query timeout (s)")
    ap.add_argument("--skip-after", type=int, default=SKIP_AFTER,
                    help="consecutive timeouts of a kind before the rest of that kind is skipped (0: off)")
    ap.add_argument("--out", default=None, help="output path (single dataset only)")
    args = ap.parse_args()
    for ds in args.datasets:
        run_dataset(ds, args)


if __name__ == "__main__":
    sys.exit(main())
