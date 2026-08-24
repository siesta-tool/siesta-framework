#!/usr/bin/env python3
"""
exp_predicates.py
=================
SIESTA vs ELK benchmark: query latency & result-count as a function of the
**number of event-attribute predicates**, holding the **pattern length fixed**.

Design
------
* Independent variable : k = number of attribute predicates (0 .. MAX_PREDS)
* Held fixed           : pattern length L (default 8) and, per *base*, the
                         activity chain itself. For one base we emit the whole
                         family q_0, q_1, ..., q_K where q_k is q_{k-1} plus one
                         more predicate  -> a clean nested (paired) sweep.
* Measured             : query latency and number of matching traces.

Predicate kinds in the sweep (every predicate is derived from a *witness*
sub-trace, so each query is guaranteed to return >= 1 result):

  - literal equality      "Act"[org:resource="User_12"]
  - binding equality      "Act"[org:resource=$n]     (== n-th event's attr)
  - binding inequality    "Act"[org:resource!=$n]    (four-eyes; needs the != op
                                                      added to the SIESTA DSL)

`$n` references the n-th *positive* event (1-based) in the pattern, mirroring
SIESTA's CEP_adapter semantics. All binding predicates reference an *earlier*
position, so the witness value on both sides is well defined.

ELK handling (as requested)
---------------------------
Events are indexed per-trace (each document carries its `trace_id`). ELK cannot
evaluate cross-event / attribute predicates, so for each query we:

  Phase 0  fetch every event of traces that contain the required activity SET
           (a `terms` filter on `activity`), returning only the fields the query
           needs (structural fields + referenced attributes).
  Phase 1  group events into traces and validate the STRUCTURAL activity
           subsequence  ->  the set of traces that "validate the pattern".
  Phase 2  post-process the attribute predicates on the structurally-valid
           traces via a backtracking subsequence matcher that reproduces
           SIESTA's binding semantics exactly.

ELK latency = phase0 + phase1 + phase2 (the honest end-to-end cost). We also
break the phases out so the predicate-only cost (phase 2, which grows with k)
is visible against the fixed structural cost (phase 0 + 1).

Usage
-----
    # benchmark, assuming both stores are already indexed as <log>_events:
    python exp_predicates.py --log bpic2017

    # (re)ingest first:
    python exp_predicates.py --log bpic2017 --ingest-elk --ingest-siesta

    # just print the generated queries, no servers hit:
    python exp_predicates.py --log bpic2017 --dry-run
"""

import argparse
import json
import math
import os
import random
import re
import time
from collections import defaultdict

import requests

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG (defaults; most are overridable on the CLI)
# ─────────────────────────────────────────────────────────────────────────────
ELK_ENDPOINT = "http://localhost:9200"
SIESTA_ENDPOINT = "http://localhost:8000"

ACTIVITY_KEY = "concept:name"
API_TIMEOUT_S = 1000
ELK_PAGE_SIZE = 10000
BACKTRACK_NODE_CAP = 500_000     # guard against pathological traces

# Attributes we never turn into predicates.
BLACKLIST_ATTRS = {
    "time:timestamp", "@timestamp", "timestamp",
    "event_index", "position", "trace_id", "activity",
    "lifecycle:transition",
}

# Attribute names must be a bare DSL LABEL (letters/digits/_/:) so the pattern
# string tokenises cleanly. Values are always double-quoted + escaped instead.
_VALID_ATTR_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_:]*$")
_NUMERIC_RE = re.compile(r"^-?\d+(\.\d+)?$")


# ─────────────────────────────────────────────────────────────────────────────
# value / string helpers
# ─────────────────────────────────────────────────────────────────────────────
def norm(v):
    """Normalise an attribute value to a comparable string (or None if missing).

    SIESTA compares the raw indexed attribute values (strings) and treats a
    missing attribute as None; we mirror that so ELK counts match SIESTA."""
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    return str(v)


def dsl_quote(s):
    """Quote + escape a string for the SIESTA DSL (double-quoted STRING token)."""
    return '"' + str(s).replace("\\", "\\\\").replace('"', '\\"') + '"'


def is_usable_str_value(v):
    """Prefer categorical string attributes (e.g. org:resource) and avoid
    numeric columns, whose float/str round-tripping could desync ELK vs SIESTA."""
    if v is None:
        return False
    if isinstance(v, float) and math.isnan(v):
        return False
    s = str(v)
    if s == "" or s.lower() == "nan":
        return False
    if _NUMERIC_RE.match(s):          # skip pure numbers -> type-repr parity risk
        return False
    return True


# ─────────────────────────────────────────────────────────────────────────────
# STEP 0 — XES -> CSV  (only needed for query generation + ELK bulk load)
# ─────────────────────────────────────────────────────────────────────────────
def xes_to_csv(xes_file, csv_file):
    import pandas as pd
    from pm4py.objects.log.importer.xes import importer as xes_importer

    log = xes_importer.apply(xes_file)
    rows = []
    for tid, trace in enumerate(log):
        for pos, e in enumerate(trace):
            row = {
                "trace_id": tid,
                "position": pos,                       # within-trace order
                "activity": e.get(ACTIVITY_KEY),
                "timestamp": e.get("time:timestamp"),
            }
            for k, v in e.items():
                if k in (ACTIVITY_KEY, "time:timestamp"):
                    continue
                if isinstance(v, (str, int, float, bool)):
                    row[k] = v
            rows.append(row)

    os.makedirs(os.path.dirname(csv_file) or ".", exist_ok=True)
    pd.DataFrame(rows).to_csv(csv_file, index=False)
    print(f"[STEP 0] CSV written -> {csv_file}  ({len(rows)} events)")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1/2 — ELK index + bulk load (events carry trace_id -> "ingested as traces")
# ─────────────────────────────────────────────────────────────────────────────
def elk_create_index(index):
    requests.delete(f"{ELK_ENDPOINT}/{index}", timeout=30)
    time.sleep(1)
    mapping = {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0,
                     "refresh_interval": "-1"},
        "mappings": {
            "properties": {
                "trace_id": {"type": "keyword"},
                "activity": {"type": "keyword"},
                "position": {"type": "integer"},
                "timestamp": {"type": "date",
                              "format": "strict_date_optional_time||epoch_millis"},
            },
            "dynamic": True,
        },
    }
    r = requests.put(f"{ELK_ENDPOINT}/{index}", json=mapping, timeout=30)
    r.raise_for_status()
    print(f"[STEP 1] ELK index created -> {index}")


def elk_bulk_index(csv_file, index, batch_size=20000):
    import pandas as pd

    df = pd.read_csv(csv_file)
    df["timestamp"] = df["timestamp"].apply(lambda ts: str(ts).replace(" ", "T"))
    total = len(df)
    print(f"[STEP 2] Indexing {total} events into {index} (batch {batch_size})")

    def sanitize(v):
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return None
        return v

    for start in range(0, total, batch_size):
        chunk = df.iloc[start:start + batch_size]
        body = []
        for _, row in chunk.iterrows():
            body.append(json.dumps({"index": {"_index": index}}))
            body.append(json.dumps({k: sanitize(v) for k, v in row.to_dict().items()},
                                   default=str))
        r = requests.post(f"{ELK_ENDPOINT}/_bulk",
                          data="\n".join(body) + "\n",
                          headers={"Content-Type": "application/x-ndjson"},
                          timeout=120)
        if r.status_code >= 400:
            raise RuntimeError(f"Bulk failed: {r.text[:500]}")
        resp = r.json()
        if resp.get("errors"):
            first = next((it for it in resp["items"] if "error" in it["index"]), None)
            raise RuntimeError(f"Bulk indexing error sample: {first}")
        print(f"[STEP 2]   {min(start + batch_size, total)}/{total}")

    requests.post(f"{ELK_ENDPOINT}/{index}/_refresh", timeout=60)
    print("[STEP 2] ELK bulk indexing done")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — SIESTA index (upload the XES directly)
# ─────────────────────────────────────────────────────────────────────────────
def siesta_create_index(xes_file_path, siesta_log, lookback="30000d", timeout=7200):
    print(f"[STEP 3] Uploading + indexing {siesta_log} in SIESTA "
          f"(lookback={lookback}, attributes=['*']) ...")
    payload = {
        "log_name": siesta_log,
        "storage_namespace": "siesta",
        "overwrite_data": True,
        "lookback": lookback,
        # NOTE: format keys ("xes") go DIRECTLY under field_mappings, and
        # trace_level_fields / timestamp_fields are top-level index_config fields.
        # The extra nesting that exp_atts.py used makes from_preprocess_config
        # read config['field_mappings']['xes'] == {} -> it silently falls back to
        # the default mapping WITHOUT attributes (attributes_mapping=None), so
        # structural queries work but every predicate sees empty attributes.
        "field_mappings": {
            "xes": {
                "activity": "concept:name",
                "trace_id": "concept:name",
                "position": None,               # SIESTA derives from XES order
                "start_timestamp": "time:timestamp",
                "attributes": ["*"],
            }
        },
        "trace_level_fields": ["trace_id"],
        "timestamp_fields": ["start_timestamp"],
    }
    with open(xes_file_path, "rb") as f:
        r = requests.post(
            f"{SIESTA_ENDPOINT}/indexing/run",
            data={"index_config": json.dumps(payload)},
            files={"log_file": (os.path.basename(xes_file_path), f,
                                "application/octet-stream")},
            timeout=timeout,
        )
    r.raise_for_status()
    print("[STEP 3] SIESTA indexing done")


# ─────────────────────────────────────────────────────────────────────────────
# TRACE RECONSTRUCTION (for query generation)
# ─────────────────────────────────────────────────────────────────────────────
def load_traces(csv_file):
    import pandas as pd

    df = pd.read_csv(csv_file)
    traces = defaultdict(list)
    for _, row in df.iterrows():
        traces[row["trace_id"]].append(row.to_dict())
    # order events exactly the way ELK/SIESTA will: (timestamp, position)
    for t in traces:
        traces[t] = sorted(
            traces[t],
            key=lambda e: (str(e.get("timestamp")), int(e.get("position", 0))),
        )
    return list(traces.values())


def valid_attr_items(event, allowed):
    """(name, value) attribute pairs usable as string-equality/binding predicates,
    restricted to the categorical `allowed` set."""
    out = []
    for k, v in event.items():
        if k in BLACKLIST_ATTRS:
            continue
        if allowed is not None and k not in allowed:
            continue
        if not _VALID_ATTR_NAME.match(str(k)):
            continue
        if is_usable_str_value(v):
            out.append((k, norm(v)))
    return out


def categorical_attrs(traces, max_distinct=1000):
    """Attributes suitable as predicates: categorical string columns with between
    2 and `max_distinct` distinct values. Excludes near-unique / ID-like columns
    (EventID, OfferID, timestamps) whose equality/inequality is trivial, and
    single-valued columns that can never filter."""
    dist = defaultdict(set)
    over = set()
    for t in traces:
        for e in t:
            for k, v in e.items():
                if k in BLACKLIST_ATTRS or k in over:
                    continue
                if not _VALID_ATTR_NAME.match(str(k)):
                    continue
                if not is_usable_str_value(v):
                    continue
                s = dist[k]
                s.add(norm(v))
                if len(s) > max_distinct:      # ID-like -> drop and stop tracking
                    over.add(k)
                    dist[k] = None
    return {k for k, s in dist.items() if k not in over and s is not None and len(s) >= 2}


# ─────────────────────────────────────────────────────────────────────────────
# PREDICATE-SWEEP GENERATION
# ─────────────────────────────────────────────────────────────────────────────
# A predicate atom:  dict(pos, attr, op, kind, value)
#   kind == "literal" : value is the normed literal string; event[pos][attr] op value
#   kind == "binding" : value is a 0-based ref position r (< pos);
#                       event[pos][attr] op event[r][attr]
#
# A "base" is one fixed length-L activity chain plus its witness events and an
# ordered (shuffled) list of up to MAX_PREDS satisfiable predicate atoms, one per
# position. Query q_k uses the first k atoms of that list.

def build_base(subtrace, max_preds, p_binding, seed_rng, allowed_attrs):
    """Return (chain, witness, atoms) or None if fewer than 1 atom is possible.

    Binding predicates tie an occurrence of an activity to that activity's FIRST
    occurrence in the chain (its "anchor"): every later occurrence of the same
    activity references the same variable $anchor, so they all associate on the
    chosen attribute -- equal (=) or, for four-eyes, different (!=). This mirrors
    how SIESTA's $N binds (N = the N-th positive event). Literal predicates pin a
    single categorical value. Only same-activity pairs form bindings, so the
    associations are meaningful (e.g. the same task handled by different resources).
    """
    L = len(subtrace)
    chain = [norm(e["activity"]) for e in subtrace]

    # normalised attribute view per position (categorical attributes only)
    attrs_at = [dict(valid_attr_items(e, allowed_attrs)) for e in subtrace]

    # anchor = first position at which each activity occurs
    first_occ = {}
    for i, a in enumerate(chain):
        first_occ.setdefault(a, i)

    atoms = []
    positions = list(range(L))
    seed_rng.shuffle(positions)

    for p in positions:
        if len(atoms) >= max_preds:
            break
        if not attrs_at[p]:
            continue

        made = None
        anchor = first_occ[chain[p]]        # same-activity anchor (may be p itself)
        # Same-activity binding: associate this occurrence with the activity's
        # first occurrence via the shared variable $anchor.
        if anchor < p and seed_rng.random() < p_binding:
            shared = [attr for attr in attrs_at[p] if attr in attrs_at[anchor]]
            if shared:
                attr = seed_rng.choice(shared)
                equal = attrs_at[p][attr] == attrs_at[anchor][attr]
                op = "=" if equal else "!="   # witness satisfies either way
                made = {"pos": p, "attr": attr, "op": op,
                        "kind": "binding", "value": anchor}

        if made is None:
            # literal equality on a randomly chosen categorical attribute
            attr = seed_rng.choice(list(attrs_at[p].keys()))
            made = {"pos": p, "attr": attr, "op": "=",
                    "kind": "literal", "value": attrs_at[p][attr]}

        atoms.append(made)

    if not atoms:
        return None

    seed_rng.shuffle(atoms)     # decorrelate predicate identity from k
    # keep the witness lean: only the attrs referenced by some atom + structural
    needed = {a["attr"] for a in atoms}
    witness = []
    for e in subtrace:
        w = {"activity": norm(e["activity"])}
        for attr in needed:
            w[attr] = norm(e.get(attr))
        witness.append(w)
    return chain, witness, atoms


def structural_count(chain, traces):
    """How many traces contain `chain` as an ordered subsequence (predicate-free).

    This is the structural match count both engines must validate, so it is a
    direct proxy for query cost. Computed in-memory over the already-loaded
    traces (no server round-trip) and equals SIESTA's/ELK's structural total.
    """
    n = len(chain)
    hits = 0
    for t in traces:
        pos = 0
        for e in t:
            if str(e.get("activity")) == chain[pos]:
                pos += 1
                if pos == n:
                    hits += 1
                    break
    return hits


def generate_bases(traces, length, num_bases, max_preds, p_binding, rng,
                   min_atoms, min_struct, max_struct, allowed_attrs,
                   min_distinct, max_repeat, dedup=True, max_tries_mult=120):
    """Sample `num_bases` length-`length` activity chains that are:
      * selective  : structural match count in [min_struct, max_struct]
      * non-degenerate: >= min_distinct distinct activities AND no single activity
                        repeated more than max_repeat times. All-one-activity /
                        heavily-repeated chains are excluded -- they make SIESTA's
                        CEP pathological (Kleene-style blow-up) and are not
                        representative. Some repetition is still allowed so
                        same-activity (four-eyes) bindings remain possible.
    """
    from collections import Counter
    bases = []
    seen_chains = set()
    tries = 0
    max_tries = num_bases * max_tries_mult
    long_traces = [t for t in traces if len(t) >= length]
    if not long_traces:
        raise RuntimeError(f"No trace has >= {length} events.")

    skipped_size = skipped_degen = skipped_dup = 0
    while len(bases) < num_bases and tries < max_tries:
        tries += 1
        t = rng.choice(long_traces)
        s = rng.randint(0, len(t) - length)
        sub = t[s:s + length]              # contiguous window (a valid subsequence)
        chain = [norm(e["activity"]) for e in sub]

        counts = Counter(chain)
        if len(counts) < min_distinct or max(counts.values()) > max_repeat:
            skipped_degen += 1
            continue
        if dedup and tuple(chain) in seen_chains:
            skipped_dup += 1
            continue

        built = build_base(sub, max_preds, p_binding, rng, allowed_attrs)
        if built is None:
            continue
        chain, witness, atoms = built
        if len(atoms) < min_atoms:
            continue
        struct = structural_count(chain, traces)
        if struct < min_struct or struct > max_struct:
            skipped_size += 1
            continue

        seen_chains.add(tuple(chain))
        bases.append({"chain": chain, "witness": witness, "atoms": atoms,
                      "structural": struct})
        print(f"[gen] base {len(bases) - 1}: struct={struct} distinct={len(counts)} "
              f"maxrep={max(counts.values())} preds={len(atoms)} chain={chain}")
    if len(bases) < num_bases:
        print(f"[gen] WARNING: only {len(bases)}/{num_bases} bases "
              f"(skipped: {skipped_size} size, {skipped_degen} degenerate, "
              f"{skipped_dup} dup; {tries} tries).")
    return bases


# ─────────────────────────────────────────────────────────────────────────────
# DSL SERIALISATION
# ─────────────────────────────────────────────────────────────────────────────
def atoms_to_pattern(chain, atoms):
    by_pos = defaultdict(list)
    for a in atoms:
        by_pos[a["pos"]].append(a)

    tokens = []
    for i, act in enumerate(chain):
        tok = dsl_quote(act)
        if by_pos[i]:
            parts = []
            for a in by_pos[i]:
                if a["kind"] == "literal":
                    parts.append(f'{a["attr"]}{a["op"]}{dsl_quote(a["value"])}')
                else:  # binding -> $ (1-based index of referenced position)
                    parts.append(f'{a["attr"]}{a["op"]}${a["value"] + 1}')
            tok += "[" + ",".join(parts) + "]"
        tokens.append(tok)
    return " ".join(tokens)


# ─────────────────────────────────────────────────────────────────────────────
# SIESTA DETECT
# ─────────────────────────────────────────────────────────────────────────────
def siesta_detect(pattern, siesta_log):
    payload = {"log_name": siesta_log, "query": {"pattern": pattern}}
    t0 = time.perf_counter()
    try:
        r = requests.post(
            f"{SIESTA_ENDPOINT}/querying/detection",
            headers={"accept": "application/json",
                     "Content-Type": "application/json"},
            json=payload, timeout=API_TIMEOUT_S,
        )
        r.raise_for_status()
        resp = r.json()
        total = int(resp.get("total", 0))
        backend = float(resp.get("time", 0.0))
        err = None
    except requests.exceptions.RequestException as e:
        total, backend, err = -1, 0.0, str(e)
        body = getattr(getattr(e, "response", None), "text", "")
        if body:
            err += f" | {body[:300]}"
    net = time.perf_counter() - t0
    return {"total": total, "backend_sec": backend, "net_sec": net, "error": err}


# ─────────────────────────────────────────────────────────────────────────────
# ELK DETECT — two phase (structural fetch/validate, then predicate post-filter)
# ─────────────────────────────────────────────────────────────────────────────
def _structural_subsequence(chain, events):
    """Greedy two-pointer: does `chain` embed as an ordered subsequence? (no preds)"""
    pos = 0
    for ev in events:
        if ev["activity"] == chain[pos]:
            pos += 1
            if pos == len(chain):
                return True
    return False


def _predicate_embed_exists(chain, events, atoms, node_counter):
    """Backtracking: exists an order-preserving embedding of `chain` in `events`
    satisfying every predicate atom, with SIESTA binding semantics?"""
    by_pos = defaultdict(list)
    for a in atoms:
        by_pos[a["pos"]].append(a)
    n, m = len(chain), len(events)
    assign = [-1] * n

    def check(p, ei):
        ev = events[ei]
        for a in by_pos[p]:
            lhs = norm(ev.get(a["attr"]))
            if a["kind"] == "literal":
                rhs = a["value"]
            else:                                  # binding, ref r < p already set
                rhs = norm(events[assign[a["value"]]].get(a["attr"]))
            if a["op"] == "=" and lhs != rhs:
                return False
            if a["op"] == "!=" and lhs == rhs:
                return False
        return True

    def bt(p, start):
        if p == n:
            return True
        for ei in range(start, m):
            node_counter[0] += 1
            if node_counter[0] > BACKTRACK_NODE_CAP:
                raise _NodeCap()
            if events[ei]["activity"] != chain[p]:
                continue
            if not check(p, ei):
                continue
            assign[p] = ei
            if bt(p + 1, ei + 1):
                return True
            assign[p] = -1
        return False

    return bt(0, 0)


class _NodeCap(Exception):
    pass


def elk_detect(chain, atoms, index, source_fields):
    """Return timings + counts. Structural validation is predicate-free; the
    predicate post-filter runs only on structurally-valid traces."""
    acts = sorted(set(chain))
    body = {
        "query": {"bool": {"filter": [{"terms": {"activity": acts}}]}},
        "size": ELK_PAGE_SIZE,
        "_source": source_fields,
        # unmapped_type lets this run against an index that lacks `position`
        # (e.g. one built by the older exp_atts.py); _doc is the final tiebreak.
        "sort": [{"timestamp": "asc"},
                 {"position": {"order": "asc", "unmapped_type": "integer"}},
                 {"_doc": "asc"}],
    }

    # ── Phase 0: fetch ────────────────────────────────────────────────────
    t0 = time.perf_counter()
    groups = defaultdict(list)
    total_events = 0
    search_after = None
    while True:
        if search_after:
            body["search_after"] = search_after
        r = requests.post(f"{ELK_ENDPOINT}/{index}/_search",
                          json=body, timeout=API_TIMEOUT_S)
        r.raise_for_status()
        hits = r.json()["hits"]["hits"]
        if not hits:
            break
        for h in hits:
            src = h["_source"]
            groups[src["trace_id"]].append(src)
            total_events += 1
        search_after = hits[-1]["sort"]
        if len(hits) < ELK_PAGE_SIZE:
            break
    t_fetch = time.perf_counter() - t0

    # events arrive globally sorted; keep per-trace order stable (already sorted)
    # ── Phase 1: structural validation ────────────────────────────────────
    t1 = time.perf_counter()
    structural = [tid for tid, evs in groups.items()
                  if _structural_subsequence(chain, evs)]
    t_phase1 = time.perf_counter() - t1

    # ── Phase 2: predicate post-filter ────────────────────────────────────
    t2 = time.perf_counter()
    matches = 0
    capped = 0
    if not atoms:
        matches = len(structural)
    else:
        for tid in structural:
            node_counter = [0]
            try:
                if _predicate_embed_exists(chain, groups[tid], atoms, node_counter):
                    matches += 1
            except _NodeCap:
                capped += 1
                matches += 1        # optimistic; flagged via `capped`
    t_phase2 = time.perf_counter() - t2

    return {
        "total": matches,
        "structural": len(structural),
        "fetched_events": total_events,
        "fetched_traces": len(groups),
        "fetch_sec": t_fetch,
        "phase1_sec": t_phase1,
        "phase2_sec": t_phase2,
        "total_sec": t_fetch + t_phase1 + t_phase2,
        "capped": capped,
    }


# ─────────────────────────────────────────────────────────────────────────────
# BENCHMARK
# ─────────────────────────────────────────────────────────────────────────────
def run_benchmark(bases, args):
    results = []
    total_q = sum(min(args.max_preds, len(b["atoms"])) + 1 for b in bases)
    done = 0

    # Stream every completed record to the JSONL immediately (append + flush) so
    # the file is a live view of progress; write_outputs() finalizes it at the end.
    stream_path = f"{args.out}_queries.jsonl"
    stream = None if args.dry_run else open(stream_path, "w", buffering=1)
    if stream is not None:
        print(f"[main] streaming per-query results -> {stream_path}  "
              f"(tail -f to watch; {total_q} queries)")

    try:
        for bi, base in enumerate(bases):
            chain, atoms = base["chain"], base["atoms"]
            for k in range(0, min(args.max_preds, len(atoms)) + 1):
                sub_atoms = atoms[:k]
                pattern = atoms_to_pattern(chain, sub_atoms)

                source_fields = sorted(
                    {"trace_id", "activity", "timestamp", "position"}
                    | {a["attr"] for a in sub_atoms}
                )

                rec = {"base": bi, "k": k, "pattern": pattern,
                       "structural": base.get("structural"),
                       "n_binding": sum(1 for a in sub_atoms if a["kind"] == "binding"),
                       "n_neq": sum(1 for a in sub_atoms if a["op"] == "!="),
                       "n_literal": sum(1 for a in sub_atoms if a["kind"] == "literal")}

                if not args.dry_run:
                    if not args.skip_siesta:
                        rec["siesta"] = siesta_detect(pattern, args.siesta_log)
                    if not args.skip_elk:
                        rec["elk"] = elk_detect(chain, sub_atoms, args.elk_index,
                                                source_fields)

                    st = rec.get("siesta", {}).get("total")
                    el = rec.get("elk", {}).get("total")
                    rec["counts_match"] = (st == el) if (st is not None and
                                                         el is not None) else None
                    done += 1
                    rec["progress"] = f"{done}/{total_q}"
                    _print_row(rec)
                else:
                    print(f"[base {bi:>2} | k={k}] {pattern}")

                results.append(rec)
                if stream is not None:
                    stream.write(json.dumps(rec) + "\n")
                    stream.flush()
    finally:
        if stream is not None:
            stream.close()
    return results


def _print_row(rec):
    s = rec.get("siesta", {})
    e = rec.get("elk", {})
    match = {True: "OK", False: "MISMATCH", None: "-"}[rec.get("counts_match")]
    print(
        f"[{rec.get('progress','-'):>7}] "
        f"[base {rec['base']:>2} | k={rec['k']}] "
        f"SIESTA n={s.get('total','-'):>5} {s.get('backend_sec',0):.3f}s | "
        f"ELK n={e.get('total','-'):>5} tot={e.get('total_sec',0):.3f}s "
        f"(fetch={e.get('fetch_sec',0):.3f} p1={e.get('phase1_sec',0):.3f} "
        f"p2={e.get('phase2_sec',0):.3f}) struct={e.get('structural','-')} "
        f"| {match}"
        + (f" | SIESTA_ERR {s['error'][:80]}" if s.get("error") else "")
    )


def aggregate(results):
    """Median latency and mean result count per k (over bases)."""
    def median(xs):
        xs = sorted(xs)
        n = len(xs)
        if n == 0:
            return float("nan")
        return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2

    by_k = defaultdict(list)
    for r in results:
        by_k[r["k"]].append(r)

    rows = []
    for k in sorted(by_k):
        rs = by_k[k]
        s_backend = [r["siesta"]["backend_sec"] for r in rs
                     if r.get("siesta") and r["siesta"]["total"] >= 0]
        s_total = [r["siesta"]["total"] for r in rs
                   if r.get("siesta") and r["siesta"]["total"] >= 0]
        e_total = [r["elk"]["total_sec"] for r in rs if r.get("elk")]
        e_p2 = [r["elk"]["phase2_sec"] for r in rs if r.get("elk")]
        e_count = [r["elk"]["total"] for r in rs if r.get("elk")]
        matches = [r["counts_match"] for r in rs if r.get("counts_match") is not None]
        rows.append({
            "k": k,
            "n_queries": len(rs),
            "siesta_backend_med_s": round(median(s_backend), 4) if s_backend else None,
            "elk_total_med_s": round(median(e_total), 4) if e_total else None,
            "elk_phase2_med_s": round(median(e_p2), 4) if e_p2 else None,
            "siesta_results_mean": round(sum(s_total) / len(s_total), 1) if s_total else None,
            "elk_results_mean": round(sum(e_count) / len(e_count), 1) if e_count else None,
            "parity_rate": round(sum(matches) / len(matches), 3) if matches else None,
        })
    return rows


def write_outputs(results, summary, out_prefix):
    with open(f"{out_prefix}_queries.jsonl", "w") as f:
        for r in results:
            f.write(json.dumps(r) + "\n")

    import csv
    with open(f"{out_prefix}_summary.csv", "w", newline="") as f:
        if summary:
            w = csv.DictWriter(f, fieldnames=list(summary[0].keys()))
            w.writeheader()
            w.writerows(summary)

    print(f"\n[out] per-query   -> {out_prefix}_queries.jsonl")
    print(f"[out] per-k summary-> {out_prefix}_summary.csv")


def maybe_plot(summary, out_prefix):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as e:
        print(f"[plot] skipped ({e})")
        return
    ks = [r["k"] for r in summary]
    fig, ax1 = plt.subplots(figsize=(8, 5))
    ax1.plot(ks, [r["siesta_backend_med_s"] for r in summary], "o-", label="SIESTA backend")
    ax1.plot(ks, [r["elk_total_med_s"] for r in summary], "s-", label="ELK total")
    ax1.plot(ks, [r["elk_phase2_med_s"] for r in summary], "^--", label="ELK predicate phase")
    ax1.set_xlabel("number of attribute predicates (k)")
    ax1.set_ylabel("median latency (s)")
    ax1.legend(loc="upper left")
    ax1.set_title(f"Latency vs #predicates (fixed pattern length)")
    fig.tight_layout()
    fig.savefig(f"{out_prefix}_latency.png", dpi=130)
    print(f"[plot] -> {out_prefix}_latency.png")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--log", default="bpic2017")
    ap.add_argument("--xes", default=None, help="default: /mnt/datasets/<log>.xes")
    ap.add_argument("--csv", default=None, help="default: datasets/<log>.csv")
    ap.add_argument("--elk-index", default=None, help="default: <log>_events")
    ap.add_argument("--siesta-log", default=None, help="default: <log>_events")
    ap.add_argument("--out", default=None, help="output prefix; default exp_pred_<log>")

    ap.add_argument("--length", type=int, default=8, help="fixed pattern length")
    ap.add_argument("--max-preds", type=int, default=None,
                    help="max k in the sweep (default = length)")
    ap.add_argument("--min-preds", type=int, default=None,
                    help="require each base to support >= this many predicates "
                         "(default = max-preds)")
    ap.add_argument("--bases", type=int, default=12, help="number of activity chains")
    ap.add_argument("--p-binding", type=float, default=0.6,
                    help="probability a predicate is a cross-event binding (else literal)")
    ap.add_argument("--min-struct", type=int, default=50,
                    help="min structural matches a base must have (keeps predicates meaningful)")
    ap.add_argument("--max-struct", type=int, default=5000,
                    help="max structural matches a base may have (caps SIESTA/ELK query cost)")
    ap.add_argument("--min-distinct", type=int, default=None,
                    help="min distinct activities per chain (default max(4, length//2))")
    ap.add_argument("--max-repeat", type=int, default=3,
                    help="max occurrences of any single activity in a chain "
                         "(excludes degenerate near-Kleene chains that blow up SIESTA)")
    ap.add_argument("--timeout", type=int, default=180,
                    help="per-query client timeout in seconds (fail fast on a slow query)")
    ap.add_argument("--seed", type=int, default=42)

    ap.add_argument("--ingest-elk", action="store_true")
    ap.add_argument("--ingest-siesta", action="store_true")
    ap.add_argument("--lookback", default="30000d",
                    help="SIESTA pair-formation lookback window (needs to exceed "
                         "the longest trace's span; default 30000d)")
    ap.add_argument("--skip-elk", action="store_true")
    ap.add_argument("--skip-siesta", action="store_true")
    ap.add_argument("--dry-run", action="store_true",
                    help="generate + print queries only, hit no servers")
    ap.add_argument("--plot", action="store_true")
    args = ap.parse_args()

    args.xes = args.xes or f"/mnt/datasets/{args.log}.xes"
    args.csv = args.csv or f"datasets/{args.log}.csv"
    args.elk_index = args.elk_index or f"{args.log}_events"
    args.siesta_log = args.siesta_log or f"{args.log}_events"
    args.out = args.out or f"exp_pred_{args.log}"
    args.max_preds = args.max_preds if args.max_preds is not None else args.length
    args.min_preds = args.min_preds if args.min_preds is not None else args.max_preds
    args.min_preds = min(args.min_preds, args.length)
    if args.min_distinct is None:
        args.min_distinct = max(4, args.length // 2)

    global API_TIMEOUT_S
    API_TIMEOUT_S = args.timeout

    rng = random.Random(args.seed)

    # ── ingestion (optional) ──────────────────────────────────────────────
    if args.ingest_elk or args.ingest_siesta or not os.path.exists(args.csv):
        if not os.path.exists(args.csv):
            print(f"[main] CSV missing -> building from {args.xes}")
            xes_to_csv(args.xes, args.csv)
    if args.ingest_elk:
        elk_create_index(args.elk_index)
        elk_bulk_index(args.csv, args.elk_index)
    if args.ingest_siesta:
        siesta_create_index(args.xes, args.siesta_log, args.lookback)

    # ── query generation ──────────────────────────────────────────────────
    print(f"[main] loading traces from {args.csv} ...")
    traces = load_traces(args.csv)
    print(f"[main] {len(traces)} traces loaded; scanning categorical attributes ...")
    allowed_attrs = categorical_attrs(traces)
    print(f"[main] usable predicate attributes ({len(allowed_attrs)}): "
          f"{sorted(allowed_attrs)}")
    bases = generate_bases(traces, args.length, args.bases, args.max_preds,
                           args.p_binding, rng, args.min_preds,
                           args.min_struct, args.max_struct, allowed_attrs,
                           args.min_distinct, args.max_repeat)
    print(f"[main] {len(bases)} bases; sweeping k=0..{args.max_preds} "
          f"-> {len(bases) * (args.max_preds + 1)} queries/system\n")

    # ── run ───────────────────────────────────────────────────────────────
    results = run_benchmark(bases, args)

    if args.dry_run:
        return

    summary = aggregate(results)
    print("\n===== SUMMARY (per k) =====")
    hdr = ("k", "n", "SIESTA_s", "ELK_s", "ELK_p2_s", "SIESTA_res", "ELK_res", "parity")
    print("{:>3} {:>4} {:>9} {:>8} {:>9} {:>11} {:>9} {:>7}".format(*hdr))
    for r in summary:
        print("{:>3} {:>4} {:>9} {:>8} {:>9} {:>11} {:>9} {:>7}".format(
            r["k"], r["n_queries"],
            str(r["siesta_backend_med_s"]), str(r["elk_total_med_s"]),
            str(r["elk_phase2_med_s"]), str(r["siesta_results_mean"]),
            str(r["elk_results_mean"]), str(r["parity_rate"])))

    write_outputs(results, summary, args.out)
    if args.plot:
        maybe_plot(summary, args.out)


if __name__ == "__main__":
    main()
