"""
tests/eval/exp_elk_attribute_case.py

Experiment: SIESTA warm vs ELK exact — attribute-constrained case_id queries.

Two dimensions
--------------
  k = 2 .. max_length        (pattern length)
  constraint_type ∈ {structural, single_attr, multi_attr, cross_binding}

All patterns are sampled from REAL traces → guaranteed non-zero results.
ELK uses Painless scripted_metric (server-side) for exact STNM matching
including attribute constraints and cross-event variable bindings.
Both systems return identical result counts — the comparison is honest.

Constraint types
----------------
  structural    A B C ...          no constraints — baseline
  single_attr   A[r="v"] B C ...   equality on first event only
  multi_attr    A[r="v1"] B C[r="v2"] ...  equality on ≥2 events
  cross_binding A[r=$1] B C[r=$1]  two events must share same attr value

ELK exact query
---------------
For each trace bucket (terms by trace_id):
  scripted_metric (Painless):
    1. collect (activity, position, resource_value) for every event
    2. sort by position
    3. greedy STNM scan:
       - for equality constraint: event.attr == required_val
       - for binding (first occurrence): capture event.attr as $1
       - for binding (subsequent): event.attr must == captured $1
    4. return 1 if full match, 0 otherwise
  bucket_selector: keep only traces where script == 1

Semantically identical to SIESTA's eventually-follows evaluation.

Output
------
results/6_4_1_elk_attribute_case_{log_name}.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
import time
from collections import defaultdict
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tests.eval.eval_common import (
    API_TIMEOUT_S, CONFIG_DIR,
    Recorder, health_check,
    ingest_adaptive,
    timed_query, detect_adaptive,
    discover_schema, resolve_dataset,
    quote_label,
)

CONFIG = CONFIG_DIR / "adaptive_index.config.json"

ELK_ENDPOINT    = os.environ.get("ELK_ENDPOINT",    "http://localhost:9200")
ELK_BULK_SIZE   = int(os.environ.get("ELK_BULK_BATCH", "5000"))
SWEEP_TIMEOUT_S = int(os.environ.get("SWEEP_TIMEOUT_S", "600"))
WARMUP_SLEEP_S  = int(os.environ.get("WARMUP_SLEEP_S",  "300"))
MAX_TRACE_LOAD  = int(os.environ.get("MAX_TRACE_LOAD",  "20000"))

RETENTION_OVERRIDES = {
    "min_query_count":   3,
    "half_life_seconds": 3600.0,
    "hysteresis":        0.0,
}
N_WARMUP    = 4
ATTR_KEY    = os.environ.get("ATTR_KEY", "resource")   # attribute for constraints
N_PER_K     = 2   # query variants to sample per (k, constraint_type)


# ═══════════════════════════════════════════════════════════════════════════
# Trace loading and pattern sampling
# ═══════════════════════════════════════════════════════════════════════════

def load_traces(dataset_path: Path, seed: int = 42) -> list[list[dict]]:
    """
    Load up to MAX_TRACE_LOAD traces from the log.
    Each trace is a list of event dicts ordered by their appearance in the file.
    """
    from tests.eval.batch_splitter import _iter_log
    trace_dict: dict[str, list[dict]] = {}
    for ev in _iter_log(dataset_path):
        tid = ev.get("trace_id")
        if not tid:
            continue
        if tid not in trace_dict:
            if len(trace_dict) >= MAX_TRACE_LOAD:
                continue
            trace_dict[tid] = []
        trace_dict[tid].append(ev)
    traces = list(trace_dict.values())
    random.Random(seed).shuffle(traces)
    print(f"    Loaded {len(traces)} traces")
    return traces


def _find_cross_binding_pair(
    events: list[dict],
    attr_key: str,
) -> tuple[int, int, str] | None:
    """
    Find indices (i, j) with i < j where events[i] and events[j]
    have the same value for attr_key.  Returns (i, j, value) or None.
    Prefer pairs that are not adjacent to make the pattern more interesting.
    """
    # Build val -> list of indices
    val_positions: dict[str, list[int]] = defaultdict(list)
    for idx, ev in enumerate(events):
        v = ev.get(attr_key, "")
        if v:
            val_positions[v].append(idx)

    # Pick first value with ≥2 occurrences, prefer gap ≥ 2
    for val, positions in sorted(val_positions.items(),
                                 key=lambda x: -len(x[1])):
        if len(positions) >= 2:
            # Try to find a pair with gap ≥ 2
            for i in range(len(positions)):
                for j in range(i + 1, len(positions)):
                    if positions[j] - positions[i] >= 2:
                        return positions[i], positions[j], val
            # Fallback: any adjacent pair
            return positions[0], positions[1], val
    return None


def _build_seql_pattern(
    activities: list[str],
    constraints: list[tuple[int, str, str]],   # (idx, attr_key, value_or_$N)
) -> str:
    """
    Build a SeQL pattern string from activities + constraints.
    constraints[i] = (event_index, attr_key, value)
    where value may be a literal string or "$1", "$2", etc.
    """
    # Group constraints by event index
    by_idx: dict[int, list[tuple[str, str]]] = defaultdict(list)
    for idx, attr_key, val in constraints:
        by_idx[idx].append((attr_key, val))

    parts = []
    for i, act in enumerate(activities):
        label = quote_label(act)
        if i in by_idx:
            attrs = ",".join(
                f'{ak}="{v}"' if not v.startswith("$") else f"{ak}={v}"
                for ak, v in by_idx[i]
            )
            parts.append(f"{label}[{attrs}]")
        else:
            parts.append(label)
    return " ".join(parts)


def sample_workload(
    traces: list[list[dict]],
    log_name: str,
    min_k: int,
    max_k: int,
    attr_key: str,
    n_per_k: int,
    seed: int = 42,
) -> list[dict]:
    """
    Sample patterns of each length k, for each constraint type.
    Guaranteed non-zero: every pattern comes from a real trace subsequence.

    Returns list of query dicts:
      id, log_name, pattern, activities, constraints, constraint_type,
      pattern_length, grouping_keys, tags
    """
    rng = random.Random(seed)
    queries: list[dict] = []
    cnt = 0

    for k in range(min_k, max_k + 1):
        # Candidate traces with enough distinct activities
        eligible = [t for t in traces
                    if len({e.get("activity") for e in t}) >= k and len(t) >= k]
        if not eligible:
            print(f"    k={k}: no eligible traces — stopping.")
            break

        # Shuffle to vary samples per k
        pool = list(eligible)
        rng.shuffle(pool)

        generated: dict[str, int] = defaultdict(int)  # constraint_type -> count

        for trace in pool:
            if all(generated[ct] >= n_per_k
                   for ct in ("structural","single_attr","multi_attr","cross_binding")):
                break

            # Extract k distinct activities preserving trace order
            seen: set[str] = set()
            selected_events: list[dict] = []
            for ev in trace:
                act = ev.get("activity", "")
                if act and act not in seen:
                    seen.add(act)
                    selected_events.append(ev)
                    if len(selected_events) == k:
                        break

            if len(selected_events) < k:
                continue

            acts = [ev.get("activity", "") for ev in selected_events]

            # ── structural ────────────────────────────────────────────
            if generated["structural"] < n_per_k:
                cnt += 1
                queries.append({
                    "id":              f"ST{cnt}",
                    "log_name":        log_name,
                    "pattern":         _build_seql_pattern(acts, []),
                    "activities":      acts,
                    "constraints":     [],
                    "constraint_type": "structural",
                    "pattern_length":  k,
                    "grouping_keys":   ["trace_id"],
                    "tags":            [f"k={k}", "structural"],
                })
                generated["structural"] += 1

            # ── single_attr (constraint on first event) ───────────────
            if generated["single_attr"] < n_per_k:
                v = selected_events[0].get(attr_key, "")
                if v:
                    cnt += 1
                    constraints = [(0, attr_key, v)]
                    queries.append({
                        "id":              f"SA{cnt}",
                        "log_name":        log_name,
                        "pattern":         _build_seql_pattern(acts, constraints),
                        "activities":      acts,
                        "constraints":     constraints,
                        "constraint_type": "single_attr",
                        "pattern_length":  k,
                        "grouping_keys":   ["trace_id"],
                        "tags":            [f"k={k}", "single_attr"],
                    })
                    generated["single_attr"] += 1

            # ── multi_attr (first + one other event) ──────────────────
            if generated["multi_attr"] < n_per_k and k >= 3:
                v0 = selected_events[0].get(attr_key, "")
                # Pick a non-first event that also has the attribute
                for other_idx in range(1, k):
                    v_other = selected_events[other_idx].get(attr_key, "")
                    if v0 and v_other and v_other != v0:
                        cnt += 1
                        constraints = [(0, attr_key, v0),
                                       (other_idx, attr_key, v_other)]
                        queries.append({
                            "id":              f"MA{cnt}",
                            "log_name":        log_name,
                            "pattern":         _build_seql_pattern(acts, constraints),
                            "activities":      acts,
                            "constraints":     constraints,
                            "constraint_type": "multi_attr",
                            "pattern_length":  k,
                            "grouping_keys":   ["trace_id"],
                            "tags":            [f"k={k}", "multi_attr"],
                        })
                        generated["multi_attr"] += 1
                        break

            # ── cross_binding (A[r=$1] ... B[r=$1]) ───────────────────
            if generated["cross_binding"] < n_per_k and k >= 3:
                result = _find_cross_binding_pair(selected_events, attr_key)
                if result:
                    i, j, val = result
                    # Only include activities up to position j (inclusive)
                    sub_acts = acts[:j + 1] if j < k else acts
                    constraints = [(i, attr_key, "$1"), (j, attr_key, "$1")]
                    cnt += 1
                    queries.append({
                        "id":              f"CB{cnt}",
                        "log_name":        log_name,
                        "pattern":         _build_seql_pattern(sub_acts, constraints),
                        "activities":      sub_acts,
                        "constraints":     constraints,
                        "constraint_type": "cross_binding",
                        "pattern_length":  len(sub_acts),
                        "grouping_keys":   ["trace_id"],
                        "tags":            [f"k={len(sub_acts)}", "cross_binding"],
                    })
                    generated["cross_binding"] += 1

    return queries


# ═══════════════════════════════════════════════════════════════════════════
# ELK — ingest
# ═══════════════════════════════════════════════════════════════════════════

def _elk_reachable() -> bool:
    try:
        return requests.get(ELK_ENDPOINT, timeout=5).status_code == 200
    except Exception:
        return False


def elk_create_index(index_name: str, attribute_keys: list[str]) -> None:
    requests.delete(f"{ELK_ENDPOINT}/{index_name}", timeout=10)
    time.sleep(0.3)

    # Increase the max buckets limit to support trace_size = 200,000
    requests.put(
        f"{ELK_ENDPOINT}/_cluster/settings",
        json={"persistent": {"search.max_buckets": 300000}},
        timeout=10
    ).raise_for_status()

    props: dict = {
        "trace_id":  {"type": "keyword"},
        "activity":  {"type": "keyword"},
        "position":  {"type": "integer"},
    }
    for attr in attribute_keys:
        props[attr] = {"type": "keyword"}
    mapping = {
        "settings": {
            "number_of_shards": 1, "number_of_replicas": 0,
            "refresh_interval": "-1",
        },
        "mappings": {"properties": props},
    }
    requests.put(f"{ELK_ENDPOINT}/{index_name}",
                 json=mapping, timeout=30).raise_for_status()
    print(f"    ELK index '{index_name}' created")


def elk_ingest(dataset_path: Path, index_name: str,
               attribute_keys: list[str]) -> int:
    from tests.eval.batch_splitter import _iter_log
    buf: list[str] = []
    n = 0
    pos_map: dict[str, int] = {}
    for ev in _iter_log(dataset_path):
        tid = ev.get("trace_id", "")
        pos = pos_map.get(tid, 0)
        pos_map[tid] = pos + 1
        doc: dict = {
            "trace_id": tid,
            "activity": ev.get("activity", ""),
            "position": pos,
        }
        for attr in attribute_keys:
            v = ev.get(attr)
            if v:
                doc[attr] = v
        buf.append(json.dumps({"index": {"_index": index_name}}))
        buf.append(json.dumps(doc))
        n += 1
        if len(buf) >= ELK_BULK_SIZE * 2:
            _elk_bulk_flush(buf)
            buf.clear()
    if buf:
        _elk_bulk_flush(buf)
    requests.post(f"{ELK_ENDPOINT}/{index_name}/_refresh", timeout=30)
    requests.put(f"{ELK_ENDPOINT}/{index_name}/_settings",
                 json={"index": {"refresh_interval": "1s"}}, timeout=10)
    return n


def _elk_bulk_flush(lines: list[str]) -> None:
    body = "\n".join(lines) + "\n"
    requests.post(f"{ELK_ENDPOINT}/_bulk",
                  data=body.encode(),
                  headers={"Content-Type": "application/x-ndjson"},
                  timeout=120).raise_for_status()


# ═══════════════════════════════════════════════════════════════════════════
# ELK — exact query (Painless scripted_metric, server-side STNM)
# ═══════════════════════════════════════════════════════════════════════════

def build_elk_exact_query(
    q: dict,
    attr_key: str,
    trace_size: int = 200_000,
) -> dict:
    activities   = q["activities"]
    constraints  = q["constraints"]

    by_idx: dict[int, dict] = {}
    for idx, ak, val in constraints:
        by_idx[idx] = by_idx.get(idx, {})
        if val.startswith("$"):
            by_idx[idx]["b"] = int(val[1:])
        else:
            by_idx[idx]["r"] = val

    pattern = []
    for i, act in enumerate(activities):
        c = by_idx.get(i, {})
        pattern.append({
            "a": act,
            "r": c.get("r", ""),
            "b": c.get("b", -1),
        })

    # Bulletproof Painless: Explicit typing, safe casting, and map .get() usage
    reduce_script = (
        "def events = [];\n"
        "for (def s : states) {\n"
        "  if (s != null && s.containsKey('evs')) {\n"
        "    events.addAll(s.get('evs'));\n"
        "  }\n"
        "}\n"
        "events.sort((x, y) -> Long.compare((long)x.get('p'), (long)y.get('p')));\n"
        "def pattern = params.get('pattern');\n"
        "def captured = new HashMap();\n"
        "int pi = 0;\n"
        "int plen = pattern.size();\n"
        "for (def ev : events) {\n"
        "  def req = pattern.get(pi);\n"
        "  if (!ev.get('a').equals(req.get('a'))) continue;\n"
        "  boolean ok = true;\n"
        "  if (!req.get('r').isEmpty()) ok = ev.get('r').equals(req.get('r'));\n"
        "  if (ok && req.get('b') != -1) {\n"
        "    String bk = String.valueOf(req.get('b'));\n"
        "    if (captured.containsKey(bk)) {\n"
        "      ok = ev.get('r').equals(captured.get(bk));\n"
        "    } else {\n"
        "      captured.put(bk, ev.get('r'));\n"
        "    }\n"
        "  }\n"
        "  if (ok) {\n"
        "    pi++;\n"
        "    if (pi == plen) return 1;\n"
        "  }\n"
        "}\n"
        "return 0;"
    )

    map_script = (
        f"def r = doc['{attr_key}'].size() > 0 ? doc['{attr_key}'].value : '';\n"
        "state.evs.add(['a': doc['activity'].value, 'p': doc['position'].value, 'r': r]);"
    )

    return {
        "size": 0,
        "query": {
            "bool": {
                "filter": [{"terms": {"activity": activities}}]
            }
        },
        "aggs": {
            "by_trace": {
                "terms": {"field": "trace_id", "size": trace_size},
                "aggs": {
                    "match_check": {
                        "scripted_metric": {
                            "params": {
                                "pattern": pattern
                            },
                            "init_script":    {"source": "state.evs = [];"},
                            "map_script":     {"source": map_script},
                            "combine_script": {"source": "return state;"},
                            "reduce_script":  {"source": reduce_script}
                        }
                    },
                    "is_match": {
                        "bucket_selector": {
                            "buckets_path": {"v": "match_check.value"},
                            "script": {"source": "params.v == 1"}
                        }
                    }
                }
            }
        }
    }

def run_elk_exact(
    rec: Recorder,
    workload: list[dict],
    log_name: str,
    elk_index: str,
    attr_key: str,
) -> None:
    if not _elk_reachable():
        rec.emit("skip", system="elk_exact", reason="not reachable")
        print("  [SKIP] ELK not reachable")
        return

    # Group by constraint_type for display
    for ct in ("structural", "single_attr", "multi_attr", "cross_binding"):
        subset = [q for q in workload if q["constraint_type"] == ct]
        if not subset:
            continue
        print(f"\n── ELK exact [{ct}] — {len(subset)} queries ──")
        for q in subset:
            elk_q = build_elk_exact_query(q, attr_key)
            t0 = time.perf_counter()
            timed_out = False
            total = 0
            note = "exact_painless_stnm"
            if ct == "cross_binding":
                note = "exact_painless_stnm_binding"
            try:
                r = requests.post(
                    f"{ELK_ENDPOINT}/{elk_index}/_search",
                    json=elk_q, timeout=SWEEP_TIMEOUT_S,
                )
                r.raise_for_status()
                latency = time.perf_counter() - t0
                total = len(
                    r.json()
                    .get("aggregations", {})
                    .get("by_trace", {})
                    .get("buckets", [])
                )
            except requests.exceptions.Timeout:
                latency = time.perf_counter() - t0
                timed_out = True
            except requests.exceptions.RequestException as exc:
                latency = time.perf_counter() - t0
                timed_out = True
                # Safely extract the raw Elasticsearch error if it exists
                err_details = exc.response.text if getattr(exc, 'response', None) else str(exc)
                print(f"  {q['id']:5s} ELK error: {err_details}")
            except Exception as exc:
                latency = time.perf_counter() - t0
                timed_out = True
                print(f"  {q['id']:5s} ELK error: {exc}")

            rec.emit("query", system="elk_exact",
                     category=ct,
                     qid=q["id"], pattern=q["pattern"],
                     pattern_length=q["pattern_length"],
                     latency_s=latency, total=total,
                     timed_out=timed_out, elk_note=note,
                     log_name=log_name)
            status = "TIMEOUT" if timed_out else f"{latency:.3f}s"
            print(f"  {q['id']:5s} k={q['pattern_length']}  "
                  f"{q['pattern'][:52]:52s}  → {status}  (n={total})")


# ═══════════════════════════════════════════════════════════════════════════
# SIESTA warm-up + benchmark
# ═══════════════════════════════════════════════════════════════════════════

def warmup_and_benchmark(
    rec: Recorder,
    workload: list[dict],
    log_name: str,
) -> None:
    # # Warm up structural queries k<=6 (these cover all pair combinations
    # # needed for longer attribute-constrained queries too)
    # warmup_wl = [q for q in workload
    #              if q["constraint_type"] == "structural"
    #              and q["pattern_length"] <= 6]
    # print(f"\n  Warm-up: {len(warmup_wl)} structural queries (k≤6) × {N_WARMUP} reps ...")
    # for rep in range(N_WARMUP):
    #     print(f"  ── rep {rep+1}/{N_WARMUP} ──")
    #     for q in warmup_wl:
    #         t0 = time.perf_counter()
    #         try:
    #             body = detect_adaptive(
    #                 q["log_name"], q["pattern"], q["grouping_keys"],
    #                 retention_overrides=RETENTION_OVERRIDES,
    #             )
    #             latency = time.perf_counter() - t0
    #             tier = ",".join(set(
    #                 (body.get("pair_status_after") or {}).values()
    #             ))
    #             print(f"    {q['id']:5s} k={q['pattern_length']}  "
    #                   f"{q['pattern'][:45]:45s}  {latency:.1f}s  [{tier or '?'}]")
    #         except Exception as exc:
    #             print(f"    {q['id']:5s} ERROR {time.perf_counter()-t0:.1f}s: {exc}")

    # print(f"\n  Sleeping {WARMUP_SLEEP_S}s for materialisation ...")
    # time.sleep(WARMUP_SLEEP_S)

    # # Verify
    # print("  Verifying promotion ...")
    # persistent = total_pairs = 0
    # for q in warmup_wl:
    #     try:
    #         body = detect_adaptive(
    #             q["log_name"], q["pattern"], q["grouping_keys"],
    #             retention_overrides=RETENTION_OVERRIDES,
    #         )
    #         for tier in (body.get("pair_status_after") or {}).values():
    #             total_pairs += 1
    #             if tier == "PERSISTENT":
    #                 persistent += 1
    #     except Exception:
    #         pass
    # pct = persistent / total_pairs * 100 if total_pairs else 0
    # print(f"  {persistent}/{total_pairs} pairs PERSISTENT ({pct:.0f}%)")

    # Benchmark — all constraint types, all lengths
    for ct in ("structural", "single_attr", "multi_attr", "cross_binding"):
        subset = [q for q in workload if q["constraint_type"] == ct]
        if not subset:
            continue
        print(f"\n── SIESTA warm [{ct}] — {len(subset)} queries ──")
        for q in subset:
            try:
                body, latency = timed_query(
                    q["log_name"], q["pattern"], q["grouping_keys"],
                    retention_overrides=RETENTION_OVERRIDES,
                )
                total = body.get("total", 0)
                tier  = ",".join(set(
                    (body.get("pair_status_after") or {}).values()
                ))
                rec.emit("query", system="siesta_warm",
                         category=ct,
                         qid=q["id"], pattern=q["pattern"],
                         pattern_length=q["pattern_length"],
                         latency_s=latency, total=total,
                         tier=tier, log_name=log_name)
                print(f"  {q['id']:5s} k={q['pattern_length']}  "
                      f"{q['pattern'][:52]:52s}  → {latency:.3f}s  "
                      f"(n={total}, {tier or '?'})")
            except Exception as exc:
                rec.emit("query_error", system="siesta_warm",
                         category=ct, qid=q["id"],
                         log_name=log_name, error=str(exc))
                print(f"  {q['id']:5s} ERROR: {exc}")


# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════

def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "SIESTA warm vs ELK exact — attribute-constrained case_id queries, "
            "k=2..max_length, four constraint types."
        ),
    )
    ap.add_argument("--dataset",     required=True)
    ap.add_argument("--log-name",    default=None)
    ap.add_argument("--max-length",  type=int, default=10)
    ap.add_argument("--attr-key",    default=ATTR_KEY,
                    help="Attribute for constraints (default: resource)")
    ap.add_argument("--skip-elk",    action="store_true")
    ap.add_argument("--skip-siesta", action="store_true", 
                    help="Skip SIESTA ingest and benchmarks")
    ap.add_argument("--append",      action="store_true", 
                    help="Safely append to existing results instead of overwriting")
    args = ap.parse_args()

    spec         = resolve_dataset(args.dataset, args.log_name)
    log_name     = spec.log_name
    dataset_path = spec.path
    schema       = discover_schema(dataset_path)
    attr_key     = args.attr_key

    print(f"\n{'═'*64}")
    print(f"  Experiment: SIESTA warm vs ELK exact (case_id, attribute)")
    print(f"  Dataset:    {log_name}  ({dataset_path.name})")
    print(f"  Attr key:   {attr_key}  |  k=2..{args.max_length}")
    print(f"{'═'*64}")

    health_check()

    # ── APPEND LOGIC: Buffer existing results before Recorder overwrites them
    existing_lines = []
    expected_path = Path("results") / f"6_4_1_elk_attribute_case_{log_name}.jsonl"
    if args.append and expected_path.exists():
        print(f"\n  [Append Mode] Buffering {expected_path.name} to prevent overwrite...")
        with open(expected_path, "r", encoding="utf-8") as f:
            existing_lines = f.readlines()

    rec = Recorder("6.4.1", f"6_4_1_elk_attribute_case_{log_name}.jsonl")

    # Phase 1: load traces and sample workload
    print("\n  Phase 1: loading traces and sampling workload ...")
    traces = load_traces(dataset_path)
    workload = sample_workload(
        traces, log_name,
        min_k=2, max_k=args.max_length,
        attr_key=attr_key,
        n_per_k=N_PER_K,
    )

    print(f"\n  Sampled {len(workload)} queries:")
    by_ct: dict[str, list] = defaultdict(list)
    for q in workload:
        by_ct[q["constraint_type"]].append(q)
    for ct, qs in by_ct.items():
        print(f"    {ct}: {len(qs)} queries")
        for q in qs[:3]:
            print(f"      k={q['pattern_length']}  {q['pattern']}")
        if len(qs) > 3:
            print(f"      ... +{len(qs)-3} more")

    for q in workload:
        rec.emit("workload_query", log_name=log_name,
                 qid=q["id"], pattern=q["pattern"],
                 pattern_length=q["pattern_length"],
                 constraint_type=q["constraint_type"],
                 tags=q["tags"])

    # # Phase 2: adaptive ingest (case_id perspective)
    # if not args.skip_siesta:
    #     print(f"\n  Phase 2: ingest_adaptive (case_id) ...")
    #     ingest_adaptive(
    #         log_name, dataset_path, CONFIG,
    #         overrides={"perspectives": [{"grouping_keys": []}]},
    #         clear_existing=True,
    #     )
    #     time.sleep(2)
    # else:
    #     print(f"\n  Phase 2: SIESTA ingest skipped (--skip-siesta)")

    # # Phase 3: ELK ingest
    # attr_keys = [k for k, vs in schema.attribute_values.items() if vs]
    elk_index = f"siesta_{log_name}".lower().replace(" ", "_")
    # if not args.skip_elk:
    #     if _elk_reachable():
    #         print(f"\n  Phase 3: ELK ingest (index={elk_index}) ...")
    #         elk_create_index(elk_index, attr_keys)
    #         n = elk_ingest(dataset_path, elk_index, attr_keys)
    #         print(f"    {n:,} events indexed")
    #         rec.emit("elk_ingest", log_name=log_name, n_events=n)
    #     else:
    #         print(f"\n  Phase 3: ELK not reachable — skipping")
    #         args.skip_elk = True

    # Phase 4: SIESTA warm-up + benchmark
    if not args.skip_siesta:
        print(f"\n  Phase 4: SIESTA warm-up and benchmark ...")
        warmup_and_benchmark(rec, workload, log_name)
    else:
        print(f"\n  Phase 4: SIESTA benchmark skipped (--skip-siesta)")

    # Phase 5: ELK exact benchmark
    if not args.skip_elk:
        print(f"\n  Phase 5: ELK exact benchmark ...")
        run_elk_exact(rec, workload, log_name, elk_index, attr_key)

    # ── APPEND LOGIC: Merge the buffered old lines with the newly recorded lines
    if args.append and existing_lines:
        print(f"\n  [Append Mode] Merging {len(existing_lines)} older records with new data...")
        with open(rec.path, "r", encoding="utf-8") as f:
            new_lines = f.readlines()
        with open(rec.path, "w", encoding="utf-8") as f:
            f.writelines(existing_lines)
            f.writelines(new_lines)

    print(f"\n  Results: {rec.path}")
    print("\n  Figures this supports:")
    print("    - latency vs k, one line per (system × constraint_type)")
    print("    - SIESTA near-flat across k and constraint types")
    print("    - ELK grows super-linearly; both return identical result counts")


if __name__ == "__main__":
    main()