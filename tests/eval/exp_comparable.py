"""
tests/eval/exp_comparable.py

Experiment 6.4.1 — Comparable head-to-head evaluation.

Systems
-------
adaptive_cold     Our system on first touch — SequenceTable scan, no index
                  benefit.  This is the baseline every competitor would reach
                  if they had to evaluate the same query without a perspective-
                  aware materialised index.  Equivalent to what SIESTA (eager)
                  does for non-case perspectives (it also falls back to the
                  SequenceTable there).
adaptive_warm     Our system after promotion to PERSISTENT — index read,
                  steady-state latency.
elk               Elasticsearch — term-oriented inverted index, no structural
                  evaluation.
match_recognize   MATCH_RECOGNIZE via Flink SQL Gateway — full partition scan
                  with NFA evaluation.

NOTE: SIESTA (eager) is deliberately NOT included here.  It was the comparison
target in Experiment 6.3.2 (maintenance cost).  For query execution it adds
nothing beyond adaptive_cold: for case_id queries it is as fast as adaptive_warm
(both have materialised pairs), and for non-case perspective queries it IS
adaptive_cold (both must scan the SequenceTable because the eager index is
case-centric only).  Conflating the two would also cause a MinIO storage
collision, since both write to the same path for the same log_name.

Query categories
----------------
structural_case
    Patterns with no attribute constraints under case_id grouping.
    All four systems participate.  adaptive_cold shows the SequenceTable
    baseline; adaptive_warm shows the indexed speed.

attribute_case
    Patterns with inline attribute constraints, case_id grouping.
    ELK: field-level term filters (no structural eval).
    MR:  MATCH_RECOGNIZE with WHERE-clause predicates (full scan).
    adaptive_warm: in-index attribute evaluation (no raw-log join).

length_sweep
    Fixed-ordering pattern A₀ … Aₖ₋₁ at k = 2..MAX_LENGTH, case grouping.
    Produces the diverging fan: adaptive_warm near-flat, MR super-linear,
    ELK timeouts above k ≈ 4.

Output
------
results/6_4_1_comparable.jsonl
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tests.eval.eval_common import (
    API_BASE, API_TIMEOUT_S, CONFIG_DIR,
    Recorder, health_check,
    ingest_adaptive,
    timed_query,
    discover_schema, resolve_dataset,
    quote_label,
)
from tests.eval.workload import build_workloads, WorkloadContext, fetch_pair_coverage, _pat2

CONFIG = CONFIG_DIR / "adaptive_index.config.json"

ELK_ENDPOINT = os.environ.get("ELK_ENDPOINT", "http://localhost:9200")
ELK_INDEX    = os.environ.get("ELK_INDEX", "siesta_events")
MR_ENDPOINT  = os.environ.get("MR_ENDPOINT", "http://localhost:8083")
MR_TABLE     = os.environ.get("MR_TABLE", "event_log")

# Queries that exceed this limit are recorded with timed_out=True.
# Long enough to catch genuine timeouts, short enough to not stall the run.
SWEEP_TIMEOUT_S = int(os.environ.get("SWEEP_TIMEOUT_S", "600"))

_RAW_SCALE = os.environ.get("SCALABILITY_DATASETS", "")
SCALABILITY_DATASETS: list[tuple[Path, str]] = []
for _entry in _RAW_SCALE.split(";"):
    _entry = _entry.strip()
    if ":" in _entry:
        _p, _l = _entry.rsplit(":", 1)
        SCALABILITY_DATASETS.append((Path(_p), _l))


# ---------------------------------------------------------------------------
# Multi-dataset support
# ---------------------------------------------------------------------------

from dataclasses import dataclass


@dataclass
class DatasetSpec:
    path:     Path
    log_name: str


def parse_datasets(raw: list[str]) -> list[DatasetSpec]:
    """
    Parse a list of "path:log_name" strings into DatasetSpec objects.
    Also accepts plain paths (log_name derived from stem).

    Examples
    --------
    --datasets datasets/bpic_2017.xes:bpic_2017 datasets/sepsis.xes:sepsis
    --datasets datasets/bpic_2017.xes          # log_name = bpic_2017
    """
    specs: list[DatasetSpec] = []
    for entry in raw:
        entry = entry.strip()
        if not entry:
            continue
        if ":" in entry:
            # rsplit so Windows drive letters (C:\...) don't break
            path_str, log_name = entry.rsplit(":", 1)
        else:
            path_str = entry
            log_name = Path(entry).stem
        specs.append(DatasetSpec(path=Path(path_str), log_name=log_name))
    return specs


def elk_index_for(log_name: str) -> str:
    """Derive a per-dataset ELK index name from a log name."""
    safe = re.sub(r"[^a-z0-9_]", "_", log_name.lower())
    return f"siesta_{safe}"


# ---------------------------------------------------------------------------
# Workload builders
# ---------------------------------------------------------------------------

def build_comparable_workloads(
    ctx: WorkloadContext,
    *,
    n_dense: int = 4,
    n_sparse: int = 3,
    n_attr: int = 4,
    rng_seed: int = 42,
) -> dict[str, list[dict]]:
    """
    Build structural_case and attribute_case categories using pair_coverage
    so every query is guaranteed to return non-zero results.

    DENSE pairs (covering >=50% of cases) stress all systems hardest.
    SPARSE pairs (5–20% of cases) fill out the workload with moderate load.
    attribute_case re-uses the top DENSE pairs with an attribute constraint,
    which is very likely non-null because the structural pair already is.
    """
    import itertools
    import random

    case_gk: list[str] = []
    rng = random.Random(rng_seed)

    cov = fetch_pair_coverage(ctx.log_name, case_gk)
    group_count = cov["group_count"]
    pairs_sorted = sorted(cov["pairs"], key=lambda p: p["groups"], reverse=True)

    dense_cut = 0.5 * group_count
    sparse_lo = max(1, int(0.05 * group_count))
    sparse_hi = max(sparse_lo, int(0.20 * group_count))

    dense  = [p for p in pairs_sorted if p["groups"] >= dense_cut]
    sparse = [p for p in pairs_sorted if sparse_lo <= p["groups"] <= sparse_hi]

    print(f"  [workload] structural_case: group_count={group_count}"
          f"  DENSE={len(dense)}  SPARSE={len(sparse)}")

    def take(bucket: list, k: int) -> list:
        return bucket[:k] if len(bucket) <= k else rng.sample(bucket, k)

    chosen_dense  = take(dense, n_dense)
    chosen_sparse = take(sparse, n_sparse)

    # ── structural_case ────────────────────────────────────────────────
    structural: list[dict] = []
    counter = itertools.count(1)
    for p in chosen_dense + chosen_sparse:
        tag = "DENSE" if p["groups"] >= dense_cut else "SPARSE"
        structural.append({
            "id":             f"SC{next(counter)}",
            "log_name":       ctx.log_name,
            "pattern":        _pat2(p["source"], p["target"]),
            "grouping_keys":  case_gk,
            "pattern_length": 2,
            "category":       "structural_case",
            "group_coverage": p["groups"],
            "tags":           [tag],
        })

    # ── attribute_case ─────────────────────────────────────────────────
    # Include both original XES colon-form and sanitized CSV form names
    attr_candidates = [
        "org:resource", "org_resource",
        "lifecycle:transition", "lifecycle_transition",
        "org:group", "org_group",
        "org:role", "org_role",
        "resource", "role", "cost",
    ]
    _ts_fields = {"start_timestamp", "time:timestamp", "timestamp", "position"}
    avail_attrs = [a for a in attr_candidates if ctx.attribute_values.get(a)]
    if not avail_attrs:
        avail_attrs = [k for k, vs in ctx.attribute_values.items()
                       if vs and k not in _ts_fields][:3]

    attribute: list[dict] = []
    counter_a = itertools.count(1)
    for i, p in enumerate(chosen_dense[:n_attr]):
        if not avail_attrs:
            break
        attr = avail_attrs[i % len(avail_attrs)]
        val = ctx.attribute_values[attr][0].replace('"', '\\"')
        attribute.append({
            "id":             f"AC{next(counter_a)}",
            "log_name":       ctx.log_name,
            "pattern":        f'{quote_label(p["source"])}[{attr}="{val}"] {quote_label(p["target"])}',
            "grouping_keys":  case_gk,
            "pattern_length": 2,
            "category":       "attribute_case",
            "group_coverage": p["groups"],
            "tags":           ["single_eq"],
        })

    return {"structural_case": structural, "attribute_case": attribute}


def build_length_sweep(
    ctx: WorkloadContext,
    *,
    min_length: int = 2,
    max_length: int = 6,
) -> list[dict]:
    """
    Generate patterns at lengths min_length → max_length anchored on the
    densest pair in the dataset (guaranteed results at k=2).

    Seed pair:  (A, B) — the pair with the highest case coverage.
    Extensions: activities ranked by their total pair-coverage participation
                (sum of groups across all pairs they appear in), so we
                extend with the most "connected" activities first.

    For each k:
      - structural:  A B ext₀ ext₁ …
      - attribute:   A[attr="v"] B ext₀ ext₁ …  (if an attribute exists)

    k>2 results are not guaranteed non-null but are far more likely than
    picking arbitrary activity permutations, and the latency curve is the
    point of this sweep regardless.
    """
    from collections import Counter
    import itertools

    case_gk: list[str] = []

    try:
        cov = fetch_pair_coverage(ctx.log_name, case_gk)
        pairs_sorted = sorted(cov["pairs"], key=lambda p: p["groups"], reverse=True)
        seed_src = pairs_sorted[0]["source"]
        seed_tgt = pairs_sorted[0]["target"]
        seed_cov = pairs_sorted[0]["groups"]

        # Rank remaining activities by total pair-participation weight
        act_weight: Counter = Counter()
        for p in cov["pairs"]:
            act_weight[p["source"]] += p["groups"]
            act_weight[p["target"]] += p["groups"]
        seed_set   = {seed_src, seed_tgt}
        extensions = [a for a, _ in act_weight.most_common() if a not in seed_set]
        print(f"  [workload] length_sweep seed: {seed_src}→{seed_tgt}"
              f" ({seed_cov} cases)  extensions: {extensions[:4]}")
    except Exception as exc:
        print(f"  [workload] length_sweep fallback ({exc})")
        seed_src, seed_tgt = ctx.activities[0], ctx.activities[1]
        extensions = ctx.activities[2:]

    attr_candidates = [
        "org:resource", "org_resource",
        "lifecycle:transition", "lifecycle_transition",
        "org:group", "org_group",
        "org:role", "org_role",
        "resource", "role", "cost",
    ]
    _ts_fields = {"start_timestamp", "time:timestamp", "timestamp", "position"}
    attr = next((a for a in attr_candidates if ctx.attribute_values.get(a)), None)
    if attr is None:
        attr = next((k for k, vs in ctx.attribute_values.items()
                     if vs and k not in _ts_fields), None)

    queries: list[dict] = []
    counter = itertools.count(1)
    eff_max = min(max_length, 2 + len(extensions))

    for k in range(min_length, eff_max + 1):
        acts_k = [seed_src, seed_tgt] + extensions[:k - 2]
        pat_structural = " ".join(quote_label(a) for a in acts_k)

        queries.append({
            "id":             f"LS{next(counter)}",
            "log_name":       ctx.log_name,
            "pattern":        pat_structural,
            "grouping_keys":  case_gk,
            "pattern_length": k,
            "category":       "length_sweep",
            "tags":           [f"len={k}", "structural"],
        })

        if attr and ctx.attribute_values.get(attr):
            v = ctx.attribute_values[attr][0].replace('"', '\\"')
            head = f'{quote_label(acts_k[0])}[{attr}="{v}"]'
            tail = " ".join(quote_label(a) for a in acts_k[1:])
            queries.append({
                "id":             f"LS{next(counter)}",
                "log_name":       ctx.log_name,
                "pattern":        f"{head} {tail}",
                "grouping_keys":  case_gk,
                "pattern_length": k,
                "category":       "length_sweep",
                "tags":           [f"len={k}", "attribute"],
            })

    return queries


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _log_size(path: Path) -> int:
    if path.suffix.lower() == ".csv":
        return sum(1 for _ in path.open()) - 1
    count = 0
    with path.open("rb") as f:
        for line in f:
            if b"<event" in line:
                count += 1
    return count


def _elk_reachable() -> bool:
    try:
        return requests.get(ELK_ENDPOINT, timeout=3).status_code == 200
    except Exception:
        return False


def _mr_reachable() -> bool:
    try:
        return requests.get(f"{MR_ENDPOINT}/v1/info", timeout=3).status_code == 200
    except Exception:
        return False


def _is_num(s: str) -> bool:
    try:
        float(s)
        return True
    except (TypeError, ValueError):
        return False


def _extract_activities(pattern: str) -> list[str]:
    clean = re.sub(r"\[[^\]]*\]", "", pattern)
    tokens = []
    for m in re.finditer(r'"(?:[^"\\]|\\.)*"|[A-Za-z_][A-Za-z0-9_:\\]*', clean):
        tok = m.group(0)
        tokens.append(tok[1:-1].replace('\\"', '"') if tok.startswith('"') else tok)
    return tokens


def _extract_attr_constraints(pattern: str) -> list[dict]:
    constraints = []
    for m in re.finditer(
        r'(?:"(?:[^"\\]|\\.)*"|[A-Za-z_][A-Za-z0-9_:\\]*)\[([^\]]+)\]',
        pattern,
    ):
        act_m = re.match(r'"(?:[^"\\]|\\.)*"|[A-Za-z_][A-Za-z0-9_:\\]*', m.group(0))
        act = act_m.group(0).strip('"') if act_m else "?"
        for part in m.group(1).split(","):
            cm = re.match(r'(\w[\w:]*)\s*(!=|<=|>=|<|>|=)\s*(.+)', part.strip())
            if cm:
                constraints.append({
                    "activity": act,
                    "attr":     cm.group(1),
                    "op":       cm.group(2),
                    "value":    cm.group(3).strip().strip('"'),
                })
    return constraints


def _sanitize_col(name: str) -> str:
    """Map an attribute name to a valid Flink SQL column name (mirrors mr_setup.py)."""
    return re.sub(r"[^A-Za-z0-9_]", "_", name)


def _build_mr_sql(
    pattern: str,
    table: str = "event_log",
    partition_col: str = "trace_id",
    order_col: str = "start_timestamp",   # matches the DDL in mr_setup.py
) -> str:
    activities  = _extract_activities(pattern)
    constraints = _extract_attr_constraints(pattern)
    if len(activities) < 2:
        return ""

    var_names = [f"V{i}" for i in range(len(activities))]

    var_constraints: dict[int, list[dict]] = {}
    for c in constraints:
        for idx, act in enumerate(activities):
            if c["activity"] == act:
                var_constraints.setdefault(idx, []).append(c)
                break

    defines: list[str] = []
    for idx, (var, act) in enumerate(zip(var_names, activities)):
        conds = [f"{var}.`activity` = '{act}'"]
        for c in var_constraints.get(idx, []):
            col = _sanitize_col(c["attr"])  # org:resource → org_resource
            if c["value"].startswith("$"):
                ref_idx = int(c["value"][1:]) - 1
                if 0 <= ref_idx < len(var_names):
                    conds.append(
                        f'{var}.`{col}` {c["op"]} {var_names[ref_idx]}.`{col}`'
                    )
            else:
                conds.append(f"{var}.`{col}` {c['op']} '{c['value']}'")
        defines.append(f'{var} AS ({" AND ".join(conds)})')

    return (
        f"SELECT * FROM `{table}` "
        f"MATCH_RECOGNIZE ("
        f"  PARTITION BY `{partition_col}` ORDER BY `{order_col}` "
        f"  MEASURES `{var_names[0]}`.`{order_col}` AS t0 "
        f"  ONE ROW PER MATCH AFTER MATCH SKIP TO NEXT ROW "
        f"  PATTERN ({' '.join(var_names)}) "
        f"  DEFINE {', '.join(defines)}"
        f") AS T"
    )


# ---------------------------------------------------------------------------
# SQL Gateway session management
# ---------------------------------------------------------------------------

# Module-level session handle — set once at startup, reused for all queries.
_MR_SESSION: str | None = os.environ.get("MR_SESSION_HANDLE")

MR_SESSION_FILE = os.environ.get("MR_SESSION_FILE", ".mr_session")


def _get_mr_session() -> str:
    """
    Return the SQL Gateway session handle, creating one if needed.

    Priority:
      1. MR_SESSION_HANDLE environment variable (set by mr_setup.py output)
      2. .mr_session file written by mr_setup.py
      3. Create a fresh session on the fly (table must already exist)
    """
    global _MR_SESSION
    if _MR_SESSION:
        return _MR_SESSION

    session_file = Path(MR_SESSION_FILE)
    if session_file.exists():
        _MR_SESSION = session_file.read_text().strip()
        print(f"  [MR] Using session from {session_file}: {_MR_SESSION[:8]}...")
        return _MR_SESSION

    print("  [MR] No session handle found — creating a new session.")
    print("       (Run mr_setup.py first for reliable table registration.)")
    r = requests.post(
        f"{MR_ENDPOINT}/v1/sessions",
        json={"properties": {"execution.runtime-mode": "BATCH"}},
        timeout=10,
    )
    r.raise_for_status()
    _MR_SESSION = r.json()["sessionHandle"]
    print(f"  [MR] New session: {_MR_SESSION[:8]}...")
    return _MR_SESSION


def _mr_submit_and_wait(sql: str, timeout_s: int) -> tuple[float, int, bool]:
    """Returns (latency_s, total_rows, timed_out)."""
    session = _get_mr_session()
    t0 = time.perf_counter()
    try:
        r = requests.post(
            f"{MR_ENDPOINT}/v1/sessions/{session}/statements",
            json={"statement": sql}, timeout=API_TIMEOUT_S,
        )
        r.raise_for_status()
        op = r.json().get("operationHandle", "")

        while True:
            elapsed = time.perf_counter() - t0
            if elapsed >= timeout_s:
                return elapsed, 0, True
            sr = requests.get(
                f"{MR_ENDPOINT}/v1/sessions/{session}/operations/{op}/status",
                timeout=30,
            )
            sr.raise_for_status()
            if sr.json().get("status") in ("FINISHED", "ERROR", "CANCELED"):
                break
            time.sleep(0.5)

        latency = time.perf_counter() - t0
        rr = requests.get(
            f"{MR_ENDPOINT}/v1/sessions/{session}/operations/{op}/result/0",
            timeout=30,
        )
        total = len(rr.json().get("results", {}).get("data", []))
        return latency, total, False
    except Exception:
        return time.perf_counter() - t0, 0, True


# ---------------------------------------------------------------------------
# Per-system runners
# ---------------------------------------------------------------------------

def _emit(
    rec: Recorder,
    system: str,
    category: str,
    q: dict,
    latency: float,
    total: int,
    log_size: int,
    *,
    timed_out: bool = False,
    note: str = "",
) -> None:
    rec.emit("query",
             system=system, category=category,
             qid=q["id"], pattern=q["pattern"],
             pattern_length=q.get("pattern_length"),
             has_constraints="[" in q["pattern"],
             tags=q.get("tags", []),
             latency_s=latency, timed_out=timed_out,
             total=total, log_name=q["log_name"],
             log_size=log_size, note=note)


def run_adaptive_warm(
    rec: Recorder,
    category: str,
    workload: list[dict],
    log_name: str,
    *,
    log_size: int = 0,
) -> None:
    print(f"\n── Adaptive (warm) — {category} ──")
    for q in workload:
        try:
            timed_query(q["log_name"], q["pattern"], q["grouping_keys"])
        except Exception:
            pass
    time.sleep(2)

    for q in workload:
        try:
            body, latency = timed_query(
                q["log_name"], q["pattern"], q["grouping_keys"],
            )
            _emit(rec, "adaptive_warm", category, q, latency,
                  body.get("total", 0), log_size)
            print(f"  {q['id']:6s} k={q.get('pattern_length','?')}  "
                  f"{q['pattern'][:48]:48s}  -> {latency:.3f}s")
        except Exception as exc:
            rec.emit("query_error", system="adaptive_warm", category=category,
                     qid=q["id"], log_name=log_name, error=str(exc))
            print(f"  {q['id']:6s} ERROR: {exc}")


def run_adaptive_cold(
    rec: Recorder,
    category: str,
    workload: list[dict],
    log_name: str,
    dataset_path: Path,
    *,
    log_size: int = 0,
    timeout_s: int = API_TIMEOUT_S,
) -> None:
    """
    Measure first-touch (cold) adaptive latency — i.e. the SequenceTable scan
    that occurs before any pair has been promoted.

    This is the correct lower-bound baseline: it represents what any system
    that lacks a perspective-aware materialised index would do, including
    SIESTA (eager) on non-case-id perspective queries.

    We wipe and re-ingest before measuring so no pairs are cached or persistent.
    """
    print(f"\n── Adaptive COLD (SequenceTable baseline) — {category} ──")

    # Fresh ingest — clears any existing pairs/LRU state
    ingest_adaptive(log_name, dataset_path, CONFIG,
                    overrides={"overwrite_data": True})
    time.sleep(1)

    for q in workload:
        t0 = time.perf_counter()
        timed_out = False
        try:
            body, latency = timed_query(
                q["log_name"], q["pattern"], q["grouping_keys"],
            )
            total = body.get("total", 0)
        except Exception as exc:
            latency = time.perf_counter() - t0
            total = 0
            timed_out = True
            rec.emit("query_error", system="adaptive_cold", category=category,
                     qid=q["id"], log_name=log_name, error=str(exc))

        _emit(rec, "adaptive_cold", category, q, latency, total, log_size,
              timed_out=timed_out,
              note="sequence_table_scan")
        status = "TIMEOUT" if timed_out else f"{latency:.3f}s"
        print(f"  {q['id']:6s} k={q.get('pattern_length','?')}  "
              f"{q['pattern'][:48]:48s}  -> {status}")


def run_elk(
    rec: Recorder,
    category: str,
    workload: list[dict],
    log_name: str,
    *,
    log_size: int = 0,
    timeout_s: int = API_TIMEOUT_S,
    elk_index: str = ELK_INDEX,     # per-dataset index name
) -> None:
    if not _elk_reachable():
        rec.emit("skip", system="elk",
                 reason=f"ELK not reachable at {ELK_ENDPOINT}")
        print(f"  [SKIP] ELK not reachable.")
        return

    print(f"\n── ELK — {category} ──")
    for q in workload:
        activities  = _extract_activities(q["pattern"])
        constraints = _extract_attr_constraints(q["pattern"])

        must_clauses: list[dict] = [{
            "bool": {"should": [{"term": {"activity": a}} for a in activities[:6]]}
        }]
        for c in constraints:
            if c["value"].startswith("$"):
                continue  # cross-event binding: ELK cannot evaluate
            if c["op"] == "=":
                must_clauses.append({"term": {c["attr"]: c["value"]}})
            elif c["op"] == "!=":
                must_clauses.append(
                    {"bool": {"must_not": [{"term": {c["attr"]: c["value"]}}]}}
                )

        elk_query = {"query": {"bool": {"must": must_clauses}}, "size": 0}

        t0 = time.perf_counter()
        timed_out = False
        try:
            r = requests.post(
                f"{ELK_ENDPOINT}/{elk_index}/_search",
                json=elk_query, timeout=timeout_s,
            )
            r.raise_for_status()
            latency = time.perf_counter() - t0
            total = r.json().get("hits", {}).get("total", {}).get("value", 0)
        except requests.exceptions.Timeout:
            latency = time.perf_counter() - t0
            total = 0
            timed_out = True
        except Exception as exc:
            latency = time.perf_counter() - t0
            total = 0
            timed_out = True
            rec.emit("query_error", system="elk", category=category,
                     qid=q["id"], log_name=log_name, error=str(exc))

        _emit(rec, "elk", category, q, latency, total, log_size,
              timed_out=timed_out, note="no_structural_eval")
        status = "TIMEOUT" if timed_out else f"{latency:.3f}s"
        print(f"  {q['id']:6s} k={q.get('pattern_length','?')}  "
              f"{q['pattern'][:48]:48s}  -> {status}")


def run_match_recognize(
    rec: Recorder,
    category: str,
    workload: list[dict],
    log_name: str,
    *,
    log_size: int = 0,
    timeout_s: int = API_TIMEOUT_S,
) -> None:
    if not _mr_reachable():
        rec.emit("skip", system="match_recognize",
                 reason=f"Flink gateway not reachable at {MR_ENDPOINT}")
        print(f"  [SKIP] Flink SQL gateway not reachable.")
        return

    print(f"\n── MATCH_RECOGNIZE — {category} ──")
    for q in workload:
        sql = _build_mr_sql(q["pattern"], table=MR_TABLE)
        if not sql:
            continue

        latency, total, timed_out = _mr_submit_and_wait(sql, timeout_s)
        _emit(rec, "match_recognize", category, q, latency, total, log_size,
              timed_out=timed_out)
        status = "TIMEOUT" if timed_out else f"{latency:.3f}s"
        print(f"  {q['id']:6s} k={q.get('pattern_length','?')}  "
              f"{q['pattern'][:48]:48s}  -> {status}")


# ---------------------------------------------------------------------------
# Per-dataset preparation helpers
# ---------------------------------------------------------------------------

def _elk_prepare_dataset(path: Path, log_name: str) -> str:
    """
    Bulk-load an event log into a per-dataset ELK index.
    Returns the index name used.
    Index is always recreated fresh (overwrite semantics).
    """
    from tests.eval.elk_ingest import (
        _create_index as elk_create_index,
        _bulk_load    as elk_bulk_load,
        _finalize_index as elk_finalize,
        _iter_log     as elk_iter_log,
        INDEX_MAPPING,
    )
    import tests.eval.elk_ingest as _ei

    index = elk_index_for(log_name)
    _ei.ELK_INDEX = index          # point the module at the right index

    if not _elk_reachable():
        print(f"  [SKIP ELK ingest] ELK not reachable.")
        return index

    print(f"\n  [ELK] Ingesting {path.name} → index '{index}' ...")
    elk_create_index(overwrite=True)
    indexed, errors = elk_bulk_load(elk_iter_log(path))
    elk_finalize()
    print(f"  [ELK] {indexed:,} events indexed"
          + (f", {errors} errors" if errors else ""))
    return index


def _mr_prepare_dataset(path: Path, log_name: str) -> bool:
    """
    Ensure the event_log Flink table is registered for this dataset.

    - Writes the converted CSV to ./flink_data/ (writable volume mount).
    - Skips CSV conversion if the file already exists (e.g. mr_setup.py
      already ran for this dataset).
    - Always re-registers the table (DROP + CREATE) so the correct CSV
      and column set are active — necessary when switching datasets.
    """
    if not _mr_reachable():
        print(f"  [SKIP MR setup] SQL Gateway not reachable.")
        return False

    try:
        from tests.eval.mr_setup import convert_to_flink_csv, build_table_ddl
    except ImportError:
        print("  [SKIP MR setup] mr_setup.py not importable.")
        return False

    # Write converted CSVs to the writable flink_data mount
    flink_data_dir = Path("./flink_data")
    flink_data_dir.mkdir(exist_ok=True)
    csv_out = flink_data_dir / (path.stem + "_flink.csv")
    container_path = f"/flink_data/{csv_out.name}"

    # Convert only if not already done
    if csv_out.exists():
        print(f"\n  [MR] CSV already exists: {csv_out} — skipping conversion.")
        # We still need the extra_cols for the DDL — re-derive from CSV header
        import csv as _csv
        with csv_out.open() as f:
            header = next(_csv.reader(f))
        core = {"trace_id", "activity", "start_timestamp", "position"}
        extra_cols = [c for c in header if c not in core]
    else:
        print(f"\n  [MR] Converting {path.name} → {csv_out.name} ...")
        try:
            extra_cols = convert_to_flink_csv(path, csv_out)
        except Exception as exc:
            print(f"  [MR] Conversion failed: {exc}")
            return False

    # Always re-register the table (column set may differ between datasets)
    session = _get_mr_session()
    ddl = build_table_ddl(MR_TABLE, container_path, extra_cols)
    print(f"  [MR] Re-registering table '{MR_TABLE}' for {log_name} ...")

    for sql, label in [
        (f"DROP TABLE IF EXISTS `{MR_TABLE}`", "drop old table"),
        (ddl,                                   "create table"),
        ("SET 'execution.runtime-mode' = 'BATCH'", "batch mode"),
    ]:
        try:
            r = requests.post(
                f"{MR_ENDPOINT}/v1/sessions/{session}/statements",
                json={"statement": sql}, timeout=30,
            )
            r.raise_for_status()
            op = r.json()["operationHandle"]
            deadline = time.time() + 60
            while time.time() < deadline:
                sr = requests.get(
                    f"{MR_ENDPOINT}/v1/sessions/{session}/operations/{op}/status",
                    timeout=10,
                )
                state = sr.json().get("status")
                if state == "FINISHED":
                    break
                if state in ("ERROR", "CANCELED"):
                    raise RuntimeError(f"Statement failed: {sql[:80]}")
                time.sleep(0.3)
            print(f"  [MR] OK: {label}")
        except Exception as exc:
            print(f"  [MR] Failed ({label}): {exc}")
            return False

    return True


# ---------------------------------------------------------------------------
# Single-dataset execution
# ---------------------------------------------------------------------------

def run_single_dataset(
    rec: Recorder,
    spec: "DatasetSpec",
    *,
    max_length: int,
    skip_elk: bool,
    skip_mr: bool,
) -> None:
    """Run all categories for one dataset."""
    size   = _log_size(spec.path)
    schema = discover_schema(spec.path)
    ctx    = _make_workload_context(schema, spec.log_name)

    print(f"\n{'═'*64}")
    print(f"  Dataset:   {spec.log_name}  ({spec.path.name})")
    print(f"  Events:    {size:,}")
    print(f"  Activities ({len(ctx.activities)}): {ctx.activities[:8]}"
          + (" ..." if len(ctx.activities) > 8 else ""))
    print(f"  Attr keys: {list(ctx.attribute_values)[:6]}")
    print(f"{'═'*64}")

    rec.emit("dataset_start",
             log_name=spec.log_name, path=str(spec.path),
             log_size=size, activities=ctx.activities,
             attribute_keys=list(ctx.attribute_values),
             max_length=max_length)

    # Pre-ingest so pair_coverage is available for data-driven workload building.
    # run_adaptive_cold will re-ingest (overwrite) for each category anyway.
    print("\n[0/N] Pre-ingesting for pair_coverage ...")
    ingest_adaptive(spec.log_name, spec.path, CONFIG,
                    overrides={"overwrite_data": True})
    time.sleep(2)

    # Build workloads from live pair-coverage data (guaranteed non-null results)
    comp  = build_comparable_workloads(ctx)
    sweep = build_length_sweep(ctx, min_length=2, max_length=max_length)

    all_categories = [
        ("structural_case", comp["structural_case"]),
        ("attribute_case",  comp["attribute_case"]),
        ("length_sweep",    sweep),
    ]

    for cat, wl in all_categories:
        print(f"  {cat}: {len(wl)} queries")

    for cat, wl in all_categories:
        for q in wl:
            rec.emit("workload_query", log_name=spec.log_name,
                     category=cat, **{k: v for k, v in q.items()
                                      if k not in ("log_name", "category")})

    # ── ELK: ingest once per dataset (index persists across categories) ──
    elk_index = elk_index_for(spec.log_name)
    if not skip_elk:
        _elk_prepare_dataset(spec.path, spec.log_name)

    # ── MR: re-register the table for this dataset ─────────────────────
    mr_ready = False
    if not skip_mr:
        mr_ready = _mr_prepare_dataset(spec.path, spec.log_name)

    # ── Run categories ─────────────────────────────────────────────────
    for category, workload in all_categories:
        if not workload:
            continue

        t_out = SWEEP_TIMEOUT_S if category == "length_sweep" else API_TIMEOUT_S
        print(f"\n══ [{spec.log_name}] {category} ({len(workload)} queries) ══")

        # Cold pass: wipes state → fresh SequenceTable scan baseline.
        # This also serves as the single ingest for the warm passes below.
        run_adaptive_cold(rec, category, workload, spec.log_name,
                          spec.path, log_size=size, timeout_s=t_out)

        # Warm-up: run every query twice more to trigger pair promotion.
        # No timing — we just want the pairs to reach PERSISTENT.
        print(f"  [warm-up: 2 passes to trigger promotion]")
        for _ in range(2):
            for q in workload:
                try:
                    timed_query(q["log_name"], q["pattern"],
                                q["grouping_keys"])
                except Exception:
                    pass
        print("  [sleeping 120s for background materialisation]")
        time.sleep(120)

        # Warm pass: PERSISTENT pairs, steady-state latency.
        run_adaptive_warm(rec, category, workload, spec.log_name,
                          log_size=size)

        # Competitors (case-centric only — they can't serve other perspectives)
        if not skip_elk:
            run_elk(rec, category, workload, spec.log_name,
                    log_size=size, timeout_s=t_out, elk_index=elk_index)
        if not skip_mr and mr_ready:
            run_match_recognize(rec, category, workload, spec.log_name,
                                log_size=size, timeout_s=t_out)

    rec.emit("dataset_end", log_name=spec.log_name)


def _make_workload_context(schema, log_name: str):
    """Wrap a schema object as a WorkloadContext for workload builders."""
    # WorkloadContext is a lightweight dataclass — build it from schema fields
    try:
        from tests.eval.workload import WorkloadContext
        return WorkloadContext(
            log_name=log_name,
            activities=schema.activities,
            perspectives=schema.perspective_keys if hasattr(schema, "perspective_keys") else [],
            attribute_values=schema.attribute_values,
        )
    except Exception:
        # Fallback: return schema directly if it already has the right fields
        schema.log_name = log_name
        return schema


# ---------------------------------------------------------------------------
# Scalability sweep (unchanged, kept for backward compat)
# ---------------------------------------------------------------------------

def run_scalability_sweep(
    rec: Recorder,
    default_log_name: str,
    default_dataset: Path,
    *,
    skip_elk: bool,
    skip_mr: bool,
) -> None:
    datasets = SCALABILITY_DATASETS or [(default_dataset, "default")]
    if len(datasets) < 2:
        print("\n[SKIP] Scalability sweep: fewer than 2 datasets configured.")
        rec.emit("skip", system="scalability",
                 reason="fewer than 2 scalability datasets configured")
        return

    print("\n══ Scalability sweep ══")
    first_schema = discover_schema(datasets[0][0])
    acts = first_schema.activities
    if len(acts) < 2:
        print("[SKIP] Too few activities.")
        return

    a, b = acts[0], acts[1]
    str_attr = next(
        (k for k, vs in first_schema.attribute_values.items()
         if vs and not all(_is_num(v) for v in vs)),
        None,
    )
    probes = [
        {"id": "probe_S", "pattern": f"{quote_label(a)} {quote_label(b)}",
         "grouping_keys": [], "pattern_length": 2,
         "category": "structural_case", "tags": ["probe"]},
    ]
    if str_attr:
        v = first_schema.attribute_values[str_attr][0].replace('"', '\\"')
        probes.append({
            "id": "probe_A",
            "pattern": f'{quote_label(a)}[{str_attr}="{v}"] {quote_label(b)}',
            "grouping_keys": [], "pattern_length": 2,
            "category": "attribute_case", "tags": ["probe"],
        })

    for dataset_path, label in datasets:
        if not dataset_path.exists():
            print(f"  [SKIP] {dataset_path} not found")
            continue

        size = _log_size(dataset_path)
        print(f"\n  Dataset: {label} ({size} events)")

        ingest_adaptive(label, dataset_path, CONFIG,
                        overrides={"overwrite_data": True})
        time.sleep(1)

        for probe in probes:
            try:
                timed_query(label, probe["pattern"], probe["grouping_keys"])
            except Exception:
                pass

        elk_index = elk_index_for(label)
        for probe in probes:
            probe_with_log = {**probe, "log_name": label}
            cat = probe["category"]

            # Cold
            try:
                body, latency = timed_query(
                    label, probe["pattern"], probe["grouping_keys"])
                _emit(rec, "adaptive_cold", cat, probe_with_log,
                      latency, body.get("total", 0), size,
                      note="sequence_table_scan")
            except Exception as exc:
                rec.emit("query_error", system="adaptive_cold",
                         qid=probe["id"], log_name=label, error=str(exc))

            # Warm
            try:
                body, latency = timed_query(
                    label, probe["pattern"], probe["grouping_keys"])
                _emit(rec, "adaptive_warm", cat, probe_with_log,
                      latency, body.get("total", 0), size)
            except Exception as exc:
                rec.emit("query_error", system="adaptive_warm",
                         qid=probe["id"], log_name=label, error=str(exc))

            if not skip_elk:
                run_elk(rec, cat, [probe_with_log], label,
                        log_size=size, elk_index=elk_index)
            if not skip_mr:
                run_match_recognize(rec, cat, [probe_with_log], label,
                                    log_size=size)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Experiment 6.4.1 — Comparable head-to-head evaluation.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Dataset arguments (mutually exclusive styles):
  --datasets path1:name1 path2:name2 ...   run multiple datasets in sequence
  --dataset path --log-name name           run a single dataset (legacy)

Categories per dataset (workload built from that dataset's schema):
  structural_case   A B, A B C — all systems, case grouping
  attribute_case    A[a="v"] B, A[a=$1] B[a=$1] — with workarounds
  length_sweep      k=2..MAX_LENGTH, all systems

ELK index:    siesta_{log_name}  (one per dataset, never overwritten across runs)
Flink table:  event_log (re-registered per dataset; session reused)
""",
    )
    # Dataset args — support both styles
    ds_group = ap.add_mutually_exclusive_group(required=True)
    ds_group.add_argument("--datasets", nargs="+", metavar="PATH:NAME",
                          help="One or more path:log_name pairs.")
    ds_group.add_argument("--dataset",  metavar="PATH",
                          help="Single dataset path (use with --log-name).")
    ap.add_argument("--log-name",   metavar="NAME", default=None,
                    help="Log name for --dataset (defaults to file stem).")
    ap.add_argument("--max-length", type=int, default=6)
    ap.add_argument("--skip-elk",   action="store_true")
    ap.add_argument("--skip-mr",    action="store_true")
    args = ap.parse_args()

    # Normalise to a list of DatasetSpec
    if args.datasets:
        specs = parse_datasets(args.datasets)
    else:
        log_name = args.log_name or Path(args.dataset).stem
        specs = [DatasetSpec(path=Path(args.dataset), log_name=log_name)]

    # Validate
    for spec in specs:
        if not spec.path.exists():
            print(f"[ERROR] File not found: {spec.path}")
            raise SystemExit(1)

    print(f"Experiment 6.4.1 — Comparable evaluation")
    print(f"  Datasets ({len(specs)}): "
          + ", ".join(s.log_name for s in specs))
    print(f"  Max pattern length: {args.max_length}")
    print(f"  Skip ELK: {args.skip_elk}   Skip MR: {args.skip_mr}")

    health_check()
    rec = Recorder("6.4.1", "6_4_1_comparable.jsonl")
    rec.emit("experiment_start",
             datasets=[{"path": str(s.path), "log_name": s.log_name}
                       for s in specs],
             max_length=args.max_length,
             skip_elk=args.skip_elk,
             skip_mr=args.skip_mr)

    for spec in specs:
        run_single_dataset(
            rec, spec,
            max_length=args.max_length,
            skip_elk=args.skip_elk,
            skip_mr=args.skip_mr,
        )

    # Scalability sweep (only if a single dataset given — uses SCALABILITY_DATASETS env)
    if len(specs) == 1:
        run_scalability_sweep(rec, specs[0].log_name, specs[0].path,
                              skip_elk=args.skip_elk, skip_mr=args.skip_mr)

    print(f"\nResults written to {rec.path}")
    print("\nFigures this data supports:")
    print("  - Grouped bar:   latency × system × dataset")
    print("  - Line chart:    latency vs pattern_length, one line per system")
    print("  - Cross-dataset: same query class, different logs — shows generality")


if __name__ == "__main__":
    main()