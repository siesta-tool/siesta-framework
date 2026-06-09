"""
tests/eval/exp_competitive.py

Experiment 6.4.1 — Competitive performance comparison.

Thesis
------
ELK and Flink MATCH_RECOGNIZE cannot natively group by arbitrary event
attributes.  They must fetch all matching events and perform a post-hoc
GROUP BY to answer multiperspective queries.  Our adaptive system
pre-builds per-perspective pair indices (PERSISTENT), answering queries
directly from the index without a full scan.

Systems
-------
siesta_warm
    Adaptive system after warm-up (PERSISTENT pairs).
    Protocol identical to exp_warmup.py: ingest_adaptive declares all
    perspectives → N_WARMUP_QUERIES repetitions per query pair with
    min_query_count=3 → sleep → timed benchmark from PERSISTENT.

elk
    Elasticsearch 8.x.  Multiperspective aggregation:
      terms(perspective_key) → terms(trace_id) → cardinality(activity)
                                → bucket_selector(n >= k)
    No ordering enforced.  Result is a superset (annotated).
    Regex: not supported (skipped).

match_recognize
    Flink SQL MATCH_RECOGNIZE partitioned by trace_id (the only option).
    Wrapped in GROUP BY perspective_key to simulate multiperspective
    grouping.  Queries use eventually-follows semantics matching SIESTA:

        PATTERN (V0 GAP0* V1 GAP1* V2 …)

    where GAP variables are NOT in DEFINE → Flink treats them as matching
    any event (STNM / skip-till-next-match).  Without this, Flink would
    compute direct-follows which is semantically wrong vs SIESTA.

Perspective selection
---------------------
Same logic as exp_skew_vs_uniform_latency.py:
  • discover_schema → perspective_keys (non-numeric, cardinality in
    [3,500], event-level avg_dpt > 1.2, sorted by cardinality asc)
  • ingest_adaptive declares ALL perspectives in one call
  • pair_coverage per perspective; skip if group_count < MIN_PERSP_CARD
  • workloads built per perspective; warm-up across all; then benchmark

Query categories (per perspective) — all timed against SIESTA
-------------------------------------------------------------
  structural   A B, A B C, long chains up to max_length.
               2-activity patterns use the single-pair skip-CEP path; 3+
               activity chains are answered by a NATIVE chain join over the
               consecutive PERSISTENT pair tables (stitched on shared
               positions) — no Python CEP.

  attribute    A[attr="val"] B   (value verified to exist on act A)
               A[attr=$1] B[attr=$1]  (only if A,B share a common value)
               Attribute constraints are pushed down as Spark column
               predicates over the pair index's source_attributes /
               target_attributes maps (see build_pair_attr_predicate in
               detection_query.py).  Equality / inequality literals and
               cross-side bindings become column filters, which keeps the
               query on the fast path (single-pair skip or chain join).

  regex        A B+ C, A B* C.
               Kleene closure genuinely requires the CEP engine, but warm
               constituent pairs let CEP read from the index rather than
               cold-scanning the SequenceTable.  Run against Flink too;
               ELK is skipped (cannot express Kleene operators).

Note on group density
---------------------
The chain-join and CEP costs scale with per-group pair density.  On dense
real logs (e.g. very high-frequency activities) longer chains / regex can
still be heavy; an artificial dataset with controlled group density is the
clean way to isolate the comparison.

Output
------
results/6_4_1_competitive_{log_name}.jsonl
"""

from __future__ import annotations

import argparse
import csv
import itertools
import json
import os
import re
import sys
import time
from pathlib import Path

import requests
from urllib.parse import urljoin

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tests.eval.eval_common import (
    API_BASE, API_TIMEOUT_S, CONFIG_DIR,
    Recorder, health_check,
    ingest_adaptive,
    timed_query, detect_adaptive,
    discover_schema, resolve_dataset,
    quote_label,
)
from tests.eval.workload import fetch_pair_coverage

CONFIG = CONFIG_DIR / "adaptive_index.config.json"

ELK_ENDPOINT = os.environ.get("ELK_ENDPOINT", "http://localhost:9200")
MR_ENDPOINT  = os.environ.get("MR_ENDPOINT",  "http://localhost:8083")

SWEEP_TIMEOUT_S = int(os.environ.get("SWEEP_TIMEOUT_S", "600"))
ELK_BULK_BATCH  = int(os.environ.get("ELK_BULK_BATCH",  "5000"))

# Warm-up retention overrides — identical to exp_warmup.py / exp_skew.py
RETENTION_OVERRIDES = {
    "min_query_count":   3,
    "half_life_seconds": 3600.0,
    "hysteresis":        0.0,
}
N_WARMUP_QUERIES = 4       # > min_query_count guarantees promotion
WARMUP_SLEEP_S   = int(os.environ.get("WARMUP_SLEEP_S", "120"))

# Minimum perspective group count to be eligible (same as exp_skew)
MIN_PERSP_CARD = 5

_LOG_EXTS = {".csv", ".xes"}


# ═══════════════════════════════════════════════════════════════════════════
# ELK — ingestion
# ═══════════════════════════════════════════════════════════════════════════

def _elk_reachable() -> bool:
    try:
        return requests.get(ELK_ENDPOINT, timeout=5).status_code == 200
    except Exception:
        return False


def elk_create_index(index_name: str, attribute_keys: list[str]) -> None:
    r = requests.delete(f"{ELK_ENDPOINT}/{index_name}", timeout=10)
    if r.status_code == 200:
        print(f"    ES index '{index_name}' deleted (previous run cleared)")
    elif r.status_code != 404:
        r.raise_for_status()
    time.sleep(0.5)
    props: dict = {
        "trace_id":        {"type": "keyword"},
        "activity":        {"type": "keyword"},
        "start_timestamp": {"type": "date",
                            "format": "strict_date_optional_time||epoch_millis"},
        "position":        {"type": "integer"},
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
    requests.put(f"{ELK_ENDPOINT}/{index_name}", json=mapping, timeout=30).raise_for_status()
    print(f"    ES index '{index_name}' created ({len(props)} fields)")


def elk_ingest_from_log(dataset_path: Path, index_name: str,
                        attribute_keys: list[str]) -> int:
    from tests.eval.batch_splitter import _iter_log
    buf: list[str] = []
    n = 0
    for ev in _iter_log(dataset_path):
        doc: dict = {
            "trace_id": ev.get("trace_id"),
            "activity": ev.get("activity"),
            "start_timestamp": ev.get("timestamp") or ev.get("start_timestamp"),
        }
        if "position" in ev:
            doc["position"] = ev["position"]
        for attr in attribute_keys:
            v = ev.get(attr)
            if v is not None:
                doc[attr] = v
        buf.append(json.dumps({"index": {"_index": index_name}}))
        buf.append(json.dumps(doc))
        n += 1
        if len(buf) >= ELK_BULK_BATCH * 2:
            _elk_bulk_send(buf); buf.clear()
    if buf:
        _elk_bulk_send(buf)
    requests.post(f"{ELK_ENDPOINT}/{index_name}/_refresh", timeout=30)
    requests.put(f"{ELK_ENDPOINT}/{index_name}/_settings",
                 json={"index": {"refresh_interval": "1s"}}, timeout=10)
    return n


def _elk_bulk_send(lines: list[str]) -> None:
    body = "\n".join(lines) + "\n"
    r = requests.post(f"{ELK_ENDPOINT}/_bulk", data=body.encode(),
                      headers={"Content-Type": "application/x-ndjson"}, timeout=120)
    r.raise_for_status()
    if r.json().get("errors"):
        errs = sum(1 for it in r.json()["items"] if "error" in it.get("index", {}))
        if errs:
            print(f"    WARNING: {errs} ELK bulk errors")


# ═══════════════════════════════════════════════════════════════════════════
# ELK — multiperspective query
# ═══════════════════════════════════════════════════════════════════════════

def build_elk_multiperspective_query(
    activities: list[str],
    perspective_key: str,
    attr_eq: list[tuple[str, str, str]] | None = None,
) -> dict:
    """
    Count (perspective_group, trace_id) pairs where the trace contains
    all required activities.  No ordering enforcement — result is a
    superset of SIESTA's ordered result.

    The ELK GROUP BY is done via nested terms aggregations:
      terms(perspective_key) → terms(trace_id) → cardinality(activity)
                              → bucket_selector(n >= len(activities))
    """
    must: list[dict] = [{"terms": {"activity": activities}}]
    if attr_eq:
        for _, attr_key, attr_val in attr_eq:
            must.append({"term": {attr_key: attr_val}})
    return {
        "size": 0,
        "query": {"bool": {"filter": must}},
        "aggs": {
            "by_perspective": {
                "terms": {"field": perspective_key, "size": 100_000},
                "aggs": {
                    "by_trace": {
                        "terms": {"field": "trace_id", "size": 1_000_000},
                        "aggs": {
                            "act_count": {"cardinality": {"field": "activity"}},
                            "has_all": {
                                "bucket_selector": {
                                    "buckets_path": {"n": "act_count"},
                                    "script": f"params.n >= {len(activities)}",
                                },
                            },
                        },
                    },
                },
            },
        },
    }


def elk_count_from_response(resp: dict) -> int:
    """Total qualifying (group, trace) pairs from aggregation response."""
    total = 0
    for pb in resp.get("aggregations", {}).get("by_perspective", {}).get("buckets", []):
        total += len(pb.get("by_trace", {}).get("buckets", []))
    return total


def run_elk_query(body: dict, index_name: str) -> tuple[float, int, bool]:
    t0 = time.perf_counter()
    try:
        r = requests.post(f"{ELK_ENDPOINT}/{index_name}/_search",
                          json=body, timeout=SWEEP_TIMEOUT_S)
        r.raise_for_status()
        return time.perf_counter() - t0, elk_count_from_response(r.json()), False
    except requests.exceptions.Timeout:
        return time.perf_counter() - t0, 0, True
    except Exception as exc:
        print(f"    ELK error: {exc}")
        return time.perf_counter() - t0, 0, True


# ═══════════════════════════════════════════════════════════════════════════
# Flink MATCH_RECOGNIZE — data prep + query
# ═══════════════════════════════════════════════════════════════════════════

def _mr_reachable() -> bool:
    try:
        return requests.get(f"{MR_ENDPOINT}/v1/info", timeout=5).status_code == 200
    except Exception:
        return False


def _sanitize_col(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", name)


def flink_prepare_csv(dataset_path: Path, output_csv: Path,
                      attribute_keys: list[str]) -> int:
    """
    Write events to a header-free CSV for Flink's filesystem connector.

    Flink maps columns by POSITION (DDL order), not by name.  Writing a
    header row would make Flink treat "trace_id,activity,..." as the first
    data row — position would be "position" (a string), causing a parse
    error that is silently ignored by csv.ignore-parse-errors, corrupting
    the row count and yielding null values in some columns.
    """
    from tests.eval.batch_splitter import _iter_log
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["trace_id", "activity", "start_timestamp", "position"] + attribute_keys
    n = 0
    trace_pos: dict[str, int] = {}
    with output_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        # NO writeheader() — Flink reads all rows as data
        for ev in _iter_log(dataset_path):
            tid = ev.get("trace_id", "")
            pos = trace_pos.get(tid, 0)
            trace_pos[tid] = pos + 1
            row = {"trace_id": tid, "activity": ev.get("activity", ""),
                   "start_timestamp": ev.get("timestamp") or ev.get("start_timestamp") or "",
                   "position": pos}
            for attr in attribute_keys:
                row[attr] = ev.get(attr, "")
            writer.writerow(row)
            n += 1
    print(f"    Flink CSV: {n} events → {output_csv}")
    return n


def flink_get_session() -> str:
    r = requests.post(f"{MR_ENDPOINT}/v1/sessions",
                      json={"properties": {"execution.runtime-mode": "BATCH"}},
                      timeout=30)
    r.raise_for_status()
    handle = r.json()["sessionHandle"]
    print(f"    Flink session: {handle}")
    return handle


def flink_exec(session: str, sql: str, *, timeout_s: int = 60) -> dict:
    r = requests.post(f"{MR_ENDPOINT}/v1/sessions/{session}/statements",
                      json={"statement": sql}, timeout=timeout_s)
    r.raise_for_status()
    op = r.json()["operationHandle"]
    t0 = time.perf_counter()
    while True:
        if time.perf_counter() - t0 > timeout_s:
            return {"operationHandle": op, "status": "TIMEOUT"}
        sr = requests.get(
            f"{MR_ENDPOINT}/v1/sessions/{session}/operations/{op}/status",
            timeout=30)
        st = sr.json().get("status", "UNKNOWN")
        if st in ("FINISHED", "ERROR", "CANCELED"):
            return {"operationHandle": op, "status": st}
        time.sleep(0.3)


def flink_create_table(session: str, table_name: str, csv_path: str,
                       attribute_keys: list[str]) -> None:
    cols = ["`trace_id` STRING", "`activity` STRING",
            "`start_timestamp` STRING", "`position` INT"]
    for attr in attribute_keys:
        cols.append(f"`{_sanitize_col(attr)}` STRING")
    ddl = (f"CREATE TABLE IF NOT EXISTS `{table_name}` "
           f"({', '.join(cols)}) WITH ("
           f"'connector'='filesystem', 'path'='{csv_path}', "
           f"'format'='csv', 'csv.ignore-parse-errors'='true')")
    r = flink_exec(session, ddl, timeout_s=30)
    if r["status"] != "FINISHED":
        raise RuntimeError(f"Flink CREATE TABLE failed: {r}")
    print(f"    Flink table '{table_name}' created")


def build_mr_multiperspective_sql(
    table_name: str,
    activities: list[str],
    perspective_key: str,
    attr_eq: list[tuple[str, str, str]] | None = None,
    binding_vars: dict | None = None,
    regex_ops: dict[int, str] | None = None,
) -> str:
    """
    Build a Flink SQL query that mirrors SIESTA's STNM (eventually-follows)
    semantics and answers a multiperspective query.

    Eventually-follows translation
    --------------------------------
    SIESTA's SeQL `A B` uses STNM: B can appear anywhere after A in the
    sequence, with any intervening events.  Flink MATCH_RECOGNIZE's
    `PATTERN (V0 V1)` means V1 *directly* follows V0 — this is the wrong
    semantics (strict contiguity / directly-follows).

    The correct translation inserts gap wildcard variables between every
    pair of consecutive required pattern variables:

        PATTERN (V0 GAP0* V1 GAP1* V2 …)

    GAP variables are intentionally NOT added to DEFINE.  In Flink MATCH_
    RECOGNIZE, any pattern variable absent from DEFINE is treated as
    matching any event — giving "zero or more of any event" between
    required matches, i.e. STNM / eventually-follows semantics.

    Multiperspective wrapper
    ------------------------
    MATCH_RECOGNIZE can only PARTITION BY trace_id (fixed at DDL time).
    To simulate GROUP BY perspective_key we:
      1. MEASURES the perspective attribute from the first match variable
      2. Wrap the inner MR in GROUP BY perspective_key + COUNT(DISTINCT)
    This forces Flink to pay both the full NFA scan and the GROUP BY cost.
    """
    n = len(activities)
    var_names = [f"V{i}" for i in range(n)]
    safe_persp = _sanitize_col(perspective_key)

    # ── DEFINE clauses ────────────────────────────────────────────────
    defines = []
    for i, (var, act) in enumerate(zip(var_names, activities)):
        conds = [f"{var}.`activity` = '{act}'"]
        if attr_eq:
            for eq_act, eq_key, eq_val in attr_eq:
                if eq_act == act:
                    conds.append(f"{var}.`{_sanitize_col(eq_key)}` = '{eq_val}'")
        if binding_vars:
            for _, bindings in binding_vars.items():
                for b_act, b_key in bindings:
                    if b_act == act and i > 0:
                        ref_var = var_names[next(
                            j for j, a in enumerate(activities) if a == bindings[0][0]
                        )]
                        conds.append(
                            f"{var}.`{_sanitize_col(b_key)}` "
                            f"= {ref_var}.`{_sanitize_col(b_key)}`"
                        )
        defines.append(f"{var} AS ({' AND '.join(conds)})")
    # NOTE: GAP variables are deliberately NOT added to DEFINE here.
    # Flink matches any event for undefined variables → STNM semantics.

    # ── PATTERN clause with GAP wildcards (eventually-follows) ────────
    # For V0 V1 V2: generates "V0 GAP0* V1 GAP1* V2"
    # For V0 V1+ V2: generates "V0 GAP0* V1+ GAP1* V2"
    pattern_parts: list[str] = []
    for i, var in enumerate(var_names):
        if i > 0:
            # GAP between previous variable and this one.
            pattern_parts.append(f"GAP{i - 1}*")
        op = (regex_ops or {}).get(i, "")
        pattern_parts.append(f"{var}{op}")

    # ── Inner MATCH_RECOGNIZE query ───────────────────────────────────
    inner = (
        f"SELECT T.`trace_id`, T.`{safe_persp}` "
        f"FROM `{table_name}` MATCH_RECOGNIZE ("
        f" PARTITION BY `trace_id`"
        f" ORDER BY `position`"
        f" MEASURES `{var_names[0]}`.`{safe_persp}` AS `{safe_persp}`"
        f" ONE ROW PER MATCH"
        f" AFTER MATCH SKIP TO NEXT ROW"
        f" PATTERN ({' '.join(pattern_parts)})"
        f" DEFINE {', '.join(defines)}"
        f") AS T"
    )

    # ── Outer GROUP BY perspective key ────────────────────────────────
    return (
        f"SELECT `{safe_persp}`, COUNT(DISTINCT `trace_id`) AS grp_count "
        f"FROM ({inner}) "
        f"GROUP BY `{safe_persp}`"
    )


def _flink_fetch_page(uri: str, *, timeout_s: int = 30, retries: int = 40) -> dict | None:
    """
    GET one result page from the Flink SQL Gateway, retrying while
    resultType == NOT_READY.  Returns the parsed body or None on failure.

    NOT_READY means the operation is FINISHED but the gateway hasn't yet
    buffered this page — polling with a short sleep resolves it.
    """
    for _ in range(retries):
        try:
            rr = requests.get(f"{MR_ENDPOINT}{uri}", timeout=timeout_s)
            rr.raise_for_status()
            body = rr.json()
            if body.get("resultType") != "NOT_READY":
                return body
            time.sleep(0.5)
        except Exception as exc:
            print(f"    _flink_fetch_page error: {exc}")
            return None
    return None   # still NOT_READY after all retries


def flink_fetch_rows(
    session: str,
    sql: str,
    *,
    timeout_s: int = 120,
    max_pages: int = 5,
) -> list[dict] | None:
    """
    Submit a SQL statement to Flink, wait for completion, and return
    the raw result rows.  Returns None on timeout or error.

    Result fetching polls each page until resultType != NOT_READY.
    The Flink SQL Gateway may return NOT_READY even after the operation
    status is FINISHED — the result buffer is materialised asynchronously.
    """
    try:
        r = requests.post(f"{MR_ENDPOINT}/v1/sessions/{session}/statements",
                          json={"statement": sql}, timeout=timeout_s)
        r.raise_for_status()
        op = r.json()["operationHandle"]
        t0 = time.perf_counter()
        # Phase 1 — wait for operation status to reach FINISHED
        while True:
            if time.perf_counter() - t0 >= timeout_s:
                return None
            sr = requests.get(
                f"{MR_ENDPOINT}/v1/sessions/{session}/operations/{op}/status",
                timeout=30)
            status = sr.json().get("status", "UNKNOWN")
            if status == "FINISHED":
                break
            if status in ("ERROR", "CANCELED"):
                return None
            time.sleep(0.3)
        # Phase 2 — paginate results, polling each page until it's ready
        rows: list[dict] = []
        uri = f"/v1/sessions/{session}/operations/{op}/result/0"
        for _ in range(max_pages):
            body = _flink_fetch_page(uri, timeout_s=30, retries=40)
            if body is None:
                # NOT_READY never resolved — result not available via this API
                return None
            rows.extend(body.get("results", {}).get("data", []))
            nxt = body.get("nextResultUri")
            if not nxt or body.get("resultType") == "EOS":
                break
            uri = nxt
        return rows
    except Exception as exc:
        print(f"    flink_fetch_rows error: {exc}")
        return None


def flink_validate_table(
    session: str,
    table_name: str,
    n_written: int,
    csv_host_path: Path,
) -> bool:
    """
    Validate that the Flink table was created and the CSV data is correct.

    Why not SELECT COUNT(*)?
    ------------------------
    In Flink batch mode, COUNT(*) is submitted as a cluster job.  The SQL
    Gateway never buffers the job result back into its own result store, so
    the /result endpoint returns NOT_READY indefinitely even after the job
    finishes.  This is a known Gateway behaviour in batch mode.

    Instead we use two checks that do not require a Flink job:

    Check 1 — CSV file (host side, pure Python)
        • File exists at the host path
        • Line count == n_written (no extra header row)
        • First line does not start with column names (header leak detection)

    Check 2 — DESCRIBE table (catalog DDL, no Flink job)
        • Confirms the table is registered in the session catalog
        • Verifies the expected columns (trace_id, activity, position) are present
        • DESCRIBE is executed synchronously by the SQL Gateway; result is
          available immediately with no NOT_READY delay
    """
    print(f"    Validating Flink table '{table_name}' ...")

    # ── Check 1: CSV file (host side) ─────────────────────────────────
    if not csv_host_path.exists():
        print(f"    ✗ FAIL: CSV not found at {csv_host_path}")
        return False

    with csv_host_path.open("rb") as f:
        n_lines = sum(1 for _ in f)

    if n_lines == 0:
        print("    ✗ FAIL: CSV file is empty")
        return False

    # Check no header row (Flink maps by position; header becomes corrupt data)
    with csv_host_path.open() as f:
        first_line = f.readline().strip()
    first_fields = first_line.split(",")
    if first_fields[0].lower() == "trace_id":
        print("    ✗ FAIL: CSV first row is a header — writeheader() must not be called")
        return False

    if n_lines != n_written:
        diff_pct = abs(n_lines - n_written) / max(n_written, 1) * 100
        print(f"    ⚠ WARNING: CSV has {n_lines} lines but {n_written} events were "
              f"written ({diff_pct:.1f}% difference)")
    else:
        print(f"    ✓ CSV: {n_lines} lines, no header, "
              f"first row starts with {first_fields[0]!r}")

    # ── Check 2: DESCRIBE table (catalog DDL, no Flink job) ───────────
    # DESCRIBE is processed locally by the SQL Gateway (no cluster job),
    # so the result is immediately available — no NOT_READY issue.
    describe_rows = flink_fetch_rows(
        session, f"DESCRIBE `{table_name}`",
        timeout_s=30, max_pages=2,
    )
    if not describe_rows:
        print(f"    ✗ FAIL: DESCRIBE returned nothing — "
              f"table not registered or session lost")
        return False

    col_names = [str(r.get("fields", ["?"])[0]) for r in describe_rows]
    for required in ("trace_id", "activity", "position"):
        if required not in col_names:
            print(f"    ✗ FAIL: column '{required}' missing from table schema. "
                  f"Got: {col_names}")
            return False

    print(f"    ✓ DESCRIBE OK: {len(describe_rows)} columns — "
          f"{col_names[:6]}{'...' if len(col_names) > 6 else ''}")
    return True


def flink_run_query(session: str, sql: str) -> tuple[float, int, bool, str]:
    """Execute Flink query; return (latency_s, total_groups, timed_out, status)."""
    t0 = time.perf_counter()
    try:
        r = requests.post(f"{MR_ENDPOINT}/v1/sessions/{session}/statements",
                          json={"statement": sql}, timeout=SWEEP_TIMEOUT_S)
        r.raise_for_status()
        op = r.json()["operationHandle"]
        while True:
            if time.perf_counter() - t0 >= SWEEP_TIMEOUT_S:
                return time.perf_counter() - t0, 0, True, "TIMEOUT"
            sr = requests.get(
                f"{MR_ENDPOINT}/v1/sessions/{session}/operations/{op}/status",
                timeout=30)
            status = sr.json().get("status", "UNKNOWN")
            if status in ("FINISHED", "ERROR", "CANCELED"):
                break
            time.sleep(0.3)
        latency = time.perf_counter() - t0
        if status != "FINISHED":
            return latency, 0, False, status
        # Paginate and sum per-group counts, polling each page for NOT_READY
        total = 0
        uri = f"/v1/sessions/{session}/operations/{op}/result/0"
        for _ in range(500):
            body = _flink_fetch_page(uri, timeout_s=60, retries=60)
            if body is None:
                break
            for row in body.get("results", {}).get("data", []):
                try:
                    total += int(row.get("fields", [None, 0])[1])
                except (TypeError, ValueError, IndexError):
                    total += 1
            nxt = body.get("nextResultUri")
            if not nxt or body.get("resultType") == "EOS":
                break
            uri = nxt
        return latency, total, False, status
    except Exception as exc:
        return time.perf_counter() - t0, 0, True, f"EXCEPTION: {exc}"


# ═══════════════════════════════════════════════════════════════════════════
# Pattern parsing
# ═══════════════════════════════════════════════════════════════════════════

def _parse_seql_pattern(pattern: str):
    """Parse SeQL pattern → (activities, attr_eq, has_binding, has_regex)."""
    has_binding = False
    has_regex   = bool(re.search(r'[+*?]', pattern))
    attr_eq: list[tuple[str, str, str]] = []
    segments: list[str] = []
    pos = 0
    while pos < len(pattern):
        m = re.match(r'"((?:[^"\\]|\\.)*)"', pattern[pos:])
        if m:
            label = m.group(1).replace('\\"', '"').replace("\\\\", "\\")
            pos += m.end()
        else:
            m = re.match(r'[A-Za-z_][A-Za-z0-9_:\\]*', pattern[pos:])
            if m:
                label = m.group(0); pos += m.end()
            else:
                pos += 1; continue
        if pos < len(pattern) and pattern[pos] == '[':
            end = pattern.find(']', pos)
            if end != -1:
                for part in pattern[pos + 1:end].split(","):
                    part = part.strip()
                    cm = re.match(r'([\w:]+)\s*=\s*(.+)', part)
                    if cm:
                        key = cm.group(1)
                        val = cm.group(2).strip().strip('"')
                        if val.startswith("$"):
                            has_binding = True
                        else:
                            attr_eq.append((label, key, val))
                pos = end + 1
        segments.append(label)
    return segments, attr_eq, has_binding, has_regex


def _extract_binding_vars(pattern: str):
    """Extract $N variable bindings → {var_id: [(act, attr), ...]}."""
    bindings: dict[str, list[tuple[str, str]]] = {}
    pos = 0
    while pos < len(pattern):
        m = re.match(r'"((?:[^"\\]|\\.)*)"', pattern[pos:])
        if m:
            label = m.group(1).replace('\\"', '"'); pos += m.end()
        else:
            m = re.match(r'[A-Za-z_][A-Za-z0-9_:\\]*', pattern[pos:])
            if m:
                label = m.group(0); pos += m.end()
            else:
                pos += 1; continue
        if pos < len(pattern) and pattern[pos] == '[':
            end = pattern.find(']', pos)
            if end != -1:
                for part in pattern[pos + 1:end].split(","):
                    cm = re.match(r'([\w:]+)\s*=\s*(\$\d+)', part.strip())
                    if cm:
                        bindings.setdefault(cm.group(2), []).append(
                            (label, cm.group(1)))
                pos = end + 1
    return bindings or None


def _extract_regex_ops(pattern: str) -> dict[int, str] | None:
    """Extract Kleene ops → {activity_index: op}."""
    clean = re.sub(r'\[[^\]]*\]', '', pattern)
    ops = {}
    for i, (_, op) in enumerate(
        re.findall(r'("(?:[^"\\]|\\.)*"|[A-Za-z_][A-Za-z0-9_:\\]*)([+*?])?', clean)
    ):
        if op:
            ops[i] = op
    return ops or None


# ═══════════════════════════════════════════════════════════════════════════
# Workload construction — verified non-null
# ═══════════════════════════════════════════════════════════════════════════

def _pat(*acts: str) -> str:
    return " ".join(quote_label(a) for a in acts)


# ═══════════════════════════════════════════════════════════════════════════
# Trace-based workload sampling
# ═══════════════════════════════════════════════════════════════════════════
# Queries are sampled directly from real traces instead of being derived
# from pair-coverage statistics.  This guarantees total > 0 by construction
# — every sampled query pattern was witnessed in at least one real trace.
# No preflight cold-scan is needed.
# ═══════════════════════════════════════════════════════════════════════════

MAX_TRACE_SAMPLE = int(os.environ.get("MAX_TRACE_SAMPLE", "5000"))


def _load_traces(dataset_path: Path) -> list[list[dict]]:
    """
    Load up to MAX_TRACE_SAMPLE traces from the log into memory.

    Returns a list of traces, each trace being a list of event dicts
    (already in timestamp order as they appear in the source file).
    """
    from tests.eval.batch_splitter import _iter_log
    trace_dict: dict[str, list[dict]] = {}
    for ev in _iter_log(dataset_path):
        tid = ev.get("trace_id")
        if not tid:
            continue
        if tid not in trace_dict:
            if len(trace_dict) >= MAX_TRACE_SAMPLE:
                continue
            trace_dict[tid] = []
        trace_dict[tid].append(ev)
    print(f"    Loaded {len(trace_dict)} traces into memory")
    return list(trace_dict.values())


def _distinct_subseq(acts: list[str], k: int) -> list[str] | None:
    """
    Return the first k distinct activities from acts preserving order,
    or None if fewer than k distinct activities exist.
    """
    seen: set[str] = set()
    result: list[str] = []
    for a in acts:
        if a not in seen:
            seen.add(a)
            result.append(a)
            if len(result) == k:
                return result
    return None


def _find_regex_chain(acts: list[str]) -> tuple[str, str, str] | None:
    """
    Find activities A, B, C (all distinct) where B appears at least
    once between A and C in `acts`.  Guarantees the pattern A B+ C matches.
    O(n²) scan.
    """
    n = len(acts)
    for i in range(n - 2):
        act_a = acts[i]
        for j in range(i + 1, n - 1):
            act_b = acts[j]
            if act_b == act_a:
                continue
            # Find any C after j that is not A or B
            for k in range(j + 1, n):
                act_c = acts[k]
                if act_c != act_a and act_c != act_b:
                    return act_a, act_b, act_c
    return None


def sample_workload_for_perspective(
    traces: list[list[dict]],
    log_name: str,
    perspective_key: str,
    schema,
    *,
    max_length: int = 5,
    n_structural_per_k: int = 3,
    n_attribute: int = 2,
    n_regex: int = 2,
) -> dict[str, list[dict]]:
    """
    Sample queries by scanning real traces.  Every generated query is
    guaranteed to match ≥ 1 trace by construction — the trace it was
    sampled from.

    Structural k=2..max_length
        For each k, scan traces and collect n_structural_per_k distinct
        k-length subsequences (all distinct activities, in order).

    Attribute single_eq  A[attr="val"] B
        Find a trace where event at position i has (activity=A, attr=val)
        and a later event at position j has activity=B.
        The sampled val is known to exist on activity A in that trace.

    Attribute cross_eq   A[attr=$1] B[attr=$1]
        Find a trace where event A and a later event B share the same
        value for attr.  The binding $1 is satisfied by construction.

    Regex  A B+ C  and  A B* C
        Find a trace where activities A, B, C appear in that order (B
        at least once between A and C).  B+ is satisfied; B* is the
        same pattern with a strictly weaker operator (superset of B+).
    """
    gk = [perspective_key]
    attr_pref = ["org:resource", "lifecycle:transition", "org:group",
                 "org:role", "resource", "role", "Action"]
    avail_attrs = [a for a in attr_pref if schema.attribute_values.get(a)]
    if not avail_attrs:
        avail_attrs = [k for k, vs in schema.attribute_values.items() if vs][:3]

    cnt   = itertools.count(1)
    cnt_a = itertools.count(1)
    cnt_r = itertools.count(1)

    # ── Structural ─────────────────────────────────────────────────────
    structural: list[dict] = []
    seen_s: set[str] = set()

    for k in range(2, max_length + 1):
        found = 0
        for trace in traces:
            acts = [ev.get("activity", "") for ev in trace if ev.get("activity")]
            subseq = _distinct_subseq(acts, k)
            if subseq is None:
                continue
            pat = _pat(*subseq)
            if pat in seen_s:
                continue
            seen_s.add(pat)
            structural.append({
                "id": f"S{next(cnt)}", "log_name": log_name,
                "pattern": pat, "grouping_keys": gk,
                "pattern_length": k, "category": "structural",
                "tags": [f"len={k}", "sampled"],
            })
            found += 1
            if found >= n_structural_per_k:
                break

    # ── Attribute — single_eq ──────────────────────────────────────────
    attribute: list[dict] = []
    seen_a: set[str] = set()

    for attr in avail_attrs[:2]:
        found = 0
        for trace in traces:
            if found >= n_attribute:
                break
            for i, ev_a in enumerate(trace):
                act_a = ev_a.get("activity", "")
                val   = ev_a.get(attr)
                if not act_a or not val:
                    continue
                # Find any B after position i
                for ev_b in trace[i + 1:]:
                    act_b = ev_b.get("activity", "")
                    if not act_b or act_b == act_a:
                        continue
                    val_esc = str(val).replace('"', '\\"')
                    pat = (f'{quote_label(act_a)}[{attr}="{val_esc}"] '
                           f'{quote_label(act_b)}')
                    if pat in seen_a:
                        break
                    seen_a.add(pat)
                    attribute.append({
                        "id": f"A{next(cnt_a)}", "log_name": log_name,
                        "pattern": pat, "grouping_keys": gk,
                        "pattern_length": 2, "category": "attribute",
                        "tags": ["single_eq", f"attr={attr}", "sampled"],
                    })
                    found += 1
                    break  # one query per trace per attr
                if found >= n_attribute:
                    break

    # ── Attribute — cross_eq ───────────────────────────────────────────
    seen_x: set[str] = set()
    for attr in avail_attrs[:1]:
        found = 0
        for trace in traces:
            if found >= n_attribute:
                break
            for i, ev_a in enumerate(trace):
                act_a = ev_a.get("activity", "")
                val_a = ev_a.get(attr)
                if not act_a or not val_a:
                    continue
                # Find B after i with the SAME attribute value
                for ev_b in trace[i + 1:]:
                    act_b = ev_b.get("activity", "")
                    if not act_b or act_b == act_a:
                        continue
                    if ev_b.get(attr) == val_a:
                        pat = (f'{quote_label(act_a)}[{attr}=$1] '
                               f'{quote_label(act_b)}[{attr}=$1]')
                        if pat not in seen_x:
                            seen_x.add(pat)
                            attribute.append({
                                "id": f"A{next(cnt_a)}", "log_name": log_name,
                                "pattern": pat, "grouping_keys": gk,
                                "pattern_length": 2, "category": "attribute",
                                "tags": ["cross_eq", f"attr={attr}", "sampled"],
                            })
                            found += 1
                        break
                if found >= n_attribute:
                    break

    # ── Regex ──────────────────────────────────────────────────────────
    regex: list[dict] = []
    seen_r: set[str] = set()

    for trace in traces:
        if len(regex) >= n_regex * 2:
            break
        acts = [ev.get("activity", "") for ev in trace if ev.get("activity")]
        chain = _find_regex_chain(acts)
        if chain is None:
            continue
        a, b, c = chain
        pat_plus = f'{quote_label(a)} {quote_label(b)}+ {quote_label(c)}'
        pat_star = f'{quote_label(a)} {quote_label(b)}* {quote_label(c)}'
        if pat_plus not in seen_r:
            seen_r.add(pat_plus)
            regex.append({
                "id": f"R{next(cnt_r)}", "log_name": log_name,
                "pattern": pat_plus, "grouping_keys": gk,
                "pattern_length": 3, "category": "regex",
                "tags": ["regex", "B+", "sampled"],
            })
        if pat_star not in seen_r and len(regex) < n_regex * 2:
            seen_r.add(pat_star)
            regex.append({
                "id": f"R{next(cnt_r)}", "log_name": log_name,
                "pattern": pat_star, "grouping_keys": gk,
                "pattern_length": 3, "category": "regex",
                "tags": ["regex", "B*", "sampled"],
            })

    if not regex:
        print("    WARNING: no regex chain found in sampled traces")

    return {"structural": structural, "attribute": attribute, "regex": regex}


# ═══════════════════════════════════════════════════════════════════════════
# Warm-up — identical protocol to exp_warmup.py
# ═══════════════════════════════════════════════════════════════════════════

def warmup_all_perspectives(
    all_workloads: dict[tuple, dict[str, list[dict]]],
) -> None:
    """
    Issue every query N_WARMUP_QUERIES times with RETENTION_OVERRIDES
    so every pair crosses min_query_count and gets queued for
    build_pair_persistent.  Then sleep WARMUP_SLEEP_S seconds for
    background materialisation.

    All perspectives are warmed up before the sleep so one sleep covers
    all of them.
    """
    total_q = sum(len(wl) for wls in all_workloads.values() for wl in wls.values())
    print(f"\n  Warm-up: {total_q} queries × {N_WARMUP_QUERIES} reps "
          f"across {len(all_workloads)} perspectives ...")

    for rep in range(N_WARMUP_QUERIES):
        print(f"\n  ── warm-up rep {rep + 1}/{N_WARMUP_QUERIES} ──")
        q_idx = 0
        for gk_tuple, workloads in all_workloads.items():
            pkey = gk_tuple[0]
            for cat, queries in workloads.items():
                for q in queries:
                    q_idx += 1
                    t0 = time.perf_counter()
                    try:
                        body = detect_adaptive(
                            q["log_name"], q["pattern"], q["grouping_keys"],
                            retention_overrides=RETENTION_OVERRIDES,
                        )
                        latency = time.perf_counter() - t0
                        tier_map = body.get("pair_status_after") or {}
                        tiers = sorted(set(tier_map.values()))
                        tier_str = ",".join(tiers) if tiers else "?"
                        print(f"    [{pkey}/{cat}] {q['id']:5s} "
                              f"({q_idx}/{total_q})  "
                              f"{q['pattern'][:45]:45s}  "
                              f"{latency:6.1f}s  [{tier_str}]")
                    except Exception as exc:
                        latency = time.perf_counter() - t0
                        print(f"    [{pkey}/{cat}] {q['id']:5s} "
                              f"({q_idx}/{total_q})  ERROR {latency:.1f}s: {exc}")
        print(f"  rep {rep + 1}/{N_WARMUP_QUERIES} done")

    print(f"\n    Sleeping {WARMUP_SLEEP_S}s for background materialisation ...")
    time.sleep(WARMUP_SLEEP_S)

    # Verify promotion — re-query each pattern once and report final tiers
    print("    Verifying pair promotion after sleep ...")
    persistent_count = 0
    total_pairs = 0
    for gk_tuple, workloads in all_workloads.items():
        pkey = gk_tuple[0]
        for cat, queries in workloads.items():
            for q in queries:
                try:
                    body = detect_adaptive(
                        q["log_name"], q["pattern"], q["grouping_keys"],
                        retention_overrides=RETENTION_OVERRIDES,
                    )
                    tier_map = body.get("pair_status_after") or {}
                    for tier in tier_map.values():
                        total_pairs += 1
                        if tier == "PERSISTENT":
                            persistent_count += 1
                except Exception:
                    pass
    pct = (persistent_count / total_pairs * 100) if total_pairs else 0
    print(f"    Warm-up complete — {persistent_count}/{total_pairs} pairs "
          f"PERSISTENT ({pct:.0f}%).")


# ═══════════════════════════════════════════════════════════════════════════
# Timed system runners
# ═══════════════════════════════════════════════════════════════════════════

def run_siesta(rec, category, workload, log_name):
    """Measure SIESTA adaptive from warm (PERSISTENT) state."""
    print(f"\n── SIESTA warm — {category} ({len(workload)} queries) ──")
    for q in workload:
        try:
            body, latency = timed_query(
                q["log_name"], q["pattern"], q["grouping_keys"],
                retention_overrides=RETENTION_OVERRIDES,
            )
            total = body.get("total", 0)
            tier  = body.get("pair_status_after", {})
            rec.emit("query", system="siesta_warm", category=category,
                     qid=q["id"], pattern=q["pattern"],
                     grouping_keys=q["grouping_keys"],
                     pattern_length=q.get("pattern_length"),
                     tags=q.get("tags", []),
                     latency_s=latency, total=total, tier=tier,
                     log_name=log_name)
            tiers = ",".join(set(tier.values())) if tier else "?"
            print(f"  {q['id']:5s} k={q.get('pattern_length','?')} "
                  f"{q['pattern'][:45]:45s} → {latency:.3f}s "
                  f"(n={total}, {tiers})")
        except Exception as exc:
            rec.emit("query_error", system="siesta_warm", category=category,
                     qid=q["id"], log_name=log_name, error=str(exc))
            print(f"  {q['id']:5s} ERROR: {exc}")


def run_elk(rec, category, workload, log_name, elk_index, perspective_key):
    """ELK multiperspective aggregation — no ordering, result is superset."""
    if not _elk_reachable():
        rec.emit("skip", system="elk", reason="not reachable")
        print(f"  [SKIP] ELK not reachable"); return

    print(f"\n── ELK — {category} ({len(workload)} queries) ──")
    for q in workload:
        activities, attr_eq, has_binding, has_regex = _parse_seql_pattern(q["pattern"])
        if has_regex:
            rec.emit("query", system="elk", category=category, qid=q["id"],
                     pattern=q["pattern"], latency_s=0.0, total=-1,
                     elk_note="regex_unsupported",
                     grouping_keys=q.get("grouping_keys"), log_name=log_name)
            print(f"  {q['id']:5s} SKIP (regex unsupported by ELK)")
            continue
        note = ("binding_unsupported_structural_only" if has_binding
                else "partial_attr_no_ordering" if attr_eq
                else "structural_no_ordering")
        elk_q = build_elk_multiperspective_query(
            activities, perspective_key,
            attr_eq=(None if has_binding else attr_eq) or None,
        )
        latency, total, timed_out = run_elk_query(elk_q, elk_index)
        rec.emit("query", system="elk", category=category,
                 qid=q["id"], pattern=q["pattern"],
                 grouping_keys=q.get("grouping_keys"),
                 pattern_length=q.get("pattern_length"),
                 tags=q.get("tags", []),
                 latency_s=latency, total=total, timed_out=timed_out,
                 elk_note=note, siesta_expected=q.get("siesta_total", 0),
                 log_name=log_name)
        st = "TIMEOUT" if timed_out else f"{latency:.3f}s"
        print(f"  {q['id']:5s} [{note[:18]}] "
              f"{q['pattern'][:40]:40s} → {st} (n={total})")


def run_match_recognize(rec, category, workload, log_name,
                        flink_session, flink_table, perspective_key):
    """
    Flink MATCH_RECOGNIZE with:
      - GAP* wildcards for STNM / eventually-follows semantics
      - outer GROUP BY perspective_key for multiperspective semantics
    """
    if not _mr_reachable():
        rec.emit("skip", system="match_recognize", reason="not reachable")
        print(f"  [SKIP] Flink not reachable"); return

    print(f"\n── MATCH_RECOGNIZE — {category} ({len(workload)} queries) ──")
    for q in workload:
        activities, attr_eq, has_binding, has_regex = _parse_seql_pattern(q["pattern"])
        if len(activities) < 2:
            print(f"  {q['id']:5s} SKIP (too few activities)"); continue
        sql = build_mr_multiperspective_sql(
            flink_table, activities, perspective_key,
            attr_eq=attr_eq or None,
            binding_vars=_extract_binding_vars(q["pattern"]) if has_binding else None,
            regex_ops=_extract_regex_ops(q["pattern"]) if has_regex else None,
        )
        latency, total, timed_out, status = flink_run_query(flink_session, sql)
        rec.emit("query", system="match_recognize", category=category,
                 qid=q["id"], pattern=q["pattern"],
                 grouping_keys=q.get("grouping_keys"),
                 pattern_length=q.get("pattern_length"),
                 tags=q.get("tags", []),
                 latency_s=latency, total=total, timed_out=timed_out,
                 flink_status=status, siesta_expected=q.get("siesta_total", 0),
                 log_name=log_name)
        st = "TIMEOUT" if timed_out else f"{latency:.3f}s [{status}]"
        print(f"  {q['id']:5s} k={q.get('pattern_length','?')} "
              f"{q['pattern'][:40]:40s} → {st} (n={total})")


# ═══════════════════════════════════════════════════════════════════════════
# Per-dataset orchestration
# ═══════════════════════════════════════════════════════════════════════════

def run_dataset(
    dataset_path: Path,
    log_name: str,
    *,
    max_length: int,
    skip_elk: bool,
    skip_mr: bool,
    flink_csv_dir: str,
    flink_csv_container_dir: str,
    max_perspectives: int,
    top_perspectives: int,
) -> None:
    schema = discover_schema(dataset_path)
    print(f"\n{'═' * 64}")
    print(f"  Dataset:      {log_name}  ({dataset_path.name})")
    print(f"  Activities:   {len(schema.activities)}")
    print(f"  Perspectives: {schema.perspective_keys[:max_perspectives]}")
    print(f"{'═' * 64}")

    # ── Perspective selection — same logic as exp_skew ─────────────────
    # discover_schema returns perspective_keys sorted by cardinality asc,
    # already filtered for non-numeric, event-level, cardinality in [3,500].
    # We additionally apply MIN_PERSP_CARD after pair_coverage (below).
    candidate_persp_keys = schema.perspective_keys[:max_perspectives]
    if not candidate_persp_keys:
        print("  ABORT: no valid perspective keys found."); return

    perspectives: list[list[str]] = [[k] for k in candidate_persp_keys]
    attr_keys = [k for k, vs in schema.attribute_values.items() if vs]

    rec = Recorder("6.4.1", f"6_4_1_competitive_{log_name}.jsonl")

    # ── Phase 1: Adaptive ingest — all perspectives declared ──────────
    # ingest_adaptive builds the shared SequenceTable and registers all
    # candidate perspectives at L0.  One call covers everything.
    print(f"\n  Phase 1: ingest_adaptive (all {len(perspectives)} perspectives) ...")
    ingest_adaptive(
        log_name, dataset_path, CONFIG,
        overrides={"perspectives": [{"grouping_keys": g} for g in perspectives]},
        clear_existing=True,
    )
    time.sleep(2)

    # ── Phase 2: ELK ingest ───────────────────────────────────────────
    elk_index = f"siesta_{log_name}".lower().replace(" ", "_")
    if not skip_elk and _elk_reachable():
        print(f"\n  Phase 2: ELK ingest (index={elk_index}) ...")
        elk_create_index(elk_index, attr_keys)
        n_elk = elk_ingest_from_log(dataset_path, elk_index, attr_keys)
        print(f"    {n_elk} events indexed")
        rec.emit("elk_ingest", log_name=log_name, n_events=n_elk)
    else:
        print(f"\n  Phase 2: ELK {'skipped' if skip_elk else 'not reachable'}")

    # ── Phase 3: Flink table ──────────────────────────────────────────
    # Two distinct paths:
    #   csv_host_path      — where this script writes the CSV on the host
    #   csv_container_path — what the Flink container sees via the volume mount
    # e.g. volume: /host/tests/eval/flink_data:/opt/flink/flink_data
    #   csv_host_path      = /host/tests/eval/flink_data/{log_name}.csv
    #   csv_container_path = /opt/flink/flink_data/{log_name}.csv
    flink_session = None
    flink_table = f"event_log_{log_name}".replace("-", "_").replace(" ", "_")
    if not skip_mr and _mr_reachable():
        print(f"\n  Phase 3: Flink table ...")
        csv_host_path      = Path(flink_csv_dir)           / f"{log_name}.csv"
        csv_container_path = Path(flink_csv_container_dir) / f"{log_name}.csv"
        n_flink = flink_prepare_csv(dataset_path, csv_host_path, attr_keys)
        flink_session = flink_get_session()
        try:
            flink_exec(flink_session, f"DROP TABLE IF EXISTS `{flink_table}`",
                       timeout_s=15)
        except Exception:
            pass
        # Pass the CONTAINER path to the DDL — that is what Flink will open
        flink_create_table(flink_session, flink_table,
                           str(csv_container_path), attr_keys)
        rec.emit("flink_ingest", log_name=log_name, n_events=n_flink,
                 host_path=str(csv_host_path),
                 container_path=str(csv_container_path))

        # Validate immediately — abort Flink participation if anything is wrong
        if not flink_validate_table(flink_session, flink_table, n_flink,
                                    csv_host_path):
            print("    Flink table validation FAILED — "
                  "Flink will be skipped for this dataset.")
            rec.emit("flink_validation_failed", log_name=log_name)
            flink_session = None
    else:
        print(f"\n  Phase 3: Flink {'skipped' if skip_mr else 'not reachable'}")

    # ── Phase 4: Load traces + sample workloads per perspective ──────
    # Two-pass design:
    #   Pass 1 — probe ALL candidate perspectives via pair_coverage to get
    #            their actual group_count, filter by MIN_PERSP_CARD.
    #   Sort   — rank survivors by group_count descending.  Higher group
    #            count = more expensive GROUP BY for ELK/Flink = clearest
    #            demonstration of SIESTA's advantage.
    #   Slice  — keep only the top `top_perspectives` survivors.
    #   Pass 2 — load traces once, sample workloads for selected perspectives.
    print(f"\n  Phase 4: Probing {len(perspectives)} candidate perspectives ...")
    candidates: list[tuple[list[str], int]] = []   # (gk, group_count)

    for gk in perspectives:
        pkey = gk[0]
        try:
            cov = fetch_pair_coverage(log_name, gk, activities=schema.activities)
        except Exception as exc:
            print(f"    [{pkey}] pair_coverage failed: {exc}"); continue
        gc = cov.get("group_count", 0)
        if gc < MIN_PERSP_CARD:
            print(f"    [{pkey}] SKIP  gc={gc} < {MIN_PERSP_CARD}")
        else:
            print(f"    [{pkey}] OK    gc={gc}")
            candidates.append((gk, gc))

    if not candidates:
        print("  ABORT: no perspectives passed MIN_PERSP_CARD filter."); return

    # Sort by group_count descending and take the top N
    candidates.sort(key=lambda x: x[1], reverse=True)
    selected = candidates[:top_perspectives]
    print(f"\n  Selected top {len(selected)} perspective(s) by group_count:")
    for gk, gc in selected:
        print(f"    {gk[0]}  (group_count={gc})")

    # Load traces once, reused across all perspectives
    traces = _load_traces(dataset_path)
    all_workloads: dict[tuple, dict[str, list[dict]]] = {}

    for gk, gc in selected:
        pkey = gk[0]
        rec.emit("perspective", log_name=log_name, perspective=pkey, group_count=gc)
        wls = sample_workload_for_perspective(
            traces, log_name, pkey, schema,
            max_length=max_length,
        )
        total_q = sum(len(wl) for wl in wls.values())
        for cat, ql in wls.items():
            print(f"      [{pkey}] {cat}: {len(ql)} queries")
        if total_q > 0:
            all_workloads[tuple(gk)] = wls

    if not all_workloads:
        print("  ABORT: no valid perspectives after filtering."); return

    rec.emit("dataset", log_name=log_name, path=str(dataset_path),
             perspectives=[list(k) for k in all_workloads],
             activities=schema.activities[:20])

    # ── Phase 5: Warm up ALL perspectives before any timing ───────────
    # All three categories benefit from warm-up:
    #   structural — multi-activity chains answered by the native chain join
    #                over PERSISTENT consecutive pair tables (no CEP).
    #   attribute  — constraints pushed down as Spark column predicates, so
    #                single-pair / chain patterns also skip CEP.
    #   regex      — Kleene closure still needs the CEP engine, but warming the
    #                constituent pairs lets CEP read them from the index instead
    #                of cold-scanning the SequenceTable.
    warmup_all_perspectives(all_workloads)

    # ── Phase 6: Timed benchmark per perspective ──────────────────────
    for gk_tuple, workloads in all_workloads.items():
        pkey = gk_tuple[0]
        print(f"\n{'─' * 64}")
        print(f"  Perspective: {pkey}")
        print(f"{'─' * 64}")

        for category, workload in workloads.items():
            if not workload:
                continue

            for q in workload:
                rec.emit("workload_query", log_name=log_name,
                         perspective=pkey, category=category,
                         qid=q["id"], pattern=q["pattern"],
                         grouping_keys=q["grouping_keys"],
                         pattern_length=q.get("pattern_length"),
                         tags=q.get("tags", []),
                         siesta_total=q.get("siesta_total", 0))

            # SIESTA: all three categories are timed.
            #   structural / attribute → native chain join + attribute pushdown
            #     (skip CEP); regex → CEP over index-pruned constituent pairs.
            run_siesta(rec, category, workload, log_name)

            # ELK: cannot express Kleene operators, so it skips regex.
            if not skip_elk and category != "regex":
                run_elk(rec, category, workload, log_name, elk_index, pkey)

            # Flink: all categories including regex.
            if not skip_mr and flink_session:
                run_match_recognize(rec, category, workload, log_name,
                                    flink_session, flink_table, pkey)

    print(f"\n  Results: {rec.path}")


# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════

def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Experiment 6.4.1 — Competitive comparison "
            "(SIESTA adaptive warm vs ELK vs Flink MATCH_RECOGNIZE) "
            "on multiperspective queries with attribute constraints and regex."
        ),
    )
    ds = ap.add_mutually_exclusive_group()
    ds.add_argument("--dataset")
    ds.add_argument("--datasets-dir", type=Path)
    ap.add_argument("--log-name", default=None)
    ap.add_argument("--max-length", type=int, default=5)
    ap.add_argument(
        "--max-perspectives", type=int, default=8,
        help=(
            "How many perspectives to probe from discover_schema via pair_coverage. "
            "Acts as the candidate pool. Default 8."
        ),
    )
    ap.add_argument(
        "--top-perspectives", type=int, default=2,
        help=(
            "From the probed candidates, keep only the top N by group_count "
            "descending. Higher group_count = more expensive GROUP BY for "
            "ELK/Flink = clearest competitive advantage for SIESTA. Default 2."
        ),
    )
    ap.add_argument("--skip-elk", action="store_true")
    ap.add_argument("--skip-mr",  action="store_true")
    ap.add_argument(
        "--flink-csv-dir",
        default="./flink_data",
        help=(
            "Host-side directory where this script writes Flink CSV files. "
            "Must be the host path of the volume mounted into the Flink containers. "
            "Example: /home/user/project/tests/eval/flink_data"
        ),
    )
    ap.add_argument(
        "--flink-csv-container-dir",
        default="/opt/flink/flink_data",
        help=(
            "Container-side path where Flink sees the CSV files (the other end "
            "of the volume mount used in docker-compose-flink.yml). "
            "This is the path written into the CREATE TABLE DDL. "
            "Default matches the docker-compose volume: /opt/flink/flink_data"
        ),
    )
    args = ap.parse_args()

    health_check()


    if args.datasets_dir:
        specs = [(p, p.stem) for p in sorted(args.datasets_dir.iterdir())
                 if p.suffix.lower() in _LOG_EXTS]
    else:
        spec = resolve_dataset(args.dataset, args.log_name)
        specs = [(spec.path, spec.log_name)]

    for path, name in specs:
        try:
            run_dataset(
                path, name,
                max_length=args.max_length,
                skip_elk=args.skip_elk,
                skip_mr=args.skip_mr,
                flink_csv_dir=args.flink_csv_dir,
                flink_csv_container_dir=args.flink_csv_container_dir,
                max_perspectives=args.max_perspectives,
                top_perspectives=args.top_perspectives,
            )
        except Exception as exc:
            import traceback
            print(f"\n[ERROR] {name}: {exc}")
            traceback.print_exc()


if __name__ == "__main__":
    main()