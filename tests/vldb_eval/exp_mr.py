"""
tests/vldb-eval/exp_mr.py — MATCH_RECOGNIZE Multiperspective Experiment

Evaluates Flink's MATCH_RECOGNIZE operator for structural sequence pattern
queries under multiple grouping perspectives.  For each (dataset, grouping,
pattern_length) cell the script runs N satisfiable queries and records
end-to-end latency and match count.

This is the MR counterpart of the SIESTA multiperspective evaluation: it
isolates the cost of changing PARTITION BY for increasing pattern lengths,
demonstrating that MR re-scans the full table for every perspective.

Usage
-----
  # docker compose up -d (from tests/vldb-eval/) first, then:
  python tests/vldb-eval/exp_mr.py \\
      --dataset /mnt/datasets/bpic2017.xes \\
      --log-name bpic2017

  # Override SQL Gateway URL:
  MR_GATEWAY=http://localhost:8083 python tests/vldb-eval/exp_mr.py ...

  # Explicit groupings (skip auto-discovery):
  python tests/vldb-eval/exp_mr.py --groupings case_id org:resource ...
"""

from __future__ import annotations

import argparse
import os
import random
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from dataclasses import dataclass

@dataclass
class PatternNode:
    activities: list[str]  # Multiple items here represent a logical OR: ['A', 'B'] -> (A | B)
    quantifier: str = ""   # Valid values: "", "*", "+"

# ---------------------------------------------------------------------------
# Path setup — works when run as a script or as a module
# ---------------------------------------------------------------------------

_REPO = Path(__file__).resolve().parents[2]
_EVAL_DIR = Path(__file__).resolve().parent   # tests/vldb-eval/
for _p in (str(_REPO), str(_EVAL_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from eval_common import Recorder, resolve_dataset, discover_schema   # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PATTERN_LENGTHS = list(range(8, 16))           # 6 .. 15 inclusive
QUERY_TIMEOUT_S = 700.0
N_PATTERNS_DEFAULT = 5

_POS_VARS = list("ABCDEFGHIJKLMNO")            # covers up to length 15

_CSV_FILENAME = "mr_exp_events.csv"

_TMP_CSV = Path("/tmp") / _CSV_FILENAME


# ---------------------------------------------------------------------------
# SQL name sanitization
# ---------------------------------------------------------------------------

def to_sql_name(raw: str) -> str:
    """Replace characters invalid in SQL identifiers."""
    return raw.replace(":", "_").replace("-", "_")


# ---------------------------------------------------------------------------
# Dataset loading and normalisation
# ---------------------------------------------------------------------------

def load_and_normalise(path: Path) -> tuple[pd.DataFrame, int]:
    """
    Read a XES or CSV event log and return (DataFrame, log_size).

    The DataFrame keeps the original attribute names so that grouping_col
    lookups match the discover_schema output.  event_ts is a tz-aware
    datetime64[ns, UTC] column.
    """
    fmt = path.suffix.lower()
    if fmt == ".xes":
        import pm4py
        # pm4py >= 2.3 returns a pandas DataFrame directly from read_xes.
        # Columns include 'case:concept:name', 'concept:name', 'time:timestamp'
        # plus any event-level attributes.
        df = pm4py.read_xes(str(path))
        rename: dict[str, str] = {}
        for c in df.columns:
            if c == "case:concept:name":
                rename[c] = "case_id"
            elif c == "concept:name":
                rename[c] = "activity"
            elif c == "time:timestamp":
                rename[c] = "event_ts"
        df = df.rename(columns=rename)
    elif fmt == ".csv":
        df = pd.read_csv(path, low_memory=False)
        rename: dict[str, str] = {}
        for c in df.columns:
            if c in ("case:concept:name", "trace_id") and "case_id" not in df.columns:
                rename[c] = "case_id"
            elif c == "concept:name" and "activity" not in df.columns:
                rename[c] = "activity"
            elif c in ("time:timestamp", "timestamp") and "event_ts" not in df.columns:
                rename[c] = "event_ts"
        df = df.rename(columns=rename)
    else:
        raise ValueError(f"Unsupported dataset format: {fmt!r}")

    for col in ("case_id", "activity", "event_ts"):
        if col not in df.columns:
            raise ValueError(f"Required column {col!r} not found — check your dataset")

    df["case_id"] = df["case_id"].astype(str)
    df["activity"] = df["activity"].astype(str)
    df["event_ts"] = pd.to_datetime(df["event_ts"], utc=True, errors="coerce")

    return df, len(df)


def build_complex_mr_sql(nodes: list[PatternNode], grouping_col_sql: str) -> str:
    pattern_parts = []
    define_parts = []
    
    measures_parts = [f"V0.{grouping_col_sql} AS grp_id"]
    
    for i, node in enumerate(nodes):
        var_name = f"V{i}"
        
        # 1. PATTERN clause: Use reluctant quantifiers (*?, +?) to prevent Flink CEP 
        # from crashing on "greedy followed by optional"
        q = node.quantifier
        reluctant_q = f"{q}?" if q in ("*", "+") else q
        
        pattern_parts.append(f"{var_name}{reluctant_q}")
        
        # 2. MEASURES clause: Grab first/last boundaries
        measures_parts.append(f"FIRST({var_name}.pos) AS first_pos_{var_name}")
        measures_parts.append(f"LAST({var_name}.pos) AS last_pos_{var_name}")
        
        # 3. DEFINE clause: Handle logical OR directly in the condition
        conditions = [
            f"{var_name}.activity = '{act.replace(chr(39), chr(39)*2)}'" 
            for act in node.activities
        ]
        define_parts.append(f"{var_name} AS {' OR '.join(conditions)}")
        
        # 4. SKIP variables: MUST be reluctant (*?) and exclude the next node
        if i < len(nodes) - 1:
            skip_var = f"S{i}"
            pattern_parts.append(f"{skip_var}*?")  # Changed from * to *?
            
            next_node = nodes[i + 1]
            exclusions = [
                f"{skip_var}.activity <> '{act.replace(chr(39), chr(39)*2)}'" 
                for act in next_node.activities
            ]
            define_parts.append(f"{skip_var} AS {' AND '.join(exclusions)}")

    pattern_clause = " ".join(pattern_parts)
    define_clause = ",\n        ".join(define_parts)
    measures_clause = ",\n        ".join(measures_parts)

    return (
        f"SELECT *\n"
        f"FROM events\n"
        f"MATCH_RECOGNIZE (\n"
        f"    PARTITION BY {grouping_col_sql}\n"
        f"    ORDER BY ts\n"
        f"    MEASURES \n        {measures_clause}\n"
        f"    ONE ROW PER MATCH\n"
        f"    AFTER MATCH SKIP TO NEXT ROW\n"
        f"    PATTERN ({pattern_clause})\n"
        f"    DEFINE\n"
        f"        {define_clause}\n"
        f") MR"
    )

# ---------------------------------------------------------------------------
# CSV writing (Flink-compatible)
# ---------------------------------------------------------------------------

def write_clean_csv(
    df: pd.DataFrame,
    dest: Path,
    keep_cols: list[str],
) -> list[str]:
    """
    Write a minimal CSV containing only the columns needed for the experiment.

    - Keeps only `keep_cols` (in that order).
    - Formats event_ts as 'YYYY-MM-DD HH:MM:SS.mmm'.
    - Sanitises column names for SQL.
    - Writes WITHOUT a header row (Flink maps positionally via DDL).
    - Replaces commas inside field values with semicolons so Flink's
      basic CSV parser does not misalign columns.

    Returns the ordered list of SQL-safe column names.
    """
    out = df[keep_cols].copy()

    # Drop rows with missing timestamps
    before = len(out)
    out = out.dropna(subset=["event_ts"])
    dropped = before - len(out)
    if dropped:
        print(f"  [warn] Dropped {dropped:,} rows with missing event_ts")

    # Sort globally by event_ts so pos values are temporally ordered
    # within every partition (case_id / resource / etc.)
    out = out.sort_values("event_ts").reset_index(drop=True)

    # Write event_ts as epoch MILLISECONDS (integer).
    # Any CAST(STRING → TIMESTAMP) in a DDL computed column or view is
    # pushed by Flink's optimizer into the CSV source at read time,
    # silently dropping all rows whose format doesn't match.
    # A BIGINT epoch_ms column avoids this: Flink reads it as Long natively,
    # and TO_TIMESTAMP_LTZ(event_ts, 3) in the view is a function call on
    # a BIGINT — unpushable into the CSV source — that produces a proper
    # TIMESTAMP_LTZ time attribute accepted by MATCH_RECOGNIZE ORDER BY.
    ts_utc = out["event_ts"].dt.tz_convert("UTC")
    out["event_ts"] = (ts_utc.astype("int64") // 1_000_000).astype("int64")

    # Sanitise column names
    out.columns = [to_sql_name(c) for c in out.columns]

    # Prepend global position column
    out.insert(0, "pos", range(len(out)))
    sql_columns = list(out.columns)

    # Replace commas in STRING columns only (event_ts is now int, skipped)
    for c in sql_columns:
        if out[c].dtype == object:
            out[c] = out[c].astype(str).str.replace(",", ";", regex=False)

    dest.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(dest, index=False, header=False,
               quoting=3,
               escapechar="\\")
    return sql_columns


# ---------------------------------------------------------------------------
# Make CSV accessible inside Flink container
# ---------------------------------------------------------------------------

def make_complex_nodes(
    base_sequence: list[str], 
    all_activities: list[str], 
    rng: random.Random
) -> list[PatternNode]:
    """
    Transforms a flat list of activities into a complex sequence of PatternNodes
    by injecting ORs and quantifiers, ensuring it remains satisfiable.
    """
    nodes = []
    for act in base_sequence:
        node_acts = [act]
        
        # 50% chance to turn this node into an OR clause by adding a noise activity
        if rng.random() > 0.5:
            noise = rng.choice(all_activities)
            if noise not in node_acts:
                node_acts.append(noise)
                
        # 33% chance for +, 33% chance for *, 33% chance for exact match (no quantifier)
        quantifier = rng.choice(["", "+", "*"])
        
        nodes.append(PatternNode(activities=node_acts, quantifier=quantifier))
        
    return nodes

def _discover_all_flink_containers() -> list[str]:
    """
    Return names of ALL running Flink-related containers.

    The SQL Gateway, JobManager, and TaskManagers are typically separate
    containers.  The file must be present in every container that runs
    actual Flink tasks — i.e. JobManager (split enumeration) and all
    TaskManagers (data reading).  We copy to ALL of them.
    """
    result = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}"],
        capture_output=True, text=True,
    )
    all_names = [n.strip() for n in result.stdout.strip().splitlines() if n.strip()]

    _FLINK_KEYWORDS = (
        "siesta_flink_gw", "siesta_flink_jm", "siesta_flink_tm",
        "flink_gw", "flink_jm", "flink_tm",
        "jobmanager", "taskmanager", "sql-gateway", "sql_gateway",
    )
    found = []
    for name in all_names:
        name_lower = name.lower()
        if any(kw in name_lower for kw in _FLINK_KEYWORDS):
            found.append(name)

    if not found:
        print(
            "[ERROR] docker ps returned no running Flink containers.\n"
            "  Tried keywords: " + ", ".join(_FLINK_KEYWORDS) + "\n"
            "  Start the stack first:\n"
            "    docker compose -f tests/vldb-eval/docker-compose-flink.yml up -d",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"[docker] Found {len(found)} Flink container(s): {found}")
    return found


def ensure_flink_accessible(host_csv: Path, override: Optional[str]) -> str:
    """
    Copy the CSV into EVERY running Flink container and return the
    in-container **directory** path.

    Two issues that cause "0 rows, no error":

    1. Separate containers: JM, TM, and Gateway run in different containers.
       The file must exist on ALL of them.

    2. Directory vs file path: Flink's filesystem connector treats `path` as a
       **directory** to scan for data files.  Pointing it at a single file
       silently produces 0 splits → 0 rows in many Flink versions.

    Fix: create `/tmp/mr_data/` on every container, put the CSV in it,
    and return the directory path for the DDL.
    """
    if override:
        print(f"[csv] Using override path: {override}")
        return override

    container_dir  = "/tmp/mr_data"
    container_file = f"{container_dir}/{host_csv.name}"
    containers = _discover_all_flink_containers()

    for container in containers:
        # Create directory and remove stale file
        subprocess.run(
            ["docker", "exec", container, "sh", "-c",
             f"mkdir -p {container_dir} && rm -f {container_dir}/*"],
            capture_output=True, text=True,
        )
        cp = subprocess.run(
            ["docker", "cp", str(host_csv), f"{container}:{container_file}"],
            capture_output=True, text=True,
        )
        if cp.returncode != 0:
            print(
                f"[ERROR] docker cp → {container} failed:\n{cp.stderr}",
                file=sys.stderr,
            )
            sys.exit(1)
        print(f"[docker cp] {host_csv.name} → {container}:{container_file}")

    # Verify in an execution container (JM or TM), not the gateway
    verify_container = next(
        (c for c in containers
         if any(kw in c.lower() for kw in ("jm", "jobmanager", "tm", "taskmanager"))),
        containers[0],
    )
    # List the directory contents so we can see what Flink will scan
    ls_dir = subprocess.run(
        ["docker", "exec", verify_container, "ls", "-lh", container_dir],
        capture_output=True, text=True,
    )
    print(f"[verify] {verify_container} dir listing ({container_dir}):")
    for line in ls_dir.stdout.strip().splitlines():
        print(f"         {line}")

    head = subprocess.run(
        ["docker", "exec", verify_container, "head", "-3", container_file],
        capture_output=True, text=True,
    )
    print(f"[verify] First 3 data lines:")
    for line in head.stdout.splitlines():
        print(f"         {line}")

    # Return the DIRECTORY — Flink scans this for data files
    return container_dir


# ---------------------------------------------------------------------------
# Flink SQL Gateway client
# ---------------------------------------------------------------------------

class FlinkGateway:
    def __init__(self, base_url: str) -> None:
        self.base = base_url.rstrip("/")

    def _url(self, path: str) -> str:
        return f"{self.base}{path}"

    def open_session(self) -> str:
        r = requests.post(
            self._url("/v1/sessions"),
            json={"properties": {"execution.runtime-mode": "batch"}},
            timeout=10,
        )
        r.raise_for_status()
        return r.json()["sessionHandle"]

    def execute(self, session: str, sql: str) -> str:
        r = requests.post(
            self._url(f"/v1/sessions/{session}/statements"),
            json={"statement": sql},
            timeout=10,
        )
        r.raise_for_status()
        return r.json()["operationHandle"]

    def status(self, session: str, op: str) -> tuple[str, list[str]]:
        """Return (status_string, errors_list). errors_list is empty when status != ERROR."""
        r = requests.get(
            self._url(f"/v1/sessions/{session}/operations/{op}/status"),
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        return data.get("status", "UNKNOWN"), data.get("errors") or []

    def fetch_results(self, session: str, op: str, max_pages: int = 20,
                      verbose: bool = False) -> list[dict]:
        """
        Fetch all result rows by following Flink's token-based pagination.
        """
        token = 0
        col_names: list[str] = []
        out: list[dict] = []

        for page in range(max_pages):
            r = requests.get(
                self._url(f"/v1/sessions/{session}/operations/{op}/result/{token}"),
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()

            if verbose:
                import json as _json
                print(f"    [fetch page={page} token={token}] "
                      f"HTTP {r.status_code}, body keys={list(data.keys())}")
                # Print truncated raw response
                raw_str = _json.dumps(data, default=str)
                if len(raw_str) > 2000:
                    raw_str = raw_str[:2000] + "..."
                print(f"    [raw] {raw_str}")

            result_type = data.get("resultType", "PAYLOAD")

            results_block = data.get("results", {})
            if not col_names:
                cols_meta = results_block.get("columns", [])
                col_names = [c["name"] for c in cols_meta]

            rows_raw = results_block.get("data", [])
            for row in rows_raw:
                fields = row.get("fields", row)
                if isinstance(fields, list):
                    out.append(dict(zip(col_names, fields)))
                elif isinstance(fields, dict):
                    out.append(fields)
                else:
                    # Flink may return a different structure
                    out.append({"_raw": fields})

            if result_type == "EOS" or not data.get("nextResultUri"):
                break

            token += 1

        return out

    def fetch_error_detail(self, session: str, op: str) -> str:
        """
        Best-effort: pull the human-readable error from the result endpoint.
        Flink sometimes puts the stack trace there rather than in status.errors.
        Returns an empty string if nothing useful is found.
        """
        try:
            r = requests.get(
                self._url(f"/v1/sessions/{session}/operations/{op}/result/0"),
                timeout=10,
            )
            body = r.json()
            # Non-2xx responses often carry the error in the body
            errs = body.get("errors") or []
            if errs:
                return "; ".join(str(e) for e in errs)
            # Some versions nest it under results.errors
            errs2 = body.get("results", {}).get("errors") or []
            if errs2:
                return "; ".join(str(e) for e in errs2)
        except Exception:
            pass
        return ""

    def cancel(self, session: str, op: str) -> None:
        try:
            requests.post(
                self._url(f"/v1/sessions/{session}/operations/{op}/cancel"),
                timeout=10,
            ).raise_for_status()
        except Exception:
            pass  # best-effort; session may have already closed

    def close_session(self, session: str) -> None:
        try:
            requests.delete(self._url(f"/v1/sessions/{session}"), timeout=10).raise_for_status()
        except Exception:
            pass


def wait_for_gateway(base_url: str, retries: int = 12, delay: float = 3.0) -> None:
    for attempt in range(retries):
        try:
            r = requests.get(f"{base_url}/v1/info", timeout=5)
            if r.status_code == 200:
                return
        except requests.ConnectionError:
            pass
        print(f"[gateway] Not ready yet (attempt {attempt + 1}/{retries}), retrying in {delay}s …")
        time.sleep(delay)
    raise RuntimeError(f"Flink SQL Gateway not reachable at {base_url} after {retries} attempts")


def _poll_until_done(
    gw: FlinkGateway,
    session: str,
    op: str,
    label: str = "",
    timeout_s: float = 120.0,
) -> None:
    t0 = time.perf_counter()
    while True:
        st, errors = gw.status(session, op)
        if st == "FINISHED":
            return
        if st in ("ERROR", "CANCELED"):
            detail = "; ".join(errors) if errors else gw.fetch_error_detail(session, op) or st
            raise RuntimeError(f"Gateway op {label!r} ended with {st}: {detail}")
        if time.perf_counter() - t0 > timeout_s:
            raise TimeoutError(f"Gateway op {label!r} timed out after {timeout_s:.0f}s")
        time.sleep(1.0)


# ---------------------------------------------------------------------------
# Session setup and table registration
# ---------------------------------------------------------------------------

def setup_session(gw: FlinkGateway, session: str) -> None:
    for sql, label in [
        ("SET 'execution.runtime-mode' = 'batch'", "set-batch-mode"),
        ("SET 'parallelism.default' = '4'",         "set-parallelism"),
    ]:
        op = gw.execute(session, sql)
        _poll_until_done(gw, session, op, label)


def register_table(
    gw: FlinkGateway,
    session: str,
    csv_columns: list[str],   # SQL-safe, in the exact order written to the CSV
    flink_path: str,
) -> None:
    """
    CREATE TABLE events with columns in the same order as the header-less CSV.
    Flink's filesystem CSV connector maps positionally, so the order must match.

    The first column is `pos BIGINT` — a global integer position assigned after
    sorting the pandas DataFrame by event_ts.  MATCH_RECOGNIZE uses `ORDER BY ts`
    to sort within partitions, avoiding all TIMESTAMP/STRING parsing issues while
    guaranteeing correct temporal ordering (pos is monotone with event_ts).

    All other columns are STRING.
    """
    col_defs = ",\n".join(
        f"    pos       BIGINT" if c == "pos"
        else f"    event_ts  BIGINT" if c == "event_ts"
        else f"    {c}  STRING"
        for c in csv_columns
    )
    # Physical table: pos and event_ts are BIGINT, everything else STRING.
    # event_ts holds epoch milliseconds — Flink reads it as Long with no
    # type-conversion pathway to push into the source.
    # The VIEW converts epoch_ms → TIMESTAMP_LTZ via TO_TIMESTAMP_LTZ(event_ts, 3),
    # a function call that Flink cannot inline into the filesystem CSV reader.
    # TIMESTAMP_LTZ is a recognised time attribute, so MATCH_RECOGNIZE ORDER BY ts
    # triggers correct within-partition sorting.
    ddl = (
        f"CREATE TABLE IF NOT EXISTS events_raw (\n"
        f"{col_defs}\n"
        f") WITH (\n"
        f"    'connector' = 'filesystem',\n"
        f"    'path' = 'file://{flink_path}',\n"
        f"    'format' = 'csv'\n"
        f")"
    )
    view_cols = ", ".join(csv_columns)
    view_ddl = (
        f"CREATE VIEW events AS\n"
        f"    SELECT {view_cols},\n"
        f"           TO_TIMESTAMP_LTZ(event_ts, 3) AS ts\n"
        f"    FROM events_raw"
    )

    for sql, label in [
        ("DROP VIEW IF EXISTS events",      "DROP VIEW"),
        ("DROP TABLE IF EXISTS events_raw", "DROP TABLE"),
    ]:
        op = gw.execute(session, sql)
        _poll_until_done(gw, session, op, label, timeout_s=30.0)

    print(f"  DDL:\n{ddl}")
    op = gw.execute(session, ddl)
    _poll_until_done(gw, session, op, "CREATE TABLE", timeout_s=120.0)

    print(f"  VIEW:\n{view_ddl}")
    op = gw.execute(session, view_ddl)
    _poll_until_done(gw, session, op, "CREATE VIEW", timeout_s=30.0)


# ---------------------------------------------------------------------------
# Table verification
# ---------------------------------------------------------------------------

def verify_table(gw: FlinkGateway, session: str) -> int:
    """
    Run COUNT(*) and LIMIT-5 spot-check against the events table.
    Prints results and returns the row count.
    Raises RuntimeError if the table appears empty or the query errors.
    """
    # COUNT
    op = gw.execute(session, "SELECT COUNT(*) AS n FROM events")
    _poll_until_done(gw, session, op, "COUNT(*)", timeout_s=300.0)
    rows = gw.fetch_results(session, op)
    count = int(rows[0]["n"]) if rows else 0
    print(f"  events table row count: {count:,}")
    if count == 0:
        raise RuntimeError(
            "events table is empty — CSV path wrong or file not visible to Flink"
        )

    # LIMIT 5 sample
    op2 = gw.execute(session, "SELECT * FROM events LIMIT 5")
    _poll_until_done(gw, session, op2, "LIMIT 5", timeout_s=120.0)
    sample = gw.fetch_results(session, op2)
    print("  Sample rows:")
    for row in sample:
        print(f"    {row}")

    return count


# ---------------------------------------------------------------------------
# Satisfiable workload generation
# ---------------------------------------------------------------------------

def generate_satisfiable_patterns(
    df: pd.DataFrame,
    grouping_col: str,      # raw column name (pre-sanitisation)
    pattern_length: int,
    n_patterns: int = N_PATTERNS_DEFAULT,
    rng_seed: int = 42,
) -> list[list[str]]:
    """
    Return activity sequences of exactly `pattern_length` items, each
    guaranteed to appear as a consecutive subsequence in at least one
    group of `grouping_col`.
    """
    rng = random.Random(rng_seed)

    # Resolve column: prefer the raw name, fall back to SQL-sanitised name
    col = grouping_col if grouping_col in df.columns else to_sql_name(grouping_col)
    if col not in df.columns:
        return []

    candidates: list[tuple[str, ...]] = []
    for _, grp in df.groupby(col, sort=False):
        acts = grp.sort_values("event_ts")["activity"].tolist()
        for i in range(len(acts) - pattern_length + 1):
            candidates.append(tuple(acts[i : i + pattern_length]))

    unique = list(dict.fromkeys(candidates))   # deduplicate, preserve order
    rng.shuffle(unique)
    return [list(seq) for seq in unique[:n_patterns]]


# ---------------------------------------------------------------------------
# MATCH_RECOGNIZE SQL builder
# ---------------------------------------------------------------------------

def build_mr_sql(activities: list[str], grouping_col_sql: str) -> str:
    pos_vars  = _POS_VARS[: len(activities)]
    skip_vars = [f"S{i}" for i in range(len(activities) - 1)]

    # Interleave: A S0* B S1* C ...
    parts: list[str] = []
    for i, pv in enumerate(pos_vars):
        parts.append(pv)
        if i < len(skip_vars):
            parts.append(f"{skip_vars[i]}*")
    pattern_clause = " ".join(parts)

    define_parts = [
        f"{v} AS {v}.activity = '{act.replace(chr(39), chr(39) * 2)}'"
        for v, act in zip(pos_vars, activities)
    ]
    for i, sv in enumerate(skip_vars):
        next_act = activities[i + 1].replace("'", "''")
        define_parts.append(f"{sv} AS {sv}.activity <> '{next_act}'")
    define_clause = ",\n        ".join(define_parts)

    # MEASURES: Explicitly project the 'pos' column for every target variable
    measures_parts = [f"{pos_vars[0]}.{grouping_col_sql} AS grp_id"]
    for v in pos_vars:
        measures_parts.append(f"{v}.pos AS pos_{v}")
    measures_clause = ",\n        ".join(measures_parts)

    # SELECT *: Return the raw match rows instead of an aggregate COUNT
    return (
        f"SELECT *\n"
        f"FROM events\n"
        f"MATCH_RECOGNIZE (\n"
        f"    PARTITION BY {grouping_col_sql}\n"
        f"    ORDER BY ts\n"
        f"    MEASURES \n        {measures_clause}\n"
        f"    ONE ROW PER MATCH\n"
        f"    AFTER MATCH SKIP TO NEXT ROW\n"
        f"    PATTERN ({pattern_clause})\n"
        f"    DEFINE\n"
        f"        {define_clause}\n"
        f") MR"
    )


# ---------------------------------------------------------------------------
# Timed query execution with timeout + cancellation
# ---------------------------------------------------------------------------

def run_timed_query(
    gw: FlinkGateway,
    session: str,
    sql: str,
    timeout_s: float = QUERY_TIMEOUT_S,
    poll_interval_s: float = 1.0,
) -> dict:
    """
    Execute a MATCH_RECOGNIZE query and collect the COUNT result.

    Flink emits many empty pages while the job runs (one per checkpoint or
    output batch).  For a query over 1.2M events the result row may arrive
    at token 500+.  The key insight: when a page is empty but nextResultUri
    exists, advance the token IMMEDIATELY — no sleep.  Only sleep when there
    is no nextResultUri (job hasn't produced anything yet) or when we need
    to poll status.  This burns through empty pages at network speed (~ms
    each) rather than 2 s each.
    """
    t0 = time.perf_counter()
    op = gw.execute(session, sql)

    token: int = 0
    col_names: list[str] = []
    collected_rows: list[dict] = []
    error_detail = None

    while True:
        elapsed = time.perf_counter() - t0

        if elapsed >= timeout_s:
            gw.cancel(session, op)
            return {
                "latency_s":      elapsed,
                "match_count":    None,
                "group_count":    None,
                "timed_out":      True,
                "gateway_status": "TIMEOUT",
                "error_detail":   None,
            }

        # ── Fetch the current result page ─────────────────────────────
        # ── Fetch the current result page ─────────────────────────────
        try:
            r = requests.get(
                gw._url(f"/v1/sessions/{session}/operations/{op}/result/{token}"),
                timeout=30,
            )
            # [FIX] If the result endpoint fails, do NOT continue. 
            # Raise an exception so the code falls through to check the job status.
            r.raise_for_status() 

            data = r.json()
            result_type = data.get("resultType", "PAYLOAD")
            
            if result_type == "NOT_READY":
                time.sleep(0.5)
                continue

            results_block = data.get("results", {})

            if not col_names:
                col_names = [c["name"] for c in results_block.get("columns", [])]

            rows_raw = results_block.get("data", [])
            for row in rows_raw:
                fields = row.get("fields", row)
                if isinstance(fields, list):
                    collected_rows.append(dict(zip(col_names, fields)))
                elif isinstance(fields, dict):
                    collected_rows.append(fields)

            if rows_raw:
                token += 1
                continue

            if result_type == "EOS":
                break

            if data.get("nextResultUri"):
                next_uri = data["nextResultUri"]
                token = int(next_uri.rstrip("/").split("/")[-1])
                continue

        except Exception:
            # [FIX] Do NOT sleep/continue here. Pass, so the loop falls down 
            # to the status check below.
            pass

        # ── No data, no nextResultUri — check status ──────────────────
        try:
            st, errors = gw.status(session, op)
            if st == "ERROR":
                error_detail = ("; ".join(errors)
                                if errors
                                else gw.fetch_error_detail(session, op) or None)
                return {
                    "latency_s":      time.perf_counter() - t0,
                    "match_count":    None,
                    "group_count":    None,
                    "timed_out":      False,
                    "gateway_status": "ERROR",
                    "error_detail":   error_detail,
                }
            if st == "FINISHED":
                # Job done — drain remaining pages until EOS.
                # Flink always emits a final EOS page; that is the definitive
                # stop signal.  We advance immediately through empty pages
                # (no sleep) since the result row is buffered at some token.
                # The outer timeout guards against pathological cases.
                drain_deadline = time.perf_counter() + min(60.0, timeout_s - elapsed)
                while time.perf_counter() < drain_deadline:
                    try:
                        r2 = requests.get(
                            gw._url(f"/v1/sessions/{session}/operations/{op}/result/{token}"),
                            timeout=30,
                        )
                        if r2.status_code != 200:
                            break
                        d2 = r2.json()
                        rb2 = d2.get("results", {})
                        if not col_names:
                            col_names = [c["name"] for c in rb2.get("columns", [])]
                        for row in rb2.get("data", []):
                            fields = row.get("fields", row)
                            if isinstance(fields, list):
                                collected_rows.append(dict(zip(col_names, fields)))
                            elif isinstance(fields, dict):
                                collected_rows.append(fields)
                        # EOS is the definitive end — stop regardless of data
                        if d2.get("resultType") == "EOS":
                            break
                        if d2.get("nextResultUri"):
                            token += 1  # advance immediately, no sleep
                        else:
                            time.sleep(0.2)  # no next page yet — brief wait
                    except Exception:
                        break
                break
        except Exception:
            pass

        time.sleep(poll_interval_s)

    latency = time.perf_counter() - t0
    
    # Calculate counts dynamically from the returned match rows
    match_count = len(collected_rows)
    distinct_groups = {row.get("grp_id") for row in collected_rows if row.get("grp_id") is not None}
    group_count = len(distinct_groups)

    # Optional: Print a preview of the actual positions to the console
    if match_count > 0:
        print(f"\n      [Matches Preview] Found {match_count} total:")

    return {
        "latency_s":      latency,
        "match_count":    match_count,
        "group_count":    group_count,
        "timed_out":      False,
        "gateway_status": "FINISHED",
        "error_detail":   error_detail,
    }


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="MATCH_RECOGNIZE multiperspective latency experiment",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--dataset",  default=None,
                        help="Path to the event log (XES or CSV). "
                             "Falls back to EVAL_DATASET env var.")
    parser.add_argument("--log-name", default=None,
                        help="Logical name for the dataset. "
                             "Falls back to EVAL_LOG_NAME env var.")
    parser.add_argument("--gateway",
                        default=os.environ.get("MR_GATEWAY", "http://localhost:8083"),
                        help="Flink SQL Gateway REST base URL.")
    parser.add_argument("--flink-data-path", default=None,
                        help="Override the in-container CSV path. "
                             "If omitted, the volume-mount path is used.")
    parser.add_argument("--n-patterns", type=int, default=N_PATTERNS_DEFAULT,
                        help="Satisfiable patterns to run per (grouping, length) cell.")
    parser.add_argument("--timeout", type=float, default=QUERY_TIMEOUT_S,
                        help="Wall-clock timeout (s) per query before cancellation.")
    parser.add_argument("--groupings", nargs="+", default=None,
                        help="Explicit grouping columns (raw names). "
                             "If omitted, auto-discovered via discover_schema + case_id.")
    args = parser.parse_args()

    spec = resolve_dataset(args.dataset, args.log_name)
    rec  = Recorder("mr_multiperspective", output_name="exp_mr.jsonl")

    _step = 0
    def step(msg: str) -> None:
        nonlocal _step
        _step += 1
        print(f"\n{'='*60}")
        print(f"  Step {_step}: {msg}")
        print(f"{'='*60}")

    # 1. Load and normalise
    step(f"Load dataset  [{spec.path.name}]")
    df, log_size = load_and_normalise(spec.path)
    print(f"  {log_size:,} events  |  {df['case_id'].nunique():,} cases")

    # 2. Discover grouping perspectives (must happen BEFORE CSV writing
    #    so we know which columns to keep)
    step("Discover grouping perspectives")
    if args.groupings:
        grouping_cols = args.groupings
        print(f"  (explicit)  {grouping_cols}")
    else:
        schema = discover_schema(spec.path)
        grouping_cols = ["case_id"] + schema.perspective_keys
        print(f"  Auto-discovered {len(grouping_cols)} grouping(s): {grouping_cols}")

    # 3. Write minimal CSV — only case_id, activity, event_ts, and grouping cols
    step(f"Write minimal CSV → {_TMP_CSV}")
    keep_cols = list(dict.fromkeys(
        ["case_id", "activity", "event_ts"] + grouping_cols
    ))  # deduplicate while preserving order (case_id already in the base set)
    print(f"  Keeping {len(keep_cols)} columns: {keep_cols}")
    csv_columns = write_clean_csv(df, _TMP_CSV, keep_cols)
    print(f"  SQL columns: {csv_columns}")
    flink_path = ensure_flink_accessible(_TMP_CSV, args.flink_data_path)
    print(f"  In-container path: {flink_path}")

    # 4. Connect to SQL Gateway
    step(f"Connect to SQL Gateway  [{args.gateway}]")
    wait_for_gateway(args.gateway)
    gw      = FlinkGateway(args.gateway)
    session = gw.open_session()
    print(f"  Session handle: {session}")
    setup_session(gw, session)
    print(f"  Session configured (batch mode, parallelism=4)")

    # 5. Register events table
    step("Register events table (DDL)")
    t_ddl = time.perf_counter()
    register_table(gw, session, csv_columns, flink_path)
    ddl_s = time.perf_counter() - t_ddl
    print(f"  Done in {ddl_s:.1f}s")
    rec.emit(
        "table_registered",
        dataset=spec.log_name,
        registration_s=ddl_s,
        flink_path=flink_path,
        log_size=log_size,
    )

    # 6. Verify data ingestion
    step("Verify events table contents")
    verify_table(gw, session)

    # 6b. Diagnostic: what does Flink actually see in each column?
    step("Diagnostic — column contents in Flink")

    # What's in the 'activity' column?
    op_act = gw.execute(session,
        "SELECT activity, COUNT(*) AS cnt FROM events GROUP BY activity ORDER BY cnt DESC LIMIT 10")
    _poll_until_done(gw, session, op_act, "activity-counts", timeout_s=120.0)
    act_rows = gw.fetch_results(session, op_act)
    print("  Top 10 activity values in Flink:")
    for r in act_rows:
        print(f"    {r}")

    # What's in the 'case_id' column?
    op_cid = gw.execute(session,
        "SELECT case_id, COUNT(*) AS cnt FROM events GROUP BY case_id ORDER BY cnt DESC LIMIT 5")
    _poll_until_done(gw, session, op_cid, "case_id-counts", timeout_s=120.0)
    cid_rows = gw.fetch_results(session, op_cid)
    print("  Top 5 case_id values in Flink:")
    for r in cid_rows:
        print(f"    {r}")

    # What pandas expects:
    pandas_acts = df["activity"].value_counts().head(10)
    print(f"\n  Top 10 activity values in Pandas:")
    for act, cnt in pandas_acts.items():
        print(f"    {act!r}: {cnt}")

    # Simple WHERE filter — does Flink find the most common activity at all?
    top_act = str(act_rows[0].get("activity", "")) if act_rows else ""
    if top_act:
        op_where = gw.execute(session,
            f"SELECT COUNT(*) AS n FROM events WHERE activity = '{top_act.replace(chr(39), chr(39)*2)}'")
        _poll_until_done(gw, session, op_where, "WHERE-filter", timeout_s=60.0)
        where_rows = gw.fetch_results(session, op_where)
        print(f"\n  WHERE activity = '{top_act}': {where_rows}")


    # 7. Run the experiment grid
    n_groupings = len(grouping_cols)
    n_lengths   = len(PATTERN_LENGTHS)
    step(f"Run experiment grid  "
         f"[{n_groupings} grouping(s) × {n_lengths} lengths × {args.n_patterns} queries]")

    # Extract all unique activities for generating OR noise
    all_acts = df["activity"].unique().tolist()
    complex_rng = random.Random(42)

    for g_idx, grouping_col in enumerate(grouping_cols, 1):
        sql_col   = to_sql_name(grouping_col)
        col_in_df = grouping_col if grouping_col in df.columns else sql_col
        n_groups  = int(df[col_in_df].nunique()) if col_in_df in df.columns else 0

        print(f"\n  ┌─ Grouping {g_idx}/{n_groupings}: {grouping_col!r}  "
              f"({n_groups:,} distinct values)")

        for length in PATTERN_LENGTHS:
            # Get the base satisfiable sequences first
            patterns = generate_satisfiable_patterns(
                df, grouping_col, length, args.n_patterns
            )
            
            if not patterns:
                print(f"  │  len={length}  [SKIP] no satisfiable patterns found")
                continue

            avail = len(patterns)
            warn  = f"  (only {avail} available)" if avail < args.n_patterns else ""
            print(f"  │  len={length}  running {avail} queries{warn}")

            for i, base_activities in enumerate(patterns):
                # Upgrade the flat sequence into complex nodes
                nodes = make_complex_nodes(base_activities, all_acts, complex_rng)
                
                # Build the SQL using the new complex function
                sql = build_complex_mr_sql(nodes, sql_col)
                
                # Generate a clean string preview for the console (e.g., A (B|C)+ D*)
                preview_parts = []
                for n in nodes:
                    inner = " | ".join(n.activities)
                    q = n.quantifier
                    preview_parts.append(f"({inner}){q}" if len(n.activities) > 1 else f"{inner}{q}")
                preview = " ".join(preview_parts)
                
                # Truncate preview if it's too long for the console
                if len(preview) > 60:
                    preview = preview[:57] + "..."

                print(f"  │    [{i + 1}/{avail}] [{preview}]", end="  ", flush=True)

                result = run_timed_query(
                    gw, session, sql, timeout_s=args.timeout
                )

                status_str = "TIMEOUT" if result["timed_out"] else result["gateway_status"]
                print(f"→ {result['latency_s']:.1f}s  "
                      f"matches={result['match_count']}  "
                      f"groups={result['group_count']}  {status_str}", flush=True)
                
                if result["gateway_status"] in ("ERROR", "CANCELED"):
                    if result.get("error_detail"):
                        print(f"       ERROR DETAIL: {result['error_detail']}")
                    print(f"       SQL submitted:\n{sql}")

                # Update Recorder emit payload
                rec.emit(
                    "query",
                    system         = "match_recognize_complex",
                    dataset        = spec.log_name,
                    grouping       = grouping_col,
                    pattern_length = length,
                    qid            = f"MRC_{grouping_col[:4]}_{length:02d}_{i:02d}",
                    sql_pattern    = preview,
                    latency_s      = result["latency_s"],
                    match_count    = result["match_count"],
                    group_count    = result["group_count"],
                    timed_out      = result["timed_out"],
                    gateway_status = result["gateway_status"],
                    error_detail   = result.get("error_detail"),
                    log_size       = log_size,
                    n_groups       = n_groups,
                )

        print(f"  └─ Grouping {g_idx}/{n_groupings} complete")

    gw.close_session(session)
    print(f"\n{'='*60}")
    print(f"  Done.  Results → {rec.path}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()