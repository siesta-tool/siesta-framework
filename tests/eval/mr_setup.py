"""
tests/eval/mr_setup.py

Setup script for the MATCH_RECOGNIZE (Flink) baseline.

Does three things:
  1. Converts the event log (XES or CSV) to a flat CSV that Flink's
     filesystem connector can read — column names are sanitized
     (colons stripped) so they are valid Flink SQL identifiers.

  2. Creates a Flink SQL Gateway session, sets BATCH mode, and
     registers the event_log table with a CREATE TABLE DDL.

  3. Prints (and optionally writes to a file) the session handle so
     that the experiment runner can reuse it across many queries
     without re-creating the session each time.

Usage
-----
# Convert + register (run once before the experiments):
python -m tests.eval.mr_setup \
    --dataset datasets/bpic_2017.xes \
    --csv-out  datasets/bpic_2017_flink.csv

# The session handle is written to .mr_session by default.
# The experiment reads it via MR_SESSION_FILE env var.

Environment variables
---------------------
MR_ENDPOINT      SQL Gateway base URL   (default: http://localhost:8083)
MR_TABLE         Flink table name       (default: event_log)
MR_SESSION_FILE  Path to save handle   (default: .mr_session)
MR_CSV_PATH      In-container CSV path (default: /data/<csv_filename>)

Column mapping
--------------
XES attribute     Flink column name
------------      ----------------
trace_id          trace_id
activity          activity
start_timestamp   start_timestamp
org:resource      org_resource
lifecycle:trans.  lifecycle_transition
org:group         org_group
org:role          org_role
resource          resource
role              role
cost              cost

The _build_mr_sql() function in the experiment uses sanitized names
(attr.replace(':', '_')) for DEFINE predicates so they match this DDL.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from pathlib import Path

import requests

MR_ENDPOINT      = os.environ.get("MR_ENDPOINT",      "http://localhost:8083")
MR_TABLE         = os.environ.get("MR_TABLE",         "event_log")
MR_SESSION_FILE  = os.environ.get("MR_SESSION_FILE",  ".mr_session")
MR_CSV_PATH_ENV  = os.environ.get("MR_CSV_PATH",      "")   # optional override

# ---------------------------------------------------------------------------
# Column sanitisation
# ---------------------------------------------------------------------------

def sanitize_col(name: str) -> str:
    """Make an attribute name a valid Flink SQL identifier."""
    return re.sub(r"[^A-Za-z0-9_]", "_", name)


# Columns always present in the DDL, in order
_CORE_COLS = ["trace_id", "activity", "start_timestamp", "position"]

# Well-known extra attributes — include in DDL so experiments can reference them
_KNOWN_ATTRS = [
    ("org:resource",           "org_resource",          "STRING"),
    ("lifecycle:transition",   "lifecycle_transition",  "STRING"),
    ("org:group",              "org_group",             "STRING"),
    ("org:role",               "org_role",              "STRING"),
    ("resource",               "resource",              "STRING"),
    ("role",                   "role",                  "STRING"),
    ("cost",                   "cost",                  "DOUBLE"),
    ("concept:name",           "concept_name",          "STRING"),
]

_ATTR_MAP: dict[str, str] = {src: dst for src, dst, _ in _KNOWN_ATTRS}
_COL_TYPE: dict[str, str] = {dst: typ for _, dst, typ in _KNOWN_ATTRS}


# ---------------------------------------------------------------------------
# Step 1 — Event log → Flink CSV
# ---------------------------------------------------------------------------

def convert_to_flink_csv(src: Path, dst: Path) -> list[str]:
    """
    Convert an XES or CSV event log to a flat CSV for Flink.
    Returns the list of extra (non-core) column names written.
    """
    suffix = src.suffix.lower()
    if suffix == ".xes":
        rows, extra_cols = _read_xes(src)
    elif suffix == ".csv":
        rows, extra_cols = _read_csv(src)
    else:
        raise ValueError(f"Unsupported format: {suffix}")

    all_cols = _CORE_COLS + extra_cols
    dst.parent.mkdir(parents=True, exist_ok=True)

    with dst.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_cols,
                                extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({c: row.get(c, "") for c in all_cols})

    print(f"  Written {dst}  ({sum(1 for _ in dst.open()) - 1:,} rows)")
    return extra_cols


def _norm_ts(ts: str) -> str:
    """Normalise timestamp to yyyy-MM-dd HH:mm:ss.SSS for Flink CSV parsing."""
    if not ts:
        return ""
    ts = ts.strip().replace("T", " ").rstrip("Z")
    # Drop timezone offset
    ts = re.sub(r"[+-]\d{2}:\d{2}$", "", ts)
    # Pad millis
    if "." not in ts:
        ts += ".000"
    return ts


def _read_xes(path: Path) -> tuple[list[dict], list[str]]:
    from lxml import etree

    NS        = "{http://www.xes-standard.org/}"
    ATTR_TAGS = {NS + t for t in ("string", "int", "float", "boolean", "date", "id")}
    TS_KEYS   = {"time:timestamp", "timestamp", "start_timestamp"}
    ACT_KEYS  = {"concept:name", "activity"}

    rows: list[dict] = []
    extra_seen: dict[str, str] = {}   # sanitized_name → type

    ctx = etree.iterparse(str(path), events=("end",), tag=NS + "trace")
    for _, trace_elem in ctx:
        # trace_id
        trace_id = "unknown"
        for ch in trace_elem:
            if ch.tag in ATTR_TAGS and ch.get("key") in {"concept:name", "case:concept:name"}:
                trace_id = ch.get("value", "unknown")
                break

        pos = 0
        for ev in trace_elem.iter(NS + "event"):
            row: dict = {"trace_id": trace_id, "position": str(pos)}
            pos += 1
            for ch in ev:
                if ch.tag not in ATTR_TAGS:
                    continue
                key = ch.get("key", "")
                val = ch.get("value", "")
                if key in ACT_KEYS and "activity" not in row:
                    row["activity"] = val
                elif key in TS_KEYS and "start_timestamp" not in row:
                    row["start_timestamp"] = _norm_ts(val)
                else:
                    col = _ATTR_MAP.get(key, sanitize_col(key))
                    row[col] = val
                    if col not in extra_seen and col not in _CORE_COLS:
                        # Guess type
                        extra_seen[col] = (
                            "DOUBLE" if _is_num(val) else "STRING"
                        )
            if "activity" in row and "start_timestamp" in row:
                rows.append(row)
        trace_elem.clear()

    # Build extra_cols in stable order: known attrs first, then discovered
    extra_cols = [dst for _, dst, _ in _KNOWN_ATTRS if dst in extra_seen]
    extra_cols += [c for c in extra_seen if c not in extra_cols]
    return rows, extra_cols


def _read_csv(path: Path) -> tuple[list[dict], list[str]]:
    TRACE_COLS = ["trace_id", "case:concept:name", "case_id", "CaseID"]
    ACT_COLS   = ["activity", "concept:name", "Activity"]
    TS_COLS    = ["start_timestamp", "time:timestamp", "timestamp", "Timestamp"]

    def _pick(row: dict, cands: list[str]) -> tuple[str, str | None]:
        for c in cands:
            if c in row:
                return c, row[c]
        return "", None

    rows: list[dict] = []
    extra_seen: dict[str, str] = {}
    trace_pos: dict[str, int] = {}

    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            tc, trace_id = _pick(raw, TRACE_COLS)
            ac, activity = _pick(raw, ACT_COLS)
            sc, ts       = _pick(raw, TS_COLS)
            if not trace_id or not activity:
                continue
            pos = trace_pos.get(trace_id, 0)
            trace_pos[trace_id] = pos + 1
            row = {
                "trace_id":        trace_id,
                "activity":        activity,
                "start_timestamp": _norm_ts(ts or ""),
                "position":        str(pos),
            }
            skip = {tc, ac, sc} | set(TRACE_COLS) | set(ACT_COLS) | set(TS_COLS)
            for k, v in raw.items():
                if k in skip or not v:
                    continue
                col = _ATTR_MAP.get(k, sanitize_col(k))
                row[col] = v
                if col not in extra_seen and col not in _CORE_COLS:
                    extra_seen[col] = "DOUBLE" if _is_num(v) else "STRING"
            rows.append(row)

    extra_cols = [dst for _, dst, _ in _KNOWN_ATTRS if dst in extra_seen]
    extra_cols += [c for c in extra_seen if c not in extra_cols]
    return rows, extra_cols


def _is_num(s: str) -> bool:
    try:
        float(s)
        return True
    except (TypeError, ValueError):
        return False


# ---------------------------------------------------------------------------
# Step 2 — Flink SQL Gateway session + table registration
# ---------------------------------------------------------------------------

def _wait_for_gateway(timeout: int = 60) -> None:
    print(f"Waiting for SQL Gateway at {MR_ENDPOINT} ...", end="", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{MR_ENDPOINT}/v1/info", timeout=3)
            if r.status_code == 200:
                print(" ready.")
                return
        except Exception:
            pass
        print(".", end="", flush=True)
        time.sleep(2)
    raise TimeoutError(f"SQL Gateway not reachable after {timeout}s")


def create_session() -> str:
    """Open a new SQL Gateway session and return its handle."""
    r = requests.post(
        f"{MR_ENDPOINT}/v1/sessions",
        json={"properties": {"execution.runtime-mode": "BATCH"}},
        timeout=10,
    )
    r.raise_for_status()
    handle = r.json()["sessionHandle"]
    print(f"  Session: {handle}")
    return handle


def _run_statement(session: str, sql: str, label: str = "") -> None:
    """Submit a statement and poll until it finishes."""
    r = requests.post(
        f"{MR_ENDPOINT}/v1/sessions/{session}/statements",
        json={"statement": sql},
        timeout=30,
    )
    r.raise_for_status()
    op = r.json()["operationHandle"]

    deadline = time.time() + 60
    while time.time() < deadline:
        sr = requests.get(
            f"{MR_ENDPOINT}/v1/sessions/{session}/operations/{op}/status",
            timeout=10,
        )
        sr.raise_for_status()
        state = sr.json().get("status")
        if state == "FINISHED":
            if label:
                print(f"  OK: {label}")
            return
        if state in ("ERROR", "CANCELED"):
            raise RuntimeError(f"Statement failed ({state}): {sql[:120]}")
        time.sleep(0.5)
    raise TimeoutError(f"Statement timed out: {label}")


def build_table_ddl(
    table: str,
    csv_path_in_container: str,
    extra_cols: list[str],
) -> str:
    """
    Build a CREATE TABLE DDL for the filesystem connector.
    Uses STRING for all attribute columns (Flink CSV connector reads
    everything as text; casts happen in DEFINE clauses if needed).
    """
    col_defs = [
        "  `trace_id`        STRING",
        "  `activity`        STRING",
        "  `start_timestamp` TIMESTAMP(3)",
        "  `position`        INT",
    ]
    for col in extra_cols:
        typ = _COL_TYPE.get(col, "STRING")
        col_defs.append(f"  `{col}` {typ}")

    cols_sql = ",\n".join(col_defs)

    return f"""
CREATE TABLE IF NOT EXISTS `{table}` (
{cols_sql}
) WITH (
  'connector'              = 'filesystem',
  'path'                   = 'file://{csv_path_in_container}',
  'format'                 = 'csv',
  'csv.field-delimiter'    = ',',
  'csv.ignore-parse-errors'= 'true'
)
""".strip()


def setup_session(
    session: str,
    csv_path_in_container: str,
    extra_cols: list[str],
) -> None:
    """Configure the session and register the event_log table."""
    # Ensure batch mode is set (belt-and-suspenders)
    _run_statement(session,
                   "SET 'execution.runtime-mode' = 'BATCH'",
                   "batch mode")
    # Drop old table if present (idempotent re-runs)
    _run_statement(session,
                   f"DROP TABLE IF EXISTS `{MR_TABLE}`",
                   "drop old table")
    # Create the table
    ddl = build_table_ddl(MR_TABLE, csv_path_in_container, extra_cols)
    print(f"\n  DDL:\n{ddl}\n")
    _run_statement(session, ddl, f"create table {MR_TABLE}")

    # Verify with a COUNT
    r = requests.post(
        f"{MR_ENDPOINT}/v1/sessions/{session}/statements",
        json={"statement": f"SELECT COUNT(*) FROM `{MR_TABLE}`"},
        timeout=30,
    )
    r.raise_for_status()
    op = r.json()["operationHandle"]

    deadline = time.time() + 120
    while time.time() < deadline:
        sr = requests.get(
            f"{MR_ENDPOINT}/v1/sessions/{session}/operations/{op}/status",
            timeout=10,
        )
        if sr.json().get("status") == "FINISHED":
            rr = requests.get(
                f"{MR_ENDPOINT}/v1/sessions/{session}/operations/{op}/result/0",
                timeout=30,
            )
            data = rr.json().get("results", {}).get("data", [])
            count = data[0].get("fields", [0])[0] if data else "?"
            print(f"  Verified: {count} rows in {MR_TABLE}")
            return
        time.sleep(1)
    print("  [WARN] COUNT verification timed out — table may still be fine")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Convert event log to Flink CSV and register it in SQL Gateway.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Steps performed:
  1. Convert XES/CSV → flat Flink-friendly CSV (sanitized column names)
  2. Open a SQL Gateway session with BATCH mode
  3. CREATE TABLE event_log pointing to the CSV
  4. Write the session handle to .mr_session (or MR_SESSION_FILE)

The session handle is then read by the experiment runners so they don't
need to create a new session (and re-register the table) per query.
""",
    )
    ap.add_argument("--dataset",  required=True,
                    help="Source event log (.xes or .csv).")
    ap.add_argument("--csv-out",  default=None,
                    help="Destination for the Flink CSV. "
                         "Defaults to datasets/<stem>_flink.csv")
    ap.add_argument("--container-path", default=None,
                    help="Path to the CSV *inside* the Flink containers "
                         "(default: /data/<csv_filename>). "
                         "Must match the volume mount in docker-compose-flink.yml")
    ap.add_argument("--session-file", default=MR_SESSION_FILE,
                    help=f"File to write session handle (default: {MR_SESSION_FILE})")
    args = ap.parse_args()

    src = Path(args.dataset)
    if not src.exists():
        print(f"[ERROR] File not found: {src}")
        sys.exit(1)

    # Destination CSV — default to ./flink_data/ (the writable volume mount)
    if args.csv_out:
        dst = Path(args.csv_out)
    else:
        flink_data_dir = Path("./flink_data")
        flink_data_dir.mkdir(exist_ok=True)
        dst = flink_data_dir / (src.stem + "_flink.csv")

    # Path as seen from inside the container
    container_path = args.container_path or f"/flink_data/{dst.name}"

    print(f"MATCH_RECOGNIZE setup")
    print(f"  Source:         {src}")
    print(f"  Flink CSV:      {dst}")
    print(f"  Container path: {container_path}")
    print(f"  Gateway:        {MR_ENDPOINT}")
    print(f"  Table:          {MR_TABLE}")

    # Step 1 — convert
    print("\n[1/3] Converting event log to Flink CSV ...")
    extra_cols = convert_to_flink_csv(src, dst)
    print(f"  Extra columns:  {extra_cols}")

    # Step 2 — gateway
    print("\n[2/3] Creating SQL Gateway session ...")
    _wait_for_gateway()
    session = create_session()

    # Step 3 — register table
    print("\n[3/3] Registering event_log table ...")
    setup_session(session, container_path, extra_cols)

    # Save session handle
    Path(args.session_file).write_text(session)
    print(f"\nSession handle written to {args.session_file}")
    print(f"\nRun the experiment with:")
    print(f"  MR_SESSION_HANDLE={session} \\")
    print(f"  python -m tests.eval.exp_comparable --skip-elk ...")


if __name__ == "__main__":
    main()