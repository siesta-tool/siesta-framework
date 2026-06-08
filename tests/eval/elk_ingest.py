"""
tests/eval/elk_ingest.py

Ingest an event log (XES or CSV) into Elasticsearch for the ELK baseline.

Creates the index with a mapping that mirrors SIESTA's event model:
  trace_id        keyword   — case / trace identifier
  activity        keyword   — event label
  start_timestamp date      — event timestamp (ISO-8601)
  position        integer   — within-trace position (0-based)
  <attr>          keyword   — every extra attribute is a flat keyword field

Usage
-----
# From a XES file:
python -m tests.eval.elk_ingest \
    --dataset datasets/bpic_2017.xes \
    --log-name bpic_2017

# From a CSV file:
python -m tests.eval.elk_ingest \
    --dataset datasets/bpic_2017.csv \
    --log-name bpic_2017

# Wipe and re-create the index first (idempotent re-runs):
python -m tests.eval.elk_ingest \
    --dataset datasets/bpic_2017.xes \
    --log-name bpic_2017 \
    --overwrite

Environment variables
---------------------
ELK_ENDPOINT   Elasticsearch base URL  (default: http://localhost:9200)
ELK_INDEX      Index name              (default: siesta_events)
ELK_BATCH_SIZE Documents per bulk call (default: 500)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Iterator

import requests

ELK_ENDPOINT  = os.environ.get("ELK_ENDPOINT",  "http://localhost:9200")
ELK_INDEX     = os.environ.get("ELK_INDEX",     "siesta_events")
ELK_BATCH     = int(os.environ.get("ELK_BATCH_SIZE", "500"))

# ---------------------------------------------------------------------------
# Index mapping
# ---------------------------------------------------------------------------

INDEX_MAPPING = {
    "settings": {
        "number_of_shards":   1,
        "number_of_replicas": 0,          # single-node, no replicas needed
        "refresh_interval":   "30s",       # bulk-friendly; reset to 1s after ingest
    },
    "mappings": {
        "dynamic_templates": [
            {
                "extra_attrs_as_keyword": {
                    "match_mapping_type": "string",
                    "mapping": {"type": "keyword"}
                }
            }
        ],
        "properties": {
            "trace_id":        {"type": "keyword"},
            "activity":        {"type": "keyword"},
            "start_timestamp": {"type": "date",    "format": "strict_date_optional_time||epoch_millis"},
            "position":        {"type": "integer"},
            # Common process-mining attributes — add more as needed
            "org:resource":           {"type": "keyword"},
            "org:group":              {"type": "keyword"},
            "org:role":               {"type": "keyword"},
            "lifecycle:transition":   {"type": "keyword"},
            "concept:name":           {"type": "keyword"},
            "cost":                   {"type": "float"},
            "resource":               {"type": "keyword"},
            "role":                   {"type": "keyword"},
        }
    }
}


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def _iter_xes(path: Path) -> Generator[dict, None, None]:
    """
    Stream events from an XES file.
    Yields flat dicts with trace_id, activity, start_timestamp, position,
    and all extra event-level attributes.
    """
    from lxml import etree

    ATTR_TYPES = {"string", "int", "float", "boolean", "date", "id"}
    TS_KEYS = {"time:timestamp", "timestamp", "start_timestamp"}
    ACT_KEYS = {"concept:name", "activity"}
    ID_KEYS  = {"concept:name", "case:concept:name", "trace_id"}

    def _val(elem) -> str | None:
        return elem.get("value") or elem.get("key") or None

    context = etree.iterparse(str(path), events=("end",), tag="{http://www.xes-standard.org/}trace")
    for _, trace_elem in context:
        # Extract trace_id from trace attributes
        trace_id = None
        for child in trace_elem:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            key = child.get("key", "")
            if tag in ATTR_TYPES and key in ID_KEYS:
                trace_id = child.get("value", "")
                break
        if trace_id is None:
            trace_id = "unknown"

        pos = 0
        for event_elem in trace_elem.iter("{http://www.xes-standard.org/}event"):
            doc: dict = {"trace_id": trace_id, "position": pos}
            pos += 1

            for child in event_elem:
                tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                if tag not in ATTR_TYPES:
                    continue
                key = child.get("key", "")
                val = child.get("value", "")
                if key in ACT_KEYS and "activity" not in doc:
                    doc["activity"] = val
                if key in TS_KEYS and "start_timestamp" not in doc:
                    # Normalise to ISO-8601 with Z suffix
                    ts = val.replace(" ", "T")
                    if not ts.endswith("Z") and "+" not in ts[-6:]:
                        ts += "Z"
                    doc["start_timestamp"] = ts
                else:
                    doc[key] = val

            if "activity" not in doc:
                continue
            if "start_timestamp" not in doc:
                doc["start_timestamp"] = datetime.now(timezone.utc).isoformat()

            yield doc

        trace_elem.clear()


def _iter_csv(path: Path) -> Generator[dict, None, None]:
    """
    Stream events from a CSV file.
    Expects columns: trace_id (or case:concept:name), activity (or concept:name),
    start_timestamp (or time:timestamp), plus any extras.
    """
    import csv

    TRACE_COLS = ["trace_id", "case:concept:name", "case_id", "CaseID"]
    ACT_COLS   = ["activity", "concept:name", "Activity", "event"]
    TS_COLS    = ["start_timestamp", "time:timestamp", "timestamp", "Timestamp", "time"]

    def _pick(row: dict, candidates: list[str]) -> tuple[str, str | None]:
        for c in candidates:
            if c in row:
                return c, row[c]
        return "", None

    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        trace_pos: dict[str, int] = {}

        for row in reader:
            _, trace_id = _pick(row, TRACE_COLS)
            _, activity = _pick(row, ACT_COLS)
            ts_col, ts  = _pick(row, TS_COLS)

            if not trace_id or not activity:
                continue

            pos = trace_pos.get(trace_id, 0)
            trace_pos[trace_id] = pos + 1

            if ts:
                ts = ts.strip().replace(" ", "T")
                if not ts.endswith("Z") and "+" not in ts[-6:]:
                    ts += "Z"
            else:
                ts = datetime.now(timezone.utc).isoformat()

            doc: dict = {
                "trace_id":        trace_id,
                "activity":        activity,
                "start_timestamp": ts,
                "position":        pos,
            }

            # Include remaining columns as flat attributes
            skip = {ts_col} | set(TRACE_COLS) | set(ACT_COLS) | set(TS_COLS)
            for k, v in row.items():
                if k not in skip and v:
                    doc[k] = v

            yield doc


def _iter_log(path: Path) -> Generator[dict, None, None]:
    suffix = path.suffix.lower()
    if suffix == ".xes":
        yield from _iter_xes(path)
    elif suffix == ".csv":
        yield from _iter_csv(path)
    else:
        raise ValueError(f"Unsupported format: {suffix}. Use .xes or .csv")


# ---------------------------------------------------------------------------
# Elasticsearch helpers
# ---------------------------------------------------------------------------

def _wait_for_elk(timeout: int = 60) -> None:
    print(f"Waiting for Elasticsearch at {ELK_ENDPOINT} ...", end="", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{ELK_ENDPOINT}/_cluster/health", timeout=3)
            if r.status_code == 200 and r.json().get("status") != "red":
                print(" ready.")
                return
        except Exception:
            pass
        print(".", end="", flush=True)
        time.sleep(2)
    raise TimeoutError(f"Elasticsearch not reachable after {timeout}s")


def _create_index(overwrite: bool) -> None:
    url = f"{ELK_ENDPOINT}/{ELK_INDEX}"
    exists = requests.head(url, timeout=10).status_code == 200

    if exists and overwrite:
        requests.delete(url, timeout=10)
        print(f"  Deleted existing index '{ELK_INDEX}'.")
        exists = False

    if not exists:
        r = requests.put(url, json=INDEX_MAPPING, timeout=10)
        r.raise_for_status()
        print(f"  Created index '{ELK_INDEX}'.")
    else:
        print(f"  Index '{ELK_INDEX}' already exists — appending documents.")


def _bulk_load(docs: Iterator[dict]) -> tuple[int, int]:
    """
    POST to _bulk in batches of ELK_BATCH.
    Returns (total_indexed, total_errors).
    """
    indexed = 0
    errors  = 0
    batch: list[dict] = []

    def _flush(b: list[dict]) -> tuple[int, int]:
        lines: list[str] = []
        for d in b:
            lines.append(json.dumps({"index": {"_index": ELK_INDEX}}))
            lines.append(json.dumps(d))
        payload = "\n".join(lines) + "\n"
        r = requests.post(
            f"{ELK_ENDPOINT}/_bulk",
            data=payload,
            headers={"Content-Type": "application/x-ndjson"},
            timeout=120,
        )
        r.raise_for_status()
        body = r.json()
        ok  = sum(1 for i in body["items"] if i.get("index", {}).get("status") in (200, 201))
        err = len(body["items"]) - ok
        return ok, err

    for doc in docs:
        batch.append(doc)
        if len(batch) >= ELK_BATCH:
            ok, err = _flush(batch)
            indexed += ok
            errors  += err
            batch = []
            if indexed % 10_000 == 0:
                print(f"  ... {indexed:,} documents indexed")

    if batch:
        ok, err = _flush(batch)
        indexed += ok
        errors  += err

    return indexed, errors


def _finalize_index() -> None:
    """Re-enable normal refresh after bulk load."""
    r = requests.put(
        f"{ELK_ENDPOINT}/{ELK_INDEX}/_settings",
        json={"index": {"refresh_interval": "1s"}},
        timeout=10,
    )
    r.raise_for_status()
    requests.post(f"{ELK_ENDPOINT}/{ELK_INDEX}/_refresh", timeout=30)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Ingest an event log (XES or CSV) into Elasticsearch.",
    )
    ap.add_argument("--dataset",   required=True,
                    help="Path to .xes or .csv event log.")
    ap.add_argument("--log-name",  default=None,
                    help="Logical name (not used by ELK, just printed).")
    ap.add_argument("--overwrite", action="store_true",
                    help="Delete and recreate the index before ingesting.")
    args = ap.parse_args()

    path = Path(args.dataset)
    if not path.exists():
        print(f"[ERROR] File not found: {path}")
        sys.exit(1)

    print(f"ELK ingest")
    print(f"  Endpoint:  {ELK_ENDPOINT}")
    print(f"  Index:     {ELK_INDEX}")
    print(f"  Dataset:   {path}")
    print(f"  Batch size:{ELK_BATCH}")

    _wait_for_elk()
    _create_index(args.overwrite)

    t0 = time.perf_counter()
    indexed, errors = _bulk_load(_iter_log(path))
    _finalize_index()
    elapsed = time.perf_counter() - t0

    print(f"\nDone in {elapsed:.1f}s")
    print(f"  Indexed: {indexed:,}")
    if errors:
        print(f"  Errors:  {errors:,}  (check _bulk response for details)")

    # Verify count
    r = requests.get(f"{ELK_ENDPOINT}/{ELK_INDEX}/_count", timeout=10)
    if r.status_code == 200:
        count = r.json().get("count", "?")
        print(f"  Count in index: {count:,}")


if __name__ == "__main__":
    main()
