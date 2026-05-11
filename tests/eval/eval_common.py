"""
tests/eval/eval_common.py

Shared helpers for the evaluation harness: API endpoints, JSONL output,
pattern-to-pairs introspection, dataset schema discovery, and timing.

Output convention
-----------------
Every experiment writes a single JSONL file under tests/eval/results/
named after the experiment.  Each line is a JSON record describing one
event (query completion, ingest batch, promotion, etc).  Records share
a common envelope:

    {
        "experiment": "6.3.1",
        "run_id": "<uuid>",
        "ts": <unix epoch>,
        "event": "<event_type>",
        ... event-specific fields ...
    }

Plotting scripts read these JSONL files and produce figures without
re-running the experiments.

Dataset selection
-----------------
The dataset and log_name are resolved per-call via `resolve_dataset()`.
Defaults can be overridden through the EVAL_DATASET and EVAL_LOG_NAME
environment variables, or by passing explicit args from each experiment
script.  Both CSV and XES files are supported — the framework selects
the parser by file extension.

Constraint encoding
-------------------
Attribute constraints are expressed inline in the SeQL pattern using
bracket notation, e.g. ``A[cost="5.0"] B[lifecycle="complete"]``.  The
query API has no separate `constraints` field; this module only sends
the pattern string.
"""

from __future__ import annotations

import csv
import json
import mimetypes
import os
import re
import time
import uuid
import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin

import requests

# ---------------------------------------------------------------------------
# Endpoint configuration
# ---------------------------------------------------------------------------

API_BASE         = "http://localhost:8000"
INDEXER_PREFIX   = "adaptive_indexing"
QUERY_PREFIX     = "adaptive_querying"
EAGER_INDEXER    = "indexing"
EAGER_QUERY      = "querying"

REPO_ROOT       = Path(__file__).resolve().parents[2]
DATASET_DIR     = REPO_ROOT / "datasets"
CONFIG_DIR      = REPO_ROOT / "config"
RESULTS_DIR     = REPO_ROOT / "tests" / "eval" / "results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Dataset / log resolution
# ---------------------------------------------------------------------------

DEFAULT_DATASET  = DATASET_DIR / "test.xes"
DEFAULT_LOG_NAME = "test"


@dataclass
class DatasetSpec:
    """Resolved dataset paired with the log_name used in API calls."""
    path: Path
    log_name: str

    @property
    def fmt(self) -> str:
        return self.path.suffix.lower().lstrip(".")


def resolve_dataset(
    dataset: str | os.PathLike | None = None,
    log_name: str | None = None,
) -> DatasetSpec:
    """
    Resolve the dataset path and log_name used for an experiment.

    Resolution order (first match wins):
      1. explicit `dataset` / `log_name` arguments
      2. EVAL_DATASET / EVAL_LOG_NAME environment variables
      3. DEFAULT_DATASET / DEFAULT_LOG_NAME

    Relative paths are interpreted against the repo root.
    """
    raw = dataset or os.environ.get("EVAL_DATASET") or DEFAULT_DATASET
    path = Path(raw)
    if not path.is_absolute():
        path = (REPO_ROOT / path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")

    name = (
        log_name
        or os.environ.get("EVAL_LOG_NAME")
        or DEFAULT_LOG_NAME
    )
    return DatasetSpec(path=path, log_name=name)


# ---------------------------------------------------------------------------
# Schema discovery
# ---------------------------------------------------------------------------

@dataclass
class DatasetSchema:
    """
    Lightweight summary of a dataset's activities and attribute values.

    `attribute_values` maps attribute_key -> list of distinct values
    observed (truncated to a configurable cap to keep memory bounded).
    `perspective_keys` lists string-typed attributes that make sensible
    grouping keys (resource, role, etc.) — numeric attributes like
    `cost` are filtered out.
    """
    activities: list[str]
    attribute_values: dict[str, list[str]]
    perspective_keys: list[str]
    sampled_events: int

    def values_for(self, attr: str, fallback: list[str] | None = None) -> list[str]:
        return self.attribute_values.get(attr) or (fallback or [])


# Attributes never useful as constraints / perspectives.  These match
# the *raw* keys as they appear in the dataset / index — we deliberately
# don't normalise XES keys, because the adaptive index keys events by
# the verbatim attribute name (e.g. `attributes["org:resource"]`).
# Normalising to "resource" here would make `grouping_keys=["resource"]`
# produce a NULL `group_id` at query time.
_BLOCKED_KEYS = {
    "concept:name", "time:timestamp",
    "activity", "trace_id", "timestamp", "position",
    "case:concept:name", "case_id",
}

# XES attribute element tags that carry key/value pairs.
_XES_ATTR_TAGS = {"string", "date", "int", "float", "boolean", "id"}
_XES_NS_RE = re.compile(r"^\{[^}]+\}")


def _strip_ns(tag: str) -> str:
    return _XES_NS_RE.sub("", tag)


def discover_schema(
    dataset_path: Path,
    *,
    max_events: int = 5000,
    max_values_per_key: int = 16,
) -> DatasetSchema:
    """
    Read the first `max_events` events of a CSV or XES log and summarise
    activities, observed attribute values, and likely perspective keys.

    The result is used by the workload builders to derive realistic
    inline-bracket patterns (e.g. ``A[cost="5.0"] B``) without requiring
    the user to hand-curate per-dataset constants.
    """
    fmt = dataset_path.suffix.lower().lstrip(".")
    if fmt == "csv":
        return _discover_schema_csv(dataset_path, max_events, max_values_per_key)
    if fmt == "xes":
        return _discover_schema_xes(dataset_path, max_events, max_values_per_key)
    raise ValueError(f"Unsupported dataset format: {fmt!r}")


def _discover_schema_csv(
    path: Path, max_events: int, max_values: int,
) -> DatasetSchema:
    activities: list[str] = []
    activity_set: set[str] = set()
    values: dict[str, list[str]] = defaultdict(list)
    value_sets: dict[str, set[str]] = defaultdict(set)
    numeric_keys: set[str] = set()

    n = 0
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if n >= max_events:
                break
            n += 1
            act = row.get("activity") or row.get("concept:name")
            if act and act not in activity_set:
                activity_set.add(act)
                activities.append(act)
            for k, v in row.items():
                if k in _BLOCKED_KEYS or v is None or v == "":
                    continue
                key = k
                if key not in value_sets or len(value_sets[key]) < max_values:
                    if v not in value_sets[key]:
                        value_sets[key].add(v)
                        values[key].append(v)
                if _looks_numeric(v):
                    numeric_keys.add(key)

    perspectives = sorted(
        k for k in values
        if k not in numeric_keys and len(value_sets[k]) > 1
    )
    return DatasetSchema(
        activities=activities,
        attribute_values=dict(values),
        perspective_keys=perspectives,
        sampled_events=n,
    )


def _discover_schema_xes(
    path: Path, max_events: int, max_values: int,
) -> DatasetSchema:
    activities: list[str] = []
    activity_set: set[str] = set()
    values: dict[str, list[str]] = defaultdict(list)
    value_sets: dict[str, set[str]] = defaultdict(set)
    numeric_keys: set[str] = set()

    n = 0
    context = ET.iterparse(str(path), events=("start", "end"))
    in_event = False
    cur_attrs: dict[str, str] = {}
    for ev, elem in context:
        tag = _strip_ns(elem.tag)
        if ev == "start" and tag == "event":
            in_event = True
            cur_attrs = {}
        elif ev == "end" and tag == "event":
            act = cur_attrs.get("concept:name")
            if act and act not in activity_set:
                activity_set.add(act)
                activities.append(act)
            for k, v in cur_attrs.items():
                if k in _BLOCKED_KEYS:
                    continue
                key = k
                if v not in value_sets[key] and len(value_sets[key]) < max_values:
                    value_sets[key].add(v)
                    values[key].append(v)
                if _looks_numeric(v) and tag in _XES_ATTR_TAGS:
                    numeric_keys.add(key)
            in_event = False
            elem.clear()
            n += 1
            if n >= max_events:
                break
        elif ev == "end" and in_event and tag in _XES_ATTR_TAGS:
            k = elem.attrib.get("key")
            v = elem.attrib.get("value")
            if k and v is not None:
                cur_attrs[k] = v
                if tag in {"int", "float"} and _looks_numeric(v):
                    numeric_keys.add(k)

    perspectives = sorted(
        k for k in values
        if k not in numeric_keys and len(value_sets[k]) > 1
    )
    return DatasetSchema(
        activities=activities,
        attribute_values=dict(values),
        perspective_keys=perspectives,
        sampled_events=n,
    )


def _looks_numeric(s: str) -> bool:
    try:
        float(s)
        return True
    except (TypeError, ValueError):
        return False


# ---------------------------------------------------------------------------
# JSONL recorder
# ---------------------------------------------------------------------------

class Recorder:
    """
    Append-only JSONL writer with a stable run identifier.

    Use one Recorder per experiment script.  All records share the
    same experiment label and run_id, making it trivial to filter
    when plotting.
    """

    def __init__(self, experiment: str, output_name: str | None = None):
        self.experiment = experiment
        self.run_id = str(uuid.uuid4())[:8]
        self.path = RESULTS_DIR / (output_name or f"{experiment.replace('.', '_')}.jsonl")
        # Truncate at start of run.
        self.path.write_text("")

    def emit(self, event: str, **fields: Any) -> None:
        record = {
            "experiment": self.experiment,
            "run_id":     self.run_id,
            "ts":         time.time(),
            "event":      event,
            **fields,
        }
        with self.path.open("a") as f:
            f.write(json.dumps(record) + "\n")


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def health_check() -> None:
    r = requests.get(urljoin(API_BASE, "/health"), timeout=5)
    r.raise_for_status()


def _guess_mime(path: Path) -> str:
    if path.suffix.lower() == ".xes":
        return "application/xml"
    return mimetypes.guess_type(str(path))[0] or "application/octet-stream"


def ingest_adaptive(
    log_name: str,
    dataset_path: Path,
    config_path: Path,
    overrides: dict | None = None,
) -> dict:
    """POST to the adaptive indexer.  `overrides` is merged into the JSON
    config before sending."""
    config = json.loads(config_path.read_text())
    config["log_name"] = log_name
    if overrides:
        config.update(overrides)

    with dataset_path.open("rb") as fp:
        r = requests.post(
            urljoin(API_BASE, f"/{INDEXER_PREFIX}/run"),
            files={"log_file": (dataset_path.name, fp, _guess_mime(dataset_path))},
            data={"index_config": json.dumps(config)},
            timeout=600,
        )
    r.raise_for_status()
    return r.json()


def ingest_eager(log_name: str, dataset_path: Path, config_path: Path) -> dict:
    """POST to the eager (SIESTA) indexer to populate pairs_index."""
    config = json.loads(config_path.read_text())
    config["log_name"] = log_name
    with dataset_path.open("rb") as fp:
        r = requests.post(
            urljoin(API_BASE, f"/{EAGER_INDEXER}/run"),
            files={"log_file": (dataset_path.name, fp, _guess_mime(dataset_path))},
            data={"index_config": json.dumps(config)},
            timeout=600,
        )
    r.raise_for_status()
    return r.json()


def detect_adaptive(
    log_name: str,
    pattern: str,
    grouping_keys: list[str],
    lookback: str = "3650d",
    lookback_mode: str = "time",
) -> dict:
    body = {
        "log_name":          log_name,
        "storage_namespace": "siesta",
        "method":            "detection",
        "query":             {"pattern": pattern},
        "grouping_keys":     grouping_keys,
        "lookback":          lookback,
        "lookback_mode":     lookback_mode,
        "support_threshold": 0.0,
    }
    r = requests.post(
        urljoin(API_BASE, f"/{QUERY_PREFIX}/detection"),
        json=body,
        timeout=600,
    )
    r.raise_for_status()
    print(r.json())
    return r.json()


def detect_eager(log_name: str, pattern: str) -> dict:
    body = {
        "log_name":          log_name,
        "storage_namespace": "siesta",
        "method":            "detection",
        "query":             {"pattern": pattern},
        "support_threshold": 0.0,
    }
    r = requests.post(
        urljoin(API_BASE, f"/{EAGER_QUERY}/detection"),
        json=body,
        timeout=600,
    )
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------------
# Pattern introspection
# ---------------------------------------------------------------------------

# Activity tokens are uppercase identifiers (single letters in synthetic
# tests; multi-char labels in real logs).  Operators we explicitly skip
# when extracting required pairs:
#   *  +  ?         Kleene operators
#   |               disjunction
#   ~  !  ^         negation prefix (excluded by design — see paper §4.3)
#   ()              grouping parentheses
#   [..]            attribute-constraint blocks (stripped before pair extraction)
_ACTIVITY_RE = re.compile(r"[A-Za-z][A-Za-z0-9_:]*")
_NEGATION_RE = re.compile(r"[~!^]\s*[A-Za-z][A-Za-z0-9_:]*(?:\[[^\]]*\])?")
_ATTR_BLOCK_RE = re.compile(r"\[[^\]]*\]")


def extract_required_pairs(pattern: str) -> set[tuple[str, str]]:
    """
    Return the set of consecutive activity pairs implied by `pattern`,
    excluding any pair involving a negated activity.

    Inline attribute-constraint blocks (``A[cost="5.0"]``) are stripped
    before pair extraction so they don't introduce phantom tokens.

    The implementation deliberately ignores Kleene operators and
    disjunction details — for footprint purposes we want a superset of
    what the query planner will actually look up.
    """
    cleaned = _NEGATION_RE.sub("", pattern)
    cleaned = _ATTR_BLOCK_RE.sub("", cleaned)
    cleaned = re.sub(r"[*+?]", " ", cleaned)
    tokens = [
        m.group(0)
        for chunk in re.split(r"[|()]", cleaned)
        for m in _ACTIVITY_RE.finditer(chunk)
    ]
    return set(zip(tokens, tokens[1:]))


def perspective_pair_set(
    queries: Iterable[dict],
) -> set[tuple[str, tuple[str, ...], tuple[str, str]]]:
    """
    Compute the workload footprint as a set of
    (log_name, grouping_keys_tuple, (act_a, act_b)) triples.
    """
    triples: set = set()
    for q in queries:
        gkeys = tuple(sorted(q.get("grouping_keys", [])))
        log = q.get("log_name", "?")
        for pair in extract_required_pairs(q["pattern"]):
            triples.add((log, gkeys, pair))
    return triples


# ---------------------------------------------------------------------------
# Timing utilities
# ---------------------------------------------------------------------------

def timed_query(
    log_name: str,
    pattern: str,
    grouping_keys: list[str],
    lookback: str = "3650d",
) -> tuple[dict, float]:
    """Return (response, wall_clock_seconds)."""
    t0 = time.perf_counter()
    body = detect_adaptive(log_name, pattern, grouping_keys, lookback)
    return body, time.perf_counter() - t0
