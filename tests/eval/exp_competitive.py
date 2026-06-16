"""
tests/eval/exp_competitive.py

Experiment 6.4.1 — Competitive latency benchmark.

Compares warm adaptive SIESTA against ELK (Elasticsearch) on long
structural and attribute-aware pattern queries.

Design
------
Two perspectives per dataset:
  - case_id   (trace-centric):  grouping_keys depends on format:
                                ["trace_id"] for CSV, ["case:concept:name"] for XES.
                                This is the *case* perspective — the adaptive endpoint
                                builds its own per-perspective PairsIndex over case groups
                                rather than delegating to the eager index.
  - best_alt  (multiperspective): the perspective with the most balanced group-size
                                  distribution (lowest coefficient of variation) among
                                  those discovered by discover_schema.

Query construction:
  Patterns of length 8–10 are built from real activity chains found in
  the data via pair_coverage.  We build chains greedily: start from a
  high-coverage pair, then extend by appending the best successor whose
  (last, next) pair also has high coverage.  This guarantees non-zero
  results — every consecutive sub-pair exists in the data.

  Structural queries use the chain as-is.
  Attribute-aware queries annotate one or two activities with inline
  bracket constraints derived from real attribute values.

Warm-up:
  All C(n,2) sub-pairs of each long pattern are warmed up by issuing
  short 2-activity queries (each sub-pair repeated 4× with
  min_query_count=1).  A sleep follows for async promotions.
  Then the long pattern is measured.

ELK baseline:
  For each query, ELK receives a bool/filter on the constituent activities
  plus a terms aggregation on the perspective key.  This is a *lower bound*
  on ELK's true latency — it does no ordering verification, just retrieval
  and grouping.  The paper frames this as: "even ELK's best-case retrieval
  step alone is slower than SIESTA's end-to-end pattern detection."

  For attribute-aware queries, ELK additionally receives term/range filters
  on the constrained attributes.

Output
------
results/competitive_<log_name>.jsonl

Records:
  dataset        path, log_name, activities, perspectives, case_gk, alt_gk
  query_def      qid, pattern, perspective, category, chain_pairs, n_activities
  warmup_done    perspective, n_pairs_warmed, wall_s
  query          system, perspective, category, qid, pattern, latency_s, total
  query_error    system, qid, error

Running
-------
    python -m tests.eval.exp_competitive \\
        --dataset /mnt/datasets/bpic_2017.xes --log-name bpic_2017

    python -m tests.eval.exp_competitive \\
        --datasets-dir /mnt/datasets
"""

from __future__ import annotations

import argparse
import itertools
import json
import os
import re
import statistics
import sys
import time
from collections import defaultdict
from pathlib import Path

import requests
from urllib.parse import urljoin

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tests.eval.eval_common import (
    API_BASE, API_TIMEOUT_S, CONFIG_DIR, RESULTS_DIR,
    QUERY_PREFIX,
    Recorder, health_check,
    ingest_adaptive,
    detect_adaptive, timed_query,
    discover_schema, resolve_dataset,
    quote_label, _guess_mime,
)
from tests.eval.workload import fetch_pair_coverage

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ADAPTIVE_CONFIG   = CONFIG_DIR / "adaptive_index.config.json"
# half_life_seconds must be >> warmup duration so query counts do not decay
# to zero before the promotion worker runs.  300 s caused pairs warmed early
# to have round(count * exp(-2700 * ln2 / 300)) == 0 by the end of warmup.
RETENTION_FAST    = {"min_query_count": 1, "half_life_seconds": 86400}

ELK_ENDPOINT = os.environ.get("ELK_ENDPOINT", "http://localhost:9200")
ELK_INDEX    = os.environ.get("ELK_INDEX",    "siesta_events")

MIN_CHAIN_LEN     = 8     # minimum pattern length
TARGET_CHAIN_LEN  = 10    # target pattern length
N_QUERIES_PER_CAT = 5     # queries per (perspective, category) combo
WARMUP_REPS       = 4     # repetitions per sub-pair during warm-up
PROMOTION_SLEEP_S = int(os.environ.get("PROMOTION_SLEEP_S", "90"))
MEASURE_REPS      = 3     # repeat each measurement, take median

_LOG_EXTS = {".csv", ".xes"}

# ---------------------------------------------------------------------------
# Tee — duplicate stdout/stderr to a log file
# ---------------------------------------------------------------------------

class _Tee:
    """
    Wraps a stream so every write goes to both the original stream and
    a file.  Assign to sys.stdout / sys.stderr so all print() calls and
    tracebacks are captured without changing any other code.
    """
    def __init__(self, stream, filepath: Path) -> None:
        self._stream = stream
        self._file   = filepath.open("a", buffering=1, encoding="utf-8")

    def write(self, data: str) -> int:
        self._stream.write(data)
        self._file.write(data)
        return len(data)

    def flush(self) -> None:
        self._stream.flush()
        self._file.flush()

    def fileno(self):
        return self._stream.fileno()

    def close(self) -> None:
        self._file.close()

    # Forward all other attribute lookups to the underlying stream so
    # libraries that inspect sys.stdout (e.g. tqdm) don't break.
    def __getattr__(self, name):
        return getattr(self._stream, name)


def _setup_tee(label: str) -> Path:
    """
    Redirect stdout and stderr through _Tee to a timestamped log file.
    Returns the log file path.
    """
    import datetime
    ts  = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = RESULTS_DIR / f"{label}_{ts}.txt"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    sys.stdout = _Tee(sys.stdout, log_path)
    sys.stderr = _Tee(sys.stderr, log_path)
    return log_path



# ---------------------------------------------------------------------------
# ELK helpers
# ---------------------------------------------------------------------------

def _elk_reachable() -> bool:
    try:
        return requests.get(ELK_ENDPOINT, timeout=3).status_code == 200
    except Exception:
        return False


def _elk_index_events(dataset_path: Path, index_name: str) -> int:
    """
    Bulk-index events from a CSV/XES file into Elasticsearch.
    Returns the number of indexed documents.

    Uses the _bulk API for efficiency.  Each event becomes one document
    with fields: activity, timestamp, trace_id, plus all other attributes.
    """
    from tests.eval.batch_splitter import _iter_log

    bulk_lines: list[str] = []
    count = 0
    for ev in _iter_log(dataset_path):
        action = json.dumps({"index": {"_index": index_name}})
        doc = {
            "activity":  ev.get("activity", ""),
            "timestamp": ev.get("timestamp", ""),
            "trace_id":  ev.get("trace_id", ""),
        }
        # Add all other attributes.
        for k, v in ev.items():
            if k not in ("activity", "timestamp", "trace_id") and v:
                doc[k] = v
        bulk_lines.append(action)
        bulk_lines.append(json.dumps(doc))
        count += 1

        # Flush every 5000 docs.
        if count % 5000 == 0:
            _elk_bulk_send(bulk_lines, index_name)
            bulk_lines.clear()

    if bulk_lines:
        _elk_bulk_send(bulk_lines, index_name)

    # Refresh to make documents searchable.
    requests.post(f"{ELK_ENDPOINT}/{index_name}/_refresh", timeout=30)
    return count


def _elk_bulk_send(lines: list[str], index_name: str) -> None:
    body = "\n".join(lines) + "\n"
    r = requests.post(
        f"{ELK_ENDPOINT}/_bulk",
        data=body,
        headers={"Content-Type": "application/x-ndjson"},
        timeout=120,
    )
    r.raise_for_status()
    resp = r.json()
    if resp.get("errors"):
        n_err = sum(1 for item in resp["items"] if "error" in item.get("index", {}))
        print(f"  [ELK bulk] {n_err} errors in batch")


def _elk_create_index(index_name: str) -> None:
    """Delete and recreate the ELK index with keyword mappings."""
    requests.delete(f"{ELK_ENDPOINT}/{index_name}", timeout=10)
    time.sleep(1)
    mapping = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "refresh_interval": "-1",  # disable auto-refresh during bulk
        },
        "mappings": {
            "properties": {
                "activity":  {"type": "keyword"},
                "timestamp": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
                "trace_id":  {"type": "keyword"},
            },
            "dynamic": "true",
            "dynamic_templates": [
                {"strings_as_keyword": {
                    "match_mapping_type": "string",
                    "mapping": {"type": "keyword"},
                }},
            ],
        },
    }
    r = requests.put(
        f"{ELK_ENDPOINT}/{index_name}",
        json=mapping,
        timeout=30,
    )
    r.raise_for_status()


def _elk_detect_verify(
    chain: list[str],
    constraints: dict[int, tuple[str, str]],
    perspective_key: str,
) -> tuple[float, int, int]:
    """
    Honest end-to-end ELK pattern detection, comparable to SIESTA:

      1. Retrieve all events whose activity appears in the pattern,
         with (perspective key, activity, timestamp, constrained attrs),
         sorted by timestamp, via search_after paging.
      2. Group client-side by perspective key.
      3. Greedy subsequence match per group under eventually-follows
         semantics, applying per-position attribute constraints.

    `constraints` maps pattern position -> (attr_key, attr_value).

    Greedy earliest-match is correct for plain sequential patterns with
    per-position predicates: taking the earliest event that satisfies
    position i never precludes a match that a later choice would allow.

    Returns (wall_seconds, n_matching_groups, n_events_retrieved).
    This is the equivalent work SIESTA performs end-to-end, so latency
    and totals are directly comparable.
    """
    acts = sorted(set(chain))
    constraint_attrs = sorted({attr for attr, _v in constraints.values()})
    source_fields = [perspective_key, "activity", "timestamp"] + constraint_attrs

    body: dict = {
        "query": {"bool": {"filter": [{"terms": {"activity": acts}}]}},
        "size": 10000,
        "_source": source_fields,
        # NB: sorting on _id is disallowed (fielddata on _id is
        # disabled by default) and returns HTTP 400.  _doc is the
        # supported tiebreaker; with a single shard it gives a stable
        # total order for search_after paging.
        "sort": [{"timestamp": "asc"}, {"_doc": "asc"}],
        "track_total_hits": False,
    }

    t0 = time.perf_counter()

    groups: dict[str, list[dict]] = defaultdict(list)
    n_events = 0
    search_after = None
    while True:
        if search_after is not None:
            body["search_after"] = search_after
        r = requests.post(
            f"{ELK_ENDPOINT}/{ELK_INDEX}/_search",
            json=body,
            timeout=API_TIMEOUT_S,
        )
        if r.status_code >= 400:
            raise RuntimeError(
                f"ELK search failed ({r.status_code}): {r.text[:400]}"
            )
        hits = r.json().get("hits", {}).get("hits", [])
        if not hits:
            break
        for h in hits:
            srcdoc = h.get("_source", {})
            gv = srcdoc.get(perspective_key)
            if gv is None:
                continue
            groups[str(gv)].append(srcdoc)
            n_events += 1
        search_after = hits[-1]["sort"]
        if len(hits) < body["size"]:
            break

    # ── Client-side verification ───────────────────────────────────────
    # Events arrive in global timestamp order (server-side sort), so the
    # per-group lists are already temporally ordered.
    n_match = 0
    for gv, events in groups.items():
        pos = 0
        for ev in events:
            if ev.get("activity") != chain[pos]:
                continue
            if pos in constraints:
                attr, val = constraints[pos]
                if str(ev.get(attr, "")) != val:
                    continue
            pos += 1
            if pos == len(chain):
                n_match += 1
                break

    return time.perf_counter() - t0, n_match, n_events


def _elk_query_lowerbound(
    activities: list[str],
    perspective_key: str | None,
    attr_filters: list[dict] | None = None,
) -> tuple[float, int]:
    """
    LOWER-BOUND ELK query: filter by activities + terms aggregation on
    the perspective key.  No ordering verification — measures only the
    retrieval/grouping step.  Kept for reference; totals are document
    counts, NOT comparable to SIESTA's group counts.
    """
    must_clauses = [
        {"bool": {"should": [{"term": {"activity": a}} for a in activities],
                  "minimum_should_match": 1}},
    ]
    if attr_filters:
        must_clauses.extend(attr_filters)

    body: dict = {
        "query": {"bool": {"must": must_clauses}},
        "size": 0,
        "track_total_hits": True,
    }
    if perspective_key:
        body["aggs"] = {
            "by_group": {"terms": {"field": perspective_key, "size": 100000}},
        }

    t0 = time.perf_counter()
    r = requests.post(
        f"{ELK_ENDPOINT}/{ELK_INDEX}/_search",
        json=body,
        timeout=API_TIMEOUT_S,
    )
    r.raise_for_status()
    latency = time.perf_counter() - t0
    total = r.json().get("hits", {}).get("total", {}).get("value", 0)
    return latency, total



# ---------------------------------------------------------------------------
# Chain builder — direct trace subsequence sampling
#
# IMPORTANT: chains built from pair_coverage adjacency do NOT guarantee
# non-zero results — each consecutive pair exists in *some* group, but a
# long pattern requires all pairs to coexist (in order) within *one*
# group.  The correct approach is to sample contiguous activity windows
# directly from real groups: a window taken from a real group is
# guaranteed to match that group under STNM eventually-follows semantics.
# ---------------------------------------------------------------------------

MAX_PARSE_EVENTS = int(os.environ.get("MAX_PARSE_EVENTS", "2000000"))


def _index_lookback_seconds() -> float:
    """
    Parse the indexing-time lookback λ from the adaptive config.

    CRITICAL: pairs whose time gap exceeds λ are NEVER extracted into
    the persisted PairsIndex — a query lookback larger than λ cannot
    recover them.  Sampled windows must span <= λ or some ordered pair
    (i, j) of the pattern will be absent and the group falsely
    eliminated (observed as sporadic 0-result preflights and
    undercounted totals).

    Override with env WINDOW_MAX_SPAN_S.
    """
    env = os.environ.get("WINDOW_MAX_SPAN_S")
    if env:
        return float(env)
    try:
        cfg = json.loads(ADAPTIVE_CONFIG.read_text())
        lb = str(cfg.get("lookback", "7d"))
    except Exception:
        lb = "7d"
    m = re.match(r"(\d+(?:\.\d+)?)\s*([dhms])", lb)
    if not m:
        return 7 * 86400.0
    val, unit = float(m.group(1)), m.group(2)
    return val * {"d": 86400, "h": 3600, "m": 60, "s": 1}[unit]


def sample_chains_from_traces(
    dataset_path: Path,
    group_key: str,
    target_len: int = 10,
    min_len: int = 8,
    n_chains: int = 5,
) -> list[list[dict]]:
    """
    Sample activity chains of length `target_len` as contiguous windows
    from real groups in the dataset.

    group_key: "trace_id" for case perspective, or any attribute key
               (e.g. "org:resource") for an alternative perspective.

    Guarantees: every returned chain is a contiguous activity
    subsequence of at least one real group, so the corresponding
    sequential pattern has >= 1 match under STNM semantics.

    Selection strategy: prefer windows with many *distinct* activities
    (more informative sub-pair intersections) drawn from different
    groups for diversity.  Windows whose time span exceeds the indexing
    lookback λ are REJECTED — their long-gap pairs are absent from the
    persisted index and the pattern would falsely return 0.

    Returns a list of windows; each window is a list of event dicts
    (activity, timestamp, plus all contextual attributes), preserving
    the actual attribute values for guaranteed-satisfiable constraints.
    """
    from tests.eval.batch_splitter import _iter_log, _parse_ts

    max_span = _index_lookback_seconds()
    print(f"    [sampler] max window time-span = {max_span/86400:.1f}d "
          f"(indexing lookback λ)")

    # ── Build groups: key -> [(epoch_ts, event_dict)] ──────────────────
    groups: dict[str, list[tuple[float, dict]]] = defaultdict(list)
    n_parsed = 0
    for ev in _iter_log(dataset_path):
        gv = ev.get(group_key)
        act = ev.get("activity")
        if not gv or not act:
            continue
        ts = _parse_ts(ev.get("timestamp", "")) if ev.get("timestamp") else 0.0
        groups[gv].append((ts, ev))
        n_parsed += 1
        if n_parsed >= MAX_PARSE_EVENTS:
            break

    if not groups:
        return []

    candidates: list[tuple[int, str, list[dict]]] = []
    seen_windows: set[tuple[str, ...]] = set()
    n_span_rejected = 0

    ranked_groups = sorted(groups.items(), key=lambda kv: -len(kv[1]))

    for gv, events in ranked_groups:
        if len(events) < min_len:
            continue
        events.sort(key=lambda e: e[0])

        max_wlen = min(target_len, len(events))
        if max_wlen < min_len:
            continue

        best_window = None
        best_distinct = -1
        stride = max(1, (len(events) - max_wlen) // 50 or 1)
        for start in range(0, len(events) - max_wlen + 1, stride):
            w = events[start:start + max_wlen]
            # λ check: full window span must fit in the indexing lookback.
            if w[-1][0] - w[0][0] > max_span:
                n_span_rejected += 1
                continue
            acts_w = tuple(e[1]["activity"] for e in w)
            if acts_w in seen_windows:
                continue
            nd = len(set(acts_w))
            if nd > best_distinct:
                best_distinct = nd
                best_window = [e[1] for e in w]

        if best_window:
            seen_windows.add(tuple(e["activity"] for e in best_window))
            candidates.append((best_distinct, gv, best_window))

        if len(candidates) >= n_chains * 4:
            break

    if n_span_rejected:
        print(f"    [sampler] rejected {n_span_rejected} windows exceeding λ "
              f"— consider increasing 'lookback' in the adaptive config "
              f"and re-ingesting")

    if not candidates:
        return []

    candidates.sort(key=lambda c: -c[0])
    chosen = [c[2] for c in candidates[:n_chains]]
    n_rep = sum(1 for w in chosen if len({e["activity"] for e in w}) < len(w))
    print(f"    [sampler] selected {len(chosen)} chains "
          f"(lengths={[len(w) for w in chosen]}, "
          f"repeated-activity={n_rep}/{len(chosen)})")
    return chosen


def _chain_sub_pairs(chain: list[str]) -> list[tuple[str, str]]:
    """All consecutive sub-pairs from a chain."""
    return [(chain[i], chain[i + 1]) for i in range(len(chain) - 1)]


def _chain_all_pairs(chain: list[str]) -> list[tuple[str, str]]:
    """All ordered sub-pairs (i < j) from a chain — these are what the
    adaptive detection intersects."""
    return [(chain[i], chain[j])
            for i in range(len(chain))
            for j in range(i + 1, len(chain))]


# ---------------------------------------------------------------------------
# Attribute-aware query construction
# ---------------------------------------------------------------------------

_RESERVED_KEYS = {"activity", "timestamp", "trace_id"}


def _build_attr_pattern(
    window: list[dict],
    perspective_attr: str | None = None,
) -> tuple[str | None, dict[int, tuple[str, str]]]:
    """
    Annotate the first and last positions of the window with inline
    bracket constraints whose values are taken from the window's OWN
    events.  Because the source group satisfies the structural pattern
    AND its events carry exactly these attribute values, the constrained
    pattern is guaranteed to have >= 1 match.

    `perspective_attr` is excluded as a constraint key (constraining on
    the grouping attribute itself is trivially satisfied per group).

    Returns (pattern_string, constraints) where constraints maps
    position -> (attr_key, attr_value), or (None, {}) if no usable
    attribute exists on the boundary events.
    """
    chain = [e["activity"] for e in window]
    constraints: dict[int, tuple[str, str]] = {}

    def _pick_attr(ev: dict) -> tuple[str, str] | None:
        for k, v in ev.items():
            if k in _RESERVED_KEYS or k == perspective_attr:
                continue
            if v is None or str(v).strip() == "":
                continue
            return k, str(v)
        return None

    first = _pick_attr(window[0])
    last  = _pick_attr(window[-1])
    if not first and not last:
        return None, {}
    if first:
        constraints[0] = first
    if last:
        constraints[len(window) - 1] = last

    parts = []
    for i, act in enumerate(chain):
        label = quote_label(act)
        if i in constraints:
            attr, val = constraints[i]
            safe_val = val.replace('"', '\\"')
            label = f'{label}[{attr}="{safe_val}"]'
        parts.append(label)

    return " ".join(parts), constraints


def _is_num(s: str) -> bool:
    try:
        float(s)
        return True
    except (TypeError, ValueError):
        return False


# ---------------------------------------------------------------------------
# Perspective selection — pick the best alternative perspective
# ---------------------------------------------------------------------------

MIN_ALT_GROUPS = int(os.environ.get("MIN_ALT_GROUPS", "10"))


def select_best_alt_perspective(
    log_name: str,
    perspective_keys: list[str],
) -> str | None:
    """
    Among discovered perspective keys, pick the one with the most
    balanced group-size distribution (lowest coefficient of variation),
    REQUIRING at least MIN_ALT_GROUPS groups.

    The group-count floor matters twice over: (a) a perspective with a
    handful of giant groups (e.g. a 3-value attribute over 1.2M events)
    yields enormous pseudo-sequences that blow up CEP validation, and
    (b) the sampler takes one window per group, so chain diversity is
    capped at the group count.

    Returns None (skip the alt perspective) if nothing qualifies —
    deliberately NOT falling back to an unsuitable perspective.
    """
    best_key = None
    best_cv  = float("inf")

    for pk in perspective_keys:
        try:
            cov = fetch_pair_coverage(log_name, [pk])
        except Exception:
            continue
        gc = cov.get("group_count", 0)
        if gc < MIN_ALT_GROUPS:
            print(f"    [perspective] {pk}: only {gc} groups "
                  f"(< {MIN_ALT_GROUPS}), excluded")
            continue
        pairs = cov.get("pairs", [])
        if not pairs:
            continue

        # Use pair group counts as a proxy for group-size balance.
        # More evenly distributed pairs → lower CV.
        group_counts = [p["groups"] for p in pairs]
        if len(group_counts) < 2:
            continue
        mean_g = statistics.mean(group_counts)
        if mean_g == 0:
            continue
        stdev_g = statistics.stdev(group_counts)
        cv = stdev_g / mean_g

        if cv < best_cv:
            best_cv = cv
            best_key = pk

    return best_key  # None => caller skips the alt perspective


# ---------------------------------------------------------------------------
# Warm-up: promote all sub-pairs to PERSISTENT via short 2-activity queries
# ---------------------------------------------------------------------------

def warmup_pairs(
    log_name: str,
    pairs: set[tuple[str, str]],
    grouping_keys: list[str],
    reps: int = WARMUP_REPS,
) -> int:
    """
    Issue short 2-activity queries for every pair in `pairs`, repeated
    `reps` times to ensure promotion to PERSISTENT.

    IMPORTANT: Algorithm 3 (Detect) intersects ALL ordered pairs (i<j)
    of the pattern, not only consecutive ones.  Callers must pass the
    full ordered-pair set of each pattern (see _chain_all_pairs) or the
    measured "warm" query will silently pay transient extraction cost
    for the un-warmed pairs.

    Returns the number of queries issued.
    """
    n = 0
    for a, b in sorted(pairs):
        pattern = f"{quote_label(a)} {quote_label(b)}"
        for _ in range(reps):
            try:
                detect_adaptive(
                    log_name, pattern, grouping_keys,
                    retention_overrides=RETENTION_FAST,
                )
                n += 1
            except Exception as exc:
                print(f"    [warmup] {a}->{b} error: {exc}")
    return n


PROMOTION_BUDGET_S = int(os.environ.get("PROMOTION_BUDGET_S", "1800"))
PROMOTION_POLL_S   = int(os.environ.get("PROMOTION_POLL_S", "20"))


def wait_for_promotions(
    log_name: str,
    pairs: set[tuple[str, str]],
    grouping_keys: list[str],
    budget_s: int = PROMOTION_BUDGET_S,
) -> dict[str, int]:
    """
    Poll until every pair in `pairs` reaches PERSISTENT, or the budget
    expires.

    Each detection response carries `pair_status_after` for the pairs
    it touched, so the poll is a sweep of cheap 2-activity queries over
    the not-yet-persistent set.  A blind sleep is wrong here:
    build_pair_persistent re-scans the sequence table per pair
    (~10-20 s each), so persisting ~90 pairs takes ~15-30 min of
    background work — far beyond any fixed sleep.

    Pairs may also legitimately stay TRANSIENT: the retention predicate
    is utility vs cost, and a cheap-to-extract pair can fail the cost
    gate forever.  TRANSIENT pairs are served from the in-memory LRU
    (faster than Delta), so they do not hurt warm measurements — but
    the experiment should know the tier mix, hence the returned counts.

    Returns {"PERSISTENT": n, "TRANSIENT": n, "ABSENT": n, ...} for the
    final sweep.
    """
    t0 = time.perf_counter()
    pending = set(pairs)
    final_status: dict[tuple[str, str], str] = {}

    while pending and (time.perf_counter() - t0) < budget_s:
        next_pending: set[tuple[str, str]] = set()
        for (a, b) in sorted(pending):
            pattern = f"{quote_label(a)} {quote_label(b)}"
            try:
                body = detect_adaptive(
                    log_name, pattern, grouping_keys,
                    retention_overrides=RETENTION_FAST,
                )
            except Exception:
                next_pending.add((a, b))
                continue
            statuses = body.get("pair_status_after") or {}
            st = statuses.get(f"{a}->{b}")
            if st is None and len(statuses) == 1:
                # 2-activity query touches exactly one pair; tolerate
                # key-format differences by taking the single entry.
                st = next(iter(statuses.values()))
            st = st or "UNKNOWN"
            final_status[(a, b)] = st
            if st != "PERSISTENT":
                next_pending.add((a, b))
        pending = next_pending
        if pending:
            elapsed = time.perf_counter() - t0
            print(f"    [promotions] {len(pairs)-len(pending)}/{len(pairs)} "
                  f"persistent after {elapsed:.0f}s; polling again in "
                  f"{PROMOTION_POLL_S}s ...")
            time.sleep(PROMOTION_POLL_S)

    counts: dict[str, int] = {}
    for (a, b) in pairs:
        st = final_status.get((a, b), "UNKNOWN")
        counts[st] = counts.get(st, 0) + 1
    return counts


# ---------------------------------------------------------------------------
# Preflight — test a pattern to check for non-zero results
# ---------------------------------------------------------------------------

PREFLIGHT_TIMEOUT_S = int(os.environ.get("PREFLIGHT_TIMEOUT_S", "300"))


def preflight_query(
    log_name: str,
    pattern: str,
    grouping_keys: list[str],
) -> tuple[int | None, str | None]:
    """
    Quick check whether a pattern returns non-zero results.

    Returns (total, None) on success or (None, reason) on
    timeout/error.  Callers MUST distinguish the two: a timeout means
    "couldn't tell", not "zero results" — conflating them silently
    discards valid (slow) queries.
    """
    body = {
        "log_name":          log_name,
        "storage_namespace": "siesta",
        "method":            "detection",
        "query":             {"pattern": pattern},
        "grouping_keys":     grouping_keys,
        "lookback":          "3650d",
        "lookback_mode":     "time",
        "support_threshold": 0.0,
        "min_query_count":   1,
        "half_life_seconds": 300,
    }
    try:
        r = requests.post(
            urljoin(API_BASE, f"/{QUERY_PREFIX}/detection"),
            json=body,
            timeout=PREFLIGHT_TIMEOUT_S,
        )
        r.raise_for_status()
        return r.json().get("total", 0), None
    except requests.Timeout:
        return None, f"timeout after {PREFLIGHT_TIMEOUT_S}s"
    except Exception as exc:
        return None, str(exc)[:200]


# ---------------------------------------------------------------------------
# Measurement
# ---------------------------------------------------------------------------

def measure_siesta(
    log_name: str,
    pattern: str,
    grouping_keys: list[str],
    reps: int = MEASURE_REPS,
) -> tuple[float, int, dict]:
    """
    Measure median latency over `reps` repetitions.
    Returns (median_latency, total_matches, last_response_body).
    The body carries pair_status_after and any other diagnostic fields.
    """
    latencies = []
    total = 0
    body: dict = {}
    for _ in range(reps):
        body, lat = timed_query(
            log_name, pattern, grouping_keys,
            retention_overrides=RETENTION_FAST,
        )
        latencies.append(lat)
        total = body.get("total", 0)
    return statistics.median(latencies), total, body


# ---------------------------------------------------------------------------
# ELK perspective key mapping
# ---------------------------------------------------------------------------

def _elk_perspective_key(grouping_keys: list[str], fmt: str) -> str | None:
    """
    Map SIESTA grouping_keys to the ELK document field name for the
    perspective aggregation.
    """
    if not grouping_keys:
        return None
    gk = grouping_keys[0]
    # trace_id / case:concept:name → "trace_id" in ELK docs
    if gk in ("trace_id", "case:concept:name"):
        return "trace_id"
    return gk


# ---------------------------------------------------------------------------
# Per-dataset runner
# ---------------------------------------------------------------------------

def _case_grouping_key(fmt: str) -> list[str]:
    """Return the grouping key for case-centric perspective."""
    # The adaptive endpoint needs a real attribute key that exists
    # in the event's attribute map.  For XES, trace_id is stored
    # under "case:concept:name" at trace level but the SequenceTable
    # has a top-level "trace_id" column.  The adaptive grouping uses
    # the top-level column directly when grouping_keys=["trace_id"].
    return ["trace_id"]


def run_dataset(
    dataset_path: Path,
    log_name: str,
    *,
    skip_elk: bool = False,
) -> None:
    fmt = dataset_path.suffix.lower().lstrip(".")
    print(f"\n{'='*64}")
    print(f"  Dataset: {dataset_path}  log_name={log_name}")
    print(f"{'='*64}")

    schema = discover_schema(dataset_path)
    activities = schema.activities
    persp_keys = schema.perspective_keys

    print(f"  Activities ({len(activities)}): {activities[:10]}{'...' if len(activities)>10 else ''}")
    print(f"  Perspectives: {persp_keys}")

    if len(activities) < 3:
        print(f"  SKIP: need at least 3 activities, got {len(activities)}")
        return

    rec = Recorder("6.4.1", f"competitive_{log_name}.jsonl")

    # ── Ingest ─────────────────────────────────────────────────────────
    print("\n  Ingesting (adaptive) ...")
    ingest_adaptive(log_name, dataset_path, ADAPTIVE_CONFIG,
                    overrides={"overwrite_data": True})
    time.sleep(2)

    # ── Perspective selection ──────────────────────────────────────────
    case_gk = _case_grouping_key(fmt)
    alt_key = select_best_alt_perspective(log_name, persp_keys)
    alt_gk  = [alt_key] if alt_key else None

    perspectives: list[tuple[str, list[str]]] = [("case", case_gk)]
    if alt_gk and alt_gk != case_gk:
        perspectives.append(("alt", alt_gk))

    print(f"  Case perspective:  {case_gk}")
    print(f"  Alt  perspective:  {alt_gk}")

    rec.emit("dataset", path=str(dataset_path), log_name=log_name,
             activities=activities, perspectives=persp_keys,
             case_gk=case_gk, alt_gk=alt_gk,
             fmt=fmt, n_activities=len(activities))

    # ── ELK setup ─────────────────────────────────────────────────────
    elk_available = (not skip_elk) and _elk_reachable()
    if elk_available:
        print("\n  Setting up ELK index ...")
        _elk_create_index(ELK_INDEX)
        n_elk = _elk_index_events(dataset_path, ELK_INDEX)
        # Re-enable refresh.
        requests.put(
            f"{ELK_ENDPOINT}/{ELK_INDEX}/_settings",
            json={"settings": {"refresh_interval": "1s"}},
            timeout=10,
        )
        requests.post(f"{ELK_ENDPOINT}/{ELK_INDEX}/_refresh", timeout=30)
        print(f"  ELK: indexed {n_elk} events")
        rec.emit("elk_setup", n_events=n_elk, index=ELK_INDEX)
    elif not skip_elk:
        print("  [SKIP] ELK not reachable")

    # ── Per-perspective loop ──────────────────────────────────────────
    for persp_label, gk in perspectives:
        print(f"\n  ── Perspective: {persp_label} ({gk}) ──")

        # Build chains from pair coverage.
        # group_key for sampling: "trace_id" for case perspective,
        # else the attribute key itself.
        sample_key = "trace_id" if persp_label == "case" else gk[0]
        chains = sample_chains_from_traces(
            dataset_path, sample_key,
            target_len=TARGET_CHAIN_LEN,
            min_len=MIN_CHAIN_LEN,
            n_chains=N_QUERIES_PER_CAT,
        )
        if not chains:
            print(f"    No chains of length >= {MIN_CHAIN_LEN} found, skipping.")
            continue

        # chains are windows of event dicts; derive plain activity chains.
        windows = chains
        act_chains = [[e["activity"] for e in w] for w in windows]

        print(f"    Built {len(act_chains)} chains: lengths {[len(c) for c in act_chains]}")

        # ── Warm-up all sub-pairs ─────────────────────────────────────
        # Collect ALL ordered pairs (i<j) across all chains — Algorithm 3
        # intersects every ordered pair of the pattern, so all must be
        # promoted before measurement.  Dedupe across chains.
        all_pairs_seen: set[tuple[str, str]] = set()
        for chain in act_chains:
            all_pairs_seen.update(_chain_all_pairs(chain))

        print(f"    Warming up {len(all_pairs_seen)} unique ordered pairs "
              f"(reps={WARMUP_REPS}) ...")
        t_warmup = time.perf_counter()
        total_warmed = warmup_pairs(log_name, all_pairs_seen, gk,
                                    reps=WARMUP_REPS)

        print(f"    Polling promotions (budget {PROMOTION_BUDGET_S}s) ...")
        tier_counts = wait_for_promotions(
            log_name, all_pairs_seen, gk,
        )
        warmup_wall = time.perf_counter() - t_warmup
        print(f"    Warm-up done: {total_warmed} queries, "
              f"{len(all_pairs_seen)} unique pairs, {warmup_wall:.0f}s, "
              f"tiers={tier_counts}")
        rec.emit("warmup_done", perspective=persp_label,
                 n_pairs_warmed=len(all_pairs_seen),
                 n_queries=total_warmed, wall_s=warmup_wall,
                 tier_counts=tier_counts)

        # ── Build queries ─────────────────────────────────────────────
        queries: list[dict] = []
        qid_counter = itertools.count(1)

        persp_attr = gk[0] if persp_label != "case" else None
        for ci, window in enumerate(windows):
            chain = act_chains[ci]
            # Structural query.
            struct_pat = " ".join(quote_label(a) for a in chain)
            total, pf_err = preflight_query(log_name, struct_pat, gk)
            if pf_err is not None:
                print(f"    [preflight] structural chain {ci} FAILED "
                      f"({pf_err}) — raise PREFLIGHT_TIMEOUT_S or check "
                      f"promotion state; skipping")
                continue
            if total == 0:
                print(f"    [preflight] structural chain {ci} has 0 results, "
                      f"skipping — check indexing lookback λ vs window span")
                continue

            qid = f"S{next(qid_counter)}"
            queries.append({
                "qid":         qid,
                "pattern":     struct_pat,
                "category":    "structural",
                "chain":       chain,
                "constraints": {},
                "perspective": persp_label,
            })
            rec.emit("query_def", qid=qid, pattern=struct_pat,
                     perspective=persp_label, category="structural",
                     chain_len=len(chain),
                     chain_pairs=_chain_sub_pairs(chain),
                     preflight_total=total)

            # Attribute-aware query — constraint values from the window's
            # own boundary events (guaranteed satisfiable).
            attr_pat, constraints = _build_attr_pattern(window, persp_attr)
            if attr_pat:
                total_a, pf_err_a = preflight_query(log_name, attr_pat, gk)
                if pf_err_a is not None:
                    print(f"    [preflight] attr chain {ci} FAILED "
                          f"({pf_err_a}), skipping attr variant")
                elif total_a > 0:
                    qid_a = f"A{next(qid_counter)}"
                    queries.append({
                        "qid":         qid_a,
                        "pattern":     attr_pat,
                        "category":    "attribute_aware",
                        "chain":       chain,
                        "constraints": constraints,
                        "perspective": persp_label,
                    })
                    rec.emit("query_def", qid=qid_a, pattern=attr_pat,
                             perspective=persp_label, category="attribute_aware",
                             chain_len=len(chain),
                             constraints={str(k): list(v) for k, v in constraints.items()},
                             preflight_total=total_a)
                else:
                    print(f"    [preflight] attr chain {ci} has 0 results, skipping attr variant")

        if not queries:
            print(f"    No valid queries for perspective {persp_label}")
            continue

        print(f"    Queries: {len(queries)} "
              f"(structural={sum(1 for q in queries if q['category']=='structural')}, "
              f"attr_aware={sum(1 for q in queries if q['category']=='attribute_aware')})")

        # ── Measure SIESTA ────────────────────────────────────────────
        print(f"\n    ── SIESTA (adaptive warm) ──")
        for q in queries:
            chain = q["chain"]
            n_distinct = len(set(chain))
            n_repeated = len(chain) - n_distinct
            try:
                lat, total, resp_body = measure_siesta(log_name, q["pattern"], gk)
                pair_tiers = resp_body.get("pair_status_after", {})
                tier_summary = {}
                for st in pair_tiers.values():
                    tier_summary[st] = tier_summary.get(st, 0) + 1
                rec.emit("query", system="siesta",
                         perspective=q["perspective"],
                         category=q["category"],
                         qid=q["qid"], pattern=q["pattern"],
                         latency_s=lat, total=total,
                         n_distinct_acts=n_distinct,
                         n_repeated_acts=n_repeated,
                         pair_tiers=tier_summary,
                         log_name=log_name)
                print(f"      {q['qid']:6s} {q['category']:16s} "
                      f"len={len(chain):2d}  distinct={n_distinct}  "
                      f"repeated={n_repeated}  {lat:.3f}s  total={total}  "
                      f"tiers={tier_summary}")
            except Exception as exc:
                rec.emit("query_error", system="siesta",
                         qid=q["qid"], error=str(exc),
                         log_name=log_name)
                print(f"      {q['qid']:6s} ERROR: {exc}")

        # ── Measure ELK (end-to-end: retrieve + verify ordering) ─────
        if elk_available:
            elk_persp = _elk_perspective_key(gk, fmt)
            print(f"\n    ── ELK (retrieve+verify, perspective={elk_persp}) ──")
            for q in queries:
                try:
                    lats, total, n_ev = [], 0, 0
                    for _ in range(MEASURE_REPS):
                        lat, total, n_ev = _elk_detect_verify(
                            q["chain"], q["constraints"], elk_persp,
                        )
                        lats.append(lat)
                    lat = statistics.median(lats)
                    rec.emit("query", system="elk",
                             perspective=q["perspective"],
                             category=q["category"],
                             qid=q["qid"], pattern=q["pattern"],
                             latency_s=lat, total=total,
                             n_events_retrieved=n_ev,
                             log_name=log_name)
                    print(f"      {q['qid']:6s} {q['category']:16s} "
                          f"len={len(q['chain']):2d}  {lat:.3f}s  "
                          f"total={total}  (retrieved {n_ev} events)")
                except Exception as exc:
                    rec.emit("query_error", system="elk",
                             qid=q["qid"], error=str(exc),
                             log_name=log_name)
                    print(f"      {q['qid']:6s} ERROR: {exc}")

    print(f"\n  Results → {rec.path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    global TARGET_CHAIN_LEN, MIN_CHAIN_LEN, PROMOTION_SLEEP_S
    global N_QUERIES_PER_CAT, WARMUP_REPS

    ap = argparse.ArgumentParser(
        description="Experiment 6.4.1 — Competitive latency benchmark.",
    )
    ap.add_argument("--dataset", default=None,
                    help="Path to dataset (CSV or XES).")
    ap.add_argument("--log-name", default=None,
                    help="Log name for API calls.")
    ap.add_argument("--datasets-dir", default=None,
                    help="Run on all CSV/XES files in this directory.")
    ap.add_argument("--skip-elk", action="store_true",
                    help="Skip ELK baseline.")
    ap.add_argument("--target-len", type=int, default=TARGET_CHAIN_LEN,
                    help=f"Target pattern length (default {TARGET_CHAIN_LEN}).")
    ap.add_argument("--min-len", type=int, default=MIN_CHAIN_LEN,
                    help=f"Min pattern length (default {MIN_CHAIN_LEN}).")
    ap.add_argument("--promotion-sleep", type=int, default=PROMOTION_SLEEP_S,
                    help=f"Seconds to wait after warm-up (default {PROMOTION_SLEEP_S}).")
    ap.add_argument("--n-chains", type=int, default=N_QUERIES_PER_CAT,
                    help=f"Chains (queries) per perspective "
                         f"(default {N_QUERIES_PER_CAT}).  NB: the "
                         f"sampler takes one window per group, so the "
                         f"effective count is min(n, #groups).")
    ap.add_argument("--warmup-reps", type=int, default=2,
                    help="Warm-up repetitions per pair (default 2; "
                         "with min_query_count=1 a single rep makes a "
                         "pair eligible — promotion completion is "
                         "ensured by polling, not by reps).")
    args = ap.parse_args()

    log_tag = f"competitive_{args.log_name or 'multi'}"
    log_path = _setup_tee(log_tag)
    print(f"Output log: {log_path}", flush=True)

    TARGET_CHAIN_LEN  = args.target_len
    MIN_CHAIN_LEN     = args.min_len
    PROMOTION_SLEEP_S = args.promotion_sleep
    N_QUERIES_PER_CAT = args.n_chains
    WARMUP_REPS       = args.warmup_reps

    health_check()

    if args.datasets_dir:
        ds_dir = Path(args.datasets_dir)
        for p in sorted(ds_dir.iterdir()):
            if p.suffix.lower() in _LOG_EXTS and p.is_file():
                ln = p.stem.replace(" ", "_").lower()
                try:
                    run_dataset(p, ln, skip_elk=args.skip_elk)
                except Exception as exc:
                    print(f"  FAILED: {exc}")
    else:
        spec = resolve_dataset(args.dataset, args.log_name)
        run_dataset(spec.path, spec.log_name, skip_elk=args.skip_elk)


if __name__ == "__main__":
    main()