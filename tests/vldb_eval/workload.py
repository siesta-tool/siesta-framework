"""
tests/vldb_eval/workload.py

Seeded query workloads for the indexing experiments (Exp 1-5).

Every query is a structural length-2 pattern ``A B`` under one perspective.
Queries are drawn from the perspective's *pair space*: the ordered pairs
(A, B), A != B, such that some group has an A strictly before a B.  Such a
pair always has an STNM instance, so every query has a non-empty result
and its latency reflects real matching work.

Workloads
---------
fixed(n)         the n pairs of highest group coverage, in order
skewed(n, h, r)  each query hits the hot set (the h highest-coverage pairs)
                 with probability r, otherwise a uniformly drawn cold pair
uniform(n)       each query is a uniformly drawn pair of the whole space
multiperspective each query picks a perspective uniformly, then draws a
                 skewed query within it

Coverage is computed offline from the dataset's full.csv (the same data the
experiments ingest) and cached next to it.
"""

from __future__ import annotations

import csv
import json
import random
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

from tests.vldb_eval.eval_common import API_BASE, API_TIMEOUT_S, QUERY_PREFIX, quote_label


@dataclass
class Query:
    seq: int
    perspective: str
    grouping_keys: list[str]
    source: str
    target: str
    hot: bool
    pattern: str = ""

    def __post_init__(self):
        if not self.pattern:
            self.pattern = f"{quote_label(self.source)} {quote_label(self.target)}"

    @property
    def pair(self) -> str:
        return f"{self.source}->{self.target}"


# ---------------------------------------------------------------------------
# Pair coverage
# ---------------------------------------------------------------------------

def _epoch(ts: str) -> int:
    """Seconds since epoch, as the indexer stores timestamps (int seconds)."""
    ts = ts.strip()
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return int(datetime.fromisoformat(ts).timestamp())


def pair_coverage(prep, label: str) -> dict:
    """
    {"group_count", "pairs": [{source, target, groups}]} for perspective
    ``label`` of a prepared dataset, sorted by coverage (descending, ties
    by name).  Cached as coverage_<label>.json.
    """
    from tests.vldb_eval.suite_data import CASE, safe_name

    cache = prep.root / f"coverage_{safe_name(label)}.json"
    if cache.exists():
        return json.loads(cache.read_text())

    column = "trace_id" if label == CASE else label
    # group -> activity -> [first_ts, last_ts]
    span: dict[str, dict[str, list[int]]] = defaultdict(dict)
    with prep.full_csv.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            g = row.get(column)
            if not g:
                continue
            t = _epoch(row["timestamp"])
            s = span[g].get(row["activity"])
            if s is None:
                span[g][row["activity"]] = [t, t]
            else:
                s[0] = min(s[0], t)
                s[1] = max(s[1], t)

    groups: dict[tuple[str, str], int] = defaultdict(int)
    for acts in span.values():
        items = list(acts.items())
        for a, (a_first, _a_last) in items:
            for b, (_b_first, b_last) in items:
                if a != b and a_first < b_last:
                    groups[(a, b)] += 1
    pairs = sorted(
        ({"source": a, "target": b, "groups": n} for (a, b), n in groups.items()),
        key=lambda p: (-p["groups"], p["source"], p["target"]),
    )
    out = {"perspective": label, "group_count": len(span), "pairs": pairs}
    cache.write_text(json.dumps(out))
    return out


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------

def _pairs(cov: dict) -> list[tuple[str, str]]:
    return [(p["source"], p["target"]) for p in cov["pairs"]]


def hot_set(cov: dict, n_hot: int) -> list[tuple[str, str]]:
    return _pairs(cov)[:n_hot]


def fixed(cov: dict, label: str, grouping_keys: list[str], n: int) -> list[Query]:
    return [
        Query(i, label, grouping_keys, a, b, hot=True)
        for i, (a, b) in enumerate(hot_set(cov, n))
    ]


def skewed(
    cov: dict, label: str, grouping_keys: list[str],
    n: int, n_hot: int, hot_ratio: float = 0.8, seed: int = 42,
) -> list[Query]:
    rng = random.Random(seed)
    pairs = _pairs(cov)
    hot, cold = pairs[:n_hot], pairs[n_hot:]
    out = []
    for i in range(n):
        is_hot = not cold or rng.random() < hot_ratio
        a, b = rng.choice(hot if is_hot else cold)
        out.append(Query(i, label, grouping_keys, a, b, hot=is_hot))
    return out


def uniform(
    cov: dict, label: str, grouping_keys: list[str], n: int, seed: int = 42,
) -> list[Query]:
    rng = random.Random(seed)
    pairs = _pairs(cov)
    return [
        Query(i, label, grouping_keys, *rng.choice(pairs), hot=False)
        for i in range(n)
    ]


def multiperspective(
    covs: dict[str, dict], keys: dict[str, list[str]],
    n: int, n_hot: int, hot_ratio: float = 0.8, seed: int = 42,
) -> list[Query]:
    """Each query: a uniformly chosen perspective, then a skewed draw in it."""
    rng = random.Random(seed)
    labels = sorted(covs)
    out = []
    for i in range(n):
        label = rng.choice(labels)
        pairs = _pairs(covs[label])
        hot, cold = pairs[:n_hot], pairs[n_hot:]
        is_hot = not cold or rng.random() < hot_ratio
        a, b = rng.choice(hot if is_hot else cold)
        out.append(Query(i, label, keys[label], a, b, hot=is_hot))
    return out


def save(queries: list[Query], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(asdict(q)) + "\n" for q in queries))


# ---------------------------------------------------------------------------
# Server-side coverage (kept for the complex-querying scripts)
# ---------------------------------------------------------------------------

def fetch_pair_coverage(
    log_name: str,
    grouping_keys: list[str],
    *,
    activities: list[str] | None = None,
    storage_namespace: str = "siesta",
) -> dict:
    """Call the /pair_coverage endpoint of an ingested log."""
    import requests
    from urllib.parse import urljoin

    body: dict = {
        "log_name":          log_name,
        "storage_namespace": storage_namespace,
        "grouping_keys":     grouping_keys,
    }
    if activities:
        body["activities"] = activities

    r = requests.post(
        urljoin(API_BASE, f"/{QUERY_PREFIX}/pair_coverage"),
        json=body,
        timeout=API_TIMEOUT_S,
    )
    r.raise_for_status()
    return r.json()
