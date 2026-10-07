"""
tests/vldb_eval/scaleout/shuffle.py

Capture Spark shuffle metrics for the group-colocation step of an index build.

The adaptive/eager index repartitions events into their grouping-key buckets
(case, then each perspective) — a shuffle.  We read the driver's REST metrics
(http://localhost:4040/api/v1), snapshot cumulative stage totals before a build
and again after, and diff them so each build's own shuffle cost is isolated
(the SparkSession is long-lived, so metrics accumulate across calls).

`shuffle_seconds` sums task-level shuffle write time and read fetch-wait time
when the Spark build exposes them; shuffle bytes/records are always captured as
a robust fallback signal for the figure.
"""

from __future__ import annotations

import json
import urllib.request
from typing import Any

REST = "http://localhost:4040/api/v1"

# Candidate StageData keys across Spark versions.  Bytes/records are stable;
# the *Time keys are summed into shuffle_seconds when present.
_BYTE_KEYS = (
    "shuffleReadBytes", "shuffleWriteBytes",
    "shuffleReadRecords", "shuffleWriteRecords",
    "shuffleLocalBytesRead", "shuffleRemoteBytesRead",
)
_NS_TIME_KEYS = ("shuffleWriteTime",)          # nanoseconds
_MS_TIME_KEYS = ("shuffleFetchWaitTime",)      # milliseconds
_CTX_KEYS = ("executorRunTime", "executorCpuTime")


def _get(url: str, timeout: float = 5.0) -> Any:
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read().decode())


def app_id() -> str | None:
    try:
        apps = _get(f"{REST}/applications")
        return apps[0]["id"] if apps else None
    except Exception:
        return None


def snapshot() -> dict:
    """Cumulative shuffle totals across all completed stages of the live app."""
    totals = {k: 0 for k in (*_BYTE_KEYS, *_NS_TIME_KEYS, *_MS_TIME_KEYS, *_CTX_KEYS)}
    totals["_ok"] = False
    aid = app_id()
    if not aid:
        return totals
    try:
        stages = _get(f"{REST}/applications/{aid}/stages?status=COMPLETE")
    except Exception:
        return totals
    for st in stages:
        for k in totals:
            if k == "_ok":
                continue
            v = st.get(k)
            if isinstance(v, (int, float)):
                totals[k] += v
    totals["_ok"] = True
    totals["_app"] = aid
    return totals


def diff(before: dict, after: dict) -> dict:
    """Per-field difference + derived shuffle_seconds for one build."""
    out: dict = {}
    if not (before.get("_ok") and after.get("_ok")):
        out["available"] = False
        return out
    # A driver restart (new app id) resets counters; treat `after` as the delta.
    reset = before.get("_app") != after.get("_app")
    for k in (*_BYTE_KEYS, *_NS_TIME_KEYS, *_MS_TIME_KEYS, *_CTX_KEYS):
        a, b = after.get(k, 0), 0 if reset else before.get(k, 0)
        out[k] = max(0, a - b)
    secs = sum(out[k] for k in _NS_TIME_KEYS) / 1e9 \
        + sum(out[k] for k in _MS_TIME_KEYS) / 1e3
    out["shuffle_seconds"] = secs if secs > 0 else None
    out["shuffle_bytes"] = out.get("shuffleReadBytes", 0) + out.get("shuffleWriteBytes", 0)
    out["available"] = True
    return out


if __name__ == "__main__":
    # Quick probe: print the raw keys Spark currently exposes on a stage, so the
    # field names above can be validated against this cluster's Spark build.
    aid = app_id()
    print("app:", aid)
    if aid:
        stages = _get(f"{REST}/applications/{aid}/stages")
        if stages:
            print("sample stage keys:", sorted(stages[0].keys()))
        print("snapshot:", {k: v for k, v in snapshot().items() if v})
