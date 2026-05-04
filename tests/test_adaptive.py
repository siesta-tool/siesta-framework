#!/usr/bin/env python3
"""
End-to-end smoke test for the adaptive indexer + query module.

Exercises two scenarios:

Scenario A — Single-pair lifecycle
    Drives a two-activity pattern through the full L0 -> L1 -> TRANSIENT
    -> PERSISTENT lifecycle by repeating the same query until the
    retention policy promotes the pair.

Scenario B — Mixed-level pattern
    Runs a three-activity pattern whose required pairs sit at different
    lifecycle levels simultaneously:
        (A, B) — PERSISTENT  (warmed up in Scenario A)
        (B, C) — TRANSIENT   (queried once, lives in LRU only)
        (A, C) — ABSENT      (never queried before this test)
    Verifies that the query returns correct results despite fetching
    each pair from a different source (Delta / LRU / lazy scan) and
    unioning them into a single index DataFrame.

Prerequisites
-------------
- Full stack running:  docker compose up --build siesta-api
- API reachable at:    http://localhost:8000
- Dataset at:          datasets/test_adaptive.csv
- Configs at:          config/adaptive_index.config.json
                       config/adaptive_query.config.json

Usage
-----
    python3 tests/test_adaptive_lifecycle.py
"""

import json
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

API_BASE   = "http://localhost:8000"
LOG_NAME   = "test"
NAMESPACE  = "siesta"
GROUPING   = ["resource"]
LOOKBACK   = "30d"

REPO_ROOT        = Path(__file__).resolve().parents[1]

# Route prefixes — set these to match your running container.
# Run:  curl http://localhost:8000/openapi.json | python3 -m json.tool | grep ""/adaptive"
# and read off the actual prefixes.
INDEXER_PREFIX = "adaptive_indexing"   # change to "adaptive_indexing" if needed
QUERY_PREFIX   = "adaptive_querying"  # change to "adaptive_executor" if needed
DATASET_PATH     = REPO_ROOT / "datasets" / "test.csv"
INDEX_CONFIG_PATH = REPO_ROOT / "config" / "adaptive_index.config.json"

PASS = "\033[92m PASS\033[0m"
FAIL = "\033[91m FAIL\033[0m"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def check(condition: bool, description: str) -> bool:
    tag = PASS if condition else FAIL
    print(f"  [{tag}] {description}")
    return condition


def post_ingest() -> dict:
    print("\n── Adaptive Ingest ─────────────────────────────────────────────")
    with open(INDEX_CONFIG_PATH) as f:
        index_config_str = f.read()
    with open(DATASET_PATH, "rb") as fp:
        r = requests.post(
            urljoin(API_BASE, f"/{INDEXER_PREFIX}/run"),
            files={"log_file": (DATASET_PATH.name, fp, "text/csv")},
            data={"index_config": index_config_str},
            timeout=300,
        )
    r.raise_for_status()
    body = r.json()
    check(body.get("code") == 200, f"Ingest returned code 200 (got {body.get('code')})")
    print(f"  time={body.get('time', '?'):.2f}s")
    return body


def post_eager_ingest() -> None:
    """
    Run the eager indexer to populate the shared pairs_index.

    The Scenario B cross-check calls /querying/detection which reads
    exclusively from pairs_index (built only by the eager indexer).
    Without this step, the cross-check always returns 0.
    """
    print("\n── Eager Ingest (pairs_index for cross-check) ──────────────────")
    with open(INDEX_CONFIG_PATH) as f:
        index_config_str = f.read()
    with open(DATASET_PATH, "rb") as fp:
        r = requests.post(
            urljoin(API_BASE, "/indexing/run"),
            files={"log_file": (DATASET_PATH.name, fp, "text/csv")},
            data={"index_config": index_config_str},
            timeout=300,
        )
    if r.status_code != 200:
        print(f"  WARN: eager ingest returned {r.status_code} — cross-check may be unreliable.")
        return
    body = r.json()
    ok = check(body.get("code") == 200, f"Eager ingest returned code 200 (got {body.get('code')})")
    if ok:
        t = body.get('time')
        try:
            print(f"  time={float(t):.2f}s")
        except (TypeError, ValueError):
            print(f"  time={t}s")


def post_detection(pattern: str, label: str = "") -> dict:
    tag = f" [{label}]" if label else ""
    print(f"\n  Detection{tag}: pattern='{pattern}'")
    r = requests.post(
        urljoin(API_BASE, f"/{QUERY_PREFIX}/detection"),
        json={
            "log_name":         LOG_NAME,
            "storage_namespace": NAMESPACE,
            "method":           "detection",
            "query":            {"pattern": pattern},
            "grouping_keys":    GROUPING,
            "lookback":         LOOKBACK,
            "lookback_mode":    "time",
            "support_threshold": 0.0,
        },
        timeout=300,
    )
    r.raise_for_status()
    body = r.json()
    print(
        f"    code={body.get('code')}, "
        f"total={body.get('total')}, "
        f"perspective={body.get('perspective')}, "
        f"time={body.get('time', 0):.3f}s"
    )
    return body


# ---------------------------------------------------------------------------
# Scenario A — single-pair lifecycle  (A, B)
# ---------------------------------------------------------------------------

def scenario_a() -> bool:
    """
    Warm up pair (A,B) from ABSENT all the way to PERSISTENT.

    Query 1 : cold build  -> ABSENT => lazy scan, result cached in LRU, status -> TRANSIENT
    Query 2 : LRU hit     -> TRANSIENT, served from cache
    Query 3 : LRU hit     -> TRANSIENT, served from cache; post-query promoter
              fires because query_count == min_query_count (3), promoting to PERSISTENT
    Query 4 : Delta read  -> PERSISTENT, served from per-pair Delta table

    The test cannot inspect the catalog directly, so correctness is inferred
    from timing: a Delta read is faster than a lazy scan.  The presence of
    API log lines is the primary verification mechanism.
    """
    print("\n══ Scenario A: single-pair lifecycle (A B) ═════════════════════")
    all_ok = True

    timings = []
    for i in range(1, 5):
        expected = {
            1: "cold lazy build  -> TRANSIENT",
            2: "LRU hit          -> TRANSIENT",
            3: "LRU hit + L3 promotion -> PERSISTENT",
            4: "Delta read       -> PERSISTENT",
        }[i]
        body = post_detection("A B", label=f"query {i} — {expected}")
        ok = check(body.get("code") == 200, "response code 200")
        ok &= check(body.get("total", 0) > 0, "non-empty result set")
        all_ok &= ok
        timings.append(body.get("time", 0))

    # Query 4 (Delta) should be faster than query 1 (lazy scan).
    # This is a soft check — Spark is non-deterministic on small data
    # so we only warn rather than hard-fail.
    if timings[0] > 0 and timings[3] > 0:
        if timings[3] < timings[0]:
            print(f"  [ INFO] Query 4 ({timings[3]:.3f}s) < Query 1 ({timings[0]:.3f}s) — Delta faster, good.")
        else:
            print(f"  [ WARN] Query 4 ({timings[3]:.3f}s) >= Query 1 ({timings[0]:.3f}s) — "
                  "expected Delta to be faster; check API logs.")

    print(f"\n  Scenario A: {'PASS' if all_ok else 'FAIL'}")
    return all_ok


# ---------------------------------------------------------------------------
# Scenario B — mixed-level pattern  (A B C)
# ---------------------------------------------------------------------------

def scenario_b() -> bool:
    """
    Run a three-activity pattern whose pairs span different lifecycle levels.

    At the point this scenario runs:
        (A, B) — PERSISTENT  (promoted by Scenario A)
        (B, C) — ABSENT      (never queried)
        (A, C) — ABSENT      (never queried)

    Step 1: query "B C" once to promote (B,C) to TRANSIENT.
            This puts (B,C) in the LRU cache but keeps it below
            min_query_count so it stays TRANSIENT.

    Step 2: query "A B C".
            Required pairs: (A,B) PERSISTENT, (B,C) TRANSIENT, (A,C) ABSENT.
            The query assembles three DataFrames from three different sources:
              - (A,B) : Delta read
              - (B,C) : LRU cache
              - (A,C) : lazy scan (built on the fly)
            All three are unioned and fed to the CEP validator.

    Step 3: verify correctness.
            The result must be consistent with what "A B C" returns when
            run against the eager (trace_id) query module on the same data.
            We verify by running the same pattern through the eager endpoint
            and comparing group counts (not exact group IDs, since eager
            groups by trace_id and adaptive groups by resource).
    """
    print("\n══ Scenario B: mixed-level pattern (A B C) ════════════════════")
    all_ok = True

    # Step 1: query "B C" once — promotes (B,C) to TRANSIENT only.
    print("\n  Step 1 — warm (B,C) to TRANSIENT with a single query.")
    body_bc = post_detection("B C", label="warm B C -> TRANSIENT")
    all_ok &= check(body_bc.get("code") == 200, "B C query returned code 200")
    # (B,C) is now TRANSIENT in LRU; still below min_query_count=3 so no L3 promotion.

    # Step 2: query "A B C" — mixed-level fetch.
    print("\n  Step 2 — query 'A B C' with mixed-level pairs.")
    print("           (A,B)=PERSISTENT  (B,C)=TRANSIENT  (A,C)=ABSENT")
    body_abc = post_detection("A B C", label="mixed-level fetch")
    all_ok &= check(body_abc.get("code") == 200, "A B C query returned code 200")
    all_ok &= check(
        body_abc.get("perspective") == "resource",
        f"perspective is 'resource' (got '{body_abc.get('perspective')}')",
    )
    total_adaptive = body_abc.get("total", 0)
    all_ok &= check(total_adaptive > 0, f"non-empty result set (got {total_adaptive} groups)")

    # Step 3: compare against eager endpoint for sanity.
    # The eager endpoint groups by trace_id; adaptive groups by resource.
    # We don't expect the counts to match exactly, but the eager endpoint
    # must also return a non-empty result — if it's empty, the pattern
    # genuinely has no matches and we can't use it as a reference.
    print("\n  Step 3 — cross-check against eager query endpoint.")
    r_eager = requests.post(
        urljoin(API_BASE, "/querying/detection"),
        json={
            "log_name":         LOG_NAME,
            "storage_namespace": NAMESPACE,
            "method":           "detection",
            "query":            {"pattern": "A B C"},
            "support_threshold": 0.0,
        },
        timeout=300,
    )
    if r_eager.status_code == 200:
        body_eager = r_eager.json()
        total_eager = body_eager.get("total", 0)
        print(f"    eager total={total_eager}, adaptive total={total_adaptive}")
        all_ok &= check(
            total_eager > 0,
            f"eager also finds matches (got {total_eager}) — pattern has real support",
        )
        # The adaptive result groups by resource (3 values), eager by trace_id (20 values).
        # Adaptive total must be <= number of distinct resource values.
        all_ok &= check(
            total_adaptive <= 3,
            f"adaptive total ({total_adaptive}) <= distinct resource values (3)",
        )
    else:
        print(f"    WARN: eager endpoint returned {r_eager.status_code}, skipping cross-check.")

    # Step 4: verify mixed-level assembly did not corrupt results by
    # re-running "A B" (which is PERSISTENT) and checking its count
    # is >= the "A B C" count (since A B C is a stricter pattern).
    print("\n  Step 4 — sanity: |A B| >= |A B C| under same perspective.")
    body_ab = post_detection("A B", label="re-check A B")
    total_ab = body_ab.get("total", 0)
    all_ok &= check(
        total_ab >= total_adaptive,
        f"|A B|={total_ab} >= |A B C|={total_adaptive}",
    )

    print(f"\n  Scenario B: {'PASS' if all_ok else 'FAIL'}")
    return all_ok


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    # Sanity checks before starting.
    if not DATASET_PATH.exists():
        sys.exit(f"Dataset not found: {DATASET_PATH}\n"
                 "Copy test_adaptive.csv to datasets/test_adaptive.csv first.")
    if not INDEX_CONFIG_PATH.exists():
        sys.exit(f"Config not found: {INDEX_CONFIG_PATH}\n"
                 "Copy adaptive_index.config.json to config/ first.")

    # Health check.
    try:
        r = requests.get(urljoin(API_BASE, "/health"), timeout=5)
        r.raise_for_status()
        print("API health:", r.json())
    except Exception as exc:
        sys.exit(f"API not reachable at {API_BASE}: {exc}")

    # Ingest — adaptive indexer first, then eager to populate pairs_index.
    post_ingest()
    post_eager_ingest()
    time.sleep(2)

    # Run scenarios.
    ok_a = scenario_a()
    ok_b = scenario_b()

    # Summary.
    print("\n══ Summary ═════════════════════════════════════════════════════")
    print(f"  Scenario A (single-pair lifecycle):  {'PASS' if ok_a else 'FAIL'}")
    print(f"  Scenario B (mixed-level pattern):    {'PASS' if ok_b else 'FAIL'}")

    print("\nWhat to verify in the API logs:")
    print("  Scenario A:")
    print("    - 'promoting resource L0->L1'                (query 1)")
    print("    - 'pair (A,B) under resource -> TRANSIENT'   (query 1)")
    print("    - 'persisting pair (A,B) under resource'     (query 3)")
    print("    - no rebuild line for (A,B)                  (query 4)")
    print("  Scenario B:")
    print("    - 'pair (B,C) under resource -> TRANSIENT'   (B C warm-up)")
    print("    - L3 Delta read for (A,B)                    (A B C query)")
    print("    - LRU hit for (B,C)                          (A B C query)")
    print("    - lazy scan for (A,C)                        (A B C query)")
    print("    - all three DataFrames unioned correctly      (no error lines)")

    print("\nMinIO console: http://localhost:9001  (minioadmin / minioadmin)")
    print("Look under: siesta/test_adaptive/adaptive/resource/")
    print("  pairs/A__B/   — should exist after Scenario A query 3")
    print("  pairs/B__C/   — should NOT exist (still TRANSIENT)")
    print("  pairs/A__C/   — should NOT exist (still ABSENT)")

    if not (ok_a and ok_b):
        sys.exit(1)


if __name__ == "__main__":
    main()