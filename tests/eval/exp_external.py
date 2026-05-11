"""
tests/eval/exp_external.py

Experiment 6.4 — External evaluation.

Drives the same workloads against multiple systems and records
end-to-end query latency.  Plots are produced separately from the
resulting JSONL.

Systems
-------
adaptive_warm
    The adaptive system after a warm-up pass.  Hot pairs are at
    PERSISTENT (L3); this reflects steady-state latency for a
    recurring workload.

adaptive_cold
    The adaptive system on first touch.  Everything is at L0/ABSENT;
    this is the worst-case adaptive latency.

eager
    The pre-existing SIESTA-style indexer in the same codebase.
    Attribute-constrained queries require a raw-log join in the eager
    system; that overhead is visible in the latency gap.

elk  (optional)
    Elasticsearch baseline.  Skipped if ELK_ENDPOINT is not reachable.
    Pattern queries degenerate to sequential scans; we approximate with
    a match-all query filtered to the relevant activities.

Workloads
---------
structural      — no attribute constraints, exercises ordering only
attribute_aware — inline-bracket constraints, exercises in-index eval

Both workloads are derived from the configured dataset via
`build_workloads`, so the script works on any CSV or XES log.

Scalability sweep
-----------------
If SCALABILITY_DATASETS is populated with (path, label) pairs, each
system is run on progressively larger logs.  If only the configured
default dataset is available, the sweep is skipped.

Output
------
results/6_4_external.jsonl with records of the form:

    {"event": "query", "system": "adaptive_warm",
     "category": "structural", "qid": "S1",
     "pattern": "A B", "latency_s": 0.41, "total": 3,
     "log_size": 60}
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import requests
from urllib.parse import urljoin

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tests.eval.eval_common import (
    API_BASE, API_TIMEOUT_S, CONFIG_DIR,
    EAGER_QUERY, QUERY_PREFIX,
    Recorder, health_check,
    ingest_adaptive, ingest_eager,
    timed_query, detect_eager,
    discover_schema, resolve_dataset,
    _guess_mime,
)
from tests.eval.workload import build_workloads


CONFIG = CONFIG_DIR / "adaptive_index.config.json"

# ---------------------------------------------------------------------------
# ELK configuration (optional)
# ---------------------------------------------------------------------------
ELK_ENDPOINT = os.environ.get("ELK_ENDPOINT", "http://localhost:9200")
ELK_INDEX    = os.environ.get("ELK_INDEX", "siesta_events")

# ---------------------------------------------------------------------------
# Scalability datasets
# Configure as a list of (path_str, human_label) via SCALABILITY_DATASETS
# environment variable (colon-separated "path:label" pairs).
# ---------------------------------------------------------------------------
_RAW_SCALE = os.environ.get("SCALABILITY_DATASETS", "")
SCALABILITY_DATASETS: list[tuple[Path, str]] = []
for _entry in _RAW_SCALE.split(";"):
    _entry = _entry.strip()
    if ":" in _entry:
        _p, _l = _entry.rsplit(":", 1)
        SCALABILITY_DATASETS.append((Path(_p), _l))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _elk_reachable() -> bool:
    try:
        r = requests.get(ELK_ENDPOINT, timeout=3)
        return r.status_code == 200
    except Exception:
        return False


def _log_size(path: Path) -> int:
    """Row count minus header (for CSV); event count estimate for XES."""
    if path.suffix.lower() == ".csv":
        return sum(1 for _ in path.open()) - 1
    # XES: count <event> tags.
    count = 0
    with path.open("rb") as f:
        for line in f:
            if b"<event" in line:
                count += 1
    return count


# ---------------------------------------------------------------------------
# Per-system runners
# ---------------------------------------------------------------------------

def run_adaptive_warm(
    rec: Recorder,
    category: str,
    workload: list[dict],
    log_name: str,
    *,
    log_size: int = 0,
) -> None:
    """Measure steady-state (warm) adaptive latency."""
    print(f"\n── Adaptive (warm) — {category} ──")

    # Silent warm-up pass — not recorded.
    for q in workload:
        try:
            timed_query(q["log_name"], q["pattern"], q["grouping_keys"])
        except Exception:
            pass

    # Measured pass.
    for q in workload:
        t0 = time.perf_counter()
        try:
            body, latency = timed_query(
                q["log_name"], q["pattern"], q["grouping_keys"],
            )
            rec.emit("query",
                     system="adaptive_warm", category=category,
                     qid=q["id"], pattern=q["pattern"],
                     has_constraints="[" in q["pattern"],
                     latency_s=latency,
                     total=body.get("total", 0),
                     log_name=log_name, log_size=log_size)
            print(f"  {q['id']:4s} {q['pattern']:40s} -> {latency:.3f}s")
        except Exception as exc:
            rec.emit("query_error",
                     system="adaptive_warm", category=category,
                     qid=q["id"], log_name=log_name, error=str(exc))
            print(f"  {q['id']:4s} ERROR: {exc}")


def run_adaptive_cold(
    rec: Recorder,
    category: str,
    workload: list[dict],
    log_name: str,
    dataset_path: Path,
    *,
    log_size: int = 0,
) -> None:
    """Measure first-touch (cold) adaptive latency after a clean ingest."""
    print(f"\n── Adaptive (cold) — {category} ──")

    ingest_adaptive(log_name, dataset_path, CONFIG, overrides={
        "overwrite_data": True,
    })
    time.sleep(1)

    for q in workload:
        try:
            body, latency = timed_query(
                q["log_name"], q["pattern"], q["grouping_keys"],
            )
            rec.emit("query",
                     system="adaptive_cold", category=category,
                     qid=q["id"], pattern=q["pattern"],
                     has_constraints="[" in q["pattern"],
                     latency_s=latency,
                     total=body.get("total", 0),
                     log_name=log_name, log_size=log_size)
            print(f"  {q['id']:4s} {q['pattern']:40s} -> {latency:.3f}s")
        except Exception as exc:
            rec.emit("query_error",
                     system="adaptive_cold", category=category,
                     qid=q["id"], log_name=log_name, error=str(exc))
            print(f"  {q['id']:4s} ERROR: {exc}")


def run_eager(
    rec: Recorder,
    category: str,
    workload: list[dict],
    log_name: str,
    *,
    log_size: int = 0,
) -> None:
    """Run queries through the eager SIESTA query endpoint."""
    print(f"\n── Eager (SIESTA) — {category} ──")
    for q in workload:
        t0 = time.perf_counter()
        try:
            body = detect_eager(log_name, q["pattern"])
            latency = time.perf_counter() - t0
            rec.emit("query",
                     system="eager", category=category,
                     qid=q["id"], pattern=q["pattern"],
                     has_constraints="[" in q["pattern"],
                     latency_s=latency,
                     total=body.get("total", 0),
                     log_name=log_name, log_size=log_size)
            print(f"  {q['id']:4s} {q['pattern']:40s} -> {latency:.3f}s")
        except Exception as exc:
            latency = time.perf_counter() - t0
            rec.emit("query_error",
                     system="eager", category=category,
                     qid=q["id"], log_name=log_name, error=str(exc))
            print(f"  {q['id']:4s} ERROR: {exc}")


def run_elk(
    rec: Recorder,
    category: str,
    workload: list[dict],
    log_name: str,
    *,
    log_size: int = 0,
) -> None:
    """
    ELK baseline.  Each query is approximated as a match-all filter
    on the relevant activities.  This gives a lower-bound latency for
    ELK (no structural pattern evaluation) that still reveals the
    scalability gap vs index-based approaches.
    """
    if not _elk_reachable():
        rec.emit("skip", system="elk",
                 reason=f"ELK endpoint not reachable at {ELK_ENDPOINT}")
        print(f"  [SKIP] ELK not reachable at {ELK_ENDPOINT}")
        return

    print(f"\n── ELK — {category} ──")
    for q in workload:
        # Derive activity tokens from the pattern (strip bracket blocks).
        import re
        clean = re.sub(r"\[[^\]]*\]", "", q["pattern"])
        tokens = re.findall(r"[A-Za-z][A-Za-z0-9_:]*", clean)
        if len(tokens) < 2:
            continue
        elk_query = {
            "query": {
                "bool": {
                    "should": [{"term": {"activity": t}} for t in tokens[:3]]
                }
            },
            "size": 0,
        }
        t0 = time.perf_counter()
        try:
            r = requests.post(
                f"{ELK_ENDPOINT}/{ELK_INDEX}/_search",
                json=elk_query, timeout=API_TIMEOUT_S,
            )
            r.raise_for_status()
            latency = time.perf_counter() - t0
            total = r.json().get("hits", {}).get("total", {}).get("value", 0)
            rec.emit("query",
                     system="elk", category=category,
                     qid=q["id"], pattern=q["pattern"],
                     has_constraints="[" in q["pattern"],
                     latency_s=latency, total=total,
                     log_name=log_name, log_size=log_size)
            print(f"  {q['id']:4s} {q['pattern']:40s} -> {latency:.3f}s")
        except Exception as exc:
            rec.emit("query_error",
                     system="elk", category=category,
                     qid=q["id"], log_name=log_name, error=str(exc))
            print(f"  {q['id']:4s} ERROR: {exc}")


# ---------------------------------------------------------------------------
# Scalability sweep
# ---------------------------------------------------------------------------

def run_scalability_sweep(
    rec: Recorder,
    default_log_name: str,
    default_dataset: Path,
    include_elk: bool,
) -> None:
    """
    Run a two-query probe on each dataset in SCALABILITY_DATASETS.

    Skipped if fewer than 2 datasets are configured.
    """
    datasets = SCALABILITY_DATASETS or [(default_dataset, "default")]
    if len(datasets) < 2:
        print("\n[SKIP] Scalability sweep: fewer than 2 datasets configured.")
        print("       Set SCALABILITY_DATASETS='path1:label1;path2:label2;...'")
        rec.emit("skip", system="scalability",
                 reason="fewer than 2 scalability datasets configured")
        return

    print("\n══ Scalability sweep ══")

    # Two probe queries — one structural, one attribute-aware.
    # Built dynamically from the first dataset's schema.
    first_schema = discover_schema(datasets[0][0])
    acts = first_schema.activities
    if len(acts) < 2:
        print("[SKIP] Scalability sweep: too few activities in dataset.")
        return

    a, b = acts[0], acts[1]
    persp = [[k] for k in first_schema.perspective_keys[:1]] or [[]]
    gk = persp[0]

    str_attr = next(
        (k for k, vs in first_schema.attribute_values.items() if vs
         and not all(_is_num(v) for v in vs)), None
    )
    probes = [
        {"id": "probe_S", "pattern": f"{a} {b}",   "grouping_keys": gk,
         "category": "structural"},
    ]
    if str_attr:
        v = first_schema.attribute_values[str_attr][0].replace('"', '\\"')
        probes.append({
            "id": "probe_A",
            "pattern": f'{a}[{str_attr}="{v}"] {b}',
            "grouping_keys": gk,
            "category": "attribute_aware",
        })

    for dataset_path, label in datasets:
        if not dataset_path.exists():
            print(f"  [SKIP] {dataset_path} not found")
            continue

        log_name = label
        size = _log_size(dataset_path)
        print(f"\n  Dataset: {label} ({size} events)")

        # Ingest into both systems.
        ingest_adaptive(log_name, dataset_path, CONFIG, overrides={"overwrite_data": True})
        ingest_eager(log_name, dataset_path, CONFIG)
        time.sleep(1)

        # Warm-up adaptive.
        for probe in probes:
            try:
                timed_query(log_name, probe["pattern"], probe["grouping_keys"])
            except Exception:
                pass

        # Measure each system on each probe.
        for probe in probes:
            cat = probe["category"]

            # Adaptive warm
            try:
                body, latency = timed_query(
                    log_name, probe["pattern"], probe["grouping_keys"])
                rec.emit("query", system="adaptive_warm", category=cat,
                         qid=probe["id"], pattern=probe["pattern"],
                         has_constraints="[" in probe["pattern"],
                         latency_s=latency,
                         total=body.get("total", 0),
                         log_name=label, log_size=size)
                print(f"    [adaptive_warm] {probe['id']} -> {latency:.3f}s")
            except Exception as exc:
                rec.emit("query_error", system="adaptive_warm",
                         qid=probe["id"], log_name=label, error=str(exc))

            # Eager
            t0 = time.perf_counter()
            try:
                body = detect_eager(log_name, probe["pattern"])
                latency = time.perf_counter() - t0
                rec.emit("query", system="eager", category=cat,
                         qid=probe["id"], pattern=probe["pattern"],
                         has_constraints="[" in probe["pattern"],
                         latency_s=latency,
                         total=body.get("total", 0),
                         log_name=label, log_size=size)
                print(f"    [eager]          {probe['id']} -> {latency:.3f}s")
            except Exception as exc:
                rec.emit("query_error", system="eager",
                         qid=probe["id"], log_name=label, error=str(exc))

            # ELK (optional)
            if include_elk and _elk_reachable():
                run_elk(rec, cat, [probe], label, log_size=size)


def _is_num(s: str) -> bool:
    try:
        float(s)
        return True
    except (TypeError, ValueError):
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Experiment 6.4 — External evaluation.",
    )
    ap.add_argument("--dataset", default=None,
                    help="Path to dataset (CSV or XES). Defaults to "
                         "EVAL_DATASET env var, then datasets/test.xes.")
    ap.add_argument("--log-name", default=None,
                    help="Log name. Defaults to EVAL_LOG_NAME env var, then 'test'.")
    ap.add_argument("--skip-elk", action="store_true",
                    help="Skip the ELK baseline even if the endpoint is reachable.")
    args = ap.parse_args()

    spec      = resolve_dataset(args.dataset, args.log_name)
    workloads = build_workloads(args.dataset, args.log_name)
    size      = _log_size(spec.path)

    print(f"Dataset:      {spec.path}  (log_name={spec.log_name})")
    print(f"Log size:     {size} events")
    print(f"Activities:   {workloads.context.activities}")
    print(f"Perspectives: {workloads.context.perspectives}")

    health_check()
    rec = Recorder("6.4", "6_4_external.jsonl")
    rec.emit("dataset", path=str(spec.path), log_name=spec.log_name,
             activities=workloads.context.activities,
             perspectives=workloads.context.perspectives,
             log_size=size)

    print("\nIngesting (adaptive + eager) ...")
    ingest_adaptive(spec.log_name, spec.path, CONFIG, overrides={"overwrite_data": True})
    ingest_eager(spec.log_name, spec.path, CONFIG)
    time.sleep(2)

    for category, workload in [
        ("structural",      workloads.structural),
        ("attribute_aware", workloads.attribute_aware),
    ]:
        print(f"\n══ Category: {category} ══")
        # Warm run first so the warm-up curve matches.
        run_adaptive_warm(rec, category, workload, spec.log_name, log_size=size)
        # Cold run resets state.
        run_adaptive_cold(rec, category, workload, spec.log_name, spec.path, log_size=size)
        run_eager(rec, category, workload, spec.log_name, log_size=size)
        if not args.skip_elk:
            run_elk(rec, category, workload, spec.log_name, log_size=size)

    run_scalability_sweep(rec, spec.log_name, spec.path,
                          include_elk=(not args.skip_elk))

    print(f"\nResults written to {rec.path}")
    print("\nFigures this data supports:")
    print("  - Latency bar: adaptive_warm vs eager, structural queries")
    print("  - Latency bar: adaptive_warm vs eager, attribute-aware queries")
    print("    (gap grows as constraints become richer)")
    print("  - Scalability line: latency vs log_size, all systems")
    print("  - Expressiveness table: built statically in the paper")


if __name__ == "__main__":
    main()