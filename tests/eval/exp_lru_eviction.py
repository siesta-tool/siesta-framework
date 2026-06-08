"""
tests/eval/exp_lru_eviction.py

Experiment 6.3.3 — Demand concentration vs. LRU eviction.

Goal
----
Show that a skewed query stream promotes hot pairs to PERSISTENT within
a handful of repetitions — achieving fast (~2 s) latency for the bulk
of subsequent queries — while a uniform stream spreading queries across
all 149 org:resource pairs (> LRU capacity of 128) produces unrelenting
ABSENT (~50 s) scans because no pair ever accumulates enough query mass
to trigger promotion.

This is the mechanism the rounds-based experiment (6.3.2) could not
reveal: with activity-filtered pair spaces that fit inside the LRU, the
uniform workload appeared fast from round 2 onwards due to LRU hits.
Here, 149 > 128 ensures that pairs queried early are evicted before
they can appear again, keeping the uniform stream permanently cold.

Design
------
A single static log is ingested once.  Only the org:resource perspective
is used.  Two streams are executed sequentially from a freshly cleared
adaptive catalog:

  skewed  : n_hot top-coverage pairs, each repeated reps_per_hot times.
            Pairs stay in the LRU between appearances, cross
            min_query_count quickly, and are promoted to PERSISTENT.
            A __sleep__ sentinel is inserted after the 3rd repetition
            to let background materialisation complete.
            n_cold bottom-coverage pairs appear once — serving as an
            in-stream control that never promotes.

  uniform : all discovered pairs, queried exactly once each in
            coverage-sorted order.  No pair crosses min_query_count=3.
            With n_pairs > LRU capacity (128), even repeated passes
            would evict earlier pairs before they could reappear —
            so the ABSENT baseline is structural, not just transient.

Key design difference from exp_skew_vs_uniform_latency (6.3.2)
--------------------------------------------------------------
That experiment used rounds (repeated passes with sleeps in between).
This experiment uses a single continuous stream per workload, which
naturally exposes the LRU eviction mechanism: hot pairs stay warm
because they reappear frequently; uniform pairs are evicted because
the pair space exceeds the cache.

Expected outcome
----------------
Skewed:  seq 0 – (n_hot×3-1)   ABSENT/TRANSIENT   ~50s / ~3s
         sleep sentinel
         seq n_hot×3 onwards    PERSISTENT          ~2s  (hot)
                                ABSENT              ~50s (cold, 1 rep)

Uniform: all 149 queries        ABSENT              ~50s throughout

Output
------
results/6_3_3_lru_eviction_<log_name>.jsonl

Record types:
  dataset      path, log_name, perspective, n_pairs, lru_capacity
  stream_start workload, n_queries
  query        workload, seq, qid, tag, shared_pair, latency_s, tier,
               pair_status_after, total
  query_error  workload, seq, qid, error

Usage
-----
    python -m tests.eval.exp_lru_eviction \\
        --dataset /mnt/datasets/bpic2017.xes --log-name bpic2017

Options
-------
    --perspective   KEY   grouping key to use      [org:resource]
    --n-hot         N     hot pairs                [3]
    --hot-reps      N     repetitions per hot pair [8]
    --n-cold        N     cold pairs (1 rep each)  [7]
    --sleep         S     materialisation sleep (s) [120]
    --skip-ingest         reuse existing ingest
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tests.eval.eval_common import (
    CONFIG_DIR,
    RESULTS_DIR,
    Recorder, health_check,
    ingest_adaptive,
    timed_query,
    resolve_dataset,
)
from tests.eval.workload import fetch_pair_coverage, quote_label


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CONFIG      = CONFIG_DIR / "adaptive_index.config.json"
LRU_CAPACITY = 128          # hard-coded in lru_cache.py get_lru_cache()

RETENTION_OVERRIDES = {
    "min_query_count":   3,
    "half_life_seconds": 3600.0,
    "hysteresis":        0.0,
}

DEFAULT_PERSPECTIVE   = "auto"
DEFAULT_N_HOT_LEVELS  = [3, 10, 30]
DEFAULT_HOT_REPS      = 8
DEFAULT_N_COLD        = 7
DEFAULT_SLEEP         = 120
# Uniform stream cap: must exceed LRU_CAPACITY to trigger eviction, but
# does not need to cover the full pair space.  256 = 2 × LRU cap gives a
# clear flat baseline while keeping runtime to ~2–3 hours.
DEFAULT_MAX_UNIFORM   = 256

_LOG_EXTS = {".csv", ".xes"}


# ---------------------------------------------------------------------------
# Perspective selection
# ---------------------------------------------------------------------------

def select_perspective(
    log_name: str,
    dataset_path: Path,
    preference: str = "auto",
) -> tuple[list[str], int]:
    """
    Return (grouping_key_list, n_pairs) for the best perspective to use.

    If preference == "auto": discover all perspectives from the schema and
    pick the one with the most co-occurring pairs — most likely to exceed
    LRU_CAPACITY and produce clear LRU-eviction behaviour.

    Otherwise treat preference as the grouping key to use directly.
    """
    if preference != "auto":
        gk  = [preference]
        cov = fetch_pair_coverage(log_name, gk)
        return gk, len(cov.get("pairs", []))

    try:
        from tests.eval.eval_common import discover_schema
        schema       = discover_schema(dataset_path)
        perspectives = [[k] for k in schema.perspective_keys] if schema.perspective_keys else []
    except (ImportError, AttributeError):
        perspectives = []

    if not perspectives:
        # Fallback: probe common perspective attribute names directly.
        perspectives = [
            ["org:resource"], ["lifecycle:transition"],
            ["Action"], ["EventOrigin"], ["org:group"],
        ]

    best_gk = perspectives[0]
    best_n  = 0
    print("  Probing perspectives for largest pair space …")
    for gk in perspectives:
        try:
            cov = fetch_pair_coverage(log_name, gk)
            n   = len(cov.get("pairs", []))
            flag = " ← best so far" if n > best_n else ""
            print(f"    {gk[0] if gk else '(none)'}: {n} pairs{flag}")
            if n > best_n:
                best_n  = n
                best_gk = gk
        except Exception as exc:
            print(f"    {gk}: ERROR — {exc}")

    print(f"  Selected: {best_gk}  ({best_n} pairs)")
    return best_gk, best_n


# ---------------------------------------------------------------------------
# Stream builders
# ---------------------------------------------------------------------------

def _q(qid: str, pair: dict, *, log_name: str, gk: list[str],
       tag: str) -> dict:
    a, b = pair["source"], pair["target"]
    return {
        "id":            qid,
        "log_name":      log_name,
        "pattern":       f"{quote_label(a)} {quote_label(b)}",
        "grouping_keys": list(gk),
        "tags":          [tag],
        "shared_pair":   f"{a}->{b}",
        "group_coverage": pair.get("groups", 0),
    }


def build_skewed_stream(
    pairs: list[dict],
    log_name: str,
    gk: list[str],
    *,
    n_hot: int,
    hot_reps: int,
    n_cold: int,
    sleep_seconds: int,
) -> list[dict]:
    """
    Build the skewed query stream.

    Hot pairs (top coverage, n_hot) appear hot_reps times each in
    round-robin order.  A __sleep__ sentinel is inserted after the
    3rd round so that background materialisation completes before the
    4th round reads from Delta.  Cold pairs (bottom coverage, n_cold)
    appear once each, interleaved between hot rounds.
    """
    n_hot  = min(n_hot,  len(pairs))
    n_cold = min(n_cold, len(pairs) - n_hot)

    hot_pairs  = pairs[:n_hot]
    cold_pairs = pairs[-(n_cold):] if n_cold else []

    ctr = [0]
    def _next(tag: str, pair: dict) -> dict:
        ctr[0] += 1
        return _q(f"{'H' if tag=='HOT' else 'C'}{ctr[0]}",
                  pair, log_name=log_name, gk=gk, tag=tag)

    # Build hot_reps rounds of the hot pairs (round-robin).
    hot_rounds: list[list[dict]] = []
    for _ in range(hot_reps):
        hot_rounds.append([_next("HOT", p) for p in hot_pairs])

    # Cold queries split evenly across the gaps between hot rounds.
    cold_qs = [_next("COLD", p) for p in cold_pairs]
    cold_per_gap = max(1, len(cold_qs) // max(1, hot_reps + 1)) if cold_qs else 0

    stream: list[dict] = []
    cold_idx = 0
    for round_idx, round_qs in enumerate(hot_rounds):
        # Insert a slice of cold queries before this hot round.
        if cold_per_gap:
            end = min(cold_idx + cold_per_gap, len(cold_qs))
            stream.extend(cold_qs[cold_idx:end])
            cold_idx = end

        stream.extend(round_qs)

        # Sleep sentinel after the round that crosses min_query_count (round 3).
        if round_idx == 2:
            stream.append({
                "id":            "__sleep__",
                "sleep_seconds": sleep_seconds,
                "log_name":      log_name,
                "pattern":       "",
                "grouping_keys": gk,
                "tags":          ["__sentinel__"],
            })

    # Any remaining cold queries at the end.
    stream.extend(cold_qs[cold_idx:])
    return stream


def build_uniform_stream(
    pairs: list[dict],
    log_name: str,
    gk: list[str],
    *,
    max_pairs: int = DEFAULT_MAX_UNIFORM,
) -> list[dict]:
    """
    Build the uniform query stream: one query per sampled pair.

    Pairs are capped at max_pairs using stratified sampling (top half by
    coverage + bottom half) so the stream spans the full density range.
    max_pairs must exceed LRU_CAPACITY (128) to guarantee eviction; the
    default of 256 = 2 × LRU keeps runtime to ~2–3 hours at ~35 s/query.
    """
    if len(pairs) > max_pairs:
        half    = max_pairs // 2
        sampled = pairs[:half] + pairs[-(max_pairs - half):]
    else:
        sampled = pairs

    ctr = [0]
    stream = []
    for pair in sampled:
        ctr[0] += 1
        stream.append(_q(f"U{ctr[0]}", pair,
                         log_name=log_name, gk=gk, tag="UNIFORM"))
    return stream


# ---------------------------------------------------------------------------
# Stream runner  (identical pattern to exp_warmup.py)
# ---------------------------------------------------------------------------

def run_stream(
    rec: Recorder,
    stream: list[dict],
    *,
    workload: str,
) -> None:
    real_n = sum(1 for q in stream if q["id"] != "__sleep__")
    print(f"\n── {workload} stream: {real_n} queries ──", flush=True)

    seq = 0
    for q in stream:
        if q.get("id") == "__sleep__":
            secs = q.get("sleep_seconds", DEFAULT_SLEEP)
            print(f"\n  [sleep] {secs}s — awaiting background materialisation …",
                  flush=True)
            time.sleep(secs)
            print(f"  [sleep] resuming.\n", flush=True)
            continue

        try:
            body, latency = timed_query(
                log_name=q["log_name"],
                pattern=q["pattern"],
                grouping_keys=q["grouping_keys"],
                retention_overrides=RETENTION_OVERRIDES,
            )
            statuses = body.get("pair_status_after") or {}
            vals     = list(statuses.values())
            if "PERSISTENT" in vals:
                tier = "PERSISTENT"
            elif "TRANSIENT" in vals:
                tier = "TRANSIENT"
            else:
                tier = "ABSENT"

            tag = (q.get("tags") or ["?"])[0]
            rec.emit(
                "query",
                workload=workload,
                seq=seq,
                qid=q["id"],
                tag=tag,
                shared_pair=q.get("shared_pair"),
                group_coverage=q.get("group_coverage"),
                latency_s=latency,
                total=body.get("total", 0),
                tier=tier,
                pair_status_after=statuses,
            )
            print(
                f"  seq={seq:3d}  [{tag:<7s}]  {q.get('shared_pair','?'):<35s}"
                f"  {latency:6.2f}s  {tier}",
                flush=True,
            )

        except Exception as exc:
            rec.emit("query_error", workload=workload, seq=seq,
                     qid=q["id"], error=str(exc))
            print(f"  seq={seq:3d}  ERROR: {exc}", flush=True)

        seq += 1


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

_LOG_EXTS = {".csv", ".xes"}


class AppendRecorder:
    """
    Like eval_common.Recorder but opens the existing JSONL in append mode
    instead of truncating it.  Used by --uniform-only to add uniform stream
    records to a file that already contains skewed stream records.
    """
    def __init__(self, experiment: str, output_name: str) -> None:
        self.experiment = experiment
        self.run_id     = str(uuid.uuid4())[:8]
        self.path       = RESULTS_DIR / output_name
        if not self.path.exists():
            self.path.write_text("")

    def emit(self, event: str, **fields) -> None:
        record = {
            "experiment": self.experiment,
            "run_id":     self.run_id,
            "ts":         time.time(),
            "event":      event,
            **fields,
        }
        with self.path.open("a") as fh:
            fh.write(json.dumps(record) + "\n")


def run_dataset(
    spec,
    *,
    perspective: str,
    n_hot_levels: list[int],
    hot_reps: int,
    n_cold: int,
    sleep_seconds: int,
    max_uniform: int,
    uniform_only: bool,
    skip_ingest: bool,
) -> None:
    print(f"\n{'='*60}")
    print(f"Dataset      : {spec.path}  (log_name={spec.log_name})")
    print(f"n_hot_levels : {n_hot_levels}")
    print(f"{'='*60}", flush=True)

    # ── Bootstrap ingest ─────────────────────────────────────────────────
    if skip_ingest:
        print("Skipping ingest (--skip-ingest).")
    else:
        print("Ingesting (clear_existing=True) …")
        ingest_adaptive(
            spec.log_name, spec.path, CONFIG,
            overrides={"perspectives": []},
            clear_existing=True,
        )
        print("Ingest complete.")
    time.sleep(2)

    # ── Select perspective (per-dataset when "auto") ──────────────────────
    gk, n_pairs = select_perspective(spec.log_name, spec.path, perspective)
    print(f"Perspective  : {gk}  ({n_pairs} pairs)  "
          f"[LRU={LRU_CAPACITY} → "
          f"{'exceeds ✓' if n_pairs > LRU_CAPACITY else 'fits within — no eviction'}]",
          flush=True)

    if n_pairs < 2:
        print("SKIP: fewer than 2 pairs.")
        return

    cov   = fetch_pair_coverage(spec.log_name, gk)
    pairs = sorted(cov.get("pairs", []), key=lambda p: -p["groups"])

    if n_pairs <= LRU_CAPACITY:
        print(f"WARNING: {n_pairs} pairs ≤ LRU {LRU_CAPACITY} — "
              f"eviction will NOT occur for the uniform stream.")

    uniform_stream = build_uniform_stream(pairs, spec.log_name, gk,
                                          max_pairs=max_uniform)
    n_uniform      = len(uniform_stream)

    # ── Recorder ─────────────────────────────────────────────────────────
    # --uniform-only: append to the existing file so skewed results are kept.
    # Normal mode:    Recorder truncates and writes a fresh file.
    output_name = f"6_3_3_lru_eviction_{spec.log_name}.jsonl"
    if uniform_only:
        rec = AppendRecorder("6.3.3", output_name)
        print(f"--uniform-only: appending to {rec.path}", flush=True)
    else:
        rec = Recorder("6.3.3", output_name)
        rec.emit(
            "dataset",
            path=str(spec.path),
            log_name=spec.log_name,
            perspective=gk[0] if gk else perspective,
            n_pairs=n_pairs,
            lru_capacity=LRU_CAPACITY,
            n_hot_levels=n_hot_levels,
            hot_reps=hot_reps,
            n_cold=n_cold,
            retention_overrides=RETENTION_OVERRIDES,
        )

    # ── One stream per skewness level (skipped with --uniform-only) ───────
    if not uniform_only:
        for n_hot in n_hot_levels:
            stream   = build_skewed_stream(
                pairs, spec.log_name, gk,
                n_hot=n_hot, hot_reps=hot_reps,
                n_cold=n_cold, sleep_seconds=sleep_seconds,
            )
            n_stream = sum(1 for q in stream if q["id"] != "__sleep__")
            workload = f"hot_{n_hot}"

            print(f"\n── {workload}  ({n_hot} hot × {hot_reps} reps + "
                  f"{n_cold} cold = {n_stream} queries)", flush=True)
            ingest_adaptive(
                spec.log_name, spec.path, CONFIG,
                overrides={"perspectives": [{"grouping_keys": gk}]},
                clear_existing=True,
            )
            rec.emit("stream_start", workload=workload, n_hot=n_hot,
                     n_queries=n_stream)
            run_stream(rec, stream, workload=workload)

    # ── Uniform baseline ──────────────────────────────────────────────────
    print(f"\n── uniform  ({n_uniform} queries, 1 per pair)", flush=True)
    ingest_adaptive(
        spec.log_name, spec.path, CONFIG,
        overrides={"perspectives": [{"grouping_keys": gk}]},
        clear_existing=True,
    )
    rec.emit("stream_start", workload="uniform", n_hot=n_pairs,
             n_queries=n_uniform)
    run_stream(rec, uniform_stream, workload="uniform")

    print(f"\nResults written to {rec.path}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Experiment 6.3.3 — Demand concentration vs. LRU eviction.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    src = ap.add_mutually_exclusive_group()
    src.add_argument("--dataset",      default=None,
                     help="Path to a single dataset (.csv or .xes).")
    src.add_argument("--datasets-dir", default=None, type=Path,
                     help="Run on every .csv / .xes file in this directory.")

    ap.add_argument("--log-name",    default=None)
    ap.add_argument("--perspective", default=DEFAULT_PERSPECTIVE,
                    help=f"Grouping key (default: {DEFAULT_PERSPECTIVE}).")
    ap.add_argument("--n-hot-levels", default=",".join(map(str, DEFAULT_N_HOT_LEVELS)),
                    help=f"Comma-separated hot-pair counts to evaluate "
                         f"(default: {DEFAULT_N_HOT_LEVELS}).")
    ap.add_argument("--hot-reps", type=int, default=DEFAULT_HOT_REPS,
                    help=f"Repetitions per hot pair (default: {DEFAULT_HOT_REPS}).")
    ap.add_argument("--n-cold",   type=int, default=DEFAULT_N_COLD,
                    help=f"Cold pairs, 1 rep each (default: {DEFAULT_N_COLD}).")
    ap.add_argument("--sleep",    type=int, default=DEFAULT_SLEEP,
                    help=f"Materialisation sleep in seconds (default: {DEFAULT_SLEEP}).")
    ap.add_argument("--max-uniform", type=int, default=DEFAULT_MAX_UNIFORM,
                    help=f"Max pairs in the uniform stream (default: {DEFAULT_MAX_UNIFORM}). "
                         f"Must exceed LRU capacity ({LRU_CAPACITY}) to trigger eviction. "
                         f"Runtime ≈ max_uniform × ~35 s.")
    ap.add_argument("--uniform-only", action="store_true",
                    help="Skip skewed streams and run only the uniform baseline. "
                         "Appends to the existing JSONL so skewed results are preserved.")
    ap.add_argument("--skip-ingest", action="store_true")
    args = ap.parse_args()

    n_hot_levels = [int(x.strip()) for x in args.n_hot_levels.split(",") if x.strip()]

    health_check()

    if args.datasets_dir:
        d = Path(args.datasets_dir)
        if not d.is_dir():
            ap.error(f"{d} is not a directory.")
        specs = [
            resolve_dataset(str(p), p.stem)
            for p in sorted(d.iterdir())
            if p.suffix.lower() in _LOG_EXTS
        ]
        if not specs:
            ap.error(f"No .csv / .xes files found in {d}.")
        print(f"Found {len(specs)} dataset(s) in {d}")
    else:
        specs = [resolve_dataset(args.dataset, args.log_name)]

    for spec in specs:
        try:
            run_dataset(
                spec,
                perspective=args.perspective,
                n_hot_levels=n_hot_levels,
                hot_reps=args.hot_reps,
                n_cold=args.n_cold,
                sleep_seconds=args.sleep,
                max_uniform=args.max_uniform,
                uniform_only=args.uniform_only,
                skip_ingest=args.skip_ingest,
            )
        except Exception as exc:
            import traceback
            print(f"\n[ERROR] {spec.log_name}: {exc}")
            traceback.print_exc()
            print("Continuing with next dataset …")


if __name__ == "__main__":
    main()