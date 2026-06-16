"""
exp_multiperspective.py — Reviewer W3 response experiment.

Demonstrates that the same pattern query, evaluated under different
analytical perspectives simultaneously maintained by the adaptive index,
produces meaningfully different results — grounding the multiperspective
contribution in concrete experimental evidence.

The script:
  1. Discovers the top-N perspectives from the dataset (same filter as
     Section 6.1: cardinality [3,500], avg-DPT > 1.2, top-4 by cardinality).
  2. Always includes case_id as the fixed-partition baseline.
  3. Fetches observed (A, B) pairs per perspective and builds candidate
     2-activity patterns from the intersection (guaranteeing non-zero
     structural co-occurrence under every perspective).
  4. Runs each candidate against every perspective and scores by
     cross-perspective divergence = max(rate) − min(rate).
  5. Filters to patterns where every perspective returns ≥ 1 matching group,
     ranks by divergence, and prints a formatted table of the top-K results.
  6. Emits one JSONL record per (pattern, perspective) for plotting.

Output files
------------
  results/exp_multiperspective.jsonl   — machine-readable per-probe records
  results/multiperspective_summary.txt — human-readable ranked table

Usage
-----
  # Log already ingested:
  python -m tests.eval.exp_multiperspective \\
      --log-name bpic_2017 \\
      --dataset datasets/BPIC17.xes

  # Ingest first, then probe:
  python -m tests.eval.exp_multiperspective \\
      --log-name bpic_2017 \\
      --dataset datasets/BPIC17.xes \\
      --ingest \\
      --config config/adaptive_index.config.json

  # Override probe budget:
  python -m tests.eval.exp_multiperspective \\
      --log-name bpic_2017 \\
      --dataset datasets/BPIC17.xes \\
      --max-candidates 300 --top-k 8
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional

# Allow running directly from the repo root.
_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from tests.eval.eval_common import (
    API_BASE, QUERY_PREFIX, API_TIMEOUT_S,
    CONFIG_DIR, RESULTS_DIR,
    Recorder,
    health_check,
    ingest_adaptive,
    detect_adaptive,
    discover_schema,
    quote_label,
)
from tests.eval.workload import fetch_pair_coverage


# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------

MIN_PERSP_CARD   = 5      # skip perspectives with fewer total groups
MAX_PERSPECTIVES = 4      # top non-case perspectives to include
MAX_CANDIDATES   = 200    # cap on candidate patterns to probe per run
TOP_K            = 5      # how many patterns to surface in the final table
MIN_MATCH_GROUPS = 1      # a pattern is valid only if every perspective
                          # returns at least this many matching groups
LOOKBACK         = "3650d"
ADAPTIVE_CONFIG  = CONFIG_DIR / "adaptive_index.config.json"

# Retention overrides: promote pairs aggressively so the probe is not
# contaminated by first-query cold-scan latency.
RETENTION_OVERRIDES = {"min_query_count": 1, "half_life_seconds": 300}

RNG_SEED = 42


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class PerspResult:
    perspective:    str         # human-readable key, e.g. "org:resource"
    grouping_keys:  list[str]
    total_groups:   int
    matching_groups: int
    rate:           float       # matching / total  (in [0, 1])

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass
class PatternResult:
    pattern:    str
    results:    list[PerspResult]
    divergence: float           # max(rate) - min(rate)  (in [0, 1])

    def as_dict(self) -> dict:
        return {
            "pattern":    self.pattern,
            "divergence": self.divergence,
            "results":    [r.as_dict() for r in self.results],
        }


# ---------------------------------------------------------------------------
# Perspective discovery and group-count helpers
# ---------------------------------------------------------------------------

def _persp_label(keys: list[str]) -> str:
    return "+".join(keys)


def collect_perspectives(
    log_name: str,
    dataset_path: Path,
    *,
    max_perspectives: int = MAX_PERSPECTIVES,
    min_persp_card: int = MIN_PERSP_CARD,
) -> list[tuple[str, list[str], int]]:
    """
    Return a list of (label, grouping_keys, group_count) tuples.

    Always begins with the case_id baseline; appends up to
    `max_perspectives` event-level non-case perspectives discovered
    from the log schema, filtered by group count >= `min_persp_card`.
    """
    schema = discover_schema(dataset_path)
    print(f"  raw perspective candidates: {schema.perspective_keys}")

    perspectives_meta: list[tuple[str, list[str]]] = [
        ("case_id", ["case_id"])
    ]
    for k in schema.perspective_keys[:max_perspectives]:
        perspectives_meta.append((k, [k]))

    out: list[tuple[str, list[str], int]] = []
    for label, keys in perspectives_meta:
        try:
            cov = fetch_pair_coverage(log_name, keys)
            gc  = int(cov.get("group_count", 0))
        except Exception as exc:
            print(f"  [warn] group_count failed for {label}: {exc}")
            gc = 0

        if gc >= min_persp_card:
            out.append((label, keys, gc))
            print(f"  ✓  {label:<30}  {gc:>8,} groups")
        else:
            print(f"  ✗  {label:<30}  {gc:>8,} groups  (< {min_persp_card}, skipped)")

    return out


# ---------------------------------------------------------------------------
# Candidate pattern builder
# ---------------------------------------------------------------------------

def build_candidates(
    log_name: str,
    perspectives: list[tuple[str, list[str], int]],
    *,
    max_candidates: int = MAX_CANDIDATES,
    rng_seed: int = RNG_SEED,
) -> list[str]:
    """
    Build 2-activity candidate patterns from pairs observed in the log.

    Strategy:
      1. Fetch pair_coverage for each perspective to obtain the set of
         activity pairs (A, B) that actually co-occur within at least one
         group of that perspective.
      2. Take the intersection across all perspectives — pairs present in
         every perspective.  This maximises the chance of non-zero detection
         results under every grouping.
      3. If the intersection is too small (< 5), fall back to the union.
      4. Shuffle and cap at `max_candidates`.
    """
    rng = random.Random(rng_seed)
    per_persp: list[set[tuple[str, str]]] = []

    for label, keys, _ in perspectives:
        try:
            cov = fetch_pair_coverage(log_name, keys)
            pair_set = frozenset(
                (p["source"], p["target"]) for p in cov.get("pairs", [])
            )
            per_persp.append(set(pair_set))
            print(f"  [pairs] {label}: {len(pair_set)} observed pairs")
        except Exception as exc:
            print(f"  [warn] pair_coverage failed for {label}: {exc}")
            per_persp.append(set())

    if not per_persp:
        return []

    common: set[tuple[str, str]] = per_persp[0].copy()
    for s in per_persp[1:]:
        common &= s

    if len(common) < 5:
        print(f"  [candidates] intersection has only {len(common)} pairs; "
              f"expanding to union")
        common = set().union(*per_persp)
    else:
        print(f"  [candidates] intersection: {len(common)} pairs shared "
              f"across all {len(per_persp)} perspectives")

    candidates = [
        f"{quote_label(a)} {quote_label(b)}" for a, b in common
    ]
    rng.shuffle(candidates)
    return candidates[:max_candidates]


# ---------------------------------------------------------------------------
# Per-pattern probe
# ---------------------------------------------------------------------------

def probe_pattern(
    log_name: str,
    pattern:  str,
    perspectives: list[tuple[str, list[str], int]],
    *,
    min_match: int = MIN_MATCH_GROUPS,
) -> Optional[PatternResult]:
    """
    Run `pattern` against every perspective via detect_adaptive.

    Returns None if any perspective yields < `min_match` matching groups
    (pattern is not universally supported; not a useful demo candidate).
    """
    results: list[PerspResult] = []
    for label, keys, total_groups in perspectives:
        try:
            resp = detect_adaptive(
                log_name, pattern, keys, LOOKBACK,
                retention_overrides=RETENTION_OVERRIDES,
            )
            # The detection endpoint returns `total` = |R|, the count of
            # groups satisfying the query.
            matching = int(resp.get("total", 0))
        except Exception as exc:
            print(f"    [warn] {label} raised: {exc}")
            return None

        if matching < min_match:
            return None  # zero-result perspective: skip this pattern

        rate = matching / total_groups if total_groups > 0 else 0.0
        results.append(PerspResult(
            perspective=label,
            grouping_keys=keys,
            total_groups=total_groups,
            matching_groups=matching,
            rate=rate,
        ))

    if not results:
        return None

    rates = [r.rate for r in results]
    divergence = max(rates) - min(rates)
    return PatternResult(pattern=pattern, results=results, divergence=divergence)


# ---------------------------------------------------------------------------
# Table formatter
# ---------------------------------------------------------------------------

_COL_WIDTHS = (30, 10, 10, 8)  # perspective, groups, matching, rate


def _hline(char: str = "─") -> str:
    return char * (sum(_COL_WIDTHS) + 3 * 3 + 2)


def print_table(ranked: list[PatternResult], *, file=None) -> None:
    """Print a ranked ASCII table to `file` (defaults to stdout)."""
    out = file or sys.stdout

    def w(s: str, i: int) -> str:
        return s.ljust(_COL_WIDTHS[i])

    print(_hline("═"), file=out)
    print(f"  MULTIPERSPECTIVE DIVERGENCE RESULTS  (top {len(ranked)})", file=out)
    print(_hline("═"), file=out)

    for rank, pr in enumerate(ranked, 1):
        print(f"\n  Rank {rank}  │  {pr.pattern}", file=out)
        print(f"  Divergence (max − min satisfaction rate): "
              f"{pr.divergence * 100:.1f} pp", file=out)
        print(_hline(), file=out)
        hdr = (
            f"  {w('Perspective', 0)}   "
            f"{w('Groups', 1)}   "
            f"{w('Matching', 2)}   "
            f"{'Rate':>8}"
        )
        print(hdr, file=out)
        print(_hline(), file=out)
        for r in pr.results:
            rate_bar = "█" * int(r.rate * 20)   # visual 0–20-cell bar
            print(
                f"  {w(r.perspective, 0)}   "
                f"{r.total_groups:>{_COL_WIDTHS[1]},}   "
                f"{r.matching_groups:>{_COL_WIDTHS[2]},}   "
                f"{r.rate * 100:>6.1f}%  {rate_bar}",
                file=out,
            )
        print(_hline(), file=out)

    print(file=out)


# ---------------------------------------------------------------------------
# Main experiment entry point
# ---------------------------------------------------------------------------

def run(
    log_name:        str,
    dataset_path:    Path,
    *,
    config_path:     Optional[Path] = None,
    do_ingest:       bool           = False,
    top_k:           int            = TOP_K,
    max_candidates:  int            = MAX_CANDIDATES,
    max_perspectives: int           = MAX_PERSPECTIVES,
    min_persp_card:  int            = MIN_PERSP_CARD,
    rng_seed:        int            = RNG_SEED,
) -> list[PatternResult]:

    rec = Recorder("w3_multiperspective", "exp_multiperspective.jsonl")

    # ── Optional ingest ──────────────────────────────────────────────────────
    if do_ingest:
        cfg = config_path or ADAPTIVE_CONFIG
        print(f"\n[ingest] {dataset_path.name} → log_name={log_name}")
        t0 = time.perf_counter()
        ingest_adaptive(log_name, dataset_path, cfg, clear_existing=True)
        elapsed = time.perf_counter() - t0
        print(f"[ingest] done in {elapsed:.1f}s")
        rec.emit("ingest", log_name=log_name, dataset=str(dataset_path),
                 elapsed_s=elapsed)

    # ── Discover perspectives ────────────────────────────────────────────────
    print(f"\n[perspectives] discovering from {dataset_path.name} …")
    perspectives = collect_perspectives(
        log_name, dataset_path,
        max_perspectives=max_perspectives,
        min_persp_card=min_persp_card,
    )
    if len(perspectives) < 2:
        raise RuntimeError(
            "Fewer than 2 valid perspectives found — cannot demonstrate "
            "cross-perspective divergence.  Lower --min-persp-card or "
            "increase --max-perspectives."
        )

    rec.emit("perspectives", log_name=log_name, perspectives=[
        {"label": lbl, "keys": keys, "group_count": gc}
        for lbl, keys, gc in perspectives
    ])

    # ── Build candidate patterns ─────────────────────────────────────────────
    print(f"\n[candidates] fetching pair coverage per perspective …")
    candidates = build_candidates(
        log_name, perspectives,
        max_candidates=max_candidates,
        rng_seed=rng_seed,
    )
    print(f"  probing {len(candidates)} candidate patterns")

    # ── Probe ────────────────────────────────────────────────────────────────
    print(f"\n[probe] running detect_adaptive across all perspectives …")
    valid: list[PatternResult] = []
    zero_skip = 0

    for i, pat in enumerate(candidates, 1):
        print(f"  [{i:3d}/{len(candidates)}]  {pat:<50}", end="  ", flush=True)
        pr = probe_pattern(log_name, pat, perspectives)
        if pr is not None:
            print(f"div={pr.divergence * 100:5.1f}pp  ✓")
            valid.append(pr)
            rec.emit("pattern_probe",
                     log_name=log_name,
                     pattern=pat,
                     divergence=pr.divergence,
                     results=[r.as_dict() for r in pr.results])
        else:
            print("zero-result perspective  ✗")
            zero_skip += 1

        # Early exit once we have enough material to rank.
        if len(valid) >= top_k * 10:
            print(f"  (stopping early — {len(valid)} valid patterns collected)")
            break

    print(f"\n[summary] {len(valid)} valid / {zero_skip} skipped "
          f"(zero-result perspective) / {len(candidates)} probed")

    if not valid:
        print("[warn] no pattern produced non-zero results under every "
              "perspective.  Try --min-candidates or check that the log "
              "is ingested and perspectives are non-trivial.")
        return []

    # ── Rank and report ──────────────────────────────────────────────────────
    valid.sort(key=lambda x: x.divergence, reverse=True)
    top = valid[:top_k]

    print("\n" + "=" * 80)
    print("  TOP RESULTS")
    print("=" * 80)
    print_table(top)

    summary_path = RESULTS_DIR / "multiperspective_summary.txt"
    with summary_path.open("w") as f:
        print_table(top, file=f)
        print(f"Dataset : {dataset_path.name}", file=f)
        print(f"Log name: {log_name}", file=f)
        print(f"Perspectives probed: "
              f"{[lbl for lbl, _, _ in perspectives]}", file=f)
        print(f"Candidates probed: {len(candidates)}", file=f)
        print(f"Valid (non-zero everywhere): {len(valid)}", file=f)

    print(f"[done] summary → {summary_path}")
    rec.emit("experiment_complete",
             log_name=log_name,
             n_perspectives=len(perspectives),
             n_candidates=len(candidates),
             n_valid=len(valid),
             top_k=top_k,
             top_results=[p.as_dict() for p in top])
    return top


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description=("Multiperspective divergence experiment — "
                     "reviewer W3 response."),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("--log-name",  required=True,
                    help="Name under which the log is stored in SIESTA.")
    ap.add_argument("--dataset",   required=True, type=Path,
                    help="Path to the event log (XES or CSV).")
    ap.add_argument("--config",    type=Path, default=None,
                    help="Adaptive ingest config JSON (only needed with "
                         "--ingest; defaults to config/adaptive_index.config.json).")
    ap.add_argument("--ingest",    action="store_true",
                    help="Ingest the dataset before probing.  If the log "
                         "is already indexed, omit this flag.")
    ap.add_argument("--top-k",           type=int, default=TOP_K)
    ap.add_argument("--max-candidates",  type=int, default=MAX_CANDIDATES,
                    help="Maximum number of candidate patterns to probe.")
    ap.add_argument("--max-perspectives", type=int, default=MAX_PERSPECTIVES,
                    help="Maximum number of non-case perspectives to include.")
    ap.add_argument("--min-persp-card",  type=int, default=MIN_PERSP_CARD,
                    help="Minimum group count for a perspective to be included.")
    ap.add_argument("--seed",            type=int, default=RNG_SEED,
                    help="RNG seed for candidate shuffling.")
    return ap


if __name__ == "__main__":
    args = _build_parser().parse_args()
    health_check()
    run(
        log_name=args.log_name,
        dataset_path=args.dataset,
        config_path=args.config,
        do_ingest=args.ingest,
        top_k=args.top_k,
        max_candidates=args.max_candidates,
        max_perspectives=args.max_perspectives,
        min_persp_card=args.min_persp_card,
        rng_seed=args.seed,
    )