"""
tests/eval/exp_expressiveness.py

Experiment 6.4.2 — Expressiveness demonstration.

Shows that adaptive SIESTA handles advanced pattern operators that ELK
and MATCH_RECOGNIZE cannot natively express, and measures warm-state
latency on these queries.

Design
------
Systems:
  siesta (adaptive, warm)  — full measurement
  elk                      — marked N/A (cannot express these queries)
  match_recognize          — marked N/A (no native support for most)

Dataset:
  Synthetic (eval_synthetic.csv) with controlled activity set and
  balanced attribute distributions.  Falls back to any provided dataset.

Query categories:
  kleene         — A B+ C           (one-or-more repetition)
  negation       — A ~B C           (absence between matches)
  disjunction    — (A|B) C D        (branching patterns)
  cross_attr     — A[$1] B[$1]      (cross-activity variable binding)
  combined       — A+ ~B C[$1] D[$1] (multiple operators)

All queries have length >= 8 activities in the expanded form, built
from real co-occurring activity chains to guarantee non-zero results.

Approach:
  1. Ingest dataset (adaptive only).
  2. Build a pool of candidate patterns per category from real activity
     chains discovered via pair_coverage.
  3. Preflight each candidate: query with a short timeout and keep only
     those with total > 0.
  4. Warm up the surviving patterns (4 rounds of 2-activity sub-pairs
     + sleep for promotions).
  5. Measure warm-state latency (median of 3 reps).

No cold pass is needed — the paper narrative for this experiment is
"what SIESTA can do that others cannot" plus steady-state latency.

Output
------
results/expressiveness_<log_name>.jsonl

Records:
  dataset        path, log_name, activities, n_activities
  candidate_pool category, n_candidates, n_surviving
  warmup_done    n_pairs_warmed, wall_s
  query          system, category, qid, pattern, latency_s, total
  capability     system, category, supported (bool), reason

Running
-------
    python -m tests.eval.exp_expressiveness \\
        --dataset /mnt/datasets/eval_synthetic.csv --log-name eval_synthetic

    python -m tests.eval.exp_expressiveness \\
        --dataset /mnt/datasets/bpic_2017.xes --log-name bpic_2017
"""

from __future__ import annotations

import argparse
import itertools
import os
import sys
import time
import statistics
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
    quote_label,
)
from tests.eval.workload import fetch_pair_coverage

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ADAPTIVE_CONFIG   = CONFIG_DIR / "adaptive_index.config.json"
RETENTION_FAST    = {"min_query_count": 1, "half_life_seconds": 300}

WARMUP_REPS       = 4
PROMOTION_SLEEP_S = int(os.environ.get("PROMOTION_SLEEP_S", "90"))
MEASURE_REPS      = 3
PREFLIGHT_TIMEOUT  = int(os.environ.get("PREFLIGHT_TIMEOUT_S", "120"))

# How many candidate patterns to generate per category before preflight.
CANDIDATES_PER_CAT = 10
# How many surviving patterns to actually measure per category.
MEASURE_PER_CAT    = 3

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
# Pattern templates
# ---------------------------------------------------------------------------

def _ql(act: str) -> str:
    """Quote-label shorthand."""
    return quote_label(act)


def build_kleene_patterns(chains: list[list[str]]) -> list[dict]:
    """
    Build Kleene patterns of the form: A B+ C D E F G H
    where B+ covers one activity with one-or-more repetition.
    Chain length ≥ 8 guaranteed by chain input.
    """
    patterns = []
    for chain in chains:
        if len(chain) < 8:
            continue
        # Place Kleene on the second activity.
        parts = [_ql(chain[0]), f"{_ql(chain[1])}+"]
        parts.extend(_ql(a) for a in chain[2:])
        patterns.append({
            "category": "kleene",
            "pattern":  " ".join(parts),
            "chain":    chain,
            "desc":     f"{chain[0]} {chain[1]}+ {' '.join(chain[2:])}",
        })
    return patterns


def build_negation_patterns(chains: list[list[str]], all_activities: list[str]) -> list[dict]:
    """
    Build negation patterns: A ~X B C D E F G H
    where X is an activity NOT in the chain (guaranteed absent between A and B).
    Chain length ≥ 8.
    """
    patterns = []
    chain_set_cache: dict[int, set[str]] = {}
    for ci, chain in enumerate(chains):
        if len(chain) < 8:
            continue
        chain_set = set(chain)
        # Find an activity not in the chain for the negation.
        neg_act = None
        for act in all_activities:
            if act not in chain_set:
                neg_act = act
                break
        if not neg_act:
            continue

        # Pattern: first_act ~neg_act second_act rest...
        parts = [_ql(chain[0]), f"~{_ql(neg_act)}", _ql(chain[1])]
        parts.extend(_ql(a) for a in chain[2:])
        patterns.append({
            "category": "negation",
            "pattern":  " ".join(parts),
            "chain":    chain,
            "neg_act":  neg_act,
            "desc":     f"{chain[0]} ~{neg_act} {' '.join(chain[1:])}",
        })
    return patterns


def build_disjunction_patterns(chains: list[list[str]]) -> list[dict]:
    """
    Build disjunction patterns: (A|B) C D E F G H I
    where the first position accepts either of two activities.
    Need ≥ 2 chains sharing a suffix to construct meaningful disjunctions.
    """
    patterns = []
    for i, chain in enumerate(chains):
        if len(chain) < 8:
            continue
        # Use the first two activities as disjunctive alternatives
        # for the head, then continue with the rest.
        # Pattern: (chain[0]|chain[1]) chain[2] chain[3] ... chain[N]
        # This is semantically: either chain[0] or chain[1] is followed by chain[2]...
        if len(chain) < 9:
            continue  # Need 9 to get 8 after collapsing first two into disjunction.
        head = f"({_ql(chain[0])}|{_ql(chain[1])})"
        tail = [_ql(a) for a in chain[2:]]
        parts = [head] + tail
        patterns.append({
            "category":   "disjunction",
            "pattern":    " ".join(parts),
            "chain":      chain,
            "desc":       f"({chain[0]}|{chain[1]}) {' '.join(chain[2:])}",
        })
    return patterns


def build_cross_attr_patterns(
    chains: list[list[str]],
    schema,
) -> list[dict]:
    """
    Build cross-activity attribute constraint patterns:
    A[attr=$1] B C D E F G H[attr=$1]
    where A and H must share the same value for `attr`.
    """
    # Find a non-numeric attribute.
    str_attr = None
    for k, vs in schema.attribute_values.items():
        if not vs:
            continue
        try:
            [float(v) for v in vs]
            continue  # numeric
        except (ValueError, TypeError):
            pass
        str_attr = k
        break

    if not str_attr:
        return []

    patterns = []
    for chain in chains:
        if len(chain) < 8:
            continue
        parts = [f"{_ql(chain[0])}[{str_attr}=$1]"]
        parts.extend(_ql(a) for a in chain[1:-1])
        parts.append(f"{_ql(chain[-1])}[{str_attr}=$1]")
        patterns.append({
            "category":  "cross_attr",
            "pattern":   " ".join(parts),
            "chain":     chain,
            "attr":      str_attr,
            "desc":      f"{chain[0]}[{str_attr}=$1] ... {chain[-1]}[{str_attr}=$1]",
        })
    return patterns


def build_combined_patterns(
    chains: list[list[str]],
    all_activities: list[str],
    schema,
) -> list[dict]:
    """
    Combined patterns exercising multiple operators:
    A+ ~X B C[attr=$1] D E F G H[attr=$1]
    """
    str_attr = None
    for k, vs in schema.attribute_values.items():
        if not vs:
            continue
        try:
            [float(v) for v in vs]
            continue
        except (ValueError, TypeError):
            pass
        str_attr = k
        break

    patterns = []
    for chain in chains:
        if len(chain) < 9:
            continue

        chain_set = set(chain)
        neg_act = None
        for act in all_activities:
            if act not in chain_set:
                neg_act = act
                break
        if not neg_act:
            continue

        # A+ ~X B C[attr=$1] D ... H[attr=$1]
        parts = [f"{_ql(chain[0])}+", f"~{_ql(neg_act)}", _ql(chain[1])]
        if str_attr:
            parts.append(f"{_ql(chain[2])}[{str_attr}=$1]")
            start_rest = 3
        else:
            parts.append(_ql(chain[2]))
            start_rest = 3

        for a in chain[start_rest:-1]:
            parts.append(_ql(a))

        if str_attr:
            parts.append(f"{_ql(chain[-1])}[{str_attr}=$1]")
        else:
            parts.append(_ql(chain[-1]))

        patterns.append({
            "category":  "combined",
            "pattern":   " ".join(parts),
            "chain":     chain,
            "desc":      f"{chain[0]}+ ~{neg_act} ... [{str_attr}=$1] ...",
        })
    return patterns


# ---------------------------------------------------------------------------
# Chain builder (reused from exp_competitive logic)
# ---------------------------------------------------------------------------

def _build_chains(
    log_name: str,
    grouping_keys: list[str],
    target_len: int = 10,
    min_len: int = 8,
    n_chains: int = CANDIDATES_PER_CAT,
) -> list[list[str]]:
    """Build activity chains from pair coverage."""
    from collections import defaultdict

    cov = fetch_pair_coverage(log_name, grouping_keys)
    pairs = cov.get("pairs", [])
    if not pairs:
        return []

    adj: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for p in pairs:
        adj[p["source"]].append((p["target"], p["groups"]))

    for src in adj:
        adj[src].sort(key=lambda x: -x[1])

    chains: list[list[str]] = []
    used_starters: set[str] = set()
    starters = sorted(adj.keys(), key=lambda s: sum(g for _, g in adj[s]), reverse=True)

    for starter in starters:
        if len(chains) >= n_chains:
            break
        if starter in used_starters:
            continue

        chain = [starter]
        used_in = {starter}
        for _ in range(target_len - 1):
            last = chain[-1]
            extended = False
            for nxt, _g in adj.get(last, []):
                if nxt not in used_in:
                    chain.append(nxt)
                    used_in.add(nxt)
                    extended = True
                    break
            if not extended:
                for nxt, _g in adj.get(last, []):
                    if nxt != last:
                        chain.append(nxt)
                        extended = True
                        break
            if not extended:
                break

        if len(chain) >= min_len:
            chains.append(chain)
            used_starters.add(starter)

    return chains


# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------

def _preflight(
    log_name: str,
    pattern: str,
    grouping_keys: list[str],
) -> int | None:
    """Quick check: returns total count or None on error."""
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
            timeout=PREFLIGHT_TIMEOUT,
        )
        r.raise_for_status()
        return r.json().get("total", 0)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Warm-up
# ---------------------------------------------------------------------------

def _warmup_chain(
    log_name: str,
    chain: list[str],
    grouping_keys: list[str],
    reps: int = WARMUP_REPS,
) -> int:
    """Warm up all consecutive sub-pairs of a chain."""
    n = 0
    for i in range(len(chain) - 1):
        pat = f"{_ql(chain[i])} {_ql(chain[i+1])}"
        for _ in range(reps):
            try:
                detect_adaptive(log_name, pat, grouping_keys,
                                retention_overrides=RETENTION_FAST)
                n += 1
            except Exception:
                pass
    return n


# ---------------------------------------------------------------------------
# Measurement
# ---------------------------------------------------------------------------

def _measure(
    log_name: str,
    pattern: str,
    grouping_keys: list[str],
    reps: int = MEASURE_REPS,
) -> tuple[float, int]:
    """Median latency over reps."""
    lats = []
    total = 0
    for _ in range(reps):
        body, lat = timed_query(
            log_name, pattern, grouping_keys,
            retention_overrides=RETENTION_FAST,
        )
        lats.append(lat)
        total = body.get("total", 0)
    return statistics.median(lats), total


# ---------------------------------------------------------------------------
# Capability table
# ---------------------------------------------------------------------------

CAPABILITY = {
    "kleene":     {"elk": False, "match_recognize": False,
                   "elk_reason": "No sequence-aware Kleene evaluation",
                   "mr_reason":  "No native Kleene over event sequences"},
    "negation":   {"elk": False, "match_recognize": False,
                   "elk_reason": "No absence-between-positions semantics",
                   "mr_reason":  "No native negation between pattern elements"},
    "disjunction": {"elk": False, "match_recognize": True,
                    "elk_reason": "No sequential disjunction",
                    "mr_reason":  "Supported via DEFINE alternatives"},
    "cross_attr": {"elk": False, "match_recognize": True,
                   "elk_reason": "No cross-document attribute binding",
                   "mr_reason":  "Supported via DEFINE predicates"},
    "combined":   {"elk": False, "match_recognize": False,
                   "elk_reason": "Cannot combine sequence operators",
                   "mr_reason":  "No Kleene + negation combination"},
}


# ---------------------------------------------------------------------------
# Per-dataset runner
# ---------------------------------------------------------------------------

def run_dataset(
    dataset_path: Path,
    log_name: str,
) -> None:
    fmt = dataset_path.suffix.lower().lstrip(".")
    print(f"\n{'='*64}")
    print(f"  Dataset: {dataset_path}  log_name={log_name}")
    print(f"{'='*64}")

    schema = discover_schema(dataset_path)
    activities = schema.activities

    print(f"  Activities ({len(activities)}): {activities[:12]}{'...' if len(activities)>12 else ''}")

    if len(activities) < 3:
        print(f"  SKIP: need >= 3 activities, got {len(activities)}")
        return

    rec = Recorder("6.4.2", f"expressiveness_{log_name}.jsonl")
    rec.emit("dataset", path=str(dataset_path), log_name=log_name,
             activities=activities, n_activities=len(activities))

    # ── Ingest ─────────────────────────────────────────────────────────
    print("\n  Ingesting (adaptive) ...")
    ingest_adaptive(log_name, dataset_path, ADAPTIVE_CONFIG,
                    overrides={"overwrite_data": True})
    time.sleep(2)

    # Use trace_id perspective for expressiveness queries.
    gk = ["trace_id"]

    # ── Build chains ──────────────────────────────────────────────────
    chains = _build_chains(log_name, gk, target_len=10, min_len=8,
                           n_chains=CANDIDATES_PER_CAT * 2)
    if not chains:
        print("  No chains found — aborting.")
        return
    print(f"  Built {len(chains)} candidate chains (lengths: {[len(c) for c in chains]})")

    # ── Generate candidate patterns per category ──────────────────────
    all_candidates: dict[str, list[dict]] = {
        "kleene":      build_kleene_patterns(chains),
        "negation":    build_negation_patterns(chains, activities),
        "disjunction": build_disjunction_patterns(chains),
        "cross_attr":  build_cross_attr_patterns(chains, schema),
        "combined":    build_combined_patterns(chains, activities, schema),
    }

    # ── Preflight: keep only patterns with non-zero results ───────────
    surviving: dict[str, list[dict]] = {}
    for cat, candidates in all_candidates.items():
        ok = []
        for cand in candidates:
            if len(ok) >= MEASURE_PER_CAT:
                break
            total = _preflight(log_name, cand["pattern"], gk)
            if total is not None and total > 0:
                cand["preflight_total"] = total
                ok.append(cand)
            else:
                pass  # silently skip zero-result patterns

        surviving[cat] = ok
        rec.emit("candidate_pool", category=cat,
                 n_candidates=len(candidates),
                 n_surviving=len(ok))
        print(f"  {cat:14s}: {len(candidates):3d} candidates → {len(ok)} surviving")

    total_queries = sum(len(v) for v in surviving.values())
    if total_queries == 0:
        print("  No surviving patterns — aborting.")
        return

    # ── Warm-up all chains used by surviving patterns ─────────────────
    print(f"\n  Warming up sub-pairs ...")
    t0 = time.perf_counter()
    warmed_chains: set[int] = set()
    total_warmup_qs = 0
    for cat, pats in surviving.items():
        for p in pats:
            chain_id = id(p["chain"])  # identity, not content
            # Warm up based on chain content to avoid duplicates.
            chain_key = tuple(p["chain"])
            if chain_key not in warmed_chains:
                warmed_chains.add(chain_key)
                total_warmup_qs += _warmup_chain(log_name, p["chain"], gk)

    print(f"  Waiting {PROMOTION_SLEEP_S}s for async promotions ...")
    time.sleep(PROMOTION_SLEEP_S)
    warmup_wall = time.perf_counter() - t0
    print(f"  Warm-up: {total_warmup_qs} queries, {len(warmed_chains)} chains, "
          f"{warmup_wall:.0f}s")
    rec.emit("warmup_done", n_chains=len(warmed_chains),
             n_queries=total_warmup_qs, wall_s=warmup_wall)

    # ── Measure ───────────────────────────────────────────────────────
    qid_counter = itertools.count(1)
    print(f"\n  ── Measurements ──")

    for cat in ["kleene", "negation", "disjunction", "cross_attr", "combined"]:
        pats = surviving.get(cat, [])
        if not pats:
            print(f"    {cat:14s}: no surviving patterns")
            continue

        print(f"\n    ── {cat} ──")
        for p in pats:
            qid = f"E{next(qid_counter)}"
            try:
                lat, total = _measure(log_name, p["pattern"], gk)
                rec.emit("query", system="siesta",
                         category=cat, qid=qid,
                         pattern=p["pattern"],
                         latency_s=lat, total=total,
                         log_name=log_name)
                print(f"      {qid:6s}  {lat:7.3f}s  total={total:5d}  "
                      f"{p['pattern'][:70]}")
            except Exception as exc:
                rec.emit("query_error", system="siesta",
                         category=cat, qid=qid,
                         pattern=p["pattern"],
                         error=str(exc), log_name=log_name)
                print(f"      {qid:6s}  ERROR: {exc}")

    # ── Capability table ──────────────────────────────────────────────
    print(f"\n  ── Capability summary ──")
    for cat, cap in CAPABILITY.items():
        for sys_name in ["elk", "match_recognize"]:
            supported = cap[sys_name]
            reason = cap.get(f"{sys_name}_reason", "")
            rec.emit("capability", system=sys_name,
                     category=cat, supported=supported, reason=reason)
        status_elk = "✓" if cap["elk"] else "✗"
        status_mr  = "✓" if cap["match_recognize"] else "✗"
        print(f"    {cat:14s}  ELK={status_elk}  MR={status_mr}")

    print(f"\n  Results → {rec.path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    global PROMOTION_SLEEP_S

    ap = argparse.ArgumentParser(
        description="Experiment 6.4.2 — Expressiveness demonstration.",
    )
    ap.add_argument("--dataset", default=None,
                    help="Path to dataset (CSV or XES).")
    ap.add_argument("--log-name", default=None,
                    help="Log name for API calls.")
    ap.add_argument("--datasets-dir", default=None,
                    help="Run on all CSV/XES files in this directory.")
    ap.add_argument("--promotion-sleep", type=int, default=PROMOTION_SLEEP_S,
                    help=f"Seconds to wait after warm-up (default {PROMOTION_SLEEP_S}).")
    args = ap.parse_args()

    log_tag = f"expressiveness_{args.log_name or 'multi'}"
    log_path = _setup_tee(log_tag)
    print(f"Output log: {log_path}", flush=True)

    PROMOTION_SLEEP_S = args.promotion_sleep

    health_check()

    if args.datasets_dir:
        ds_dir = Path(args.datasets_dir)
        for p in sorted(ds_dir.iterdir()):
            if p.suffix.lower() in {".csv", ".xes"} and p.is_file():
                ln = p.stem.replace(" ", "_").lower()
                try:
                    run_dataset(p, ln)
                except Exception as exc:
                    print(f"  FAILED: {exc}")
    else:
        spec = resolve_dataset(args.dataset, args.log_name)
        run_dataset(spec.path, spec.log_name)


if __name__ == "__main__":
    main()