"""
tests/eval/exp_expressiveness.py

Experiment 6.4.2 — Expressiveness beyond competitors.

Demonstrates query classes that ONLY the adaptive system supports.
No competitor comparison — this is a capabilities showcase with
absolute latency measurements.

The cold→warm delta IS the paper's story here: it quantifies the
adaptive index payoff for each uniquely-supported feature class.

Categories
----------
multiperspective
    The same real co-occurring pairs, but grouped by org:resource,
    lifecycle:transition, etc. instead of case_id.  ELK/MR cannot
    do this — their partitioning is fixed at index/table creation.

attribute_multiperspective
    Attribute-constrained patterns under non-case perspectives.
    Requires both dynamic grouping AND in-index attribute evaluation.

operators
    Negation (A !B C), alternation (A (B||C) D), Kleene+ (A+ B).
    Built on top of real pairs so the base patterns are guaranteed
    to have non-zero results.  Operators modify the structural
    skeleton; the CEP engine evaluates them.

combined
    All features at once: operators + attributes + non-case perspective.
    The hardest queries the system can handle.

For each category, we measure:
  - COLD latency  — fresh ingest, empty catalog, SequenceTable scan.
  - WARM latency  — after promotion to PERSISTENT, index read.
  - The ratio tells the paper's story: adaptive indexing pays off
    even for the most expressive queries.

Output
------
results/6_4_2_expressiveness_{log_name}.jsonl
"""

from __future__ import annotations

import argparse
import itertools
import os
import re
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tests.eval.eval_common import (
    CONFIG_DIR,
    Recorder, health_check,
    ingest_adaptive,
    timed_query,
    discover_schema, resolve_dataset,
    quote_label,
)
from tests.eval.workload import fetch_pair_coverage

CONFIG = CONFIG_DIR / "adaptive_index.config.json"

PROMOTION_SLEEP = int(os.environ.get("PROMOTION_SLEEP", "120"))
MIN_PERSP_CARD  = 5

RETENTION_OVERRIDES = {
    "min_query_count":   3,
    "half_life_seconds": 3600.0,
    "hysteresis":        0.0,
}

_LOG_EXTS = {".csv", ".xes"}


# ---------------------------------------------------------------------------
# Workload construction from pair_coverage per perspective
# ---------------------------------------------------------------------------

def _pat(*acts: str) -> str:
    return " ".join(quote_label(a) for a in acts)


def build_expressiveness_workloads(
    perspective_coverage: dict[tuple, dict],
    log_name: str,
    schema,
    *,
    max_length: int = 6,
) -> dict[str, list[dict]]:
    """
    Build all expressiveness categories from real pairs under non-case perspectives.

    `perspective_coverage` maps (gk_tuple) -> pair_coverage_response.
    Each response has {"group_count": N, "pairs": [{"source": A, "target": B, "groups": G}, ...]}.
    """
    cnt = itertools.count(1)

    attr_pref = ["org:resource", "lifecycle:transition", "org:group",
                 "org:role", "resource", "role", "Action"]
    available_attrs = [a for a in attr_pref if schema.attribute_values.get(a)]
    if not available_attrs:
        available_attrs = [k for k, vs in schema.attribute_values.items() if vs][:3]

    mp_structural:  list[dict] = []
    mp_attribute:   list[dict] = []
    operators:      list[dict] = []
    combined:       list[dict] = []

    for gk_tuple, cov in perspective_coverage.items():
        gk = list(gk_tuple)
        ptag = f"persp={'|'.join(gk)}"
        pairs = cov.get("pairs", [])
        gc = cov.get("group_count", 0)
        if not pairs or gc < MIN_PERSP_CARD:
            continue

        # Build adjacency for chaining
        adj: dict[str, list[dict]] = {}
        for p in pairs:
            adj.setdefault(p["source"], []).append(p)

        # ── Multiperspective structural ───────────────────────────────
        # Top pairs by coverage — these will have the most results
        for p in pairs[:6]:
            mp_structural.append({
                "id": f"MS{next(cnt)}", "log_name": log_name,
                "pattern": _pat(p["source"], p["target"]),
                "grouping_keys": gk, "pattern_length": 2,
                "category": "multiperspective",
                "tags": ["pair", ptag, f"groups={p['groups']}"],
            })

        # k=3 chains
        chains_found = 0
        for p in pairs[:15]:
            if chains_found >= 3:
                break
            for cont in adj.get(p["target"], []):
                if cont["target"] != p["source"]:
                    mp_structural.append({
                        "id": f"MS{next(cnt)}", "log_name": log_name,
                        "pattern": _pat(p["source"], p["target"], cont["target"]),
                        "grouping_keys": gk, "pattern_length": 3,
                        "category": "multiperspective",
                        "tags": ["triple", ptag],
                    })
                    chains_found += 1
                    break

        # ── Multiperspective + attribute ──────────────────────────────
        for attr in available_attrs[:2]:
            vals = schema.attribute_values[attr]
            for p in pairs[:5]:
                val = vals[0].replace('"', '\\"')
                mp_attribute.append({
                    "id": f"MA{next(cnt)}", "log_name": log_name,
                    "pattern": f'{quote_label(p["source"])}[{attr}="{val}"] '
                               f'{quote_label(p["target"])}',
                    "grouping_keys": gk, "pattern_length": 2,
                    "category": "attribute_multiperspective",
                    "tags": ["single_eq", ptag, f"attr={attr}"],
                })
                break

            # Cross-event binding
            for p in pairs[:5]:
                mp_attribute.append({
                    "id": f"MA{next(cnt)}", "log_name": log_name,
                    "pattern": f'{quote_label(p["source"])}[{attr}=$1] '
                               f'{quote_label(p["target"])}[{attr}=$1]',
                    "grouping_keys": gk, "pattern_length": 2,
                    "category": "attribute_multiperspective",
                    "tags": ["cross_eq", ptag, f"attr={attr}"],
                })
                break

        # ── Operators on real pairs ───────────────────────────────────
        # For each length k from 3 to max_length, find a real chain
        # and overlay operators onto it.  The base chain has non-zero
        # results; operators constrain further but the query is non-trivial.

        for k in range(3, min(max_length + 1, 7)):
            chain = _find_chain(adj, pairs, k)
            if not chain:
                continue

            # Pick a "forbidden" activity for negation — must be one
            # that actually appears in the log but isn't in the chain
            all_acts_in_log = set(schema.activities)
            chain_set = set(chain)
            forbidden_candidates = [a for a in schema.activities[:15]
                                    if a not in chain_set]

            if forbidden_candidates:
                forbidden = forbidden_candidates[0]
                operators.append({
                    "id": f"OP{next(cnt)}", "log_name": log_name,
                    "pattern": (
                        f"{quote_label(chain[0])} "
                        f"!{quote_label(forbidden)} "
                        + " ".join(quote_label(a) for a in chain[1:])
                    ),
                    "grouping_keys": gk,
                    "pattern_length": k + 1,
                    "category": "operators",
                    "tags": ["negation", f"len={k+1}", ptag],
                })

            # Alternation: first activity, then (second || alternative), then rest
            alt_candidates = [a for a in schema.activities[:10]
                              if a != chain[0] and a != chain[1]]
            if alt_candidates and k >= 3:
                alt = alt_candidates[0]
                operators.append({
                    "id": f"OP{next(cnt)}", "log_name": log_name,
                    "pattern": (
                        f"{quote_label(chain[0])} "
                        f"({quote_label(chain[1])}||{quote_label(alt)}) "
                        + " ".join(quote_label(a) for a in chain[2:])
                    ),
                    "grouping_keys": gk,
                    "pattern_length": k + 1,
                    "category": "operators",
                    "tags": ["alternation", f"len={k+1}", ptag],
                })

            # Kleene+
            operators.append({
                "id": f"OP{next(cnt)}", "log_name": log_name,
                "pattern": (
                    f"{quote_label(chain[0])}+ "
                    + " ".join(quote_label(a) for a in chain[1:])
                ),
                "grouping_keys": gk,
                "pattern_length": k,
                "category": "operators",
                "tags": ["kleene_plus", f"len={k}", ptag],
            })

        # ── Combined: operators + attributes + multiperspective ───────
        if available_attrs:
            attr = available_attrs[0]
            val = schema.attribute_values[attr][0].replace('"', '\\"')

            for k in range(3, min(max_length + 1, 7)):
                chain = _find_chain(adj, pairs, k)
                if not chain:
                    continue

                forbidden_candidates = [a for a in schema.activities[:15]
                                        if a not in set(chain)]
                if forbidden_candidates:
                    combined.append({
                        "id": f"CB{next(cnt)}", "log_name": log_name,
                        "pattern": (
                            f'{quote_label(chain[0])}[{attr}="{val}"] '
                            f'!{quote_label(forbidden_candidates[0])} '
                            + " ".join(quote_label(a) for a in chain[1:])
                        ),
                        "grouping_keys": gk,
                        "pattern_length": k + 1,
                        "category": "combined",
                        "tags": ["negation_attr", f"len={k+1}", ptag],
                    })

                # Variable binding + Kleene+
                combined.append({
                    "id": f"CB{next(cnt)}", "log_name": log_name,
                    "pattern": (
                        f"{quote_label(chain[0])}[{attr}=$1]+ "
                        + " ".join(quote_label(a) for a in chain[1:-1])
                        + f" {quote_label(chain[-1])}[{attr}=$1]"
                    ),
                    "grouping_keys": gk,
                    "pattern_length": k,
                    "category": "combined",
                    "tags": ["kleene_var", f"len={k}", ptag],
                })

    return {
        "multiperspective":             mp_structural,
        "attribute_multiperspective":   mp_attribute,
        "operators":                    operators,
        "combined":                     combined,
    }


def _find_chain(adj, pairs, length):
    for start in pairs[:20]:
        chain = [start["source"], start["target"]]
        while len(chain) < length:
            last = chain[-1]
            found = False
            for p in adj.get(last, []):
                if p["target"] not in chain:
                    chain.append(p["target"])
                    found = True
                    break
            if not found:
                break
        if len(chain) == length:
            return chain
    return None


# ---------------------------------------------------------------------------
# Cold + warm runner
# ---------------------------------------------------------------------------

def run_cold_warm(
    rec, category, workload, log_name, dataset_path, *, log_size=0,
    perspectives,
):
    """
    Cold pass (fresh ingest → SequenceTable scan) then warm pass
    (after promotion → PERSISTENT index read).  The delta is the
    paper's story for each category.
    """
    if not workload:
        print(f"\n  [SKIP] {category}: no queries.")
        rec.emit("skip", category=category, reason="no queries")
        return

    # ── Cold ──────────────────────────────────────────────────────────
    print(f"\n── COLD — {category} ({len(workload)} queries) ──")
    ingest_adaptive(log_name, dataset_path, CONFIG,
                    overrides={
                        "overwrite_data": True,
                        "perspectives": [{"grouping_keys": gk}
                                         for gk in perspectives],
                    },
                    clear_existing=True)
    time.sleep(2)

    cold_latencies = []
    for q in workload:
        try:
            body, latency = timed_query(
                q["log_name"], q["pattern"], q["grouping_keys"],
                retention_overrides=RETENTION_OVERRIDES,
            )
            cold_latencies.append(latency)
            rec.emit("query", system="adaptive_cold", category=category,
                     qid=q["id"], pattern=q["pattern"],
                     grouping_keys=q["grouping_keys"],
                     pattern_length=q.get("pattern_length"),
                     tags=q.get("tags", []),
                     latency_s=latency, total=body.get("total", 0),
                     tier=body.get("pair_status_after", {}),
                     log_name=log_name, log_size=log_size)
            print(f"  COLD {q['id']:5s} k={q.get('pattern_length','?')} "
                  f"{q['pattern'][:45]:45s} -> {latency:.3f}s "
                  f"(n={body.get('total','?')})")
        except Exception as exc:
            rec.emit("query_error", system="adaptive_cold", category=category,
                     qid=q["id"], log_name=log_name, error=str(exc))
            print(f"  COLD {q['id']:5s} ERROR: {exc}")

    # ── Warm-up: run queries 3 more times to cross min_query_count ────
    print(f"\n  Warm-up: 3 more passes ...")
    for _ in range(3):
        for q in workload:
            try:
                timed_query(q["log_name"], q["pattern"],
                            q["grouping_keys"],
                            retention_overrides=RETENTION_OVERRIDES)
            except Exception:
                pass

    print(f"  Sleeping {PROMOTION_SLEEP}s for materialisation ...")
    time.sleep(PROMOTION_SLEEP)

    # ── Warm ──────────────────────────────────────────────────────────
    print(f"\n── WARM — {category} ({len(workload)} queries) ──")
    warm_latencies = []
    for q in workload:
        try:
            body, latency = timed_query(
                q["log_name"], q["pattern"], q["grouping_keys"],
                retention_overrides=RETENTION_OVERRIDES,
            )
            warm_latencies.append(latency)
            rec.emit("query", system="adaptive_warm", category=category,
                     qid=q["id"], pattern=q["pattern"],
                     grouping_keys=q["grouping_keys"],
                     pattern_length=q.get("pattern_length"),
                     tags=q.get("tags", []),
                     latency_s=latency, total=body.get("total", 0),
                     tier=body.get("pair_status_after", {}),
                     log_name=log_name, log_size=log_size)
            print(f"  WARM {q['id']:5s} k={q.get('pattern_length','?')} "
                  f"{q['pattern'][:45]:45s} -> {latency:.3f}s "
                  f"(n={body.get('total','?')})")
        except Exception as exc:
            rec.emit("query_error", system="adaptive_warm", category=category,
                     qid=q["id"], log_name=log_name, error=str(exc))
            print(f"  WARM {q['id']:5s} ERROR: {exc}")

    # Summary
    if cold_latencies and warm_latencies:
        c_med = statistics.median(cold_latencies)
        w_med = statistics.median(warm_latencies)
        speedup = c_med / w_med if w_med > 0 else float("inf")
        print(f"\n  {category} summary: "
              f"cold_median={c_med:.2f}s  warm_median={w_med:.2f}s  "
              f"speedup={speedup:.1f}×")
        rec.emit("category_summary", category=category,
                 cold_median=c_med, warm_median=w_med, speedup=speedup,
                 n_queries=len(workload))


# ---------------------------------------------------------------------------
# Per-dataset execution
# ---------------------------------------------------------------------------

def run_dataset(dataset_path, log_name, *, max_length, max_perspectives):
    schema = discover_schema(dataset_path)
    perspectives = [[k] for k in schema.perspective_keys[:max_perspectives]]
    if not perspectives or not any(gk for gk in perspectives):
        print(f"  SKIP {log_name}: no non-case perspectives.")
        return

    print(f"\n{'═'*64}")
    print(f"  Dataset:      {log_name}  ({dataset_path.name})")
    print(f"  Perspectives: {perspectives}")
    print(f"{'═'*64}")

    rec = Recorder("6.4.2", f"6_4_2_expressiveness_{log_name}.jsonl")

    # Bootstrap: ingest with perspectives for pair_coverage discovery
    print("\n  Bootstrap ingest with perspectives ...")
    ingest_adaptive(log_name, dataset_path, CONFIG,
                    overrides={
                        "perspectives": [{"grouping_keys": gk}
                                         for gk in perspectives],
                    },
                    clear_existing=True)
    time.sleep(2)

    # Discover real pairs per perspective
    persp_coverage: dict[tuple, dict] = {}
    for gk in perspectives:
        try:
            cov = fetch_pair_coverage(log_name, gk, activities=schema.activities)
            gc = cov.get("group_count", 0)
            n_pairs = len(cov.get("pairs", []))
            if gc >= MIN_PERSP_CARD and n_pairs >= 2:
                persp_coverage[tuple(gk)] = cov
                print(f"  {gk}: {gc} groups, {n_pairs} pairs")
            else:
                print(f"  {gk}: skipped (groups={gc}, pairs={n_pairs})")
        except Exception as exc:
            print(f"  {gk}: ERROR — {exc}")

    if not persp_coverage:
        print("  ABORT: no usable perspectives.")
        rec.emit("abort", reason="no usable perspectives")
        return

    rec.emit("dataset", log_name=log_name, path=str(dataset_path),
             activities=schema.activities,
             perspectives=perspectives,
             perspective_coverage={
                 "|".join(k): {"group_count": v["group_count"],
                               "n_pairs": len(v["pairs"])}
                 for k, v in persp_coverage.items()
             })

    # Build workloads
    workloads = build_expressiveness_workloads(
        persp_coverage, log_name, schema, max_length=max_length,
    )

    total = sum(len(wl) for wl in workloads.values())
    print(f"\n  Total: {total} queries (cold+warm = {total*2} executions)")
    for cat, wl in workloads.items():
        print(f"  {cat}: {len(wl)} queries")
        for q in wl[:3]:
            print(f"    {q['id']} k={q.get('pattern_length','?')}  "
                  f"{q['pattern'][:55]}  gk={q['grouping_keys']}")
        if len(wl) > 3:
            print(f"    ... and {len(wl)-3} more")
        rec.emit("workload_summary", category=cat, n_queries=len(wl))
        for q in wl:
            rec.emit("workload_query", log_name=log_name, category=cat,
                     **{k: v for k, v in q.items() if k != "log_name"})

    # Run cold→warm for each category
    for category, workload in workloads.items():
        run_cold_warm(rec, category, workload, log_name, dataset_path,
                      perspectives=perspectives)

    print(f"\n  Results: {rec.path}")
    print("\n  Figures this data supports:")
    print("    - Grouped bar: cold vs warm per category → adaptive payoff")
    print("    - Per-operator bars: negation / alternation / kleene+ speedup")
    print("    - Cross-perspective: same queries under different groupings")
    print("    - Paper table: 'not supported by competitors' with warm latencies")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Experiment 6.4.2 — Expressiveness beyond competitors.",
    )
    ds = ap.add_mutually_exclusive_group()
    ds.add_argument("--dataset")
    ds.add_argument("--datasets-dir", type=Path)
    ap.add_argument("--log-name", default=None)
    ap.add_argument("--max-length", type=int, default=6)
    ap.add_argument("--max-perspectives", type=int, default=4)
    args = ap.parse_args()

    health_check()

    if args.datasets_dir:
        specs = [(p, p.stem) for p in sorted(args.datasets_dir.iterdir())
                 if p.suffix.lower() in _LOG_EXTS]
    else:
        spec = resolve_dataset(args.dataset, args.log_name)
        specs = [(spec.path, spec.log_name)]

    for path, name in specs:
        try:
            run_dataset(path, name,
                        max_length=args.max_length,
                        max_perspectives=args.max_perspectives)
        except Exception as exc:
            import traceback
            print(f"\n[ERROR] {name}: {exc}")
            traceback.print_exc()


if __name__ == "__main__":
    main()
