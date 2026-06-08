"""
tests/eval/exp_expressive.py

Experiment 6.4.2 — Expressive query showcase.

Measures query execution latency for query classes that ONLY the
adaptive system supports natively.  No competitor comparison is
performed — this experiment demonstrates what our framework uniquely
enables and reports absolute latencies.

Query categories
----------------
multiperspective_structural
    Structural patterns (no constraints) under non-case perspectives
    (resource, org:group, etc.), at lengths 2 and 3.
    Competitors require a full SequenceTable scan + on-the-fly
    regrouping — not natively supported.

multiperspective_attribute
    Attribute-constrained patterns under non-case perspectives.
    Combines dynamic grouping with in-index attribute evaluation.
    Not supported by any competitor.

complex_structural
    Patterns using advanced SeQL features under non-case perspectives,
    at lengths 3–6:
      k=3  Negation (A !B C), alternation (A (B||C) D trimmed to 3)
      k=4  Negation (A !B C D), alternation (A (B||C) D), Kleene+ (A+ B C D)
      k=5  Double negation, alternation in middle, Kleene+ with constraint
      k=6  Multi-negation, combined features (Kleene+ + negation + alternation)

combined_complex
    Complex patterns + attribute constraints + multiperspective grouping:
    negation + equality, variable binding + Kleene+, alternation + variable,
    and the fully combined k=5,6 variants.

Cold and warm passes
--------------------
Each category runs cold (fresh ingest, no index) then warm (after
promotion to PERSISTENT).  The delta shows the adaptive index payoff
even for the most complex queries.

Output
------
results/6_4_2_expressive.jsonl
"""

from __future__ import annotations

import argparse
import itertools
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tests.eval.eval_common import (
    CONFIG_DIR,
    Recorder, health_check,
    ingest_adaptive,
    timed_query,
    resolve_dataset,
    quote_label,
)
from tests.eval.workload import build_workloads, WorkloadContext

CONFIG = CONFIG_DIR / "adaptive_index.config.json"


# ---------------------------------------------------------------------------
# Workload builder
# ---------------------------------------------------------------------------

def build_expressive_workloads(ctx: WorkloadContext) -> dict[str, list[dict]]:
    """
    Build all four expressive categories.
    All queries use non-case perspectives.
    complex_structural and combined_complex include patterns at lengths 3–6.
    """
    acts        = ctx.activities
    perspectives = ctx.perspectives

    if not perspectives or not any(gk for gk in perspectives):
        raise ValueError(
            "No non-case perspectives available. "
            "The expressive experiment requires at least one event-level "
            "perspective attribute (e.g. org:resource)."
        )

    attr_candidates = ["org:resource", "resource", "role", "org:group",
                       "lifecycle:transition", "lifecycle"]
    available_attrs = [a for a in attr_candidates if ctx.attribute_values.get(a)]
    if not available_attrs:
        available_attrs = [
            k for k, vs in ctx.attribute_values.items()
            if vs and len(vs) >= 2
        ][:3]

    pairs   = [(a, b) for a, b in itertools.permutations(acts, 2)]
    triples = list(itertools.permutations(acts, 3))
    eff_max = min(6, len(acts))

    # Quoted activity labels for pattern construction
    qa = [quote_label(x) for x in acts[:eff_max]]

    def _q(qid: str, pat: str, gk: list[str], tags: list[str],
           length: int | None = None) -> dict:
        return {
            "id":             qid,
            "log_name":       ctx.log_name,
            "pattern":        pat,
            "grouping_keys":  list(gk),
            "pattern_length": length or _pat_length(pat),
            "category":       "expressive",
            "tags":           tags,
        }

    def _pat_length(pat: str) -> int:
        clean = re.sub(r"\[[^\]]*\]", "", pat)
        return len(re.findall(
            r'"(?:[^"\\]|\\.)*"|[A-Za-z_][A-Za-z0-9_:\\]*', clean
        ))

    # ─────────────────────────────────────────────────────────────────
    # 1. Multiperspective structural
    # ─────────────────────────────────────────────────────────────────
    mp_structural: list[dict] = []
    cnt = itertools.count(1)

    for gk in perspectives:
        if not gk:
            continue
        persp_tag = f"persp={'|'.join(gk)}"
        for a, b in pairs[:4]:
            mp_structural.append(_q(
                f"MS{next(cnt)}",
                f"{quote_label(a)} {quote_label(b)}",
                gk, ["pair", persp_tag], length=2,
            ))
        if triples:
            a, b, c = triples[0]
            mp_structural.append(_q(
                f"MS{next(cnt)}",
                f"{quote_label(a)} {quote_label(b)} {quote_label(c)}",
                gk, ["triple", persp_tag], length=3,
            ))

    # ─────────────────────────────────────────────────────────────────
    # 2. Multiperspective + attribute constraints
    # ─────────────────────────────────────────────────────────────────
    mp_attribute: list[dict] = []
    cnt_a = itertools.count(1)

    for gk in perspectives:
        if not gk:
            continue
        persp_tag = f"persp={'|'.join(gk)}"

        for attr in available_attrs[:2]:
            a, b = pairs[len(mp_attribute) % len(pairs)]
            val = ctx.attribute_values[attr][0].replace('"', '\\"')
            mp_attribute.append(_q(
                f"MA{next(cnt_a)}",
                f'{quote_label(a)}[{attr}="{val}"] {quote_label(b)}',
                gk, ["single_eq", persp_tag], length=2,
            ))

        for attr in available_attrs[:1]:
            a, b = pairs[(len(mp_attribute) + 1) % len(pairs)]
            mp_attribute.append(_q(
                f"MA{next(cnt_a)}",
                f"{quote_label(a)}[{attr}=$1] {quote_label(b)}[{attr}=$1]",
                gk, ["cross_eq", persp_tag], length=2,
            ))

    # ─────────────────────────────────────────────────────────────────
    # 3. Complex structural — lengths 3 → eff_max, all perspectives
    # ─────────────────────────────────────────────────────────────────
    complex_structural: list[dict] = []
    cnt_c = itertools.count(1)

    for gk in perspectives:
        if not gk:
            continue
        persp_tag = f"persp={'|'.join(gk)}"

        # k = 3 ──────────────────────────────────────────────────────
        if eff_max >= 3:
            a0, a1, a2 = qa[0], qa[1], qa[2]

            complex_structural.append(_q(
                f"CX{next(cnt_c)}",
                f"{a0} !{a1} {a2}",
                gk, ["negation", "len=3", persp_tag], length=3,
            ))
            complex_structural.append(_q(
                f"CX{next(cnt_c)}",
                f"{a0} ({a1}||{a2})",
                gk, ["alternation", "len=3", persp_tag], length=3,
            ))
            complex_structural.append(_q(
                f"CX{next(cnt_c)}",
                f"{a0}+ {a1} {a2}",
                gk, ["kleene_plus", "len=3", persp_tag], length=3,
            ))

        # k = 4 ──────────────────────────────────────────────────────
        if eff_max >= 4:
            a0, a1, a2, a3 = qa[0], qa[1], qa[2], qa[3]

            complex_structural.append(_q(
                f"CX{next(cnt_c)}",
                f"{a0} !{a1} {a2} {a3}",
                gk, ["negation", "len=4", persp_tag], length=4,
            ))
            complex_structural.append(_q(
                f"CX{next(cnt_c)}",
                f"{a0} ({a1}||{a2}) {a3}",
                gk, ["alternation", "len=4", persp_tag], length=4,
            ))
            complex_structural.append(_q(
                f"CX{next(cnt_c)}",
                f"{a0}+ {a1} {a2} {a3}",
                gk, ["kleene_plus", "len=4", persp_tag], length=4,
            ))
            complex_structural.append(_q(
                f"CX{next(cnt_c)}",
                f"{a0} !({a1}||{a2}) {a3}",
                gk, ["negation_alt", "len=4", persp_tag], length=4,
            ))

        # k = 5 ──────────────────────────────────────────────────────
        if eff_max >= 5:
            a0, a1, a2, a3, a4 = qa[0], qa[1], qa[2], qa[3], qa[4]

            complex_structural.append(_q(
                f"CX{next(cnt_c)}",
                f"{a0} !{a1} {a2} !{a3} {a4}",
                gk, ["double_negation", "len=5", persp_tag], length=5,
            ))
            complex_structural.append(_q(
                f"CX{next(cnt_c)}",
                f"{a0} {a1} ({a2}||{a3}) {a4}",
                gk, ["alternation", "len=5", persp_tag], length=5,
            ))
            complex_structural.append(_q(
                f"CX{next(cnt_c)}",
                f"{a0}+ {a1} {a2} {a3} {a4}",
                gk, ["kleene_plus", "len=5", persp_tag], length=5,
            ))

        # k = 6 ──────────────────────────────────────────────────────
        if eff_max >= 6:
            a0, a1, a2, a3, a4, a5 = qa[0], qa[1], qa[2], qa[3], qa[4], qa[5]

            complex_structural.append(_q(
                f"CX{next(cnt_c)}",
                f"{a0} !{a1} {a2} !{a3} {a4} {a5}",
                gk, ["double_negation", "len=6", persp_tag], length=6,
            ))
            # Kleene+ + negation + alternation in one pattern
            complex_structural.append(_q(
                f"CX{next(cnt_c)}",
                f"{a0}+ !{a1} ({a2}||{a3}) {a4} {a5}",
                gk, ["combined", "len=6", persp_tag], length=6,
            ))

    # ─────────────────────────────────────────────────────────────────
    # 4. Combined complex — all features, lengths 2 → eff_max
    # ─────────────────────────────────────────────────────────────────
    combined: list[dict] = []
    cnt_cb = itertools.count(1)

    for gk in perspectives:
        if not gk:
            continue
        persp_tag = f"persp={'|'.join(gk)}"

        if not (eff_max >= 3 and available_attrs):
            continue

        attr = available_attrs[0]
        val  = ctx.attribute_values[attr][0].replace('"', '\\"')
        a0, a1, a2 = qa[0], qa[1], qa[2]

        # k = 3: negation + attribute equality
        combined.append(_q(
            f"CB{next(cnt_cb)}",
            f'{a0}[{attr}="{val}"] !{a1} {a2}',
            gk, ["negation_attr", "len=3", persp_tag], length=3,
        ))

        # k = 3: variable binding + Kleene+
        combined.append(_q(
            f"CB{next(cnt_cb)}",
            f"{a0}[{attr}=$1]+ {a1}[{attr}=$1]",
            gk, ["kleene_var", "len=3", persp_tag], length=3,
        ))

        if eff_max >= 4:
            a3 = qa[3]

            # k = 4: alternation + cross-event variable
            combined.append(_q(
                f"CB{next(cnt_cb)}",
                f"{a0}[{attr}=$1] ({a1}||{a2}) {a3}[{attr}=$1]",
                gk, ["alt_var", "len=4", persp_tag], length=4,
            ))
            # k = 4: negation + multi-attr
            combined.append(_q(
                f"CB{next(cnt_cb)}",
                f'{a0}[{attr}="{val}"] !{a1} {a2} {a3}',
                gk, ["negation_attr", "len=4", persp_tag], length=4,
            ))

        if eff_max >= 5:
            a3, a4 = qa[3], qa[4]

            # k = 5: Kleene+ + negation + variable binding
            combined.append(_q(
                f"CB{next(cnt_cb)}",
                f"{a0}[{attr}=$1]+ !{a1} {a2} {a3} {a4}[{attr}=$1]",
                gk, ["kleene_negation_var", "len=5", persp_tag], length=5,
            ))
            # k = 5: alternation + attribute
            combined.append(_q(
                f"CB{next(cnt_cb)}",
                f'{a0}[{attr}="{val}"] {a1} ({a2}||{a3}) {a4}',
                gk, ["alt_attr", "len=5", persp_tag], length=5,
            ))

        if eff_max >= 6:
            a3, a4, a5 = qa[3], qa[4], qa[5]

            # k = 6: fully combined — constraint + negation + alternation + variable
            combined.append(_q(
                f"CB{next(cnt_cb)}",
                f'{a0}[{attr}="{val}"] !{a1} ({a2}||{a3}) {a4} {a5}[{attr}=$1]',
                gk, ["fully_combined", "len=6", persp_tag], length=6,
            ))

    return {
        "multiperspective_structural": mp_structural,
        "multiperspective_attribute":  mp_attribute,
        "complex_structural":          complex_structural,
        "combined_complex":            combined,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _log_size(path: Path) -> int:
    if path.suffix.lower() == ".csv":
        return sum(1 for _ in path.open()) - 1
    count = 0
    with path.open("rb") as f:
        for line in f:
            if b"<event" in line:
                count += 1
    return count


# ---------------------------------------------------------------------------
# Runner: cold then warm for one category
# ---------------------------------------------------------------------------

def run_category_cold_warm(
    rec: Recorder,
    category: str,
    workload: list[dict],
    log_name: str,
    dataset_path: Path,
    *,
    log_size: int = 0,
) -> None:
    """
    Cold pass: fresh ingest, no index, first-touch latency.
    Warm pass: after two silent passes (pair promotion), steady-state.
    """

    # ── Cold ──────────────────────────────────────────────────────────
    print(f"\n── Adaptive COLD — {category} ──")
    ingest_adaptive(log_name, dataset_path, CONFIG,
                    overrides={"overwrite_data": True})
    time.sleep(1)

    for q in workload:
        try:
            body, latency = timed_query(
                q["log_name"], q["pattern"], q["grouping_keys"],
            )
            rec.emit("query",
                     system="adaptive_cold", category=category,
                     qid=q["id"], pattern=q["pattern"],
                     grouping_keys=q["grouping_keys"],
                     pattern_length=q.get("pattern_length"),
                     has_constraints="[" in q["pattern"],
                     tags=q.get("tags", []),
                     latency_s=latency,
                     total=body.get("total", 0),
                     pair_status=body.get("pair_status", {}),
                     log_name=log_name, log_size=log_size)
            print(f"  COLD {q['id']:6s} k={q.get('pattern_length','?')}  "
                  f"{q['pattern'][:44]:44s}  -> {latency:.3f}s  "
                  f"(n={body.get('total','?')})")
        except Exception as exc:
            rec.emit("query_error", system="adaptive_cold", category=category,
                     qid=q["id"], log_name=log_name, error=str(exc))
            print(f"  COLD {q['id']:6s} ERROR: {exc}")

    # ── Warm-up (two silent passes to push past min_query_count) ──────
    print(f"\n  [warm-up: 2 silent passes to trigger promotion]")
    for _ in range(2):
        for q in workload:
            try:
                timed_query(q["log_name"], q["pattern"], q["grouping_keys"])
            except Exception:
                pass

    print("  [sleeping 120s for background pair materialisation]")
    time.sleep(120)

    # ── Warm ──────────────────────────────────────────────────────────
    print(f"\n── Adaptive WARM — {category} ──")
    for q in workload:
        try:
            body, latency = timed_query(
                q["log_name"], q["pattern"], q["grouping_keys"],
            )
            rec.emit("query",
                     system="adaptive_warm", category=category,
                     qid=q["id"], pattern=q["pattern"],
                     grouping_keys=q["grouping_keys"],
                     pattern_length=q.get("pattern_length"),
                     has_constraints="[" in q["pattern"],
                     tags=q.get("tags", []),
                     latency_s=latency,
                     total=body.get("total", 0),
                     pair_status=body.get("pair_status", {}),
                     log_name=log_name, log_size=log_size)
            print(f"  WARM {q['id']:6s} k={q.get('pattern_length','?')}  "
                  f"{q['pattern'][:44]:44s}  -> {latency:.3f}s  "
                  f"(n={body.get('total','?')})")
        except Exception as exc:
            rec.emit("query_error", system="adaptive_warm", category=category,
                     qid=q["id"], log_name=log_name, error=str(exc))
            print(f"  WARM {q['id']:6s} ERROR: {exc}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Experiment 6.4.2 — Expressive query showcase.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Dataset arguments (mutually exclusive styles):
  --datasets path1:name1 path2:name2 ...   run multiple datasets
  --dataset path --log-name name           single dataset (legacy)

Categories per dataset (built from that dataset's schema):
  multiperspective_structural   A B, A B C  — dynamic grouping
  multiperspective_attribute    A[a=$1] B[a=$1]  — grouping + attrs
  complex_structural            Negation/alternation/Kleene+ at k=3..6
  combined_complex              All features combined at k=3..6

Datasets without non-case perspectives are skipped with a warning.
Cold and warm passes per category quantify the adaptive index payoff.
""",
    )
    ds_group = ap.add_mutually_exclusive_group(required=True)
    ds_group.add_argument("--datasets", nargs="+", metavar="PATH:NAME")
    ds_group.add_argument("--dataset",  metavar="PATH", default=None)
    ap.add_argument("--log-name",      default=None)
    ap.add_argument("--skip-combined", action="store_true")
    args = ap.parse_args()

    # Normalise to list of (path, log_name)
    if args.datasets:
        specs = [(Path(e.rsplit(":", 1)[0]),
                  e.rsplit(":", 1)[1] if ":" in e else Path(e).stem)
                 for e in args.datasets]
    else:
        log_name = args.log_name or Path(args.dataset).stem
        specs = [(Path(args.dataset), log_name)]

    for path, _ in specs:
        if not path.exists():
            print(f"[ERROR] File not found: {path}")
            raise SystemExit(1)

    print(f"Experiment 6.4.2 — Expressive query showcase")
    print(f"  Datasets ({len(specs)}): "
          + ", ".join(n for _, n in specs))

    health_check()
    rec = Recorder("6.4.2", "6_4_2_expressive.jsonl")
    rec.emit("experiment_start",
             datasets=[{"path": str(p), "log_name": n} for p, n in specs])

    categories_to_run = [
        "multiperspective_structural",
        "multiperspective_attribute",
        "complex_structural",
    ]
    if not args.skip_combined:
        categories_to_run.append("combined_complex")

    for spec_path, log_name in specs:
        size   = _log_size(spec_path)
        schema = _discover_schema(spec_path)

        print(f"\n{'═'*64}")
        print(f"  Dataset:    {log_name}  ({spec_path.name})")
        print(f"  Events:     {size:,}")
        print(f"  Activities: {schema.activities[:8]}"
              + (" ..." if len(schema.activities) > 8 else ""))
        print(f"{'═'*64}")

        # Build a WorkloadContext from this dataset's schema
        ctx = _make_ctx(schema, log_name)

        rec.emit("dataset_start", log_name=log_name,
                 path=str(spec_path), log_size=size,
                 activities=schema.activities,
                 attribute_keys=list(schema.attribute_values))

        try:
            expr_workloads = build_expressive_workloads(ctx)
        except ValueError as exc:
            print(f"\n  [SKIP] {log_name}: {exc}")
            rec.emit("dataset_skip", log_name=log_name, reason=str(exc))
            continue

        total = sum(len(expr_workloads.get(c, [])) for c in categories_to_run)
        print(f"\n  Total queries: {total}  "
              f"(cold + warm = {total * 2} executions)")
        for cat in categories_to_run:
            wl = expr_workloads.get(cat, [])
            print(f"  {cat}: {len(wl)} queries")

        # Emit workload manifest
        for cat in categories_to_run:
            for q in expr_workloads.get(cat, []):
                rec.emit("workload_query", log_name=log_name,
                         category=cat, **{k: v for k, v in q.items()
                                          if k != "log_name"})

        # Ingest fresh for this dataset
        ingest_adaptive(log_name, spec_path, CONFIG,
                        overrides={"overwrite_data": True})

        # Run each category
        for category in categories_to_run:
            workload = expr_workloads.get(category, [])
            if not workload:
                print(f"\n  [SKIP] {category}: no queries generated.")
                rec.emit("skip", log_name=log_name,
                         category=category, reason="no queries")
                continue

            print(f"\n══ [{log_name}] {category} ({len(workload)} queries) ══")
            run_category_cold_warm(
                rec, category, workload, log_name, spec_path, log_size=size,
            )

        rec.emit("dataset_end", log_name=log_name)

    print(f"\nResults written to {rec.path}")
    print("\nFigures this data supports:")
    print("  - Grouped bar: cold vs warm per category × dataset")
    print("  - Line chart:  warm latency vs pattern_length within complex_structural")
    print("  - Cross-dataset: same complex queries, different logs")
    print("  - Paper table: unsupported-by-competitors absolute warm latencies")


def _discover_schema(path: Path):
    return discover_schema(path)


def _make_ctx(schema, log_name: str):
    """Wrap schema as a WorkloadContext."""
    try:
        from tests.eval.workload import WorkloadContext
        return WorkloadContext(
            log_name=log_name,
            activities=schema.activities,
            perspectives=getattr(schema, "perspective_keys", []),
            attribute_values=schema.attribute_values,
        )
    except Exception:
        schema.log_name = log_name
        return schema


if __name__ == "__main__":
    main()