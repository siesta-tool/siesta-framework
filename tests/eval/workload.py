"""
Shared workload definitions for the evaluation harness.

Every experiment imports from this module so that results are
comparable across experiments and reruns.  Workloads are defined as
lists of dicts with the schema:

    {
        "id":             "<unique label for this query>",
        "log_name":       "<dataset>",
        "pattern":        "<SeQL pattern, with any constraints embedded>",
        "grouping_keys":  [<attribute keys>],
        "category":       "structural" | "attribute_aware",
        "tags":           [...],   # free-form labels
    }

Attribute predicates are expressed entirely inline in `pattern` using
SeQL bracket notation, e.g.::

    "A[cost=\"5.0\"] B[lifecycle=\"complete\"]"

There is no separate `constraints` field — the executor reads only the
pattern string.

The four standard workloads are:

WORKLOAD_STRUCTURAL
    Pattern queries with no attribute constraints.  Baseline against
    which attribute-aware queries are compared.

WORKLOAD_ATTRIBUTE_AWARE
    Pattern queries with inline attribute constraints (single-event
    equality and cross-activity variable bindings).

WORKLOAD_SKEWED
    Mixed queries with deliberate skew: 80% touch the "hot" pair
    set, 20% touch the "cold" set.  Used to demonstrate that adaptive
    maintenance tracks demand, not schema.

WORKLOAD_UNIFORM
    Mixed queries spread evenly across all perspective-pair
    combinations.

All four are *derived* from the configured dataset.  At import time
this module reads the dataset selected via EVAL_DATASET / EVAL_LOG_NAME
(see eval_common.resolve_dataset) and builds workloads from observed
activities, attribute values, and perspective keys.  Experiments may
also call the `build_*` functions directly with a custom schema.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass
from typing import Sequence

from tests.eval.eval_common import (
    DatasetSchema,
    discover_schema,
    resolve_dataset,
)


# ---------------------------------------------------------------------------
# Query record helpers
# ---------------------------------------------------------------------------

def _q(
    qid: str,
    pattern: str,
    *,
    log_name: str,
    gkeys: list[str],
    category: str = "structural",
    tags: Sequence[str] | None = None,
) -> dict:
    return {
        "id":            qid,
        "log_name":      log_name,
        "pattern":       pattern,
        "grouping_keys": list(gkeys),
        "category":      category,
        "tags":          list(tags or []),
    }


# ---------------------------------------------------------------------------
# Workload context
# ---------------------------------------------------------------------------

@dataclass
class WorkloadContext:
    """
    Resolved per-dataset context that drives workload construction.

    `activities` is the list of distinct activity labels observed in
    the log, in first-seen order.  `perspectives` is the list of
    grouping-key sets to vary over.  `attribute_values` maps each
    attribute key to a list of observed string values for use in
    inline equality constraints.
    """
    log_name: str
    activities: list[str]
    perspectives: list[list[str]]
    attribute_values: dict[str, list[str]]

    @classmethod
    def from_schema(
        cls,
        log_name: str,
        schema: DatasetSchema,
        *,
        max_activities: int = 6,
        max_perspectives: int = 2,
    ) -> "WorkloadContext":
        acts = schema.activities[:max_activities]
        if len(acts) < 2:
            raise ValueError(
                f"Dataset has too few activities to build a workload: {acts}"
            )
        # Use single-key perspectives drawn from low-cardinality string attrs.
        # Fall back to no grouping if the dataset has no usable perspective keys.
        if schema.perspective_keys:
            perspectives = [[k] for k in schema.perspective_keys[:max_perspectives]]
        else:
            perspectives = [[]]
        return cls(
            log_name=log_name,
            activities=acts,
            perspectives=perspectives,
            attribute_values=schema.attribute_values,
        )

    # ── pattern fragment helpers ─────────────────────────────────────────

    def constrain(self, activity: str, attr: str, *, fallback: str | None = None) -> str:
        """
        Return ``activity[attr="<value>"]`` using the first observed value
        for `attr`, or `fallback` if the attribute is unknown.  When no
        value is available at all, returns the bare activity (no constraint).
        """
        values = self.attribute_values.get(attr, [])
        value = values[0] if values else fallback
        if value is None:
            return activity
        # Escape any embedded double quotes in the value.
        safe = value.replace('"', '\\"')
        return f'{activity}[{attr}="{safe}"]'

    def equal_var(self, activity: str, attr: str, var_id: int) -> str:
        """``activity[attr=$N]`` — used to bind cross-activity equality."""
        return f"{activity}[{attr}=${var_id}]"


# ---------------------------------------------------------------------------
# Pattern templates
# ---------------------------------------------------------------------------

def _ordered_pairs(activities: Sequence[str]) -> list[tuple[str, str]]:
    return [(a, b) for a, b in itertools.permutations(activities, 2)]


def _ordered_triples(activities: Sequence[str]) -> list[tuple[str, str, str]]:
    return list(itertools.permutations(activities, 3))


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------

def build_structural(ctx: WorkloadContext, *, max_queries: int = 10) -> list[dict]:
    """All-pair and selected-triple structural patterns over the schema."""
    queries: list[dict] = []
    pairs   = _ordered_pairs(ctx.activities)
    triples = _ordered_triples(ctx.activities)

    counter = itertools.count(1)
    for gk in ctx.perspectives:
        for a, b in pairs:
            queries.append(_q(
                f"S{next(counter)}", f"{a} {b}",
                log_name=ctx.log_name, gkeys=gk,
            ))
            if len(queries) >= max_queries:
                return queries
    for gk in ctx.perspectives:
        for a, b, c in triples:
            queries.append(_q(
                f"S{next(counter)}", f"{a} {b} {c}",
                log_name=ctx.log_name, gkeys=gk,
            ))
            if len(queries) >= max_queries:
                return queries
    return queries


def build_attribute_aware(
    ctx: WorkloadContext,
    *,
    max_queries: int = 8,
    value_attr_candidates: Sequence[str] = ("cost", "lifecycle", "resource", "role"),
) -> list[dict]:
    """
    Generate queries with inline-bracket attribute constraints.

    Picks attributes that actually exist in the dataset.  Mixes:
      - single-event equality   ``A[attr="v"] B``
      - cross-activity equality ``A[attr=$1] B[attr=$1]``
    """
    pairs = _ordered_pairs(ctx.activities)
    if not pairs:
        return []

    # Restrict to attributes that are present and have at least one value.
    available = [
        a for a in value_attr_candidates
        if ctx.attribute_values.get(a)
    ]
    if not available:
        return []

    queries: list[dict] = []
    counter = itertools.count(1)

    # 1. Single-event equality constraints (one per attribute).
    for attr in available:
        if len(queries) >= max_queries:
            break
        a, b = pairs[len(queries) % len(pairs)]
        gk = ctx.perspectives[len(queries) % len(ctx.perspectives)]
        pat = f"{ctx.constrain(a, attr)} {b}"
        queries.append(_q(
            f"A{next(counter)}", pat,
            log_name=ctx.log_name, gkeys=gk,
            category="attribute_aware",
        ))

    # 2. Cross-activity equality (variable binding).
    for attr in available:
        if len(queries) >= max_queries:
            break
        a, b = pairs[(len(queries) + 1) % len(pairs)]
        gk = ctx.perspectives[len(queries) % len(ctx.perspectives)]
        pat = f"{ctx.equal_var(a, attr, 1)} {ctx.equal_var(b, attr, 1)}"
        queries.append(_q(
            f"A{next(counter)}", pat,
            log_name=ctx.log_name, gkeys=gk,
            category="attribute_aware",
        ))

    # 3. Multi-attribute single-event constraints if room remains.
    if len(queries) < max_queries and len(available) >= 2:
        a, b = pairs[len(queries) % len(pairs)]
        gk = ctx.perspectives[0]
        attr1, attr2 = available[0], available[1]
        v1 = ctx.attribute_values[attr1][0]
        v2 = ctx.attribute_values[attr2][0]
        pat = f'{a}[{attr1}="{v1}",{attr2}="{v2}"] {b}'
        queries.append(_q(
            f"A{next(counter)}", pat,
            log_name=ctx.log_name, gkeys=gk,
            category="attribute_aware",
        ))

    return queries


def build_skewed(
    ctx: WorkloadContext,
    *,
    n_queries: int = 50,
    hot_ratio: float = 0.8,
) -> list[dict]:
    """
    Skewed mix: 80% of queries hit a small hot pair set, 20% the cold set.
    """
    n_hot = int(n_queries * hot_ratio)
    n_cold = n_queries - n_hot

    pairs = _ordered_pairs(ctx.activities)
    if not pairs:
        return []

    n_hot_combos = min(2, len(pairs))
    primary_persp = ctx.perspectives[0]
    secondary_persp = ctx.perspectives[1] if len(ctx.perspectives) > 1 else primary_persp

    hot_templates = [(f"{a} {b}", primary_persp) for a, b in pairs[:n_hot_combos]]
    cold_templates: list[tuple[str, list[str]]] = []
    for a, b in pairs[n_hot_combos:n_hot_combos + 3]:
        cold_templates.append((f"{a} {b}", primary_persp))
    if secondary_persp != primary_persp:
        for a, b in pairs[:2]:
            cold_templates.append((f"{a} {b}", secondary_persp))
    if not cold_templates:
        cold_templates = hot_templates[-1:]

    queries: list[dict] = []
    for i in range(n_hot):
        pat, gk = hot_templates[i % len(hot_templates)]
        queries.append(_q(
            f"H{i}", pat,
            log_name=ctx.log_name, gkeys=gk,
            tags=["hot"],
        ))
    for i in range(n_cold):
        pat, gk = cold_templates[i % len(cold_templates)]
        queries.append(_q(
            f"C{i}", pat,
            log_name=ctx.log_name, gkeys=gk,
            tags=["cold"],
        ))

    # Interleave so cold queries don't all bunch at the end.
    interleaved: list[dict] = []
    hi = ci = 0
    hot_qs  = [q for q in queries if "hot"  in q["tags"]]
    cold_qs = [q for q in queries if "cold" in q["tags"]]
    n_total = len(queries)
    for k in range(n_total):
        if ci < len(cold_qs) and (k * len(cold_qs)) // n_total > ci - 1:
            interleaved.append(cold_qs[ci]); ci += 1
        elif hi < len(hot_qs):
            interleaved.append(hot_qs[hi]); hi += 1
        elif ci < len(cold_qs):
            interleaved.append(cold_qs[ci]); ci += 1
    return interleaved


def build_uniform(ctx: WorkloadContext, *, n_queries: int = 50) -> list[dict]:
    pairs = _ordered_pairs(ctx.activities)
    if not pairs:
        return []
    templates = [
        (f"{a} {b}", gk)
        for gk in ctx.perspectives
        for a, b in pairs
    ]
    queries = []
    for i in range(n_queries):
        pat, gk = templates[i % len(templates)]
        queries.append(_q(
            f"U{i}", pat,
            log_name=ctx.log_name, gkeys=gk,
            tags=["uniform"],
        ))
    return queries


# ---------------------------------------------------------------------------
# Convenience: derive all four workloads from the configured dataset
# ---------------------------------------------------------------------------

@dataclass
class Workloads:
    structural:      list[dict]
    attribute_aware: list[dict]
    skewed:          list[dict]
    uniform:         list[dict]
    context:         WorkloadContext

    def as_dict(self) -> dict[str, list[dict]]:
        return {
            "structural":      self.structural,
            "attribute_aware": self.attribute_aware,
            "skewed":          self.skewed,
            "uniform":         self.uniform,
        }


def build_workloads(
    dataset: str | None = None,
    log_name: str | None = None,
    *,
    n_skewed: int = 50,
    n_uniform: int = 50,
) -> Workloads:
    """
    Build all four standard workloads from a dataset.  See
    `eval_common.resolve_dataset` for selection rules.
    """
    spec = resolve_dataset(dataset, log_name)
    schema = discover_schema(spec.path)
    ctx = WorkloadContext.from_schema(spec.log_name, schema)
    return Workloads(
        structural      = build_structural(ctx),
        attribute_aware = build_attribute_aware(ctx),
        skewed          = build_skewed(ctx, n_queries=n_skewed),
        uniform         = build_uniform(ctx, n_queries=n_uniform),
        context         = ctx,
    )


# ---------------------------------------------------------------------------
# Module-level defaults: derive once for the configured dataset.
# ---------------------------------------------------------------------------
# These remain available for code that imports the constants directly.
# Set EVAL_DATASET / EVAL_LOG_NAME to switch datasets.

_DEFAULT = build_workloads()

WORKLOAD_STRUCTURAL      = _DEFAULT.structural
WORKLOAD_ATTRIBUTE_AWARE = _DEFAULT.attribute_aware
WORKLOAD_SKEWED          = _DEFAULT.skewed
WORKLOAD_UNIFORM         = _DEFAULT.uniform
LOG_NAME                 = _DEFAULT.context.log_name


def all_workloads() -> dict[str, list[dict]]:
    return _DEFAULT.as_dict()


if __name__ == "__main__":
    print(f"log_name: {LOG_NAME}")
    print(f"activities: {_DEFAULT.context.activities}")
    print(f"perspectives: {_DEFAULT.context.perspectives}")
    print(f"attribute_values keys: {list(_DEFAULT.context.attribute_values)}")
    for name, wl in all_workloads().items():
        print(f"\n{name}: {len(wl)} queries")
        if name in ("skewed", "uniform"):
            continue
        for q in wl[:5]:
            print(f"  {q['id']}: {q['pattern']!r}  group={q['grouping_keys']}")
