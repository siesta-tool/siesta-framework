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

from tests.vldb_eval.eval_common import (
    API_BASE,
    QUERY_PREFIX,
    API_TIMEOUT_S,
    DatasetSchema,
    discover_schema,
    quote_label,
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
        max_perspectives: int = 4,
    ) -> "WorkloadContext":
        acts = schema.activities[:max_activities]
        if len(acts) < 2:
            raise ValueError(
                f"Dataset has too few activities to build a workload: {acts}"
            )
        # Perspectives are already sorted by cardinality ascending by
        # _select_perspectives.  Take the first max_perspectives so the
        # workload builder covers a range from low to high cardinality.
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
        value is available at all, returns the bare (quoted if needed) activity.

        The activity label is always passed through quote_label() so that
        multi-word activity names (e.g. "W Completeren aanvraag") are
        correctly wrapped in double quotes for the SeQL parser.
        """
        values = self.attribute_values.get(attr, [])
        value = values[0] if values else fallback
        ql = quote_label(activity)
        if value is None:
            return ql
        safe = value.replace('"', '\\"')
        return f'{ql}[{attr}="{safe}"]'

    def equal_var(self, activity: str, attr: str, var_id: int) -> str:
        """``activity[attr=$N]`` — used to bind cross-activity equality."""
        return f"{quote_label(activity)}[{attr}=${var_id}]"


# ---------------------------------------------------------------------------
# Pattern templates
# ---------------------------------------------------------------------------

def _ordered_pairs(activities: Sequence[str]) -> list[tuple[str, str]]:
    return [(a, b) for a, b in itertools.permutations(activities, 2)]


def _ordered_triples(activities: Sequence[str]) -> list[tuple[str, str, str]]:
    return list(itertools.permutations(activities, 3))


def _pat2(a: str, b: str) -> str:
    """Two-activity pattern, quoting labels that contain spaces."""
    return f"{quote_label(a)} {quote_label(b)}"


def _pat3(a: str, b: str, c: str) -> str:
    """Three-activity pattern, quoting labels that contain spaces."""
    return f"{quote_label(a)} {quote_label(b)} {quote_label(c)}"


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
                f"S{next(counter)}", _pat2(a, b),
                log_name=ctx.log_name, gkeys=gk,
            ))
            if len(queries) >= max_queries:
                return queries
    for gk in ctx.perspectives:
        for a, b, c in triples:
            queries.append(_q(
                f"S{next(counter)}", _pat3(a, b, c),
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
        pat = f'{quote_label(a)}[{attr1}="{v1}",{attr2}="{v2}"] {quote_label(b)}'
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

    hot_templates = [(_pat2(a, b), primary_persp) for a, b in pairs[:n_hot_combos]]
    cold_templates: list[tuple[str, list[str]]] = []
    for a, b in pairs[n_hot_combos:n_hot_combos + 3]:
        cold_templates.append((_pat2(a, b), primary_persp))
    if secondary_persp != primary_persp:
        for a, b in pairs[:2]:
            cold_templates.append((_pat2(a, b), secondary_persp))
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
        (_pat2(a, b), gk)
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
# Result-bearing workload (data-driven, stratified by result density)
# ---------------------------------------------------------------------------

def fetch_pair_coverage(
    log_name: str,
    grouping_keys: list[str],
    *,
    activities: list[str] | None = None,
    storage_namespace: str = "siesta",
) -> dict:
    """
    Call the /pair_coverage endpoint and return the parsed JSON.

    Separated from build_result_bearing_workload so callers can cache
    the result (one call per (log, perspective) is enough — the coverage
    distribution is stable within an experiment run).
    """
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


def build_result_bearing_workload(
    ctx: WorkloadContext,
    *,
    k_dense: int = 4,
    k_sparse: int = 4,
    k_singleton: int = 2,
    dense_threshold: float = 0.5,
    sparse_lo: float = 0.05,
    sparse_hi: float = 0.20,
    min_perspective_cardinality: int = 6,
    rng_seed: int = 0,
) -> list[dict]:
    """
    Build a stratified, data-driven structural workload.

    For each perspective in ctx.perspectives whose group count is at
    least `min_perspective_cardinality`, fetch the pair-coverage
    distribution from the /pair_coverage endpoint and sample queries
    from three buckets:

      DENSE     — pairs covering >= dense_threshold * group_count groups.
                  "Most groups qualify."
      SPARSE    — pairs covering [sparse_lo, sparse_hi] * group_count.
                  "A few real groups qualify."
      SINGLETON — pairs covering exactly 1 group.
                  "Almost-empty result, but non-trivial."

    Perspectives below `min_perspective_cardinality` are skipped:
    below 6 groups, the DENSE/SPARSE/SINGLETON thresholds collapse
    (e.g. with 5 groups, SPARSE = [0, 1] groups, indistinguishable
    from SINGLETON).  For meaningful stratification prefer >= 20.

    Sampling within each bucket is deterministic given `rng_seed`.

    Returns the full query list across all retained perspectives with:
      - `id` values numbered globally (S1, S2, ...)
      - `bucket` tag  (DENSE / SPARSE / SINGLETON)
      - `group_coverage`  — observed number of qualifying groups
      - `perspective_groups` — total groups in this perspective
    """
    import random

    queries: list[dict] = []
    counter = itertools.count(1)
    rng = random.Random(rng_seed)

    for gk in ctx.perspectives:
        try:
            cov = fetch_pair_coverage(
                ctx.log_name,
                gk,
                activities=ctx.activities,
            )
        except Exception as exc:
            print(f"  [workload] skipping perspective {gk}: {exc}")
            continue

        group_count = cov["group_count"]
        if group_count < min_perspective_cardinality:
            print(
                f"  [workload] skipping perspective {gk}: "
                f"only {group_count} groups (< {min_perspective_cardinality})"
            )
            continue

        pairs = cov["pairs"]
        if not pairs:
            print(f"  [workload] skipping perspective {gk}: no co-occurring pairs")
            continue

        dense_cut   = dense_threshold * group_count
        sparse_lo_n = max(1, int(sparse_lo * group_count))
        sparse_hi_n = max(sparse_lo_n, int(sparse_hi * group_count))

        dense     = [p for p in pairs if p["groups"] >= dense_cut]
        sparse    = [p for p in pairs if sparse_lo_n <= p["groups"] <= sparse_hi_n]
        singleton = [p for p in pairs if p["groups"] == 1]

        def take(bucket: list[dict], k: int) -> list[dict]:
            if len(bucket) <= k:
                return list(bucket)
            return rng.sample(bucket, k)

        chosen = (
            [("DENSE",     p) for p in take(dense,     k_dense)]
            + [("SPARSE",    p) for p in take(sparse,    k_sparse)]
            + [("SINGLETON", p) for p in take(singleton, k_singleton)]
        )

        print(
            f"  [workload] perspective {gk}: "
            f"group_count={group_count}  "
            f"DENSE={len(dense)} (took {min(len(dense), k_dense)})  "
            f"SPARSE={len(sparse)} (took {min(len(sparse), k_sparse)})  "
            f"SINGLETON={len(singleton)} (took {min(len(singleton), k_singleton)})"
        )

        for bucket_name, pair in chosen:
            q = _q(
                f"S{next(counter)}",
                _pat2(pair["source"], pair["target"]),
                log_name=ctx.log_name,
                gkeys=gk,
                category="structural",
                tags=[bucket_name],
            )
            q["group_coverage"]     = pair["groups"]
            q["perspective_groups"] = group_count
            queries.append(q)

    return queries


# ---------------------------------------------------------------------------
# Shared-structure workload (phase 2 of 6.3.1)
# ---------------------------------------------------------------------------

def build_shared_structure_workload(
    ctx: WorkloadContext,
    *,
    n_hot_pairs: int = 5,
    n_cold_pairs: int = 8,
    reps_per_hot_pair: int = 4,
    reps_per_cold_pair: int = 1,
    min_perspective_cardinality: int = 5,
    sleep_seconds: int = 120,
    rng_seed: int = 0,
) -> list[dict]:
    """
    Build a workload that demonstrates demand-driven materialisation
    across a realistic query stream.

    All patterns are 2-activity ("A B") so the skip-CEP path fires on
    every query.  Variety comes from WHICH pairs are queried and HOW
    OFTEN, not from pattern complexity.

    For each perspective in ctx.perspectives that has at least
    `min_perspective_cardinality` groups:

      1. Fetch pair coverage, sort by group_count descending.
      2. Pick `n_hot_pairs` from the top — pairs that cover the most
         groups, so every query returns meaningful results.
      3. Pick `n_cold_pairs` from further down — pairs with lower
         coverage, representing infrequent demand.
      4. Emit `reps_per_hot_pair` query records for each hot pair and
         `reps_per_cold_pair` for each cold pair.
      5. Interleave hot and cold deterministically so the stream looks
         realistic — hot queries are spread through the stream, not
         bunched at the start.

    The resulting stream looks like:

        seq=0   (A,B) HOT  rep 1  → lazy scan  ~20s   ABSENT
        seq=1   (X,Y) COLD rep 1  → lazy scan  ~20s   ABSENT
        seq=2   (C,D) HOT  rep 1  → lazy scan  ~20s   ABSENT
        seq=3   (A,B) HOT  rep 2  → LRU hit    ~4s    TRANSIENT
        seq=4   (C,D) HOT  rep 2  → LRU hit    ~4s    TRANSIENT
        seq=5   (A,B) HOT  rep 3  → LRU hit    ~4s    TRANSIENT ← cost gate fires
        seq=6   (C,D) HOT  rep 3  → LRU hit    ~4s    TRANSIENT ← cost gate fires
        ...
        seq=10  (A,B) HOT  rep 4  → Delta read ~5s    PERSISTENT
        seq=11  (X,Y) COLD rep 1  → lazy scan  ~20s   ABSENT    ← never promoted

    HOT pairs accumulate query_count across their reps, cross the
    min_query_count threshold, and get promoted to PERSISTENT.
    COLD pairs appear only once, stay at ABSENT or TRANSIENT, and
    serve as a within-stream control showing that the system does NOT
    blindly promote everything.

    Each query record carries:
      - tags:           ["HOT"] or ["COLD"]
      - shared_pair:    "A->B" — the pair this query targets
      - group_coverage: from pair_coverage
      - hot_rep:        which repetition of this pair (1-indexed)
                        so the plotter can show the per-pair learning curve
    """
    import random

    queries: list[dict] = []
    counter = itertools.count(1)
    rng = random.Random(rng_seed)

    for gk in ctx.perspectives:
        try:
            cov = fetch_pair_coverage(
                ctx.log_name,
                gk,
                # activities=ctx.activities,
            )
        except Exception as exc:
            print(f"  [workload] skipping perspective {gk}: {exc}")
            continue

        group_count = cov["group_count"]
        if group_count < min_perspective_cardinality:
            print(
                f"  [workload] skipping perspective {gk}: "
                f"only {group_count} groups (< {min_perspective_cardinality})"
            )
            continue

        pairs = cov["pairs"]   # sorted by groups desc
        if not pairs:
            print(f"  [workload] skipping perspective {gk}: no co-occurring pairs")
            continue

        n_hot  = min(n_hot_pairs,  len(pairs))
        # Pick cold pairs from the bottom of the sorted list (fewest groups).
        # Ensure no overlap with hot pairs.
        n_cold = min(n_cold_pairs, max(0, len(pairs) - n_hot))

        hot_pairs  = pairs[:n_hot]
        cold_pairs = pairs[len(pairs) - n_cold:] if n_cold else []

        print(
            f"  [workload] perspective {gk}: "
            f"group_count={group_count}  "
            f"hot={n_hot} (top coverage)  cold={n_cold} (bottom coverage)"
        )

        # Build per-hot-pair and per-cold-pair query lists before
        # interleaving, so we can shuffle within each group independently.

        # Hot pair queries: reps_per_hot_pair copies of each, in
        # round-robin order across pairs so the stream looks like
        #   pair0-rep1, pair1-rep1, pair2-rep1, pair0-rep2, ...
        # rather than all reps of pair0, then all of pair1, etc.
        hot_rounds: list[list[dict]] = []
        for rep_idx in range(reps_per_hot_pair):
            round_queries: list[dict] = []
            for pair in hot_pairs:
                a, b = pair["source"], pair["target"]
                q = _q(
                    f"H{next(counter)}", _pat2(a, b),
                    log_name=ctx.log_name, gkeys=gk,
                    category="structural",
                    tags=["HOT"],
                )
                q["shared_pair"]    = f"{a}->{b}"
                q["group_coverage"] = pair["groups"]
                q["hot_rep"]        = rep_idx + 1
                round_queries.append(q)
            # Shuffle the order of pairs within each round so consecutive
            # rounds don't always hit the same pair first.
            rng.shuffle(round_queries)
            hot_rounds.append(round_queries)

        # Cold pair queries: one copy each, shuffled.
        cold_queries: list[dict] = []
        for pair in cold_pairs:
            a, b = pair["source"], pair["target"]
            for _ in range(reps_per_cold_pair):
                q = _q(
                    f"C{next(counter)}", _pat2(a, b),
                    log_name=ctx.log_name, gkeys=gk,
                    category="structural",
                    tags=["COLD"],
                )
                q["shared_pair"]    = f"{a}->{b}"
                q["group_coverage"] = pair["groups"]
                q["hot_rep"]        = 1
                cold_queries.append(q)
        rng.shuffle(cold_queries)

        # Interleave: spread cold queries evenly across the hot rounds.
        # Strategy: insert cold queries between hot rounds.
        perspective_queries: list[dict] = []
        cold_per_gap = max(1, len(cold_queries) // max(1, reps_per_hot_pair + 1))

        cold_idx = 0
        for round_idx, round_qs in enumerate(hot_rounds):
            # Insert a slice of cold queries before this hot round.
            slice_end = min(cold_idx + cold_per_gap, len(cold_queries))
            perspective_queries.extend(cold_queries[cold_idx:slice_end])
            cold_idx = slice_end
            # Then all hot queries for this round.
            perspective_queries.extend(round_qs)

            # After the 3rd round (index 2), every hot pair has been
            # queried 3 times and the cost gate has been crossed.
            # The background worker needs time to finish
            # build_pair_persistent for each pair before the 4th round
            # starts reading from Delta.  Insert a sleep sentinel so
            # run_category pauses here.
            # Budget: n_hot_pairs x ~20s per build, serialised = ~100s.
            # 120s gives a comfortable margin.
            if round_idx == 2:
                perspective_queries.append({
                    "id":            "__sleep__",
                    "sleep_seconds": sleep_seconds,
                    "log_name":      ctx.log_name,
                    "pattern":       "",
                    "grouping_keys": gk,
                    "tags":          ["__sentinel__"],
                })

        # Append any remaining cold queries at the end.
        perspective_queries.extend(cold_queries[cold_idx:])

        queries.extend(perspective_queries)

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
    max_perspectives: int = 4,
) -> Workloads:
    """
    Build all four standard workloads from a dataset.

    Perspectives are drawn from event-level, non-numeric attributes
    with cardinality in [3, 500], sorted by cardinality ascending.
    The case-level filter (avg distinct values per trace > 1.2)
    removes lifted trace-level metadata (loan amounts, patient ages,
    etc.) that would otherwise pass the cardinality check.
    """
    spec = resolve_dataset(dataset, log_name)
    schema = discover_schema(spec.path)
    ctx = WorkloadContext.from_schema(spec.log_name, schema,
                                     max_perspectives=max_perspectives)
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