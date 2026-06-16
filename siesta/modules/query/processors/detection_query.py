import time
from siesta.core.logger import timed
from siesta.core.sparkManager import get_spark_session
from siesta.core.storageFactory import get_storage_manager
from siesta.model.StorageModel import MetaData
from typing import Dict, Any
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from siesta.modules.query.parse_seql import Quantifier as SeqlQuantifier, RespondedPair, extract_info_pairs, parse_pattern, extract_responded_pairs
from siesta.modules.query.CEP_adapter import find_occurrences_dsl
import json
import logging
from functools import reduce
logger = logging.getLogger(__name__)

pattern = ""


def detect(pattern: str, config: Dict[str, Any], metadata: MetaData):
    storage = get_storage_manager()
    support_threshold = config.get("support_threshold", 0.0)

    # lf optimizer
    pair_branches = set(extract_responded_pairs(pattern))

    branch_required: dict[int, set[tuple[str, str]]] = {}
    for rp in pair_branches:
        if rp.source_quantifier == SeqlQuantifier.STAR or rp.target_quantifier == SeqlQuantifier.STAR:
            continue  # STAR-involved pairs are optional -> skip for pruning
        branch_required.setdefault(rp.branch_id, set()).add(
            (rp.source.label, rp.target.label)
        )

    # All responded pairs (including STAR) needed for data fetch
    all_responded = set(
        (rp.source.label, rp.target.label, rp.branch_id) for rp in pair_branches
    )
    
    info_pairs = extract_info_pairs(pattern)
    all_pairs = list(info_pairs.union(all_responded))
    all_pairs_2d = {(p[0], p[1]) for p in all_pairs}
    all_pred = build_exact_pair_predicate(all_pairs_2d)


    index_table = storage.read_pairs_index(metadata)

    tagged_df = index_table.where(all_pred)

    # Per branch pruning
    branch_pruned_dfs = []
    for bid, required_2d in branch_required.items():
        if not required_2d:
            continue
        branch_pred = build_exact_pair_predicate(required_2d)
        branch_pruned = (
            tagged_df
            .where(branch_pred)
            .dropDuplicates(["trace_id", "source", "target"])
            .groupBy("trace_id")
            .agg(F.count("*").alias("pair_count"))
            .filter(F.col("pair_count") == len(required_2d))
            .select("trace_id")
        )
        branch_pruned_dfs.append(branch_pruned)

    if not branch_pruned_dfs:
        # No required pairs (e.g. all STAR) -> every trace with matching data qualifies
        pruned_trace_ids = tagged_df.select("trace_id").distinct()
    elif len(branch_pruned_dfs) == 1:
        pruned_trace_ids = branch_pruned_dfs[0].distinct()
    else:
        pruned_trace_ids = reduce(lambda a, b: a.union(b), branch_pruned_dfs).distinct()

    pair_positions_df = (
        tagged_df
        .join(pruned_trace_ids, on="trace_id", how="inner")
        .repartition("trace_id")
    )

    matches_df = pair_positions_df.rdd.map(lambda r: (
            r.trace_id,
            {
                "source": r.source,
                "target": r.target,
                "source_position":    r.source_position,
                "target_position":    r.target_position,
                "source_timestamp":   r.source_timestamp,
                "target_timestamp":   r.target_timestamp,
                "source_attributes":  r.source_attributes,
                "target_attributes":  r.target_attributes,
            }
        ))

    def validate_trace(trace_id_rows):
        trace_id, rows = trace_id_rows
        rows = list(rows)

        # Reconstruct event list (same logic as mine_trace)
        seen_positions = {}
        for r in rows:
            for side in [("source", "source_position", "source_timestamp", "source_attributes"),
                         ("target", "target_position", "target_timestamp", "target_attributes")]:
                name_k, pos_k, ts_k, attr_k = side
                pos = r[pos_k]
                if pos not in seen_positions:
                    seen_positions[pos] = {
                        "name":       r[name_k],
                        "position":   pos,
                        "timestamp":  r[ts_k],
                    }
                    attrs: dict = r[attr_k]
                    if attrs:
                        for key, value in attrs.items():
                            seen_positions[pos][key] = value


        events = sorted(seen_positions.values(), key=lambda e: int(e["timestamp"]))

        positions = find_occurrences_dsl(
            [e["name"] for e in events], pattern, events=events
        )

        return (trace_id, positions)
    
    return matches_df.groupByKey().map(validate_trace).filter(lambda result: len(result[1]) > support_threshold * metadata.trace_count if metadata.trace_count else 0).collect()


def build_exact_pair_predicate(pairs_2d: set[tuple[str, str]]):
    """
    Builds a Spark filter predicate that matches EXACT (source, target) pairs.

    Input schema:  pairs_2d  - set of (source: str, target: str)
    Applied to df schema:  source: str, target: str, trace_id: str, ...

    Avoids the cross-product false-positive problem of:
        col("source").isin(sources) & col("target").isin(targets)
    which would admit (A, D) even if only (A, C) and (B, D) are valid.

    Strategy: group all allowed targets per source, then emit one clause per
    source:  (source == A AND target IN {C}) OR (source == B AND target IN {D})
    This is equivalent to an exact-pair membership check without a join.
    """
    # Group all allowed targets by their source activity
    # e.g. {(A,C),(A,D),(B,E)} -> {A: {C,D}, B: {E}}
    by_source: dict[str, set[str]] = {}
    for s, t in pairs_2d:
        by_source.setdefault(s, set()).add(t)

    # One conjunctive clause per source: (source == s AND target IN allowed_targets)
    clauses = [
        (F.col("source") == s) & F.col("target").isin(list(targets))
        for s, targets in by_source.items()
    ]
    # If no pairs were provided, nothing should match
    if not clauses:
        return F.lit(False)
    # OR all clauses together: a row matches if it satisfies any (source, targets) group
    return reduce(lambda a, b: a | b, clauses)

def _literal_attr_predicate(side_map: str, name: str, op: str, value):
    """
    Map a single literal attribute constraint to a Spark Column predicate
    over a MapType column, or return None if the (op, value) combination is
    not expressible as a pure column predicate.

    `side_map` is the MapType column name ("source_attributes" or
    "target_attributes"); the pair row stores each endpoint's attributes
    there.  A missing key yields a NULL accessor, so equality / ordering
    comparisons against a missing attribute evaluate to NULL (the row is
    dropped) — which is the correct "constraint not satisfied" behaviour.
    For ``!=`` we additionally admit NULL so that ``attr != "x"`` matches
    events that simply lack the attribute.
    """
    from siesta.modules.query.parse_seql import StringLiteral, NumberLiteral

    accessor = F.col(side_map)[name]

    if isinstance(value, StringLiteral):
        if op == "=":
            return accessor == value.value
        if op == "!=":
            return accessor.isNull() | (accessor != value.value)
        # Ordering comparisons on string literals are not well-defined here.
        return None

    if isinstance(value, NumberLiteral):
        # Attribute values are stored as strings; cast for numeric compare so
        # "5" and "5.0" are treated as equal.
        num = accessor.cast("double")
        if op == "=":
            return num == value.value
        if op == "!=":
            return accessor.isNull() | (num != value.value)
        if op == "<":
            return num < value.value
        if op == "<=":
            return num <= value.value
        if op == ">":
            return num > value.value
        if op == ">=":
            return num >= value.value
        return None

    # VarExpr (a $-binding) is handled by the caller as a cross-side equality.
    return None

def build_pair_attr_predicate(rp: RespondedPair):
    """
    Build a Spark column predicate that a pair row of ``rp`` must satisfy to
    honour the attribute constraints declared on its source and target
    activities, and report whether *every* constraint was expressible.

    Returns
    -------
    (predicate, all_pushable) : (Column | None, bool)
        predicate     — conjunction of all expressible attribute predicates,
                        applied to the ``source_attributes`` / ``target_attributes``
                        MapType columns of the pair table.  None when there is
                        nothing to push down.
        all_pushable  — True iff every constraint on both endpoints was turned
                        into a column predicate.  When True the downstream CEP
                        validation is redundant for this pair: the surviving
                        rows already satisfy the full constraint set.

    Supported, pushable constraint forms
    -------------------------------------
      * literal equality / inequality:  ``A[attr="x"]`` , ``A[amount>=5]``
      * cross-side binding equality:     ``A[attr=$1] B[attr=$1]``  →
        ``source_attributes[attr] == target_attributes[attr]``

    Not pushable (forces CEP):
      * a binding used on only one side (no cross-side equality to form)
      * a binding with an arithmetic offset (``$1+5``) — value arithmetic
        across events is left to the CEP engine
      * ordering comparison against a string literal
    """
    from collections import defaultdict
    from siesta.modules.query.parse_seql import VarExpr

    preds = []
    all_pushable = True

    # ── literal / inequality constraints on each side ──────────────────────
    for side_map, node in (("source_attributes", rp.source),
                           ("target_attributes", rp.target)):
        for c in node.constraints:
            if isinstance(c.value, VarExpr):
                continue  # bindings handled below
            p = _literal_attr_predicate(side_map, c.name, c.op, c.value)
            if p is None:
                all_pushable = False
            else:
                preds.append(p)

    # ── cross-side binding equalities ($N appearing on both endpoints) ─────
    # Collect every binding site: var_id -> [(side_map, attr_name, varexpr), ...]
    var_sites = defaultdict(list)
    for side_map, node in (("source_attributes", rp.source),
                           ("target_attributes", rp.target)):
        for c in node.constraints:
            if isinstance(c.value, VarExpr):
                var_sites[c.value.var_id].append((side_map, c.name, c.value))

    for var_id, sites in var_sites.items():
        sides = {s[0] for s in sites}
        no_offset = all(v.op is None for _, _, v in sites)
        if (len(sites) == 2 and no_offset
                and sides == {"source_attributes", "target_attributes"}):
            # e.g. A[x=$1] B[y=$1]  →  source_attributes[x] == target_attributes[y]
            (m0, n0, _), (m1, n1, _) = sites
            preds.append(F.col(m0)[n0] == F.col(m1)[n1])
        else:
            # single-use binding or arithmetic offset — leave to CEP
            all_pushable = False

    if not preds:
        return None, all_pushable
    return reduce(lambda a, b: a & b, preds), all_pushable

def process_detection_query(config: Dict[str, Any], metadata: MetaData):
    new_pattern = config.get("query", {}).get("pattern", "")
    support_threshold = config.get("support_threshold", 0.0)

    start = time.time()
    result = detect(new_pattern, config, metadata)
    end = time.time()
    

    result = [{"trace_id": trace_id, "support": len(positions) / metadata.trace_count if metadata.trace_count else 0, "positions": positions} for trace_id, positions in result if len(positions) >= support_threshold]
    logger.info(f"Parsing query took: {end - start}")
    return {"code": 200, "total": len(result), "detected": result, "time": end - start}