import time
from siesta.core.logger import timed
from siesta.core.sparkManager import get_spark_session
from siesta.core.storageFactory import get_storage_manager
from siesta.model.StorageModel import MetaData
from typing import Dict, Any
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from siesta.modules.query.parse_seql import Quantifier as SeqlQuantifier, RespondedPair, extract_info_pairs, parse_pattern, extract_responded_pairs, can_match_single_event, pattern_labels
from siesta.modules.query.CEP_adapter import find_occurrences_dsl
from siesta.modules.query.processors.predicates import build_event_keep_predicate
import json
import logging
from functools import reduce
logger = logging.getLogger(__name__)

pattern = ""


def detect(pattern: str, config: Dict[str, Any], metadata: MetaData):
    """(trace_id, positions of the first match) for every trace that matches ``pattern``."""
    storage = get_storage_manager()

    if can_match_single_event(pattern):
        # A single-event match involves no pair, so the pairs index cannot
        # deliver it: hand CEP every event of the pattern's activities.
        events_rdd = (
            storage.read_activity_events(metadata, sorted(pattern_labels(pattern)))
            .rdd.map(lambda r: (r.trace_id, _event(r.activity, r.position, r.start_timestamp, r.attributes)))
            .groupByKey()
        )
    else:
        events_rdd = _events_from_pairs(pattern, storage, metadata)

    return (
        events_rdd
        .map(lambda kv: (kv[0], _first_match(pattern, kv[1])))
        .filter(lambda result: len(result[1]) > 0)
        .collect()
    )


def _event(name, position, timestamp, attributes) -> dict:
    """An event dict as CEP expects it; attributes override the base fields."""
    event = {"name": name, "position": position, "timestamp": timestamp}
    if attributes:
        event.update(attributes)
    return event


def _first_match(pattern: str, events) -> list:
    """Positions of the first CEP match of ``pattern`` over one trace's events."""
    events = sorted(events, key=lambda e: int(e["position"]))
    positions = find_occurrences_dsl([e["name"] for e in events], pattern, events=events)
    return [int(events[i]["position"]) for i in positions]


def _events_from_pairs(pattern: str, storage, metadata: MetaData):
    """
    (trace_id, events) for the traces that survive pair pruning, with each
    trace's events rebuilt from the fetched pair rows (responded pairs plus
    the info pairs that make those rows deliver every event CEP may need).
    """
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

    # Attribute pushdown: drop rows whose endpoints can never be part of a
    # match before they reach CEP.  Pruning above must keep using the
    # unfiltered rows (see build_event_keep_predicate).
    keep_pred = build_event_keep_predicate(pattern)
    cep_rows_df = tagged_df.where(keep_pred) if keep_pred is not None else tagged_df

    pair_positions_df = (
        cep_rows_df
        .join(pruned_trace_ids, on="trace_id", how="inner")
        .repartition("trace_id")
    )

    def rows_to_events(trace_id_rows):
        trace_id, rows = trace_id_rows
        # Reconstruct the event list (same logic as mine_trace)
        seen_positions = {}
        for r in rows:
            for name, pos, ts, attrs in ((r.source, r.source_position, r.source_timestamp, r.source_attributes),
                                         (r.target, r.target_position, r.target_timestamp, r.target_attributes)):
                if pos not in seen_positions:
                    seen_positions[pos] = _event(name, pos, ts, attrs)
        return trace_id, list(seen_positions.values())

    return pair_positions_df.rdd.map(lambda r: (r.trace_id, r)).groupByKey().map(rows_to_events)


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


def process_detection_query(config: Dict[str, Any], metadata: MetaData):
    new_pattern = config.get("query", {}).get("pattern", "")
    support_threshold = config.get("support_threshold", 0.0)

    start = time.time()
    result = detect(new_pattern, config, metadata)
    end = time.time()
    

    # Support is the fraction of traces that match the pattern; below the
    # threshold the pattern counts as not detected.
    support = len(result) / metadata.trace_count if metadata.trace_count else 0.0
    detected = [] if support < support_threshold else [
        {"trace_id": trace_id, "support": support, "positions": positions}
        for trace_id, positions in result
    ]
    logger.info(f"Parsing query took: {end - start}")
    return {"code": 200, "total": len(detected), "support": support, "detected": detected, "time": end - start}
