import time
import pandas as pd
from siesta_framework.core.logger import timed
from siesta_framework.core.sparkManager import get_spark_session
from siesta_framework.core.storageFactory import get_storage_manager
from siesta_framework.model.StorageModel import MetaData
from siesta_framework.model.SystemModel import Query_Config, Pattern
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from siesta_framework.modules.Query.parse_seql import RespondedPair, extract_info_pairs, parse_pattern, extract_responded_pairs
from siesta_framework.modules.Query.CEP_adapter import find_occurrences_dsl
import json
import logging
from functools import reduce
logger = logging.getLogger("Query Processors")

pattern = ""

def _extract_consecutive_pairs(pattern: Pattern):
    activities = sorted( pattern, key=lambda x: x.get("position", 0))
    consecutive_pairs = set(zip(map(lambda x: x.get("activity"), activities), map(lambda x: x.get("activity"), activities[1:])))
    return consecutive_pairs



def process_stats_query(config: Query_Config, metadata: MetaData) -> list[any]|None|str: # type: ignore
    """
    Splits the query events in pairs and retrieves the statistics for each pair from the count table.
    """
    spark = get_spark_session()
    count_table = get_storage_manager().read_count_table(metadata)
    
    pairs = _extract_consecutive_pairs(config.get("query", {}).get("pattern", []))
    
    pairs_df = spark.createDataFrame(pairs, ["source", "target"])
    df = count_table.join(pairs_df, on=["source", "target"], how="inner")

    return str(df.collect())


def optimize_lf(query_pairs: set[RespondedPair], metadata:MetaData):
    """
    Input: List of pair labels + branch [(source, target, branch_id)]
    Output: LF optimized query (alr optimized)
    """

    #TODO: Compare w/sorting - probably worse/equal

    true_pairs = list(set(list(map(lambda x: (x.source.label, x.target.label, x.branch_id), query_pairs))))

    return true_pairs


def process_detection_query(config: Query_Config, metadata: MetaData):
    spark = get_spark_session()
    storage = get_storage_manager()
    logger.info(config)
    # 1. Generating pairs from sequence (abc -> (a,b), (b,c), (c,d))
    new_pattern = config.get("query", {}).get("alt_pattern", "")
    
    logger.info(f"Querying pattern: {new_pattern}")
    
    #optimizer
    start = time.time()
    pair_branches = set(extract_responded_pairs(new_pattern))
    true_pairs = optimize_lf(pair_branches, metadata)

    attribute_pairs = extract_info_pairs(new_pattern)
    logger.info(f"Attribute pairs: {attribute_pairs}")
    all_pairs = list(attribute_pairs.union(set(true_pairs)))

    true_pairs_set = set(map(tuple, true_pairs))

    truer_source_len = len(set([_[0] for _ in all_pairs]))
    true_source = spark.sparkContext.broadcast(set([_[0] for _ in all_pairs]))
    true_target = spark.sparkContext.broadcast(set([_[1] for _ in all_pairs]))

    index_table = storage.read_pairs_index(metadata)#.filter(col("source").isin(relevant_source))
    tagged_df = (
        index_table
        .where(col("source").isin(true_source.value) & col("target").isin(true_target.value))
    )

    pruned_trace_ids = (
        tagged_df
        .dropDuplicates(["trace_id", "source", "target"])
        .groupBy("trace_id")
        .agg(F.count("*").alias("pair_count"))
        .filter(F.col("pair_count").eqNullSafe(truer_source_len))
        .select("trace_id")
        .distinct()
    )

    pair_positions_df = (
        tagged_df
        .join(pruned_trace_ids, on="trace_id", how="inner")
        .repartition("trace_id")  # see point 3
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


        events = sorted(seen_positions.values(), key=lambda e: int(e["position"]))
        positions = find_occurrences_dsl(
            [e["name"] for e in events], new_pattern, events=events
        )
        return (trace_id, positions)
    
    logger.info(f"Parsing query took: {time.time() - start}")
    res = matches_df.groupByKey().map(validate_trace).collect()

    return str(res)


def process_detection_query_local(config: Query_Config, metadata: MetaData):
    spark = get_spark_session()
    storage = get_storage_manager()
    logger.info(config)
    # 1. Generating pairs from sequence (abc -> (a,b), (b,c), (c,d))
    new_pattern = config.get("query", {}).get("alt_pattern", "")
    
    logger.info(f"Querying pattern: {new_pattern}")
    
    #optimizer
    start = time.time()
    pair_branches = set(extract_responded_pairs(new_pattern))
    true_pairs = optimize_lf(pair_branches, metadata)

    info_pairs = extract_info_pairs(new_pattern)
    logger.info(f"Attribute pairs: {info_pairs}")
    all_pairs = list(info_pairs.union(set(true_pairs)))

    true_pairs_set = set(map(tuple, true_pairs))
    logger.info(f"true_pairs_set: {true_pairs_set}")

    truer_source_len = len(true_pairs_set)
    true_source = spark.sparkContext.broadcast(set([_[0] for _ in true_pairs_set]))
    true_target = spark.sparkContext.broadcast(set([_[1] for _ in true_pairs_set]))
    print(true_source.value)
    print(true_target.value)
    true_all = spark.sparkContext.broadcast(set([_[0] for _ in true_pairs_set]).union(set([_[1] for _ in true_pairs_set])))
    all_pairs_source = spark.sparkContext.broadcast(set([_[0] for _ in all_pairs]))
    all_pairs_target = spark.sparkContext.broadcast(set([_[1] for _ in all_pairs]))
    all_pairs_all = spark.sparkContext.broadcast(set([_[0] for _ in all_pairs]).union(set([_[1] for _ in all_pairs])))

    true_pairs_2d = {(p[0], p[1]) for p in true_pairs_set}
    all_pairs_2d = {(p[0], p[1]) for p in all_pairs}

    # true_pairs_df = F.broadcast(
    #     spark.createDataFrame(list(true_pairs_2d), ["source", "target"])
    # )
    # all_pairs_df = F.broadcast(
    #     spark.createDataFrame(list(all_pairs_2d), ["source", "target"])
    # )

    index_table = storage.read_pairs_index(metadata)

    def build_exact_pair_predicate(pairs_2d: set[tuple[str, str]]):
        """
        Builds a Spark filter predicate that matches EXACT (source, target) pairs.

        Input schema:  pairs_2d  — set of (source: str, target: str)
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

    all_pred = build_exact_pair_predicate(all_pairs_2d)
    true_pred = build_exact_pair_predicate(true_pairs_2d)

    tagged_df = index_table.where(all_pred)

    pruned_trace_ids = (
        tagged_df
        .where(true_pred)
        .dropDuplicates(["trace_id", "source", "target"])
        .groupBy("trace_id")
        .agg(F.count("*").alias("pair_count"))
        .filter(F.col("pair_count") == len(true_pairs_2d))
        .select("trace_id")
        .distinct()
    )



    # tagged_df = (
    #     index_table.join(all_pairs_df, on=["source", "target"], how="inner")
    # )

    # pruned_trace_ids = (
    #     tagged_df
    #     .join(true_pairs_df, on=["source", "target"], how="inner")
    #     .dropDuplicates(["trace_id", "source", "target"])
    #     .groupBy("trace_id")
    #     .agg(F.count("*").alias("pair_count"))
    #     .filter(F.col("pair_count").eqNullSafe(truer_source_len))
    #     .select("trace_id")
    #     .distinct()
    # )

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
        # print("Called")
        # print(trace_id_rows)
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


        events = sorted(seen_positions.values(), key=lambda e: int(e["position"]))
        # print(events)
        # print([e["name"] for e in events])
        # if len(events) != 3:
        #     return (trace_id, 0)
        positions = find_occurrences_dsl(
            [e["name"] for e in events], new_pattern, events=events
        )

        return (trace_id, positions)
    
    logger.info(f"Parsing query took: {time.time() - start}")
    matches_df_collected = matches_df.groupByKey().collectAsMap()
    res = []
    print(len(matches_df_collected.keys()))
    for match in matches_df_collected.keys():
        result = validate_trace((match, list(matches_df_collected[match])))
        if result[1]:
            res.append(result)

    return f"Total: {len(res)}, Occurrences: {res}"



('Application_1523673583', [
    {'source': 'O_Created', 'target': 'W_Complete application', 'source_position': 12, 'target_position': 17, 'source_timestamp': 1475076434, 'target_timestamp': 1475076487, 'source_attributes': {'OfferID': 'Offer_971612292', 'EventOrigin': 'Offer', 'EventID': 'OfferState_1547291615', 'Action': 'statechange', 'org:resource': 'User_46', 'lifecycle:transition': 'complete'}, 'target_attributes': {'EventOrigin': 'Workflow', 'EventID': 'Workitem_791501640', 'Action': 'Deleted', 'org:resource': 'User_46', 'lifecycle:transition': 'complete'}}
    , {'source': 'O_Created', 'target': 'O_Sent (mail and online)', 'source_position': 12, 'target_position': 28, 'source_timestamp': 1475076434, 'target_timestamp': 1476274046, 'source_attributes': {'OfferID': 'Offer_971612292', 'EventOrigin': 'Offer', 'EventID': 'OfferState_1547291615', 'Action': 'statechange', 'org:resource': 'User_46', 'lifecycle:transition': 'complete'}, 'target_attributes': {'OfferID': 'Offer_1387971816', 'EventOrigin': 'Offer', 'EventID': 'OfferState_328856876', 'Action': 'statechange', 'org:resource': 'User_28', 'lifecycle:transition': 'complete'}}
    ]) # type: ignore