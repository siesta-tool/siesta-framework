import time
from typing import List
from pprint import pprint
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
import pandas as pd
from siesta_framework.core.logger import timed
from siesta_framework.core.sparkManager import get_spark_session
from siesta_framework.core.storageFactory import get_storage_manager
from siesta_framework.model.StorageModel import MetaData
from siesta_framework.model.SystemModel import Query_Config, Pattern
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from siesta_framework.modules.Query.parse_seql import BoundActivity, Quantifier, RespondedPair, expand_compact, extract_attribute_pairs, parse_pattern, extract_responded_pairs, extract_siesta_pairs #TODO Update api names
from siesta_framework.modules.Query.CEP_adapter import find_occurrences_dsl
from pyspark.sql.functions import broadcast
from functools import reduce
import logging
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

    true_pairs = list(set(list(map(lambda x: (x.source.label, x.target.label), query_pairs))))

    return true_pairs

MINE_TRACE_SCHEMA = StructType([
    StructField("trace_id", StringType(), nullable=False),
    StructField("positions", ArrayType(IntegerType()), nullable=False),
])

def mine_trace(inputs: pd.DataFrame, pattern: str) -> pd.DataFrame:
    
    trace_id = inputs["trace_id"].iloc[0]
    # return pd.DataFrame([{"trace_id": trace_id, "positions": inputs.shape}])
    # return pd.DataFrame([{"trace_id": trace_id, "positions": [0,1]}])
    # Extract source events and target events as uniform records
    source_events = inputs[["source", "source_position", "source_timestamp", "source_attributes"]].rename(columns={
        "source": "name",
        "source_position": "position",
        "source_timestamp": "timestamp",
        "source_attributes": "attributes"
    })

    target_events = inputs[["target", "target_position", "target_timestamp", "target_attributes"]].rename(columns={
        "target": "name",
        "target_position": "position",
        "target_timestamp": "timestamp",
        "target_attributes": "attributes"
    })

    # Union, deduplicate by position (all cols should be consistent for same position), sort
    events = (
        pd.concat([source_events, target_events])
        .drop_duplicates(subset=["position"])
        .sort_values("position", key=lambda col: col.astype(int))
        .to_dict(orient="records")  # -> [{"activity": ..., "position": ..., ...}, ...]
    )

    res = find_occurrences_dsl([event['name'] for event in events], pattern, events=events)

    return pd.DataFrame([{"trace_id": trace_id, "positions": res}])


def process_detection_query_testing(config: Query_Config, metadata: MetaData):
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

    attribute_pairs = extract_attribute_pairs(new_pattern)
    logger.info(f"Attribute pairs: {attribute_pairs}")
    all_pairs = list(attribute_pairs.union(set(true_pairs)))



    query_pairs_df = spark.createDataFrame(true_pairs, ["source", "target"])
    all_pairs_df = spark.createDataFrame(all_pairs, ["source", "target"])


    relevant_source = set([_[0] for _ in all_pairs])
    relevan_target = set([_[1] for _ in all_pairs])

    index_table = storage.read_pairs_index(metadata).filter(col("source").isin(relevant_source) | col("target").isin(relevan_target))


    pruned_trace_ids = (
        index_table
        .join(query_pairs_df, on=["source", "target"], how="inner")
        .groupBy("trace_id")
        .agg(F.count_distinct("source", "target").alias("pair_count"))
        # Only keep IDs that appeared in every single pair
        .filter(F.col("pair_count") == len(true_pairs))
        .select("trace_id")
        .distinct()
    )

    pair_positions_df = (
        index_table
        .join(pruned_trace_ids, on="trace_id", how="inner")
        .join(all_pairs_df, on=["source", "target"], how="left")
    )
    # pair_positions_df.show()

    logger.info(f"Parsing query took: {time.time() - start}")
    def mine_call_wrapper(inputs: pd.DataFrame) -> pd.DataFrame:
        return mine_trace(inputs, new_pattern)

    matches_df = pair_positions_df.groupBy("trace_id").applyInPandas(mine_call_wrapper, schema=MINE_TRACE_SCHEMA)


    return str(matches_df.collect())



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

    attribute_pairs = extract_attribute_pairs(new_pattern)
    logger.info(f"Attribute pairs: {attribute_pairs}")
    all_pairs = list(attribute_pairs.union(set(true_pairs)))



    query_pairs_df = spark.createDataFrame(true_pairs, ["source", "target", "branch_id"])
    all_pairs_df = spark.createDataFrame(all_pairs, ["source", "target", "branch_id"])

    branch_sizes_df = query_pairs_df.groupby("branch_id").count().withColumnRenamed("count", "num_pairs")

    relevant_source = set([_[0] for _ in all_pairs])
    relevan_target = set([_[1] for _ in all_pairs])

    index_table = storage.read_pairs_index(metadata).filter(col("source").isin(relevant_source) | col("target").isin(relevan_target))

    # Single join: tag all rows with branch info and pair membership
    # tagged_df = (
    #     index_table.filter(col("source").isin(relevant_source) | col("target").isin(relevan_target))
    #     .join(broadcast(all_pairs_df), on=["source", "target"], how="left")  # adds branch_id
    # )

    # # Pruning: only look at rows matching query pairs (non-null branch_id from true_pairs)
    # query_pairs_broadcast = broadcast(query_pairs_df)
    # pruned_trace_ids = (
    #     tagged_df
    #     .join(broadcast(query_pairs_df.select("source", "target")),  # no branch_id pulled in
    #         on=["source", "target"], how="inner")
    #     .select("trace_id", "branch_id", "source", "target")
    #     .distinct()
    #     .groupBy("trace_id", "branch_id")
    #     .agg(F.count("*").alias("pair_count"))
    #     .join(broadcast(branch_sizes_df), on="branch_id")
    #     .filter(F.col("pair_count") == F.col("num_pairs"))
    #     .select("trace_id")
    #     .distinct()
    # )

    # # Fetch positions: reuse tagged_df instead of re-joining index_table
    # pair_positions_df = tagged_df.join(broadcast(pruned_trace_ids), on="trace_id", how="inner").repartition("trace_id")
    # logger.info(f"Parsing query took: {time.time() - start}")
    # def mine_call_wrapper(inputs: pd.DataFrame) -> pd.DataFrame:
    #     return mine_trace(inputs, new_pattern)

    # matches_df = pair_positions_df.groupBy("trace_id").applyInPandas(mine_call_wrapper, schema=MINE_TRACE_SCHEMA)
    
    # return str(matches_df.collect())


    pruned_trace_ids = (
        index_table
        .join(query_pairs_df, on=["source", "target"], how="inner")
        .groupBy("trace_id", "branch_id")
        .agg(F.count_distinct("source", "target").alias("pair_count"))
        .join(branch_sizes_df, on="branch_id")
        # Only keep IDs that appeared in every single pair
        .filter(F.col("pair_count") == F.col("num_pairs"))
        .select("trace_id")
        .distinct()
    )

    pair_positions_df = (
        index_table
        .join(pruned_trace_ids, on="trace_id", how="inner")
        .join(all_pairs_df, on=["source", "target"], how="left")
    )
    # pair_positions_df.show()

    logger.info(f"Parsing query took: {time.time() - start}")
    def mine_call_wrapper(inputs: pd.DataFrame) -> pd.DataFrame:
        return mine_trace(inputs, new_pattern)

    matches_df = pair_positions_df.groupBy("trace_id").applyInPandas(mine_call_wrapper, schema=MINE_TRACE_SCHEMA)


    return str(matches_df.collect())
