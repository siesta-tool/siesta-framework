from typing import List
from pprint import pprint
from siesta_framework.core.logger import timed
from siesta_framework.core.sparkManager import get_spark_session
from siesta_framework.core.storageFactory import get_storage_manager
from siesta_framework.model.StorageModel import MetaData
from siesta_framework.model.SystemModel import Query_Config, Pattern
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from siesta_framework.modules.Query.parse_seql import BoundActivity, Quantifier, expand_compact, parse_pattern, extract_responded_pairs, extract_siesta_pairs #TODO Update api names
from siesta_framework.modules.Query.CEP_adapter import find_occurrences_dsl

from functools import reduce
import logging
logger = logging.getLogger("Query Processors")


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


def optimize_lf(query_pairs: list[tuple[str, str, int]], metadata:MetaData):
    """
    Input: List of pair labels + branch [(source, target, branch_id)]
    Output: LF optimized query (alr optimized)
    """

    #TODO: Compare w/sorting - probably worse/equal

    return query_pairs



def process_detection_query(config: Query_Config, metadata: MetaData):
    spark = get_spark_session()
    storage = get_storage_manager()

    # 1. Generating pairs from sequence (abc -> (a,b), (b,c), (c,d))
    # pairs = _extract_consecutive_pairs(config.get("query", {}).get("pattern", []))
    new_pattern = config.get("query", {}).get("alt_pattern", "")
    
    logger.info(new_pattern)
    
    # pair_branches = extract_siesta_pairs(new_pattern)
    pair_branches = set(extract_responded_pairs(new_pattern))
    
    #optimizer

    labels_list = optimize_lf(list(map(lambda x: (x.source.label, x.target.label, x.branch_id), pair_branches)), metadata)
 
    query_pairs_df = spark.createDataFrame(labels_list, ["source", "target", "branch_id"])

    branch_sizes_df = query_pairs_df.groupby("branch_id").count().withColumnRenamed("count", "num_pairs")

    index_table = storage.read_pairs_index(metadata)


    intersected_ids = (
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

    # print(intersected_ids.count())

    first_trace = intersected_ids.first()["trace_id"] # type: ignore
    print(first_trace)

    sequence_table = list(storage.read_sequence_table(metadata).where(col("trace_id").eqNullSafe(first_trace)).select("activity").toPandas()["activity"])
    print(sequence_table)

    logger.info(new_pattern)
    res = find_occurrences_dsl(sequence_table, new_pattern, events=[{"name": _, "user": "test"} for _ in sequence_table])
    print(res)


    return "test"