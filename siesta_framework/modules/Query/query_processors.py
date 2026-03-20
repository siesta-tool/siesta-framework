from siesta_framework.core.logger import timed
from siesta_framework.core.sparkManager import get_spark_session
from siesta_framework.core.storageFactory import get_storage_manager
from siesta_framework.model.StorageModel import MetaData
from siesta_framework.model.SystemModel import Query_Config, Pattern
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from siesta_framework.modules.Query.parse_seql import expand_compact, parse_pattern, extract_responded_pairs #TODO Update api names

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


def process_detection_query(config: Query_Config, metadata: MetaData):
    spark = get_spark_session()
    storage = get_storage_manager()

    # 1. Generating pairs from sequence (abc -> (a,b), (b,c), (c,d))
    # pairs = _extract_consecutive_pairs(config.get("query", {}).get("pattern", []))
    new_pattern = config.get("query", {}).get("alt_pattern", "")
    
    pair_branches = extract_responded_pairs(new_pattern)
    print(pair_branches)

    branch_sizes_df = spark.createDataFrame(
        [(branch[0].branch_id, len(branch)) for branch in pair_branches],
        ["branch_id", "num_pairs"]
    )

    pairs = [pair for branch in pair_branches for pair in branch]

    num_pairs = len(pairs)
    pairs_df = spark.createDataFrame(list(map(lambda pair: (pair.source.label, pair.target.label, pair.branch_id), pairs)), ["source", "target", "branch_id"])
    index_table = storage.read_pairs_index(metadata)
    intersected_ids = (
        index_table
        .join(pairs_df, on=["source", "target"], how="inner")
        .groupBy("trace_id", "branch_id")
        .agg(F.count_distinct("source", "target").alias("pair_count"))
        .join(branch_sizes_df, on="branch_id")
        # Only keep IDs that appeared in every single pair
        .filter(F.col("pair_count") == F.col("num_pairs"))
        .select("trace_id")
        .distinct()
    )

    print(intersected_ids.count())
    
    return "result"

    # 2. TODO: Optimizer



    # 3. Find trace_ids common on all pairs
    # inner-join -> count -> filter for 100% support
    num_pairs = len(branches)
    pairs_df = spark.createDataFrame(branches, ["source", "target"])
    index_table = storage.read_pairs_index(metadata)
    intersected_ids = (
        index_table
        .join(pairs_df, on=["source", "target"], how="inner")
        .groupBy("trace_id")
        .agg(F.count_distinct("source", "target").alias("pair_count"))
        # Only keep IDs that appeared in every single pair
        .filter(F.col("pair_count") == num_pairs)
        .select("trace_id")
    )

    count = intersected_ids.count()
    print(count)
    return str(count)
    
