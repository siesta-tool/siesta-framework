import time
from typing import Optional
from siesta.core.logger import timed
from siesta.core.sparkManager import get_spark_session
from siesta.core.storageFactory import get_storage_manager
from siesta.model.StorageModel import MetaData
from siesta.model.SystemModel import Query_Config, Pattern
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import col
from siesta.modules.Query.parse_seql import split_pattern_to_list
from siesta.modules.Query.processors.detection_query import detect

import logging
logger = logging.getLogger(__name__)


def explore(pattern_suffix: str, config: Query_Config, metadata: MetaData,  candidate_targets: Optional[list] = None) -> DataFrame:
    """
    Retrieves candidate target activities that follow the given pattern suffix and calculates their support.
    
    :param pattern_suffix: The last activity in the current pattern to find candidate targets for.
    :param config: The current query configuration containing the pattern to explore.
    :param metadata: The metadata containing information about the event log, including trace count.
    :param candidate_targets: Optional list of candidate target activities to filter the results.
    :return: A DataFrame with candidate target activities and their corresponding support values, sorted by support.
    """
    candidate_targets_df = get_storage_manager().read_count_table(metadata)\
        .filter(col("source") == pattern_suffix)
    if candidate_targets is not None:
        candidate_targets_df = candidate_targets_df.filter(col("target").isin(candidate_targets))

    spark = get_spark_session()

    target_rows = candidate_targets_df.select("target").distinct().collect()
    targets = [row["target"] for row in target_rows]
    if not targets:
        return spark.createDataFrame([], "next_activity string, support double")

    base_pattern = config.get("query", {}).get("pattern", "").strip()
    trace_count = metadata.trace_count if metadata.trace_count else 0
    if trace_count <= 0:
        return spark.createDataFrame([(t, 0.0) for t in targets], ["next_activity", "support"]) \
            .sort(col("support"), ascending=False)

    candidate_patterns = {target: f"{base_pattern} {target}".strip() for target in targets}

    propositions = []
    for target, pattern in candidate_patterns.items():
        logger.info(f"Exploring candidate target '{target}' with pattern '{pattern}'")
        support = len(detect(pattern, config, metadata))
        propositions.append((target, support))

    # Fix support by dividing by total trace count, and sort by support
    result_df = spark.createDataFrame(propositions, ["next_activity", "support"]) \
        .withColumn("support", col("support") / trace_count) \
        .sort(col("support"), ascending=False)

    return result_df


def accurate_exploration(config: Query_Config, metadata: MetaData) -> DataFrame:
    """
    Retrieves candidate target activities based on the last activity in the current pattern 
    and calculates their support using the detection query.

    :param config: The current query configuration containing the pattern to explore.
    :param metadata: The metadata containing information about the event log, including trace count.
    :return: A DataFrame with candidate target activities and their corresponding support values, sorted by support.
    """
    # Extract the current pattern from the config and determine the last activity to find candidate targets
    pattern_data = split_pattern_to_list(config.get("query", {}).get("pattern", ""))
    activities_pattern = list(map(lambda x: x.get("label"), pattern_data))
    pattern_suffix = activities_pattern[-1] if activities_pattern else ""
    
    # Retrieve candidate target activities that follow the last activity in the pattern and calculate their support
    return explore(pattern_suffix, config, metadata)


def fast_exploration(config: Query_Config, metadata: MetaData) -> DataFrame:
    """
    Retrieves candidate target activities based on the last activity in the current pattern 
    and estimates their support using pre-aggregated counts.

    :param config: The current query configuration containing the pattern to explore.
    :param metadata: The metadata containing information about the event log, including trace count.
    :return: A DataFrame with candidate target activities and their estimated support values, sorted by support.
    """
    spark = get_spark_session()
    storage = get_storage_manager()

    # Extract the current pattern from the config
    pattern_data = split_pattern_to_list(config.get("query", {}).get("alt_pattern", ""))
    activities_pattern = list(map(lambda x: x.get("label"), pattern_data))
    pattern_suffix = activities_pattern[-1] if activities_pattern else ""
    
    count_table_df = storage.read_count_table(metadata)
    
    # Find an upper bound of the total times the given pattern has been completed,
    # which is determined by the least frequent consecutive pair in the pattern 
    upper_bound = float('inf')
    
    if len(activities_pattern) > 1:
        consecutive_pairs_df = spark.createDataFrame(list(zip(activities_pattern, activities_pattern[1:])), ["source", "target"])
        upper_bound = count_table_df \
            .join(consecutive_pairs_df, on=["source", "target"], how="inner") \
            .agg(F.min("total_completions").alias("upper_bound")) \
            .collect()[0]["upper_bound"]

    # Find candidate target activities that follow the last activity in the pattern 
    # and calculate their support using the upper bound as a heuristic
    propositions_df = count_table_df.filter(col("source") == pattern_suffix) \
        .withColumn("support", F.min(col("total_completions"), F.lit(upper_bound))) \
        .select(col("target").alias("next_activity"), "support") \
        .sort(col("support"), ascending=False)
    
    return propositions_df


def hybrid_exploration(config: Query_Config, metadata: MetaData) -> DataFrame:
    # Extract the current pattern from the config
    pattern_data = split_pattern_to_list(config.get("query", {}).get("pattern", ""))
    activities_pattern = list(map(lambda x: x.get("label"), pattern_data))
    pattern_suffix = activities_pattern[-1] if activities_pattern else ""
    
    # Determine the exploration method based on the config (default to fast exploration (0))
    k = config.get("query", {}).get("explore_k", 0)

    fast_propositions = fast_exploration(config, metadata)
    if k == 0:
        return fast_propositions
    else:
        top_k = [row["next_activity"] for row in fast_propositions.limit(k).select(col("next_activity")).collect()]
        return explore(pattern_suffix, config, metadata, candidate_targets=top_k)
    

def process_exploration_query(config: Query_Config, metadata: MetaData) -> DataFrame:
    """
    Process an exploration query based on the specified method in the config.
    
    :param config: The query configuration containing the method and pattern to explore.
    :param metadata: The metadata containing information about the event log, including trace count.
    :return: A dict with the exploration results and execution time (sec).
    """
    mode = config.get("query", {}).get("explore_mode", "accurate")
    support_threshold = config.get("support_threshold", 0.0) 
    result = None

    start = time.time()

    if mode == "fast":
        result = fast_exploration(config, metadata).collect()
    elif mode == "accurate":
        result = accurate_exploration(config, metadata).collect()
    elif mode == "hybrid":
        result = hybrid_exploration(config, metadata).collect()
    else:
        raise ValueError(f"Invalid exploration mode: {mode}. Supported modes are 'fast', 'accurate', and 'hybrid'.")
    
    timedif = time.time() - start

    result = [row.asDict() for row in result if row["support"] >= support_threshold]

    logger.info(f"Exploration query processed in {timedif:.2f} seconds using mode '{mode}'.")

    return {"explored": result, "time": timedif}