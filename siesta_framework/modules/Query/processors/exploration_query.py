from typing import Optional
from siesta_framework.core.logger import timed
from siesta_framework.core.sparkManager import get_spark_session
from siesta_framework.core.storageFactory import get_storage_manager
from siesta_framework.model.StorageModel import MetaData
from siesta_framework.model.SystemModel import Query_Config, Pattern
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import col, array
from siesta_framework.modules.Query.parse_seql import split_pattern_to_list
from siesta_framework.modules.Query.processors.detection_query import process_detection_query
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType 

import logging
logger = logging.getLogger(__name__)


@udf(returnType=StringType())
def support_udf(target, config: Query_Config, metadata: MetaData) -> float:
    """
    UDF to calculate the support of a candidate target activity given the current pattern.
    It updates the pattern in the config with the candidate target and calls the detection query 
    to get the number of occurrences of the new pattern. The support is then calculated as 
    the ratio of the new pattern occurrences to the total number of traces in the metadata.

    :param target: The candidate target activity to evaluate.
    :param config: The current query configuration containing the pattern.
    :param metadata: The metadata containing information about the event log, including trace count.
    :return: The support value for the candidate target activity.
    """
    config["query"]["pattern"] = config.get("query", {}).get("pattern", "").strip() + " " + target
    return len(process_detection_query(config, metadata)["occurrences"]) / metadata.trace_count


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

    return candidate_targets_df.withColumn('support', support_udf('target', config, metadata)) \
            .select(col("target").alias("next_activity"), "support") \
            .sort(col("support"), ascending=False)


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
    pattern_data = split_pattern_to_list(config.get("query", {}).get("alt_pattern", ""))
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
    :return: A DataFrame with candidate target activities and their corresponding support values, sorted by support.
    """
    mode = config.get("query", {}).get("explore_mode", "fast")
    if mode == "fast":
        return fast_exploration(config, metadata)
    elif mode == "accurate":
        return accurate_exploration(config, metadata)
    elif mode == "hybrid":
        return hybrid_exploration(config, metadata)
    else:
        raise ValueError(f"Invalid exploration mode: {mode}. Supported modes are 'fast', 'accurate', and 'hybrid'.")