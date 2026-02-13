from typing import Any, Dict

from siesta_framework.core.storageFactory import get_storage_manager
from siesta_framework.model.StorageModel import MetaData
from siesta_framework.model.MiningModel import Constraint
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, max as _max, array, lit, collect_set, size, count, sum as _sum
from pyspark.sql.window import Window


def discover_positional(evolved: DataFrame, metadata: MetaData) -> DataFrame:
    '''
    Positional constraints are of two types:
    1. Init constraints: Activities that can only occur at the beginning of a trace.
    2. End constraints: Activities that can only occur at the end of a trace.

    This function updates the existing positional constraints related to the evolved traces, 
    by checking the first and last activities of each trace against the existing constraints,
    and creating new constraints if necessary.
    '''
    storage = get_storage_manager()

    # Get Init and End constraints that are related to unevolved traces.
    old_valid = storage.read_positional_constraints(metadata, filter_out_df=evolved)

    # Create Init constraints for events with position 0
    init_events = evolved.where(col("position") == 0)
    init_constraints = init_events.groupBy("activity").agg(
        collect_set("trace_id").alias("trace_ids"),
        count("*").alias("source_count")
    ).withColumn("template", lit("Init")) \
     .withColumn("category", lit("Positional").cast("string")) \
     .withColumn("sources", array(col("activity"))) \
     .withColumn("targets", lit(None).cast("array<string>")) \
     .withColumn("occurrences", lit(None).cast("int")) \
     .withColumn("support", (size("trace_ids") / metadata.trace_count).cast("float")) \
     .drop("activity")
    
    # Create End constraints for events with the highest position in their trace
    # First find max position per trace
    max_positions = evolved.groupBy("trace_id").agg(_max("position").alias("max_position"))
    
    # Join with evolved to get events at max position
    end_events = evolved.join(max_positions, on="trace_id") \
                        .where(col("position") == col("max_position")) \
                        .drop("max_position")
    
    end_constraints = end_events.groupBy("activity").agg(
        collect_set("trace_id").alias("trace_ids"),
        count("*").alias("source_count")
    ).withColumn("template", lit("End")) \
     .withColumn("category", lit("Positional").cast("string")) \
     .withColumn("sources", array(col("activity"))) \
     .withColumn("targets", lit(None).cast("array<string>")) \
     .withColumn("occurrences", lit(None).cast("int")) \
     .withColumn("support", (size("trace_ids") / metadata.trace_count).cast("float")) \
     .drop("activity")
    
    # Calculate confidence for Init constraints
    init_window = Window.partitionBy("template")
    init_constraints = init_constraints.withColumn("total_count", _sum("source_count").over(init_window)) \
                                       .withColumn("confidence", (col("source_count") / col("total_count")).cast("float")) \
                                       .drop("source_count", "total_count") \
                                       .select("category", "template", "sources", "targets", "occurrences", "support", "confidence", "trace_ids")
    
    # Calculate confidence for End constraints
    end_window = Window.partitionBy("template")
    end_constraints = end_constraints.withColumn("total_count", _sum("source_count").over(end_window)) \
                                     .withColumn("confidence", (col("source_count") / col("total_count")).cast("float")) \
                                     .drop("source_count", "total_count") \
                                     .select("category", "template", "sources", "targets", "occurrences", "support", "confidence", "trace_ids")
    
    # Combine new constraints with old valid ones
    new_constraints = init_constraints.union(end_constraints)
    positional_constraints = old_valid.union(new_constraints)
    
    return positional_constraints