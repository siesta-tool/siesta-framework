from typing import Any, Dict

from siesta.core.storageFactory import get_storage_manager
from siesta.model.StorageModel import MetaData
from siesta.model.MiningModel import Constraint
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, max as _max, lit


def discover_positional(evolved: DataFrame, metadata: MetaData) -> DataFrame:
    """
    Positional constraints are of two types:
    1. Init constraints: Activities that can only occur at the beginning of a trace.
    2. End constraints: Activities that can only occur at the end of a trace.

    This function updates the existing positional constraints related to the evolved traces, 
    by checking the first and last activities of each trace against the existing constraints,
    and creating new constraints if necessary.

    :param evolved: DataFrame of type DataModel.Event that contains all events with timestamps after the last mining timestamp.
    :param metadata: MetaData object containing information about the log and existing constraints.
    :return: DataFrame of type Constraint containing the updated positional constraints.
    """
    storage = get_storage_manager()

    # Get Init and End constraints that are related to unevolved traces (flat ConstraintEntry rows).
    old_valid = storage.read_positional_constraints(metadata, filter_out_df=evolved).cache()
    old_valid.count()

    # Init constraints: one flat row per (activity, trace_id) at position 1
    init_events = evolved.where(col("position") == 1)
    init_constraints = init_events.withColumn("template", lit("init")).select(
        col("template"),
        col("activity").alias("source"),
        col("trace_id")
    )

    # End constraints: one flat row per (activity, trace_id) at the highest position in each trace
    max_positions = evolved.groupBy("trace_id").agg(_max("position").alias("max_position"))
    end_events = evolved.join(max_positions, on="trace_id") \
                        .where(col("position") == col("max_position")) \
                        .drop("max_position")
    end_constraints = end_events.withColumn("template", lit("end")).select(
        col("template"),
        col("activity").alias("source"),
        col("trace_id")
    )

    # Combine new flat constraints with old valid ones
    new_constraints = init_constraints.unionByName(end_constraints)
    positional_constraints = old_valid.unionByName(new_constraints)

    # Force materialization before writing
    positional_constraints = positional_constraints.cache()
    positional_constraints.count()
    old_valid.unpersist()

    storage.write_positional_constraints(metadata, positional_constraints)
    
    return positional_constraints