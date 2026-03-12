from typing import Any, Dict
from pyparsing import col
from pyspark.sql import DataFrame, functions as F
from siesta_framework.model.StorageModel import MetaData
from siesta_framework.core.storageFactory import get_storage_manager

def discover_existential(evolved_df, metadata: MetaData) -> DataFrame:
    """
    Existential constraints
    1. Existence(a,n): If activity a occurs, it must occur at least n times in the same trace.
    2. Absence(a,n): If activity a occurs, it must occur at most n-1 times in the same trace.
    3. Exactly(a,n): If activity a occurs, it must occur exactly n times in the same trace.
    
    This function updates the existing existential constraints related to the evolved traces, 
    by counting the occurrences of each activity in each trace, and creating new constraints if necessary.
    
    :param evolved_df: DataFrame of type DataModel.Event that contains all events with timestamps after the last mining timestamp.
    :param metadata: MetaData object containing information about the log and existing constraints.
    :return: DataFrame of type Constraint containing the updated existential constraints.
    """
    storage = get_storage_manager()

    # Cache immediately so the S3 read is materialized before we overwrite the same path
    old_constraints_df = storage.read_existential_constraints(metadata).cache()
    old_constraints_df.count()

    # Take the evolved events and re-evaluate the exactly existence on their traces
    new_constraints_df = evolved_df.groupBy("trace_id", "activity") \
        .agg(F.count("*").alias("occurrences")) \
        .withColumn("template", F.lit("exactly")) \
        .withColumn("source",F.col("activity")) \
        .withColumn("trace_id", F.col("trace_id")) \
        .select("template", "source", "trace_id", "occurrences")
    
    # Filter out old constraints that are contradicted by the new constraints 
    # (i.e. if an old constraint says "exactly(a,2)" but the new constraint 
    # for the same trace and activity says "occurrences=3", 
    # then we know that the old constraint is no longer valid for that trace)
    old_constraints_df = old_constraints_df.join(new_constraints_df, on=["trace_id", "source"], how="left_anti") \
        .select("template", "source", "trace_id", "occurrences")
    
    # We store only Exactly constraints, since Existence and Absence can be derived 
    # from them (Existence(a,n) is satisfied if there are no Exactly(a,m) constraints with m < n, 
    # and Absence(a,n) is satisfied if there are no Exactly(a,m) constraints with m >= n).
    exactly_df = old_constraints_df.unionByName(new_constraints_df)

    # Force materialization before writing
    exactly_df = exactly_df.cache()
    exactly_df.count()
    old_constraints_df.unpersist()

    storage.write_existential_constraints(metadata, exactly_df)

    # We now derive the remaining existential templates (Existence and Absence)
    # based on the Exactly constraints, by counting the occurrences of each activity 
    # in each trace and applying the definitions of Existence and Absence.
    existence_df = exactly_df.withColumn("template", F.lit("existence")) \
        .select("template", "source", "trace_id", "occurrences")
    absence_df = exactly_df.withColumn("template", F.lit("absence")) \
        .withColumn("occurrences", F.col("occurrences") + 1) \
        .select("template", "source", "trace_id", "occurrences")
    
    result = exactly_df.unionByName(existence_df).unionByName(absence_df)
    result = result.cache()
    result.count()
    exactly_df.unpersist()

    return result