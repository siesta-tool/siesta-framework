from typing import Dict, List, Set
import pandas as pd
from pyspark.sql import DataFrame, functions as F
from siesta_framework.model.StorageModel import ConstraintEntry, MetaData
from siesta_framework.core.storageFactory import get_storage_manager

import logging
logger = logging.getLogger("Mining")


# ---------------------------------------------------------------------------
# Pandas UDF: mine all unordered templates for a single trace
# ---------------------------------------------------------------------------
def _mine_trace_unordered_pandas(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Receives all activities of ONE trace (columns: trace_id, activity, all_activities_str).
    The 'all_activities_str' column contains comma-separated global activities (broadcast).
    Returns a DataFrame with columns (template, source, trace_id, target, occurrences).
    """
    if pdf.empty:
        return pd.DataFrame(columns=["template", "source", "trace_id", "target", "occurrences"])
    
    trace_id = pdf["trace_id"].iloc[0]
    trace_activities: Set[str] = set(pdf["activity"].unique())
    
    # Get global activities from the broadcast column
    all_activities_str = pdf["all_activities_str"].iloc[0]
    all_activities: List[str] = sorted(all_activities_str.split(",")) if all_activities_str else []
    
    rows: List = []
    
    # Iterate over all unique ordered pairs (a, b) where a < b
    for i, a in enumerate(all_activities):
        a_in = a in trace_activities
        for b in all_activities[i + 1:]:
            b_in = b in trace_activities
            
            # Skip pairs where neither activity is present (no constraint applies)
            if not a_in and not b_in:
                continue
            
            # Co-existence: both a and b are present in the trace
            if a_in and b_in:
                rows.append(("coexistence", a, trace_id, b, None))
            
          
            # Choice: at least one of a or b is present
            if a_in or b_in:
                rows.append(("choice", a, trace_id, b, None))
            
            # Exclusive Choice: exactly one of a or b is present
            if a_in != b_in:
                rows.append(("exclusive_choice", a, trace_id, b, None))
    
    return pd.DataFrame(rows, columns=["template", "source", "trace_id", "target", "occurrences"])


def discover_unordered(evolved_df: DataFrame, metadata: MetaData) -> DataFrame:
    """
    Mine unordered constraints (symmetric relations between activity pairs 
    that don't depend on ordering within a trace).

    Templates discovered (per activity-pair, per trace):
      1. Co-existence(a,b)          both 'a' and 'b' occur in the same trace.
      2. Choice(a,b)                at least one of 'a' or 'b' occurs in the trace.
      3. Exclusive Choice(a,b)      exactly one of 'a' or 'b' occurs in the trace.
      
    Incremental strategy:
    - Old constraints for traces that appear in ``evolved_df`` are invalidated.
    - For evolved traces, the complete activity set is reconstructed from 
      SingleTable (historical) + evolved_df (new events).
    - Constraints are re-mined from scratch for these traces using the complete activity set.
    - Untouched traces keep their existing constraints.

    :param evolved_df: DataFrame[Event] with events after the last mining timestamp.
    :param metadata:   MetaData with storage paths and log context.
    :return: DataFrame with columns (template, source, target, trace_id, occurrences).
    """
    
    storage = get_storage_manager()

    # Load existing constraints and materialize before potential overwrite
    old_constraints_df = storage.read_unordered_constraints(metadata).cache()
    old_constraints_df.count()

    # Identify evolved traces and keep unchanged constraints
    evolved_trace_ids = evolved_df.select("trace_id").distinct()
    unchanged_constraints = old_constraints_df.join(
        evolved_trace_ids, on="trace_id", how="left_anti"
    )

    # Build complete activity set per evolved trace
    single_table = storage.read_activity_index_table(metadata)
    
    # Complete activity set for evolved traces
    complete_activities = single_table.join(
        evolved_trace_ids, on="trace_id", how="inner"
    ).select("trace_id", "activity").distinct()

    # Compute global activity set for generating all pairs
    global_activities = single_table.select("activity").distinct()
    
    # Collect global activities to a comma-separated string for broadcasting
    global_activities_list = [row.activity for row in global_activities.collect()]     # TODO: if global activity set is very large, this could be a bottleneck. In that case, consider alternative strategies (e.g., partitioning by activity subsets).
    global_activities_str = ",".join(sorted(global_activities_list))
    
    # Mine constraints for evolved traces
    if evolved_trace_ids.count() == 0:
        # No evolved traces, return unchanged constraints
        unordered_constraints = unchanged_constraints
    else:
        # Add broadcast column with global activities
        complete_activities_with_global = complete_activities.withColumn(
            "all_activities_str", F.lit(global_activities_str)
        )
        
        # Apply UDF per trace
        new_constraints = complete_activities_with_global \
            .groupBy("trace_id") \
            .applyInPandas(_mine_trace_unordered_pandas, ConstraintEntry.get_schema())
        
        # Union unchanged + new constraints
        unordered_constraints = unchanged_constraints.unionByName(
            new_constraints, allowMissingColumns=True
        )

    # Materialize and persist
    unordered_constraints = unordered_constraints.cache()
    unordered_constraints.count()
    old_constraints_df.unpersist()

    storage.write_unordered_constraints(metadata, unordered_constraints)

    return unordered_constraints
