from pyspark.sql import DataFrame, functions as F
from typing import Optional, Union
import logging

from siesta.modules.analyser.durations import _resolve_group_cols

logger = logging.getLogger(__name__)


def resolve_trace_labels(
    events_df: DataFrame,
    separating_key: Optional[Union[str, list]],
    separating_groups: list,
) -> DataFrame:
    """Label each trace 1/0 by whether any event's `separating_key` value falls in
    separating_groups[0] (the target set).

    `separating_key` may be a top-level Event column (e.g. "activity") or an
    attributes-map key (e.g. "decision") - resolved exactly like durations.py
    resolves grouping_key. None defaults to "activity" (the historical behavior
    of the comparator module, which always keyed off the activity column).

    Returns a DataFrame with columns (trace_id, label), label in {0, 1}.
    """
    key = separating_key if separating_key is not None else "activity"
    events_df, cols = _resolve_group_cols(events_df, key)
    label_col = cols[0]

    target_values = separating_groups[0] if separating_groups else []

    return (
        events_df
        .withColumn("label", F.when(F.col(label_col).isin(target_values), 1).otherwise(0))
        .groupBy("trace_id")
        .agg(F.max("label").alias("label"))
    )
