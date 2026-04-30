from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from typing import Optional, Union
import logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _resolve_group_cols(
    events_df: DataFrame,
    grouping_key: Optional[Union[str, list]],
) -> tuple[DataFrame, list[str]]:
    """Resolve grouping column names, falling back to trace_id when key is None.

    Top-level Event columns (trace_id, activity, position, start_timestamp) are
    used directly. Any other key is extracted from the attributes MapType under a
    _grp_<key> alias.
    """
    if grouping_key is None:
        return events_df, ["trace_id"]

    keys = [grouping_key] if isinstance(grouping_key, str) else list(grouping_key)
    existing_cols = set(events_df.columns)
    group_cols = []
    for key in keys:
        if key in existing_cols:
            group_cols.append(key)           # top-level column, use directly
        else:
            col_name = f"_grp_{key}"
            events_df = events_df.withColumn(col_name, F.col("attributes").getItem(key))
            group_cols.append(col_name)
    return events_df, group_cols


def _apply_group_value_filter(
    events_df: DataFrame,
    group_col: str,
    grouping_value: Union[str, list],
) -> DataFrame:
    allowed = [grouping_value] if isinstance(grouping_value, str) else list(grouping_value)
    return events_df.filter(F.col(group_col).isin(allowed))


def _rename_grp_cols(df: DataFrame, group_cols: list[str]) -> DataFrame:
    """Strip the internal '_grp_' prefix from group column names in the output."""
    for gc in group_cols:
        if gc.startswith("_grp_"):
            df = df.withColumnRenamed(gc, gc[5:])
    return df


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

def compute_activity_durations(
    events_df: DataFrame,
    end_time: Optional[str] = None,
    grouping_key: Optional[Union[str, list]] = None,
    grouping_value: Optional[Union[str, list]] = None,
    per_group: bool = False,
) -> DataFrame:
    """Compute duration statistics per activity type, optionally broken down by group.

    Args:
        events_df: Sequence table DataFrame (activity, trace_id, position,
                   start_timestamp, attributes).
        end_time: Attribute key holding the event's end timestamp (epoch seconds).
                  When provided, duration = attributes[end_time] - start_timestamp
                  (actual activity duration). When None, duration =
                  next_event.start_timestamp - start_timestamp (transition time),
                  computed within each trace.
        grouping_key: Attribute key(s) that define a group. None = trace_id.
        grouping_value: When provided, only events belonging to matching groups
                        are included.
        per_group: When True, statistics are computed per (group, activity) pair
                   instead of globally per activity. Requires resolving group
                   columns regardless of grouping_value.

    Returns:
        Spark DataFrame with columns:
            [<group_col(s)>,] activity, avg_duration_sec, min_duration_sec,
            max_duration_sec, occurrence_count.
            Group columns are present only when per_group=True.
    """
    # Always resolve group columns - needed for per_group breakdown and/or filtering
    events_df, group_cols = _resolve_group_cols(events_df, grouping_key)

    if grouping_value is not None:
        events_df = _apply_group_value_filter(events_df, group_cols[0], grouping_value)

    if end_time is not None:
        # end_time attribute is stored as a datetime string; cast through timestamp to epoch seconds.
        events_df = events_df.withColumn(
            "duration_sec",
            F.col("attributes").getItem(end_time).cast("timestamp").cast("double") - F.col("start_timestamp").cast("double"),
        )
    else:
        # Transition time: next event's start minus this event's start, within each trace
        w = Window.partitionBy("trace_id").orderBy("position")
        events_df = (
            events_df
            .withColumn("_next_start_ts", F.lead("start_timestamp").over(w))
            .filter(F.col("_next_start_ts").isNotNull())
            .withColumn(
                "duration_sec",
                F.col("_next_start_ts").cast("double") - F.col("start_timestamp").cast("double"),
            )
        )

    agg_key = (group_cols if per_group else []) + ["activity"]

    result_df = (
        events_df
        .groupBy(*agg_key)
        .agg(
            F.avg("duration_sec").alias("avg_duration_sec"),
            F.min("duration_sec").alias("min_duration_sec"),
            F.max("duration_sec").alias("max_duration_sec"),
            F.count("*").alias("occurrence_count"),
        )
        .orderBy(*agg_key)
    )

    if per_group:
        result_df = _rename_grp_cols(result_df, group_cols)

    return result_df


def compute_group_durations(
    events_df: DataFrame,
    end_time: Optional[str] = None,
    grouping_key: Optional[Union[str, list]] = None,
    grouping_value: Optional[Union[str, list]] = None,
) -> DataFrame:
    """Compute duration per group instance (e.g. per trace).

    Args:
        events_df: Sequence table DataFrame.
        end_time: Attribute key holding the event's end timestamp (epoch seconds).
                  When provided, group duration = sum of per-event durations
                  (attributes[end_time] - start_timestamp) across all events in
                  the group. When None, group duration =
                  last_event.start_timestamp - first_event.start_timestamp.
        grouping_key: Attribute key(s) that define a group. None = trace_id.
        grouping_value: Optional value(s) to restrict the output to specific groups.

    Returns:
        Spark DataFrame with columns: <group_key(s)>, duration_sec -
        one row per group, ordered by the primary group column.
    """
    events_df, group_cols = _resolve_group_cols(events_df, grouping_key)

    if grouping_value is not None:
        events_df = _apply_group_value_filter(events_df, group_cols[0], grouping_value)

    if end_time is not None:
        # end_time attribute is stored as a datetime string; cast through timestamp to epoch seconds.
        events_df = events_df.withColumn(
            "_event_dur",
            F.col("attributes").getItem(end_time).cast("timestamp").cast("double") - F.col("start_timestamp").cast("double"),
        )
        result_df = events_df.groupBy(*group_cols).agg(
            F.sum("_event_dur").alias("duration_sec"),
        )
    else:
        # Span = start_timestamp of the last event minus start_timestamp of the first
        result_df = events_df.groupBy(*group_cols).agg(
            (
                F.max(F.col("start_timestamp").cast("double")) -
                F.min(F.col("start_timestamp").cast("double"))
            ).alias("duration_sec"),
        )

    result_df = _rename_grp_cols(result_df, group_cols)
    clean_primary = group_cols[0][5:] if group_cols[0].startswith("_grp_") else group_cols[0]
    return result_df.orderBy(clean_primary)
