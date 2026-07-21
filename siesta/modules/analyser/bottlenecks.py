from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from typing import Optional, Union
import logging

from siesta.modules.analyser.durations import _resolve_group_cols, _apply_group_value_filter

logger = logging.getLogger(__name__)


def _compute_pair_stats(events_df: DataFrame, group_cols: list[str], total_groups: int,
                         end_time: Optional[str]) -> DataFrame:
    """Per-(source,target) duration statistics, generalized to arbitrary group_cols
    (instead of directly_follows.py's hardcoded trace_id) and extended with
    occurrence_count, needed for bottleneck impact scoring. Local to this module -
    directly_follows.py itself is left untouched.
    """
    w = Window.partitionBy(*group_cols).orderBy("position")
    pairs = (
        events_df
        .withColumn("target", F.lead("activity").over(w))
        .withColumn("next_start_timestamp", F.lead("start_timestamp").over(w))
        .filter(F.col("target").isNotNull())
        .withColumnRenamed("activity", "source")
    )

    if end_time is not None:
        pairs = pairs.withColumn(
            "duration_sec",
            F.col("attributes").getItem(end_time).cast("timestamp").cast("double") - F.col("start_timestamp").cast("double")
        )
    else:
        pairs = pairs.withColumn(
            "duration_sec",
            F.col("next_start_timestamp").cast("double") - F.col("start_timestamp").cast("double")
        )

    return pairs.groupBy("source", "target").agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.min("duration_sec").alias("min_duration_sec"),
        F.max("duration_sec").alias("max_duration_sec"),
        F.count(F.lit(1)).alias("occurrence_count"),
        (F.countDistinct(*group_cols) / F.lit(total_groups)).alias("support"),
    )


def compute_bottlenecks(
    events_df: DataFrame,
    trace_count: int,
    end_time: Optional[str] = None,
    grouping_key: Optional[Union[str, list]] = None,
    grouping_value: Optional[Union[str, list]] = None,
    zscore_threshold: float = 3.5,
    top_k: Optional[int] = None,
) -> DataFrame:
    """Surfaces activity pairs with anomalously high inter-event durations relative
    to the rest of the process, ranked by their contribution to overall cycle time.

    Args:
        events_df: Sequence table DataFrame (activity, trace_id, position,
                   start_timestamp, attributes).
        trace_count: Total number of distinct traces, used as the group population
                     size when grouping_key is None.
        end_time: Optional attributes-map key for event end timestamp. When
                  provided, duration = attributes[end_time] - start_timestamp.
                  When None, duration = next event's start - this event's start.
        grouping_key: Attribute key(s) to group by instead of trace_id. None
                      defaults to trace_id.
        grouping_value: Optional value(s) to restrict to matching groups.
        zscore_threshold: Robust (MAD-based) z-score threshold above which a pair's
                          avg_duration_sec is flagged as anomalous relative to the
                          rest of the process. Same formula as
                          attribute_deviations.py's numeric scoring.
        top_k: Keep only the k highest-impact pairs. None = keep all.

    Returns:
        Spark DataFrame with columns: source, target, avg_duration_sec,
        min_duration_sec, max_duration_sec, occurrence_count, support, zscore,
        impact_score, flagged. Ordered by impact_score desc. impact_score =
        avg_duration_sec * occurrence_count: a frequency-weighted measure that
        ranks a frequent-and-slow pair above a rare-but-slow one, since the
        former disproportionately inflates overall cycle time. grouping_key only
        scopes which events are considered "adjacent" (partitioned/ordered by the
        group instead of by trace_id) and what population "support" is measured
        against - it does not produce a per-group breakdown.
    """
    events_df, group_cols = _resolve_group_cols(events_df, grouping_key)

    if grouping_value is not None:
        events_df = _apply_group_value_filter(events_df, group_cols[0], grouping_value)

    if grouping_key is None and grouping_value is None:
        total_groups = trace_count
    else:
        total_groups = events_df.select(*group_cols).distinct().count()

    stats = _compute_pair_stats(events_df, group_cols, total_groups, end_time)
    stats.cache()

    # Robust (MAD-based) z-score of avg_duration_sec across the population of pairs.
    med_row = stats.agg(F.percentile_approx("avg_duration_sec", 0.5).alias("_med")).collect()[0]
    med = med_row["_med"]
    mad_row = (
        stats
        .withColumn("_ad", F.abs(F.col("avg_duration_sec") - F.lit(med)))
        .agg(F.percentile_approx("_ad", 0.5).alias("_mad"))
        .collect()[0]
    )
    mad = mad_row["_mad"]

    if mad is None or mad == 0:
        zscore_expr = F.lit(0.0)
    else:
        zscore_expr = F.abs(F.lit(0.6745) * (F.col("avg_duration_sec") - F.lit(med)) / F.lit(mad))

    result_df = (
        stats
        .withColumn("zscore", zscore_expr)
        .withColumn("impact_score", F.col("avg_duration_sec") * F.col("occurrence_count"))
        .withColumn("flagged", F.col("zscore") >= F.lit(zscore_threshold))
        .orderBy(F.col("impact_score").desc())
    )
    stats.unpersist()

    if top_k is not None:
        result_df = result_df.limit(top_k)

    return result_df
