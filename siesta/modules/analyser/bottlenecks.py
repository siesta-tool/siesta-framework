from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from typing import Optional, Union
import logging

from siesta.modules.analyser.durations import _resolve_group_cols, _apply_group_value_filter
from siesta.modules.analyser.duration_format import add_human_duration_columns

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
        F.percentile_approx("duration_sec", 0.5).alias("median_duration_sec"),
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
    support_threshold: float = 0.0,
    sigma_multiplier: float = 3.0,
    order_by: str = "impact_score",
) -> DataFrame:
    """Surfaces activity-pair "regions" that are both common among traces and
    take unusually long relative to other common regions.

    That definition has two clauses, each with its own knob:
      1. "common among traces"          -> support_threshold (default 0.0, a
                                            no-op: every pair with >=1
                                            occurrence already has support > 0)
      2. "unusually...than other common
          regions"                      -> a pair is flagged `is_bottleneck`
                                            when its avg_duration_sec exceeds
                                            mean + sigma_multiplier * stddev,
                                            computed over the pairs that passed
                                            clause 1 (so the baseline is "other
                                            common regions", not the full,
                                            possibly rare-pair-polluted population)

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
                          avg_duration_sec is flagged (`flagged`) as anomalous
                          relative to the rest of the process (ALL pairs, not just
                          the support_threshold-filtered "common" ones). Same
                          formula as attribute_deviations.py's numeric scoring.
                          Independent of support_threshold/sigma_multiplier/
                          is_bottleneck below - kept for backward compatibility.
        top_k: Keep only the k pairs ranked first by `order_by`. None = keep all.
        support_threshold: Minimum fraction of groups a pair must occur in to
                           count as "common"; pairs below this are dropped
                           entirely (clause 1). Default 0.0 keeps every pair.
        sigma_multiplier: How many standard deviations above the mean
                          avg_duration_sec (over the clause-1 population)
                          a pair must exceed to be flagged `is_bottleneck`
                          (clause 2). Default 3.0.
        order_by: Which column to rank/truncate by: "impact_score" (default,
                  avg_duration_sec * occurrence_count - a frequency-weighted
                  measure so a frequent-and-slow pair ranks above a rare-but-
                  slower one), "avg_duration_sec", or "median_duration_sec".

    Returns:
        Spark DataFrame with columns: source, target, avg_duration_sec,
        median_duration_sec, min_duration_sec, max_duration_sec,
        occurrence_count, support, zscore, impact_score, flagged,
        bottleneck_cutoff_sec, is_bottleneck. Every *_sec column is followed by
        a human-readable *_human companion (e.g. "2.5h"); see
        duration_format.add_human_duration_columns. Ordered by `order_by` desc, after
        dropping pairs below support_threshold. grouping_key only scopes which
        events are considered "adjacent" (partitioned/ordered by the group
        instead of by trace_id) and what population "support" is measured
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

    # Robust (MAD-based) z-score of avg_duration_sec across ALL pairs - unchanged
    # from the original implementation, independent of support_threshold below.
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

    scored = (
        stats
        .withColumn("zscore", zscore_expr)
        .withColumn("impact_score", F.col("avg_duration_sec") * F.col("occurrence_count"))
        .withColumn("flagged", F.col("zscore") >= F.lit(zscore_threshold))
    )
    stats.unpersist()

    # Clause 1: keep only "common" pairs.
    common = scored.filter(F.col("support") >= F.lit(support_threshold))
    common.cache()

    # Clause 2: mean + sigma_multiplier*stddev cutoff, over the common population.
    agg_row = common.agg(
        F.avg("avg_duration_sec").alias("_mean"),
        F.stddev("avg_duration_sec").alias("_std"),
    ).collect()[0]
    mean_dur = float(agg_row["_mean"] or 0.0)
    std_dur = float(agg_row["_std"] or 0.0)
    cutoff = mean_dur + sigma_multiplier * std_dur

    result_df = (
        common
        .withColumn("bottleneck_cutoff_sec", F.lit(cutoff))
        .withColumn("is_bottleneck", F.col("avg_duration_sec") > F.lit(cutoff))
    )
    common.unpersist()

    # Attach human-readable companions (avg_duration_human, ...,
    # bottleneck_cutoff_human) beside every *_sec column before ordering.
    result_df = add_human_duration_columns(result_df)

    order_col = order_by if order_by in ("impact_score", "avg_duration_sec", "median_duration_sec") else "impact_score"
    result_df = result_df.orderBy(F.col(order_col).desc())

    if top_k is not None:
        result_df = result_df.limit(top_k)

    return result_df
