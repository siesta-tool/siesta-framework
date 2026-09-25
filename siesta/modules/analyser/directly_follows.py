from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from typing import Optional
import logging

logger = logging.getLogger(__name__)


def compute_directly_follows(
    events_df: DataFrame,
    trace_count: int,
    end_time: Optional[str] = None,
    support_threshold: Optional[float] = None,
    filter_out: bool = False,
    include_traces: bool = False,
) -> DataFrame:
    """Find directly-following activity pairs from a sequence table.

    Args:
        events_df: Spark DataFrame with columns: activity, trace_id, position,
                   start_timestamp, attributes (MapType).
        trace_count: Total number of distinct traces in the log, used to compute
                     support as a fraction in [0, 1].
        end_time: Optional key in the attributes map containing the event's end
                  timestamp (epoch seconds, same unit as start_timestamp). When
                  provided, duration = attributes[end_time] - start_timestamp
                  (actual activity duration in seconds). When None, duration =
                  next event's start_timestamp - start_timestamp (transition time
                  in seconds).
        support_threshold: Optional support fraction [0, 1] for filtering pairs.
        filter_out: When True, removes pairs whose support > threshold (keep rare).
                    When False (default), removes pairs whose support < threshold
                    (keep frequent).
        include_traces: When True, adds a trace_ids column listing all trace IDs
                        that contain each pair.

    Returns:
        Spark DataFrame with columns: source, target, support (fraction [0,1]),
        avg_duration_sec, min_duration_sec, max_duration_sec, and optionally
        trace_ids.
    """
    w = Window.partitionBy("trace_id").orderBy("position")

    pairs_df = (
        events_df
        .withColumn("target", F.lead("activity").over(w))
        .withColumn("next_start_timestamp", F.lead("start_timestamp").over(w))
        .filter(F.col("target").isNotNull())
        .withColumnRenamed("activity", "source")
    )

    if end_time is not None:
        # end_time attribute is stored as a datetime string (e.g. '2026-01-30 14:27:37');
        # cast through timestamp to epoch seconds before subtracting.
        pairs_df = pairs_df.withColumn(
            "duration_sec",
            F.col("attributes").getItem(end_time).cast("timestamp").cast("double") - F.col("start_timestamp").cast("double")
        )
    else:
        pairs_df = pairs_df.withColumn(
            "duration_sec",
            F.col("next_start_timestamp").cast("double") - F.col("start_timestamp").cast("double")
        )

    agg_exprs = [
        (F.countDistinct("trace_id") / trace_count).alias("support"),
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.min("duration_sec").alias("min_duration_sec"),
        F.max("duration_sec").alias("max_duration_sec"),
    ]

    if include_traces:
        agg_exprs.append(F.collect_set("trace_id").alias("trace_ids"))

    result_df = pairs_df.groupBy("source", "target").agg(*agg_exprs)

    if support_threshold is not None:
        if filter_out:
            result_df = result_df.filter(F.col("support") <= support_threshold)
        else:
            result_df = result_df.filter(F.col("support") >= support_threshold)

    return result_df
