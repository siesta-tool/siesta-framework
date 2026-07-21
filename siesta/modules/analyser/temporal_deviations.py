"""
Temporal deviation analysis - duration-based discriminating rules between groups
of traces.

Generalizes ngrams.py's discriminative statistics (balance / confidence / support
between a label-1 and label-0 group of traces) from "does this n-gram occur" to
"is the time gap between two activities below some threshold". For example: in a
bank scenario, this can reveal that applications where a manual review and a
credit reassessment occur within five minutes of each other are significantly
more likely to be rejected - a duration-conditioned pattern standard sequence-only
comparison (ngrams) cannot express.
"""
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from typing import Optional
import logging

from siesta.modules.analyser.directly_follows import compute_directly_follows

logger = logging.getLogger(__name__)

_DECILES = [i / 10 for i in range(1, 10)]


def _resolve_candidate_pairs(
    events_df: DataFrame,
    trace_count: int,
    activity_pairs: Optional[list],
    max_auto_pairs: int,
) -> list[tuple[str, str]]:
    """Explicit (source, target) pairs if given; otherwise auto-derive from the
    top max_auto_pairs most-frequent directly-follows pairs, to keep the cost of
    the self-join in _compute_gaps predictable regardless of alphabet size.
    """
    if activity_pairs:
        return [tuple(p) for p in activity_pairs]

    pairs_df = compute_directly_follows(
        events_df=events_df,
        trace_count=trace_count,
        end_time=None,
        support_threshold=None,
        filter_out=False,
        include_traces=False,
    )
    rows = (
        pairs_df.orderBy(F.col("support").desc())
        .limit(max_auto_pairs)
        .select("source", "target")
        .collect()
    )
    return [(r["source"], r["target"]) for r in rows]


def _compute_gaps(events_df: DataFrame, candidate_pairs: list[tuple[str, str]]) -> DataFrame:
    """Minimum time gap (seconds) between an occurrence of `source` and a LATER
    occurrence of `target` within the same trace, per candidate (source, target)
    pair. Not restricted to directly-following/adjacent activities - matches the
    "occur within N minutes of each other" framing, which has no adjacency
    requirement.

    Implemented as a self-join bounded by the (small, broadcast) candidate-pairs
    list, not O(alphabet^2). Known scaling caveat: a trace with many repeated
    occurrences of a candidate source/target activity produces local join fan-out
    bounded by (occurrences_of_source_in_trace * occurrences_of_target_in_trace)
    within that trace - acceptable for typical logs, not mitigated further here.

    Returns a DataFrame with columns (trace_id, source_act, target_act, gap_sec).
    """
    sources = sorted({p[0] for p in candidate_pairs})
    targets = sorted({p[1] for p in candidate_pairs})

    src_df = events_df.filter(F.col("activity").isin(sources)).select(
        "trace_id",
        F.col("activity").alias("source_act"),
        F.col("position").alias("source_pos"),
        F.col("start_timestamp").alias("source_ts"),
    )
    tgt_df = events_df.filter(F.col("activity").isin(targets)).select(
        "trace_id",
        F.col("activity").alias("target_act"),
        F.col("position").alias("target_pos"),
        F.col("start_timestamp").alias("target_ts"),
    )
    pairs_df = events_df.sparkSession.createDataFrame(
        [(s, t) for s, t in candidate_pairs], ["source_act", "target_act"]
    )

    joined = (
        src_df
        .join(F.broadcast(pairs_df), on="source_act")
        .join(tgt_df, on=["trace_id", "target_act"])
        .filter(F.col("target_pos") > F.col("source_pos"))
        .withColumn(
            "gap_sec",
            F.col("target_ts").cast("double") - F.col("source_ts").cast("double"),
        )
    )

    return joined.groupBy("trace_id", "source_act", "target_act").agg(
        F.min("gap_sec").alias("gap_sec")
    )


def compute_temporal_deviations(
    events_df: DataFrame,
    trace_labels: DataFrame,
    trace_count: int,
    activity_pairs: Optional[list] = None,
    max_auto_pairs: int = 50,
    min_group_size: int = 5,
    top_k_per_pair: Optional[int] = None,
) -> DataFrame:
    """Discovers duration-based discriminating rules of the form
    "gap(source -> target) <= threshold_sec" between two trace groups.

    Args:
        events_df: Sequence table DataFrame (activity, trace_id, position,
                   start_timestamp, attributes).
        trace_labels: DataFrame(trace_id, label) with label in {0, 1} - see
                      trace_labels.resolve_trace_labels.
        trace_count: Total number of distinct traces (used to auto-derive
                     candidate pairs when activity_pairs is not given).
        activity_pairs: Optional explicit list of [source, target] pairs to
                        check. None = auto-derive top max_auto_pairs frequent
                        pairs.
        max_auto_pairs: Upper bound on auto-derived candidate pairs.
        min_group_size: Minimum (count_1 + count_0) required to keep a
                        (pair, threshold) result - same role as
                        attribute_deviations.py's min_group_size.
        top_k_per_pair: Keep only the k best (by |balance|) thresholds per
                        (source, target) pair. None = keep all decile candidates.

    Returns:
        Spark DataFrame with columns: source, target, threshold_sec, count_1,
        count_0, balance, confidence_1, confidence_0, support_1, support_0,
        support, direction, rule. Ordered by |balance| desc. Statistics use the
        exact formulas from ngrams.py's discover_ngrams, with "n-gram present"
        replaced by "gap <= threshold":
          balance      = (count_1/total_1) - (count_0/total_0)        in [-1, 1]
          confidence_1 = count_1 / (count_1 + count_0)                 P(label_1 | gap<=t)
          confidence_0 = count_0 / (count_1 + count_0)                 P(label_0 | gap<=t)
          support_1    = count_1 / total_1
          support_0    = count_0 / total_0
          support      = (count_1 + count_0) / (total_1 + total_0)
    """
    candidate_pairs = _resolve_candidate_pairs(events_df, trace_count, activity_pairs, max_auto_pairs)
    if not candidate_pairs:
        return events_df.sparkSession.createDataFrame([], schema=(
            "source string, target string, threshold_sec double, count_1 long, count_0 long, "
            "balance double, confidence_1 double, confidence_0 double, support_1 double, "
            "support_0 double, support double, direction string, rule string"
        ))

    gaps = _compute_gaps(events_df, candidate_pairs).join(trace_labels, on="trace_id")

    label_counts = {r["label"]: r["total"] for r in trace_labels.groupBy("label").agg(F.count("trace_id").alias("total")).collect()}
    total_1 = label_counts.get(1, 0) or 1
    total_0 = label_counts.get(0, 0) or 1

    deciles = (
        gaps.groupBy("source_act", "target_act")
        .agg(F.percentile_approx("gap_sec", _DECILES).alias("thresholds"))
    )
    candidates = (
        deciles
        .withColumn("threshold_sec", F.explode("thresholds"))
        .select("source_act", "target_act", "threshold_sec")
        .distinct()
    )

    below = (
        gaps.join(F.broadcast(candidates), on=["source_act", "target_act"])
        .filter(F.col("gap_sec") <= F.col("threshold_sec"))
    )
    counts = below.groupBy("source_act", "target_act", "threshold_sec").agg(
        F.countDistinct(F.when(F.col("label") == 1, F.col("trace_id"))).alias("count_1"),
        F.countDistinct(F.when(F.col("label") == 0, F.col("trace_id"))).alias("count_0"),
    )

    result = (
        counts
        .withColumn("balance", F.col("count_1") / F.lit(total_1) - F.col("count_0") / F.lit(total_0))
        .withColumn("confidence_1", F.col("count_1") / (F.col("count_1") + F.col("count_0")))
        .withColumn("confidence_0", F.col("count_0") / (F.col("count_1") + F.col("count_0")))
        .withColumn("support_1", F.col("count_1") / F.lit(total_1))
        .withColumn("support_0", F.col("count_0") / F.lit(total_0))
        .withColumn("support", (F.col("count_1") + F.col("count_0")) / F.lit(total_1 + total_0))
        .withColumn(
            "direction",
            F.when(F.col("balance") > 0, F.lit("label_1"))
             .when(F.col("balance") < 0, F.lit("label_0"))
             .otherwise(F.lit("neutral")),
        )
        .filter((F.col("count_1") + F.col("count_0")) >= min_group_size)
        .withColumn(
            "rule",
            F.concat(
                F.lit("gap("), F.col("source_act"), F.lit(" -> "), F.col("target_act"),
                F.lit(") <= "), F.col("threshold_sec").cast("int"), F.lit("s"),
            ),
        )
        .withColumnRenamed("source_act", "source")
        .withColumnRenamed("target_act", "target")
        .orderBy(F.abs(F.col("balance")).desc())
    )

    if top_k_per_pair is not None:
        w = Window.partitionBy("source", "target").orderBy(F.abs(F.col("balance")).desc())
        result = (
            result
            .withColumn("_rank", F.row_number().over(w))
            .filter(F.col("_rank") <= top_k_per_pair)
            .drop("_rank")
        )

    return result
