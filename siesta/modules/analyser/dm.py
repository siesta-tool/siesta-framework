import json
from pyspark.sql import DataFrame, functions as F


def discover_rare_rules(
    ordered_constraints_df: DataFrame,
    trace_labels: DataFrame,
    trace_count: int,
    support_pct: float = 0.1,
) -> list:
    """
    Find constraints that are rare overall but appear in target traces.
    Returns a list of dicts: {source, target, template, trace_ids}.
    """
    support_df = ordered_constraints_df.groupBy("source", "target").agg(
        F.countDistinct("trace_id").alias("support")
    )

    low_support_threshold = support_pct * trace_count
    low_support_pairs = support_df.filter(F.col("support") <= low_support_threshold)

    low_support_label1 = (
        ordered_constraints_df
        .join(low_support_pairs.select("source", "target"), on=["source", "target"], how="inner")
        .join(trace_labels, on="trace_id", how="left")
        .fillna(0, subset=["label"])
        .filter(F.col("label") == 1)
    )

    result_df = low_support_label1.groupBy("source", "target", "template").agg(
        F.collect_set("trace_id").alias("trace_ids")
    )

    return [json.loads(r) for r in result_df.toJSON().collect()]


def discover_targeted_rules(
    ordered_constraints_df: DataFrame,
    trace_labels: DataFrame,
    target_label: int = 1,
    support_threshold: float = 0.8,
    filtering_support: float = 1.0,
) -> list:
    """
    Find constraints that predominantly belong to target traces and appear
    in at least support_threshold fraction of target traces.
    Returns a list of dicts: {source, target, template, trace_ids}.
    """
    target_trace_count = trace_labels.filter(F.col("label") == target_label).count()

    distinct_constraints = (
        ordered_constraints_df.select("source", "target", "trace_id").distinct()
        .join(trace_labels, on="trace_id", how="left")
        .fillna(0, subset=["label"])
    )

    ratio_df = distinct_constraints.groupBy("source", "target").agg(
        F.countDistinct("trace_id").alias("total_count"),
        F.countDistinct(F.when(F.col("label") == target_label, F.col("trace_id"))).alias("target_count")
    ).withColumn("target_ratio", F.col("target_count") / F.col("total_count"))

    ratio_filtered_pairs = ratio_df.filter(F.col("target_ratio") >= filtering_support)

    min_support = support_threshold * target_trace_count

    dominant_pairs = (
        distinct_constraints
        .join(ratio_filtered_pairs.select("source", "target"), on=["source", "target"], how="inner")
        .filter(F.col("label") == target_label)
        .groupBy("source", "target")
        .agg(F.countDistinct("trace_id").alias("target_support"))
        .filter(F.col("target_support") >= min_support)
    )

    result_df = (
        ordered_constraints_df
        .join(dominant_pairs.select("source", "target"), on=["source", "target"], how="inner")
        .join(trace_labels, on="trace_id", how="left")
        .fillna(0, subset=["label"])
        .select("source", "target", "template", "trace_id", F.col("label").cast("int"))
        .distinct()
        .groupBy("source", "target", "template")
        .agg(F.collect_set(F.struct(F.col("trace_id"), F.col("label"))).alias("trace_ids"))
    )

    return [json.loads(r) for r in result_df.toJSON().collect()]


def save_dm_results(result_list: list, output_path: str) -> None:
    with open(output_path, "w") as f:
        json.dump(result_list, f, indent=2)
