from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StringType
import os
import csv
import json

import logging
logger = logging.getLogger(__name__)


def discover_ngrams(events: DataFrame, target_activities: list[str], n: int = 2) -> DataFrame:
    """
    Discover consecutive activity n-tuples that discriminate between
    targeted traces (label=1) and non-targeted traces (label=0).

    Parameters
    ----------
    events : pyspark.sql.DataFrame
        Must contain columns: "activity", "trace_id", "start_timestamp".
    target_activities : list[str]
        If any of these activities appears in a trace, that trace is labeled 1 (targeted).
        Traces with none of them are labeled 0 (non-targeted).
    n : int, default 2
        Length of the consecutive activity tuples to extract.

    Returns
    -------
    pyspark.sql.DataFrame with columns:
        ngram          : str   — human-readable, e.g. "A -> B -> C"
        count_1        : int   — distinct label-1 traces containing this n-gram
        count_0        : int   — distinct label-0 traces containing this n-gram
        balance        : float — in [-1, 1]
                                 (count_1/total_1) - (count_0/total_0)
                                 +1 → n-gram appears in ALL label-1 and NO label-0 traces
                                 -1 → n-gram appears in ALL label-0 and NO label-1 traces
                                  0 → equal relative coverage in both groups
        confidence_1     : float — in [0, 1]
                                 count_1 / (count_1 + count_0)
                                  1 → every trace containing this n-gram is label-1
                                  0 → every trace containing this n-gram is label-0
                                0.5 → n-gram is equally split between both groups
        confidence_0   : float — in [0, 1]
                                 count_0 / total_0
                                 Normalised recall for label-0: what fraction of all
                                 label-0 traces contain this n-gram.
                                  1 → n-gram appears in every label-0 trace
                                  0 → n-gram never appears in label-0 traces
        support        : float — in [0, 1]
                                 (count_1 + count_0) / (total_1 + total_0)
                                 Frequency of the n-gram across ALL traces, regardless
                                 of label.  High support = common pattern.
        direction      : str   — "label_1" if balance > 0,
                                 "label_0" if balance < 0,
                                 "neutral"  if balance == 0
    """

    # ------------------------------------------------------------------ #
    # Step 1 – label each trace (1 if any target activity present, else 0)
    # ------------------------------------------------------------------ #
    trace_labels = (
        events
        .withColumn("label", F.when(F.col("activity").isin(target_activities), 1).otherwise(0))
        .groupBy("trace_id")
        .agg(F.max("label").alias("label"))
    )

    # Total traces per label — needed to normalize balance, confidence_0, and support
    label_counts = (
        trace_labels
        .groupBy("label")
        .agg(F.count("trace_id").alias("total"))
    )
    total_1_row = label_counts.filter(F.col("label") == 1).first()
    total_0_row = label_counts.filter(F.col("label") == 0).first()
    total_1 = total_1_row["total"] if total_1_row else 1   # avoid division by zero
    total_0 = total_0_row["total"] if total_0_row else 1
    total_all = total_1 + total_0

    # ------------------------------------------------------------------ #
    # Step 2 – collect ordered activity sequences per trace
    # ------------------------------------------------------------------ #
    window_spec = Window.partitionBy("trace_id").orderBy("start_timestamp")

    activity_sequences = (
        events
        .withColumn("rank", F.row_number().over(window_spec))
        .withColumn(
            "activity",
            F.concat_ws(" ", F.col("activity"), F.col("attributes").getItem("DISPLAY_NAME"))
        )
        .groupBy("trace_id")
        .agg(
            F.collect_list(
                F.struct(F.col("rank"), F.col("activity"))
            ).alias("activity_structs")
        )
        .withColumn(
            "activities_ordered",
            F.transform(
                F.array_sort(
                    F.col("activity_structs"),
                    lambda x, y:
                        F.when(x["rank"] < y["rank"], F.lit(-1))
                         .when(x["rank"] > y["rank"], F.lit(1))
                         .otherwise(F.lit(0))
                ),
                lambda x: x["activity"]
            )
        )
        .select("trace_id", "activities_ordered")
        .join(trace_labels, on="trace_id", how="inner")
    )

    # ------------------------------------------------------------------ #
    # Step 3 – extract n-grams
    # ------------------------------------------------------------------ #
    ngram_schema = ArrayType(ArrayType(StringType()))

    @F.udf(returnType=ngram_schema)
    def extract_ngrams_udf(activities: list, size: int) -> list:
        if activities is None or len(activities) < size:
            return []
        return [activities[i: i + size] for i in range(len(activities) - size + 1)]

    ngrams_df = (
        activity_sequences
        .withColumn("ngrams", extract_ngrams_udf(F.col("activities_ordered"), F.lit(n)))
        .withColumn("ngram", F.explode("ngrams"))
        .withColumn("ngram_str", F.concat_ws("|||", F.col("ngram")))
        .select("trace_id", "label", "ngram_str")
        .distinct()   # one row per (trace, ngram) - counts traces, not occurrences
    )

    # ------------------------------------------------------------------ #
    # Step 4 – compute balance, confidence_1, confidence_0, and support
    # ------------------------------------------------------------------ #
    balance_df = (
        ngrams_df
        .groupBy("ngram_str")
        .agg(
            F.sum(F.when(F.col("label") == 1, 1).otherwise(0)).alias("count_1"),
            F.sum(F.when(F.col("label") == 0, 1).otherwise(0)).alias("count_0"),
        )
        # balance: normalised coverage difference
        #   (count_1 / total_1) - (count_0 / total_0)  →  [-1, 1]
        #   +1 = in ALL label-1 traces  and  NO  label-0 traces
        #   -1 = in ALL label-0 traces  and  NO  label-1 traces
        .withColumn(
            "balance",
            (F.col("count_1") / F.lit(total_1)) - (F.col("count_0") / F.lit(total_0))
        )
        # confidence_1: purity toward label-1
        #   count_1 / (count_1 + count_0)  →  [0, 1]
        #   1.0 = every trace containing this n-gram is label-1 (regardless of coverage)
        #   0.0 = every trace containing this n-gram is label-0
        .withColumn(
            "confidence_1",
            F.col("count_1") / (F.col("count_1") + F.col("count_0"))
        )
        # confidence_0: normalised recall for label-0
        #   count_0 / total_0  →  [0, 1]
        #   1.0 = n-gram appears in every label-0 trace
        #   0.0 = n-gram never appears in any label-0 trace
        .withColumn(
            "confidence_0",
            F.col("count_0") / F.lit(total_0)
        )
        # support: overall frequency across ALL traces, regardless of label
        #   (count_1 + count_0) / (total_1 + total_0)  →  [0, 1]
        #   1.0 = n-gram appears in every trace
        #   0.0 = n-gram appears in no trace
        .withColumn(
            "support",
            (F.col("count_1") + F.col("count_0")) / F.lit(total_all)
        )
        .withColumn("ngram", F.regexp_replace(F.col("ngram_str"), r"\|\|\|", " -> "))
        .withColumn(
            "direction",
            F.when(F.col("balance") > 0, F.lit("label_1"))
             .when(F.col("balance") < 0, F.lit("label_0"))
             .otherwise(F.lit("neutral"))
        )
        .select(
            "ngram", "count_1", "count_0",
            "balance", "confidence_1", "confidence_0", "support",
            "direction", "ngram_str",
        )
        .orderBy(F.abs(F.col("balance")).desc())
    )

    return balance_df


def save_ngram_results(
    balance_df: DataFrame,
    output_path: str,
    fmt: str = "csv",
    direction: str = None,
    balance_threshold: float = None,
    balance_filter: str = None,
    confidence_1_threshold: float = None,
    confidence_1_filter: str = None,        # "gt", "lt", "gte", "lte"
    confidence_0_threshold: float = None,
    confidence_0_filter: str = None,      # "gt", "lt", "gte", "lte"
    support_threshold: float = None,
    support_filter: str = None,           # "gt", "lt", "gte", "lte"
) -> None:
    """
    Streams results from Spark to the driver's local filesystem
    partition-by-partition using toLocalIterator().

    Parameters
    ----------
    balance_df : DataFrame
        Direct output of discover_ngrams.
    output_path : str
        Local path on the driver (e.g. "/home/user/results/ngrams.csv").
    fmt : str
        "csv" or "json".
    direction : str, optional
        Filter rows by direction. One of: "label_1", "label_0", "neutral".
    balance_threshold : float, optional
        Value to filter balance against. Required if balance_filter is set.
    balance_filter : str, optional
        Comparison operator for balance filtering. One of:
            "gt"     → balance >  threshold
            "lt"     → balance <  threshold
            "gte"    → balance >= threshold
            "lte"    → balance <= threshold
            "abs_gt" → |balance| >  threshold
            "abs_lt" → |balance| <  threshold
    confidence_1_threshold : float, optional
        Value to filter confidence_1 (label-1 purity) against.
        Required if confidence_1_filter is set.
    confidence_1_filter : str, optional
        Comparison operator for confidence_1 filtering. One of: "gt", "lt", "gte", "lte".
        Example: confidence_1_threshold=0.8, confidence_1_filter="gt"
            → only n-grams where 80%+ of containing traces are label-1.
    confidence_0_threshold : float, optional
        Value to filter confidence_0 (label-0 recall) against.
        Required if confidence_0_filter is set.
    confidence_0_filter : str, optional
        Comparison operator for confidence_0 filtering. One of: "gt", "lt", "gte", "lte".
        Example: confidence_0_threshold=0.5, confidence_0_filter="gt"
            → only n-grams present in more than 50% of label-0 traces.
    support_threshold : float, optional
        Value to filter support (overall frequency) against.
        Required if support_filter is set.
    support_filter : str, optional
        Comparison operator for support filtering. One of: "gt", "lt", "gte", "lte".
        Example: support_threshold=0.1, support_filter="gt"
            → only n-grams that appear in more than 10% of all traces.
    """
    fmt = fmt.lower()
    if fmt not in ("csv", "json"):
        raise ValueError(f"Unsupported format '{fmt}'. Choose 'csv' or 'json'.")

    valid_ops        = ("gt", "lt", "gte", "lte", "abs_gt", "abs_lt")
    valid_ops_simple = ("gt", "lt", "gte", "lte")

    if balance_filter is not None and balance_threshold is None:
        raise ValueError("balance_threshold must be set when balance_filter is provided.")
    if balance_filter is not None and balance_filter not in valid_ops:
        raise ValueError(f"Invalid balance_filter '{balance_filter}'. Choose from: {valid_ops}.")

    if confidence_1_filter is not None and confidence_1_threshold is None:
        raise ValueError("confidence_1_threshold must be set when confidence_1_filter is provided.")
    if confidence_1_filter is not None and confidence_1_filter not in valid_ops_simple:
        raise ValueError(f"Invalid confidence_1_filter '{confidence_1_filter}'. Choose from: {valid_ops_simple}.")

    if confidence_0_filter is not None and confidence_0_threshold is None:
        raise ValueError("confidence_0_threshold must be set when confidence_0_filter is provided.")
    if confidence_0_filter is not None and confidence_0_filter not in valid_ops_simple:
        raise ValueError(f"Invalid confidence_0_filter '{confidence_0_filter}'. Choose from: {valid_ops_simple}.")

    if support_filter is not None and support_threshold is None:
        raise ValueError("support_threshold must be set when support_filter is provided.")
    if support_filter is not None and support_filter not in valid_ops_simple:
        raise ValueError(f"Invalid support_filter '{support_filter}'. Choose from: {valid_ops_simple}.")

    if direction is not None and direction not in ("label_1", "label_0", "neutral"):
        raise ValueError(f"Invalid direction '{direction}'. Choose from: 'label_1', 'label_0', 'neutral'.")

    # ------------------------------------------------------------------ #
    # Apply filters — pushed into the DAG, executed on cluster
    # ------------------------------------------------------------------ #
    fields = ["ngram", "count_1", "count_0", "balance", "confidence_1", "confidence_0", "support", "direction"]
    write_df = balance_df.select(*fields)

    if direction is not None:
        write_df = write_df.filter(F.col("direction") == direction)

    if balance_filter is not None:
        balance_ops = {
            "gt":     F.col("balance")        >  balance_threshold,
            "lt":     F.col("balance")        <  balance_threshold,
            "gte":    F.col("balance")        >= balance_threshold,
            "lte":    F.col("balance")        <= balance_threshold,
            "abs_gt": F.abs(F.col("balance")) >  balance_threshold,
            "abs_lt": F.abs(F.col("balance")) <  balance_threshold,
        }
        write_df = write_df.filter(balance_ops[balance_filter])

    if confidence_1_filter is not None:
        confidence_1_ops = {
            "gt":  F.col("confidence_1") >  confidence_1_threshold,
            "lt":  F.col("confidence_1") <  confidence_1_threshold,
            "gte": F.col("confidence_1") >= confidence_1_threshold,
            "lte": F.col("confidence_1") <= confidence_1_threshold,
        }
        write_df = write_df.filter(confidence_1_ops[confidence_1_filter])

    if confidence_0_filter is not None:
        confidence_0_ops = {
            "gt":  F.col("confidence_0") >  confidence_0_threshold,
            "lt":  F.col("confidence_0") <  confidence_0_threshold,
            "gte": F.col("confidence_0") >= confidence_0_threshold,
            "lte": F.col("confidence_0") <= confidence_0_threshold,
        }
        write_df = write_df.filter(confidence_0_ops[confidence_0_filter])

    if support_filter is not None:
        support_ops = {
            "gt":  F.col("support") >  support_threshold,
            "lt":  F.col("support") <  support_threshold,
            "gte": F.col("support") >= support_threshold,
            "lte": F.col("support") <= support_threshold,
        }
        write_df = write_df.filter(support_ops[support_filter])

    # ------------------------------------------------------------------ #
    # Stream partition-by-partition to driver local file
    # ------------------------------------------------------------------ #
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as f:

        if fmt == "csv":
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for row in write_df.toLocalIterator():
                writer.writerow({field: row[field] for field in fields})

        elif fmt == "json":
            f.write("[\n")
            first = True
            for row in write_df.toLocalIterator():
                if not first:
                    f.write(",\n")
                json.dump({field: row[field] for field in fields}, f, ensure_ascii=False)
                first = False
            f.write("\n]")

    logger.info(f"Results written to: {output_path} (format={fmt})")