from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType
from typing import Optional, Union
import logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# UDF schema: each detected loop is (pattern, loop_type)
# ---------------------------------------------------------------------------

_LOOP_SCHEMA = ArrayType(StructType([
    StructField("pattern", StringType(), False),
    StructField("loop_type", StringType(), False),
]))


@F.udf(_LOOP_SCHEMA)
def _find_loops_udf(sequence):
    """Detect self-loops and minimal non-self-loops in an ordered activity sequence.

    A self-loop is an activity immediately followed by itself.
    A minimal non-self-loop is a subsequence that starts and ends with the same
    activity, with at least one different activity in between, and the bounding
    activity does not appear anywhere within the middle part.

    Returns a list of (pattern, loop_type) pairs, deduplicated per group.
    """
    if not sequence:
        return []

    activities = [row["activity"] for row in sorted(sequence, key=lambda r: r["position"])]
    n = len(activities)
    loops = set()

    for i in range(n - 1):
        # Self-loop: same activity at consecutive positions
        if activities[i] == activities[i + 1]:
            loops.add((activities[i], "self_loop"))

    for i in range(n - 1):
        for j in range(i + 2, n):
            if activities[i] == activities[j]:
                # Minimal: bounding activity must not appear in the body
                if activities[i] not in activities[i + 1:j]:
                    pattern = " -> ".join(activities[i:j + 1])
                    loops.add((pattern, "non_self_loop"))

    return list(loops)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def compute_loop_detection(
    events_df: DataFrame,
    grouping_key: Optional[Union[str, list]] = None,
    grouping_value: Optional[Union[str, list]] = None,
    min_timestamp: Optional[int] = None,
    support_threshold: Optional[float] = None,
    filter_out: bool = False,
    top_k: Optional[int] = None,
    trace_based: bool = False,
) -> dict:
    """Detect loops in an indexed event log using distributed Spark processing.

    Args:
        events_df: Sequence table DataFrame (activity, trace_id, position,
                   start_timestamp, attributes).
        grouping_key: Attribute key(s) to group by instead of trace_id. A string
                      means a single attribute key; a list means a composite key.
                      None defaults to trace_id grouping.
        grouping_value: Optional value(s) to keep after extracting the group
                        column. Rows whose group column value is not in this list
                        are dropped before detection. For a composite key, supply
                        a dict mapping each key to its allowed value(s).
        min_timestamp: Optional lower bound on start_timestamp (epoch seconds).
                       Events before this timestamp are excluded.
        support_threshold: Fraction [0, 1] threshold; None = no filtering.
        filter_out: When True, keeps loops with support <= threshold (rare).
                    When False (default), keeps loops with support >= threshold
                    (frequent).
        top_k: Keep only the k most-supported loops (applied after threshold
               filtering). None = keep all.
        trace_based: When True and grouping by trace_id, adds a trace_ids field
                     to each loop entry listing the traces that contain it.

    Returns:
        Dict with keys:
            total_groups   - number of distinct groups evaluated
            grouping_key   - effective grouping key(s) used
            self_loops     - list of loop dicts, sorted by support desc
            non_self_loops - list of loop dicts, sorted by support desc

        Each loop dict contains:
            pattern      - activity name (self-loop) or "A -> B -> ... -> A"
            support      - fraction of groups containing the loop [0, 1]
            group_count  - absolute count of groups
            trace_ids    - list of trace IDs (only when trace_based=True and
                           grouping by trace_id)
    """
    # --- 1. Timestamp pre-filter ---
    if min_timestamp is not None:
        events_df = events_df.filter(F.col("start_timestamp") >= min_timestamp)

    # --- 2. Resolve grouping columns ---
    keys = [] if grouping_key is None else (
        [grouping_key] if isinstance(grouping_key, str) else list(grouping_key)
    )
    is_trace_grouping = not keys or keys == ["trace_id"]

    if is_trace_grouping:
        group_cols = ["trace_id"]
    else:
        existing_cols = set(events_df.columns)
        group_cols = []
        for key in keys:
            if key in existing_cols:
                group_cols.append(key)           # top-level column (e.g. activity)
            else:
                col_name = f"_grp_{key}"
                events_df = events_df.withColumn(col_name, F.col("attributes").getItem(key))
                group_cols.append(col_name)

    # --- 3. Grouping-value filter ---
    if grouping_value is not None:
        if isinstance(grouping_value, dict):
            # Multi-key case: {key: value(s)}
            for key, vals in grouping_value.items():
                col_name = f"_grp_{key}"
                allowed = [vals] if isinstance(vals, str) else list(vals)
                events_df = events_df.filter(F.col(col_name).isin(allowed))
        else:
            # Single key: string or list of strings applied to the first group col
            allowed = [grouping_value] if isinstance(grouping_value, str) else list(grouping_value)
            events_df = events_df.filter(F.col(group_cols[0]).isin(allowed))

    # --- 4. Count total groups ---
    total_groups = events_df.select(*group_cols).distinct().count()

    if total_groups == 0:
        return {
            "total_groups": 0,
            "grouping_key": "trace_id" if is_trace_grouping else grouping_key,
            "self_loops": [],
            "non_self_loops": [],
        }

    # --- 5. Collect ordered sequences per group and detect loops via UDF ---
    seq_df = events_df.groupBy(*group_cols).agg(
        F.collect_list(
            F.struct(F.col("activity"), F.col("position"))
        ).alias("sequence")
    )

    loops_df = seq_df.withColumn("loops", _find_loops_udf(F.col("sequence")))

    # Explode: one row per (group, loop_pattern, loop_type), deduplicated per group
    exploded = (
        loops_df
        .select(*group_cols, F.explode("loops").alias("loop"))
        .select(
            *group_cols,
            F.col("loop.pattern").alias("pattern"),
            F.col("loop.loop_type").alias("loop_type"),
        )
        .distinct()
    )
    exploded.cache()

    # --- 6. Aggregate: count groups per loop ---
    agg_exprs = [
        F.count("*").alias("group_count"),
    ]
    if trace_based and is_trace_grouping:
        agg_exprs.append(F.collect_list(F.col("trace_id")).alias("trace_ids"))

    result_df = (
        exploded
        .groupBy("pattern", "loop_type")
        .agg(*agg_exprs)
        .withColumn("support", F.col("group_count") / F.lit(total_groups))
    )

    # --- 7. Apply support threshold ---
    if support_threshold is not None:
        if filter_out:
            result_df = result_df.filter(F.col("support") <= F.lit(support_threshold))
        else:
            result_df = result_df.filter(F.col("support") >= F.lit(support_threshold))

    # --- 8. Sort and limit ---
    result_df = result_df.orderBy(F.col("support").desc())
    if top_k is not None:
        result_df = result_df.limit(top_k)

    # --- 9. Collect to driver and build output dict ---
    rows = result_df.collect()
    exploded.unpersist()

    self_loops = []
    non_self_loops = []

    for row in rows:
        entry = {
            "pattern": row["pattern"],
            "support": round(float(row["support"]), 6),
            "group_count": int(row["group_count"]),
        }
        if trace_based and is_trace_grouping:
            entry["trace_ids"] = sorted(row["trace_ids"])

        if row["loop_type"] == "self_loop":
            self_loops.append(entry)
        else:
            non_self_loops.append(entry)

    return {
        "total_groups": int(total_groups),
        "grouping_key": "trace_id" if is_trace_grouping else grouping_key,
        "self_loops": self_loops,
        "non_self_loops": non_self_loops,
    }
