from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    ArrayType, IntegerType, StringType, StructField, StructType,
)
from typing import Optional, Union
import logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# UDF schema: each detected item is (pattern, loop_type, occurrences)
# ---------------------------------------------------------------------------

_LOOP_SCHEMA = ArrayType(StructType([
    StructField("pattern",    StringType(),  False),
    StructField("loop_type",  StringType(),  False),
    StructField("occurrences", IntegerType(), False),
]))


def _make_find_loops_udf(
    detect_repeated_patterns: bool = False,
    min_pattern_length: int = 2,
    max_pattern_length: int = 10,
    min_repetitions: int = 2,
):
    """Return a PySpark UDF that detects loops (and optionally repeated patterns).

    All parameters are captured in the closure so the UDF signature stays
    ``sequence -> list[struct]`` and can be applied with a single column
    reference.

    Args:
        detect_repeated_patterns: When True, also find contiguous subsequences
            that appear at least ``min_repetitions`` times within the trace.
            Adds entries with ``loop_type = "repeated_pattern"``.  Disabled by
            default because the scan is O(n * max_pattern_length) per trace.
        min_pattern_length: Minimum length (number of activities) of a repeated
            pattern to report.  Default 2 avoids overlap with self-loops.
        max_pattern_length: Maximum length of a repeated pattern.  Capped
            internally at ``n // min_repetitions`` so only patterns that can
            actually repeat fit within the trace.
        min_repetitions: A pattern must appear at least this many times to be
            reported.  Default 2.
    """
    _detect  = detect_repeated_patterns
    _min_pat = min_pattern_length
    _max_pat = max_pattern_length
    _min_rep = min_repetitions

    @F.udf(_LOOP_SCHEMA)
    def _inner(sequence):
        if not sequence:
            return []

        activities = [
            row["activity"]
            for row in sorted(sequence, key=lambda r: r["position"])
        ]
        n = len(activities)
        results = []

        # ── Self-loops ──────────────────────────────────────────────────────
        # Count consecutive same-activity pairs; each pair is one occurrence.
        sl_counts: dict = {}
        for i in range(n - 1):
            if activities[i] == activities[i + 1]:
                act = activities[i]
                sl_counts[act] = sl_counts.get(act, 0) + 1
        for act, cnt in sl_counts.items():
            results.append((act, "self_loop", cnt))

        # ── Non-self-loops ──────────────────────────────────────────────────
        # Count all minimal cycles: every (i, j) pair where activities[i] ==
        # activities[j] and the bounding activity does not appear in between.
        # Each qualifying pair is one occurrence of that pattern.
        nsl_counts: dict = {}
        for i in range(n - 1):
            for j in range(i + 2, n):
                if activities[i] == activities[j]:
                    if activities[i] not in activities[i + 1 : j]:
                        pat = " -> ".join(activities[i : j + 1])
                        nsl_counts[pat] = nsl_counts.get(pat, 0) + 1
        for pat, cnt in nsl_counts.items():
            results.append((pat, "non_self_loop", cnt))

        # ── Repeated patterns (optional) ────────────────────────────────────
        # Find contiguous subsequences of length in [min_pat, max_pat] that
        # appear at least min_rep times anywhere in the trace (positions may
        # overlap).  Each matching start position counts as one occurrence.
        if _detect:
            max_len = min(_max_pat, n // _min_rep) if _min_rep > 0 else _max_pat
            for length in range(_min_pat, max_len + 1):
                pat_counts: dict = {}
                for i in range(n - length + 1):
                    key = tuple(activities[i : i + length])
                    pat_counts[key] = pat_counts.get(key, 0) + 1
                for pat_key, cnt in pat_counts.items():
                    if cnt >= _min_rep:
                        results.append((" -> ".join(pat_key), "repeated_pattern", cnt))

        return results

    return _inner


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def compute_loop_detection(
    events_df: DataFrame,
    grouping_key: Optional[Union[str, list]] = None,
    grouping_value: Optional[Union[str, list]] = None,
    support_threshold: Optional[float] = None,
    filter_out: bool = False,
    top_k: Optional[int] = None,
    trace_based: bool = False,
    detect_repeated_patterns: bool = False,
    min_pattern_length: int = 2,
    max_pattern_length: int = 10,
    min_repetitions: int = 2,
) -> dict:
    """Detect loops (and optionally repeated patterns) in an indexed event log.

    Args:
        events_df: Sequence table DataFrame (activity, trace_id, position,
                   start_timestamp, attributes).
        grouping_key: Attribute key(s) to group by instead of trace_id.  A
                      string means a single attribute key; a list means a
                      composite key.  None defaults to trace_id grouping.
        grouping_value: Optional value(s) to keep after extracting the group
                        column.  Rows whose group column value is not in this
                        list are dropped before detection.  For a composite key,
                        supply a dict mapping each key to its allowed value(s).
        support_threshold: Fraction [0, 1] threshold; None = no filtering.
        filter_out: When True, keeps loops with support <= threshold (rare).
                    When False (default), keeps loops with support >= threshold
                    (frequent).
        top_k: Keep only the k most-supported patterns (applied after threshold
               filtering).  None = keep all.
        trace_based: When True and grouping by trace_id, adds a
                     ``trace_occurrences`` field to each entry mapping every
                     trace that contains the pattern to its occurrence count
                     within that trace.
        detect_repeated_patterns: When True, also detect contiguous subsequence
                     patterns that repeat at least ``min_repetitions`` times
                     inside a single trace.  Results appear in a separate
                     ``repeated_patterns`` list.  Disabled by default (O(n *
                     max_pattern_length) per trace).
        min_pattern_length: Minimum length of a repeated pattern.  Default 2.
        max_pattern_length: Maximum length of a repeated pattern.  Default 10.
        min_repetitions: Minimum number of in-trace occurrences for a repeated
                     pattern to be reported.  Default 2.

    Returns:
        Dict with keys:
            total_groups      - number of distinct groups evaluated
            grouping_key      - effective grouping key(s) used
            self_loops        - list of pattern dicts, sorted by support desc
            non_self_loops    - list of pattern dicts, sorted by support desc
            repeated_patterns - list of pattern dicts (always present; populated
                                only when detect_repeated_patterns=True)

        Each pattern dict contains:
            pattern           - activity name (self-loop) or "A -> B -> ... -> A"
            support           - fraction of groups containing the pattern [0, 1]
            support_count     - absolute number of groups containing the pattern
            trace_occurrences - dict {trace_id: occurrence_count} — only when
                                trace_based=True and grouping by trace_id
    """
    # --- 1. Resolve grouping columns ---
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
                group_cols.append(key)
            else:
                col_name = f"_grp_{key}"
                events_df = events_df.withColumn(col_name, F.col("attributes").getItem(key))
                group_cols.append(col_name)

    # --- 2. Grouping-value filter ---
    if grouping_value is not None:
        if isinstance(grouping_value, dict):
            for key, vals in grouping_value.items():
                col_name = f"_grp_{key}"
                allowed = [vals] if isinstance(vals, str) else list(vals)
                events_df = events_df.filter(F.col(col_name).isin(allowed))
        else:
            allowed = [grouping_value] if isinstance(grouping_value, str) else list(grouping_value)
            events_df = events_df.filter(F.col(group_cols[0]).isin(allowed))

    # --- 3. Count total groups ---
    total_groups = events_df.select(*group_cols).distinct().count()

    if total_groups == 0:
        return {
            "total_groups": 0,
            "grouping_key": "trace_id" if is_trace_grouping else grouping_key,
            "self_loops": [],
            "non_self_loops": [],
            "repeated_patterns": [],
        }

    # --- 4. Build UDF, collect ordered sequences, run detection ---
    find_loops_udf = _make_find_loops_udf(
        detect_repeated_patterns=detect_repeated_patterns,
        min_pattern_length=min_pattern_length,
        max_pattern_length=max_pattern_length,
        min_repetitions=min_repetitions,
    )

    seq_df = events_df.groupBy(*group_cols).agg(
        F.collect_list(
            F.struct(F.col("activity"), F.col("position"))
        ).alias("sequence")
    )

    loops_df = seq_df.withColumn("loops", find_loops_udf(F.col("sequence")))

    # Explode: one row per (group, pattern, loop_type, occurrences).
    # No .distinct() needed: the UDF uses dicts internally so each
    # (pattern, loop_type) pair appears at most once per group.
    exploded = (
        loops_df
        .select(*group_cols, F.explode("loops").alias("loop"))
        .select(
            *group_cols,
            F.col("loop.pattern").alias("pattern"),
            F.col("loop.loop_type").alias("loop_type"),
            F.col("loop.occurrences").alias("occurrences"),
        )
    )
    exploded.cache()

    # --- 5. Aggregate: support count + per-trace occurrence map ---
    agg_exprs = [F.count("*").alias("support_count")]
    if trace_based and is_trace_grouping:
        agg_exprs.append(
            F.collect_list(
                F.struct(F.col("trace_id"), F.col("occurrences"))
            ).alias("trace_data")
        )

    result_df = (
        exploded
        .groupBy("pattern", "loop_type")
        .agg(*agg_exprs)
        .withColumn("support", F.col("support_count") / F.lit(total_groups))
    )

    # --- 6. Apply support threshold ---
    if support_threshold is not None:
        if filter_out:
            result_df = result_df.filter(F.col("support") <= F.lit(support_threshold))
        else:
            result_df = result_df.filter(F.col("support") >= F.lit(support_threshold))

    # --- 7. Sort and limit ---
    result_df = result_df.orderBy(F.col("support").desc())
    if top_k is not None:
        result_df = result_df.limit(top_k)

    # --- 8. Collect to driver and build output dict ---
    rows = result_df.collect()
    exploded.unpersist()

    self_loops:        list = []
    non_self_loops:    list = []
    repeated_patterns: list = []

    for row in rows:
        entry: dict = {
            "pattern":       row["pattern"],
            "support":       round(float(row["support"]), 6),
            "support_count": int(row["support_count"]),
        }
        if trace_based and is_trace_grouping:
            entry["trace_occurrences"] = {
                td["trace_id"]: int(td["occurrences"])
                for td in sorted(row["trace_data"], key=lambda x: x["trace_id"])
            }

        lt = row["loop_type"]
        if lt == "self_loop":
            self_loops.append(entry)
        elif lt == "non_self_loop":
            non_self_loops.append(entry)
        else:
            repeated_patterns.append(entry)

    return {
        "total_groups":      int(total_groups),
        "grouping_key":      "trace_id" if is_trace_grouping else grouping_key,
        "self_loops":        self_loops,
        "non_self_loops":    non_self_loops,
        "repeated_patterns": repeated_patterns,
    }