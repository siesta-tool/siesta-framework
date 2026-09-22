from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, DoubleType
from typing import Optional, Union
from collections import defaultdict
import logging

from siesta.modules.analyser.duration_format import format_duration

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# UDF schema: each detected loop occurrence for one group (trace) is a struct
# carrying both presence (pattern/loop_type) and cost (how much of the trace
# it consumes, in events and in time).
# ---------------------------------------------------------------------------

_LOOP_SCHEMA = ArrayType(StructType([
    StructField("pattern", StringType(), False),
    StructField("loop_type", StringType(), False),
    StructField("occurrences", IntegerType(), False),
    StructField("events_consumed", IntegerType(), False),
    StructField("time_consumed_sec", DoubleType(), False),
    # Relative to the group's own sequence (the trace, for the default
    # trace_id grouping) - null when the denominator (length/span) is zero.
    StructField("pct_trace_events", DoubleType(), True),
    StructField("pct_trace_time", DoubleType(), True),
]))


def _merge_intervals(intervals: list) -> list:
    """Merge (start, end) position intervals (inclusive) that share a position.

    Two occurrences that share a boundary position (e.g. self-loop instances
    (0,1) and (1,2) in "A A A", both covering the middle A) must not have that
    shared position double-counted in events_consumed - merging first and then
    measuring the merged spans is what gives the "merged, not summed" cost.
    """
    if not intervals:
        return []
    ordered = sorted(intervals)
    merged = [list(ordered[0])]
    for start, end in ordered[1:]:
        if start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return [(s, e) for s, e in merged]


def _interval_cost(intervals: list, timestamps: list) -> tuple:
    """events_consumed, time_consumed_sec for a set of already-merged intervals."""
    events = sum(e - s + 1 for s, e in intervals)
    time_sec = 0.0
    has_time = True
    for s, e in intervals:
        ts_s, ts_e = timestamps[s], timestamps[e]
        if ts_s is None or ts_e is None:
            has_time = False
            break
        time_sec += float(ts_e) - float(ts_s)
    return events, (time_sec if has_time else None)


def _find_repeated_patterns(activities: list, max_len: int) -> list:
    """Maximal-only search for contiguous blocks of length 2..max_len that recur
    at least twice, non-overlapping, within the trace.

    For each length L, every contiguous window is hashed to its start
    positions; a window with >=2 non-overlapping occurrences (chosen greedily
    left-to-right) becomes a candidate. Pure single-activity windows are
    skipped - that case is self_loop's domain, not this one's.

    Maximality: a shorter candidate is dropped when a longer kept candidate
    contains it at a fixed offset with the exact same occurrence count and the
    same (offset-shifted) start positions - i.e. the longer pattern already
    explains the repetition completely, so the shorter one adds nothing.
    """
    n = len(activities)
    candidates = []
    max_l = min(max_len, n // 2)
    for length in range(2, max_l + 1):
        windows = defaultdict(list)
        for p in range(0, n - length + 1):
            windows[tuple(activities[p:p + length])].append(p)
        for pattern, starts in windows.items():
            if len(set(pattern)) == 1:
                continue
            chosen = []
            last_end = -1
            for s in starts:
                if s > last_end:
                    chosen.append(s)
                    last_end = s + length - 1
            if len(chosen) >= 2:
                candidates.append({"length": length, "pattern": pattern, "starts": chosen})

    candidates.sort(key=lambda c: -c["length"])
    kept = []
    for c in candidates:
        subsumed = False
        for k in kept:
            if k["length"] <= c["length"] or len(k["starts"]) != len(c["starts"]):
                continue
            for offset in range(0, k["length"] - c["length"] + 1):
                if (k["pattern"][offset:offset + c["length"]] == c["pattern"]
                        and [ks + offset for ks in k["starts"]] == c["starts"]):
                    subsumed = True
                    break
            if subsumed:
                break
        if not subsumed:
            kept.append(c)
    return kept


def _find_loops(sequence, max_pattern_length: int) -> list:
    """Detect self-loops, non-self-loops and (optionally) repeated patterns in
    an ordered activity sequence, with per-trace occurrence and cost data.

    A self-loop is an activity immediately followed by itself; occurrences that
    share a position (three or more repeats in a row) are merged before their
    cost is measured, so "A A A" yields one self-loop entry with 2 occurrences
    but events_consumed=3 (not 4 - the shared middle event is not double-counted).

    A non-self-loop is a minimal cycle A -> ... -> A where A does not recur in
    the body; multiple instances of the same cycle pattern in one trace are
    collected together and their cost merged the same way.

    A repeated pattern is any contiguous block of length 2..max_pattern_length
    that recurs verbatim, non-overlapping, at least twice - reported only when
    maximal (see _find_repeated_patterns). max_pattern_length <= 0 disables
    this search entirely.

    Returns a list of dicts, at most one per (pattern, loop_type).
    """
    if not sequence or len(sequence) < 2:
        return []

    ordered = sorted(sequence, key=lambda r: r["position"])
    activities = [r["activity"] for r in ordered]
    timestamps = [r["start_timestamp"] for r in ordered]
    n = len(activities)
    group_len = n
    if timestamps[0] is not None and timestamps[-1] is not None:
        group_span = float(timestamps[-1]) - float(timestamps[0])
    else:
        group_span = None

    results = []

    def emit(pattern, loop_type, intervals):
        merged = _merge_intervals(intervals)
        events_consumed, time_consumed = _interval_cost(merged, timestamps)
        results.append({
            "pattern": pattern,
            "loop_type": loop_type,
            "occurrences": len(intervals),
            "events_consumed": events_consumed,
            "time_consumed_sec": time_consumed if time_consumed is not None else 0.0,
            "pct_trace_events": (events_consumed / group_len) if group_len else None,
            "pct_trace_time": (
                (time_consumed / group_span) if (time_consumed is not None and group_span) else None
            ),
        })

    # --- self-loops: a immediately followed by a ---
    self_raw = defaultdict(list)
    for i in range(n - 1):
        if activities[i] == activities[i + 1]:
            self_raw[activities[i]].append((i, i + 1))
    for pattern, intervals in self_raw.items():
        emit(pattern, "self_loop", intervals)

    # --- non-self-loops: minimal cycles A -> ... -> A ---
    nsl_raw = defaultdict(list)
    for i in range(n - 1):
        for j in range(i + 2, n):
            if activities[i] == activities[j] and activities[i] not in activities[i + 1:j]:
                pattern = " -> ".join(activities[i:j + 1])
                nsl_raw[pattern].append((i, j))
    for pattern, intervals in nsl_raw.items():
        emit(pattern, "non_self_loop", intervals)

    # --- repeated patterns: verbatim recurring blocks, maximal-only ---
    if max_pattern_length and max_pattern_length >= 2:
        for candidate in _find_repeated_patterns(activities, max_pattern_length):
            pattern = " -> ".join(candidate["pattern"])
            intervals = [(s, s + candidate["length"] - 1) for s in candidate["starts"]]
            emit(pattern, "repeated_pattern", intervals)

    return results


_find_loops_udf = F.udf(_find_loops, _LOOP_SCHEMA)


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
    max_pattern_length: Union[int, str] = 8,
    include_repeats: bool = True,
    with_time_stats: bool = True,
    with_distribution: bool = False,
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
        support_threshold: Fraction [0, 1] threshold; None = no filtering.
        filter_out: When True, keeps loops with support <= threshold (rare).
                    When False (default), keeps loops with support >= threshold
                    (frequent).
        top_k: Keep only the k most-supported loops (applied after threshold
               filtering, across all loop types together). None = keep all.
        trace_based: When True and grouping by trace_id, adds a trace_ids field
                     to each loop entry listing the traces that contain it.
        max_pattern_length: Longest repeated-pattern block to search for (an int
                            length, or "auto" to derive
                            clamp(round(avg_group_length / 2), 2, 8) from the
                            data). A pattern needs two non-overlapping copies,
                            so half the average length is the natural ceiling.
        include_repeats: When False, skip the repeated-pattern search entirely
                         (self-loops and non-self-loops are always detected).
        with_time_stats: When True (default), include per-pattern occurrence/
                         cost statistics (avg/median events, time, and their
                         share of the trace). When False, entries carry only
                         pattern/support/group_count, and the more expensive
                         percentile aggregations are skipped.
        with_distribution: When True, additionally emit the spread of the
                           per-trace time consumed by each pattern - min, p10,
                           p25, p50, p75, p90, max and population stddev, both in
                           absolute seconds (time_dist_*) and as a share of the
                           trace's duration (pct_time_dist_*). Opt-in and
                           strictly additive; requires with_time_stats.

    Returns:
        Dict with keys:
            total_groups      - number of distinct groups evaluated
            grouping_key      - effective grouping key(s) used
            self_loops        - list of loop dicts, sorted by support desc
            non_self_loops    - list of loop dicts, sorted by support desc
            repeated_patterns - list of loop dicts, sorted by support desc

        Each loop dict contains:
            pattern      - activity name (self-loop) or "A -> B -> ... -> A"
            support      - fraction of groups containing the loop [0, 1]
            group_count  - absolute count of groups
            trace_ids    - list of trace IDs (only when trace_based=True and
                           grouping by trace_id)
            (when with_time_stats=True, additionally:)
            avg_occurrences, avg/median_events_consumed,
            avg/median_pct_trace_events, avg/median_time_consumed_sec,
            avg/median_pct_trace_time - see module docstring semantics in
                                        _find_loops for what these measure.
            avg/median_time_consumed_human - the *_sec values rendered by
                                        duration_format.format_duration (e.g.
                                        "2.5h").
            (when with_distribution=True, the time_distribution_sec dict is
             mirrored by a time_distribution_human dict of the same keys.)
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
                group_cols.append(key)           # top-level column (e.g. activity)
            else:
                col_name = f"_grp_{key}"
                events_df = events_df.withColumn(col_name, F.col("attributes").getItem(key))
                group_cols.append(col_name)

    # --- 2. Grouping-value filter ---
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

    # --- 3. Count total groups ---
    total_groups = events_df.select(*group_cols).distinct().count()

    empty_result = {
        "total_groups": 0,
        "grouping_key": "trace_id" if is_trace_grouping else grouping_key,
        "self_loops": [],
        "non_self_loops": [],
        "repeated_patterns": [],
    }
    if total_groups == 0:
        return empty_result

    # --- 3.5. Resolve the repeated-pattern length cap ---
    effective_max_len = 0
    if include_repeats:
        if isinstance(max_pattern_length, str):
            if max_pattern_length != "auto":
                raise ValueError(
                    f"max_pattern_length must be an int or 'auto', got {max_pattern_length!r}"
                )
            total_events = events_df.count()
            avg_len = (total_events / total_groups) if total_groups else 0
            effective_max_len = max(2, min(8, round(avg_len / 2)))
        else:
            effective_max_len = int(max_pattern_length)

    # --- 4. Collect ordered sequences per group and detect loops via UDF ---
    seq_df = events_df.groupBy(*group_cols).agg(
        F.collect_list(
            F.struct(F.col("activity"), F.col("position"), F.col("start_timestamp"))
        ).alias("sequence")
    )

    loops_df = seq_df.withColumn("loops", _find_loops_udf(F.col("sequence"), F.lit(effective_max_len)))

    # Explode: one row per (group, loop_pattern, loop_type) - already unique per
    # group since _find_loops emits at most one entry per (pattern, loop_type).
    loop_cols = ["pattern", "loop_type", "occurrences", "events_consumed",
                 "time_consumed_sec", "pct_trace_events", "pct_trace_time"]
    exploded = (
        loops_df
        .select(*group_cols, F.explode("loops").alias("loop"))
        .select(*group_cols, *[F.col(f"loop.{c}").alias(c) for c in loop_cols])
    )
    exploded.cache()

    # --- 5. Aggregate: count groups per loop, plus cost statistics ---
    agg_exprs = [F.count("*").alias("group_count")]
    if with_time_stats:
        agg_exprs += [
            F.avg("occurrences").alias("avg_occurrences"),
            F.avg("events_consumed").alias("avg_events_consumed"),
            F.percentile_approx("events_consumed", 0.5).alias("median_events_consumed"),
            F.avg("pct_trace_events").alias("avg_pct_trace_events"),
            F.percentile_approx("pct_trace_events", 0.5).alias("median_pct_trace_events"),
            F.avg("time_consumed_sec").alias("avg_time_consumed_sec"),
            F.percentile_approx("time_consumed_sec", 0.5).alias("median_time_consumed_sec"),
            F.avg("pct_trace_time").alias("avg_pct_trace_time"),
            F.percentile_approx("pct_trace_time", 0.5).alias("median_pct_trace_time"),
        ]
    if with_distribution and with_time_stats:
        _qs = [0.1, 0.25, 0.5, 0.75, 0.9]
        agg_exprs += [
            F.min("time_consumed_sec").alias("time_dist_min"),
            F.percentile_approx("time_consumed_sec", _qs).alias("time_dist_pcts"),
            F.max("time_consumed_sec").alias("time_dist_max"),
            F.stddev_pop("time_consumed_sec").alias("time_dist_stddev"),
            F.min("pct_trace_time").alias("pct_time_dist_min"),
            F.percentile_approx("pct_trace_time", _qs).alias("pct_time_dist_pcts"),
            F.max("pct_trace_time").alias("pct_time_dist_max"),
            F.stddev_pop("pct_trace_time").alias("pct_time_dist_stddev"),
        ]
    if trace_based and is_trace_grouping:
        agg_exprs.append(F.collect_list(F.col("trace_id")).alias("trace_ids"))

    result_df = (
        exploded
        .groupBy("pattern", "loop_type")
        .agg(*agg_exprs)
        .withColumn("support", F.col("group_count") / F.lit(total_groups))
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

    self_loops = []
    non_self_loops = []
    repeated_patterns = []

    def _rounded(value, digits=6):
        return round(float(value), digits) if value is not None else None

    for row in rows:
        entry = {
            "pattern": row["pattern"],
            "support": round(float(row["support"]), 6),
            "group_count": int(row["group_count"]),
        }
        if with_time_stats:
            entry.update({
                "avg_occurrences": _rounded(row["avg_occurrences"], 3),
                "avg_events_consumed": _rounded(row["avg_events_consumed"], 3),
                "median_events_consumed": _rounded(row["median_events_consumed"], 3),
                "avg_pct_trace_events": _rounded(row["avg_pct_trace_events"]),
                "median_pct_trace_events": _rounded(row["median_pct_trace_events"]),
                "avg_time_consumed_sec": _rounded(row["avg_time_consumed_sec"], 3),
                "avg_time_consumed_human": format_duration(row["avg_time_consumed_sec"]),
                "median_time_consumed_sec": _rounded(row["median_time_consumed_sec"], 3),
                "median_time_consumed_human": format_duration(row["median_time_consumed_sec"]),
                "avg_pct_trace_time": _rounded(row["avg_pct_trace_time"]),
                "median_pct_trace_time": _rounded(row["median_pct_trace_time"]),
            })
        if with_distribution and with_time_stats:
            t_pcts = row["time_dist_pcts"] or [None] * 5
            p_pcts = row["pct_time_dist_pcts"] or [None] * 5
            entry["time_distribution_sec"] = {
                "min": _rounded(row["time_dist_min"], 3),
                "p10": _rounded(t_pcts[0], 3), "p25": _rounded(t_pcts[1], 3),
                "p50": _rounded(t_pcts[2], 3), "p75": _rounded(t_pcts[3], 3),
                "p90": _rounded(t_pcts[4], 3), "max": _rounded(row["time_dist_max"], 3),
                "stddev": _rounded(row["time_dist_stddev"], 3),
            }
            entry["time_distribution_human"] = {
                "min": format_duration(row["time_dist_min"]),
                "p10": format_duration(t_pcts[0]), "p25": format_duration(t_pcts[1]),
                "p50": format_duration(t_pcts[2]), "p75": format_duration(t_pcts[3]),
                "p90": format_duration(t_pcts[4]), "max": format_duration(row["time_dist_max"]),
                "stddev": format_duration(row["time_dist_stddev"]),
            }
            entry["pct_trace_time_distribution"] = {
                "min": _rounded(row["pct_time_dist_min"]),
                "p10": _rounded(p_pcts[0]), "p25": _rounded(p_pcts[1]),
                "p50": _rounded(p_pcts[2]), "p75": _rounded(p_pcts[3]),
                "p90": _rounded(p_pcts[4]), "max": _rounded(row["pct_time_dist_max"]),
                "stddev": _rounded(row["pct_time_dist_stddev"]),
            }
        if trace_based and is_trace_grouping:
            entry["trace_ids"] = sorted(row["trace_ids"])

        if row["loop_type"] == "self_loop":
            self_loops.append(entry)
        elif row["loop_type"] == "non_self_loop":
            non_self_loops.append(entry)
        else:
            repeated_patterns.append(entry)

    return {
        "total_groups": int(total_groups),
        "grouping_key": "trace_id" if is_trace_grouping else grouping_key,
        "self_loops": self_loops,
        "non_self_loops": non_self_loops,
        "repeated_patterns": repeated_patterns,
    }
