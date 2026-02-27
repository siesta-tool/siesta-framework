"""
Functions relating to the generation and filtering of valid event pairs
"""
from collections import defaultdict
from datetime import timedelta, datetime
import re
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from typing import List, Literal, Optional, Tuple
from pyspark.sql.functions import count_distinct, col, to_timestamp, unix_timestamp, sum as spark_sum, count, min as spark_min, max as spark_max, pow as spark_pow, row_number, to_json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, MapType as SparkMapType
from siesta_framework.model.DataModel import Active_Pairs_table_schema, EventPair
import logging
from siesta_framework.core.sparkManager import get_spark_session
logger = logging.getLogger("ExtractPairs")

# pair_schema = StructType([
#     StructField("eventA", StringType(), True),
#     StructField("eventB", StringType(), True),
#     StructField("trace_id", StringType(), True),
#     StructField("timestampA", IntegerType(), True),
#     StructField("timestampB", IntegerType(), True),
#     StructField("posA", IntegerType(), True),
#     StructField("posB", IntegerType(), True)
# ])

activity_index_schema = StructType([
    StructField("activity", StringType(), True),
    StructField("trace_id", StringType(), True),
    StructField("position", IntegerType(), True),
    StructField("start_timestamp", TimestampType(), True),
    StructField("attributes", StringType(), True)
])

_TIME_PATTERN = re.compile(r"(\d+(?:\.\d+)?)(d|h|m|s)")
LookbackType = Literal["time", "index"]
pair_index_schema = EventPair.get_schema()
# Schema: Source, Target, trace_id, source_timestamp, target_timestamp, source_position, target_position, source_attributes, target_attributes


type Trace_ID = str
type Event_Type = str
type Timestamp = str
type Position = int
type Attributes = str

type Event = Tuple[Event_Type, Timestamp, Position, Attributes]
type Trace = Tuple[Trace_ID, Tuple[Event]]


def extract_active_and_all_pairs(updated_sequence_table_DF: DataFrame, previous_active_pairs: DataFrame | None, lookback: str) -> Tuple[DataFrame, DataFrame]:
    """
    Extracts the event type pairs from a DF that contains the complete traces.
    Uses Spark's cogroup + applyInPandas for efficient distributed processing
    with Apache Arrow serialization, avoiding slow RDD operations.

    If there are previously indexed pairs, this process will only calculate the new event type 
    pairs by utilizing the information stored in the LastChecked table. Additionally, 
    there will be no event type pair where the two events will have greater time difference 
    than the one described by the parameter lookback.

    The generated pairs follow a Skip-till-next-match policy without overlapping in time. 
    That is, in order to be two occurrences of the same event type pair in a trace, the 
    second one must start after the first one has ended.

    Example:
        The trace t1 = (a,b,c,a,b) contains 2 occurrences of the event type (a,b). 
        However, the trace t2 = (a,a,c,b,b) only contains one occurrence of the 
        previous event type pair.

    Args:
        updated_sequence_table_DF: The DataFrame that contains the complete activity index.
            activity (str)|trace_id (str)|position (int)|start_timestamp (ts)|attributes (json)
        previous_active_pairs: The loaded values as a DataFrame from the active pairs table
            (it should be None if it is the first time that events are indexed in this log database).
        lookback: The parameter that describes the maximum time difference that two 
            events can have in order to create an event type pair.

    Returns:
        A tuple of (pairs_df, active_pairs_df) where pairs_df contains the extracted
        event type pairs and active_pairs_df contains the last timestamps per
        (trace_id, eventA, eventB) for incremental processing.
    """
    real_lookback = _parse_lookback(lookback)

    # Convert timestamps to unix epoch seconds
    updated_sequence_table_DF = updated_sequence_table_DF.withColumn(
        "start_timestamp", unix_timestamp(col("start_timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
    )

    # Ensure attributes column is a JSON string for consistent Arrow serialization
    if "attributes" in updated_sequence_table_DF.columns:
        attr_type = updated_sequence_table_DF.schema["attributes"].dataType
        if isinstance(attr_type, SparkMapType):
            updated_sequence_table_DF = updated_sequence_table_DF.withColumn(
                "attributes", to_json("attributes")
            )

    spark = get_spark_session()

    # Use empty DataFrame when no previous active pairs exist
    if previous_active_pairs is None:
        previous_active_pairs = spark.createDataFrame([], schema=Active_Pairs_table_schema)

    # Closure captures the lookback value for the pandas UDF
    lookback_value = real_lookback
    output_columns = [f.name for f in pair_index_schema.fields]

    def _compute_trace_pairs(events_pdf: pd.DataFrame, active_pdf: pd.DataFrame) -> pd.DataFrame:
        """Compute event pairs for a single trace using skip-till-next-match policy."""
        if events_pdf.empty:
            return pd.DataFrame(columns=output_columns)

        trace_id = events_pdf["trace_id"].iloc[0]

        # Build last-checked lookup from previous active pairs
        last_map = {}
        if not active_pdf.empty:
            for _, r in active_pdf.iterrows():
                last_map[(r["eventA"], r["eventB"])] = r["last_checked_timestamp"]

        # Group events by activity type, sorted by position for correct matching
        activity_groups = {}
        for activity, grp in events_pdf.groupby("activity", sort=False):
            sorted_grp = grp.sort_values("position")
            activity_groups[activity] = list(zip(
                sorted_grp["start_timestamp"], sorted_grp["position"], sorted_grp["attributes"]
            ))

        # Generate all event type combinations and compute pairs
        all_types = list(activity_groups.keys())
        combinations = _findCombinations(all_types)

        results = []
        for key1, key2 in combinations:
            src = activity_groups.get(key1)
            tgt = activity_groups.get(key2)
            if not src or not tgt:
                continue
            last_ts = last_map.get((key1, key2))
            pairs = createTuples(key1, key2, src, tgt, lookback_value, last_ts, trace_id)
            results.extend(pairs)

        if not results:
            return pd.DataFrame(columns=output_columns).astype(str)
        return pd.DataFrame(results, columns=output_columns).astype(str)

    # Distributed pair computation using cogroup + applyInPandas (Arrow serialization)
    pairs_df = (
        updated_sequence_table_DF.groupBy("trace_id")
        .cogroup(previous_active_pairs.groupBy("trace_id"))
        .applyInPandas(_compute_trace_pairs, schema=pair_index_schema)
    )

    # Derive active pairs from computed pairs using native Spark window functions
    # Active pair = last matched pair per (trace_id, source, target) group
    w = Window.partitionBy("trace_id", "source", "target").orderBy(
        col("source_position").cast("int").desc()
    )
    active_pairs_df = (
        pairs_df
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .select(
            col("trace_id"),
            col("source").alias("eventA"),
            col("target").alias("eventB"),
            col("target_timestamp").cast("integer").alias("last_checked_timestamp")
        )
    )

    return pairs_df, active_pairs_df


def createTuples(
    key1: str, 
    key2: str, 
    e_source: List[Tuple[int, int, str]], 
    e_target: List[Tuple[int, int, str]],
    lookback: Tuple[int, LookbackType], 
    last_checked: Optional[int],
    trace_id: str
) -> List[Tuple[str, str, str, str, str, int, int]]:
    """
    Creates event type pair tuples following Skip-till-next-match policy.
    
    Returns:
        List of tuples: (eventA, eventB, trace_id, timestampA, timestampB, posA, posB)
    """
    pairs = []
    lookback_number = lookback[0]
    prev = None
    i = 0
    
    for ea_ts, ea_pos, ea_attr in e_source:
        # Evaluate based on previous and last_checked
        if ((prev is None or ea_ts >= prev) and 
            (last_checked is None or ea_ts >= last_checked)):
            
            stop = False
            while i < len(e_target) and not stop:
                eb_ts, eb_pos, eb_attr = e_target[i]
                
                if ea_pos >= eb_pos:
                    # Event a is not before event b, remove it
                    i += 1
                else:
                    # Event a is before event b
                    if lookback[1] == "index":
                        if eb_pos - ea_pos <= lookback_number:
                            # Lookback satisfied, create pair
                            pairs.append((key1, key2, trace_id, ea_ts, eb_ts, ea_pos, eb_pos, ea_attr, eb_attr))
                            prev = eb_ts
                            i += 1
                    else:
                        if eb_ts - ea_ts <= lookback_number:
                            # Lookback satisfied, create pair
                            pairs.append((key1, key2, trace_id, ea_ts, eb_ts, ea_pos, eb_pos, ea_attr, eb_attr))
                            prev = eb_ts
                            i += 1
                    stop = True
    
    return pairs


def _findCombinations(event_types: List[str]) -> List[Tuple[str, str]]:
    """
    Extracts all the possible event type pairs that can occur in a trace based on the unique event types

    Args:
        event_types: The unique event types in this trace
    Returns:
        list: A list of all the possible event type pairs that can occur in this trace
    """
    return [(t1, t2) for t1 in event_types for t2 in event_types]


def extract_counts(pairs_index: DataFrame) -> DataFrame:
    """
    Extracts statistics for each event type pair.

    For every (eventA, eventB) pair, computes:
      - total_duration: sum of durations (in seconds) across all occurrences
      - total_completions: number of times this pair occurred
      - min_duration: minimum duration (seconds) across all occurrences
      - max_duration: maximum duration (seconds) across all occurrences
      - sum_squared_duration: sum of squared durations (for variance calculation)

    Args:
        pairs_index: DataFrame with schema matching EventPair (source, target,
            trace_id, source_timestamp, target_timestamp, ...)

    Returns:
        A DataFrame with columns:
            eventA, eventB, total_duration, total_completions,
            min_duration, max_duration, sum_squared_duration
    """

    # Timestamps are unix (milliseconds from the lookback arithmetic); convert to seconds
    duration_col = ((col("target_timestamp") - col("source_timestamp")) / 1000).alias("duration")

    count_table = (pairs_index
        .withColumn("duration", (col("target_timestamp").cast("long") - col("source_timestamp").cast("long")) / 1000)
        .groupBy("source", "target")
        .agg(
            spark_sum("duration").alias("total_duration").cast("float"),
            count("duration").alias("total_completions").cast("integer"),
            spark_min("duration").alias("min_duration").cast("float"),
            spark_max("duration").alias("max_duration").cast("float"),
            spark_sum(spark_pow(col("duration"), 2)).alias("sum_squared_duration"),
        ))

    return count_table

def _parse_lookback(lookback: str) -> Tuple[int, LookbackType]:
    """
    Returns a tuple(lookback_number, LookbackType)
    lookback_number is in ms for time and positions for index
    format: for time $d$m$s for index $i eg. 25d16m0.5s or 255i 
    """
    if "i" in lookback:
        return (int(lookback.split("i")[0]), 'index')
    else:
        time_matches = list(_TIME_PATTERN.finditer(lookback))
        if not time_matches:
            raise ValueError(f"Invalid lookback format: {lookback}")
        total_ms = 0.0
        for match in time_matches:
            value = float(match.group(1))
            unit = match.group(2)

            if unit == "d":
                total_ms += value * 24 * 60 * 60 * 1000
            elif unit == "h":
                total_ms += value * 60 * 60 * 1000
            elif unit == "m":
                total_ms += value * 60 * 1000
            elif unit == "s":
                total_ms += value * 1000
            else:
                raise ValueError(f"Unsupported unit: {unit}")

        return int(total_ms), "time"
    