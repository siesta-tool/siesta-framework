"""
Functions relating to the generation and filtering of valid event pairs
"""
from collections import defaultdict
from datetime import timedelta, datetime
from pyspark import RDD
from pyspark.sql import DataFrame
from typing import Iterable, List, Optional, Tuple
from pyspark.sql.functions import count_distinct, col, to_timestamp, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from siesta_framework.model.DataModel import Last_checked_table_schema
import logging
from siesta_framework.core.sparkManager import get_spark_session
logger = logging.getLogger("ExtractPairs")

pair_schema = StructType([
    StructField("eventA", StringType(), True),
    StructField("eventB", StringType(), True),
    StructField("trace_id", StringType(), True),
    StructField("timestampA", IntegerType(), True),
    StructField("timestampB", IntegerType(), True),
    StructField("posA", IntegerType(), True),
    StructField("posB", IntegerType(), True)
])

single_schema = StructType([
    StructField("activity", StringType(), True),
    StructField("trace_id", StringType(), True),
    StructField("position", IntegerType(), True),
    StructField("start_timestamp", TimestampType(), True),
    StructField("attributes", StringType(), True)
])


type Trace_ID = str
type Event_Type = str
type Timestamp = str
type Position = int

type Event = Tuple[Event_Type, Timestamp, Position]
type Trace = Tuple[Trace_ID, Tuple[Event]]


def extract_pairs(updated_sequence_table_DF: DataFrame, last_checked: DataFrame | None, lookback: int) -> Tuple[DataFrame, DataFrame]:
    """
    Extracts the event type pairs from a DF that contains the complete traces. 

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
        single_DF: The DataFrame that contains the complete single inverted index. activity (str)|trace_id (str)|position (int)|    start_timestamp (ts)|          attributes (json)
        last_checked: The loaded values as a DataFrame from the LastChecked table (it should be None 
            if it is the first time that events are indexed in this log database).
        lookback: The parameter that describes the maximum time difference that two 
            events can have in order to create an event type pair.

    Returns:
        A DataFrame with the extracted event type pairs and their corresponding last timestamps per trace.
    """
    

    # Converting the timestamps to datetime objects for easier manipulation
    # batch_single_DF = batch_single_DF.withColumn("start_timestamp", unix_timestamp(col("start_timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))
    
    # single_rdd: RDD[Tuple[Trace_ID, Event]] = batch_single_DF.rdd.map(lambda row: (
    #     row.trace_id,
    #     (row.activity, row.start_timestamp, row.position)
    # ))
    updated_sequence_table_DF = updated_sequence_table_DF.withColumn("start_timestamp", unix_timestamp(col("start_timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))
    single_rdd: RDD[Tuple[Trace_ID, Event]] = updated_sequence_table_DF.rdd.map(lambda row: (
        row.trace_id,
        (row.activity, row.start_timestamp, row.position)
    ))
    
    last_checkedRDD = last_checked.rdd if last_checked else None
    
    
    if not last_checkedRDD or last_checkedRDD.count() == 0:
        logger.info("No previously indexed pairs found. Extracting all pairs from scratch.")
        full = single_rdd.groupByKey().map(lambda x: _calculate_pairs_stnm(x, None, lookback))
    else:
        last_checkedRDD_grouped = last_checkedRDD.map(lambda row: (
            row.trace_id,
            (row.eventA, row.eventB, row.last_checked_timestamp)
        )).groupByKey()
        
        full = single_rdd.groupByKey().leftOuterJoin(last_checkedRDD_grouped).map(
            lambda x: _calculate_pairs_stnm((x[0], x[1][0]), x[1][1], lookback)
        )
    
    pairs = full.flatMap(lambda x: x[0])
    last_checked_pairs = full.flatMap(lambda x: x[1])
    spark = get_spark_session()
    pairs_df = spark.createDataFrame(pairs, schema=pair_schema)
    last_checked_df = spark.createDataFrame(last_checked_pairs, schema=Last_checked_table_schema)
    
    logger.info(f"Extracted {pairs_df.count()} event pairs")
    logger.info(f"Extracted {last_checked_df.count()} last checked entries")

    return pairs_df, last_checked_df

def _calculate_pairs_stnm(single: Tuple[Trace_ID, Iterable[Event]], last: Optional[Iterable[Tuple[str, str, int]]] | None, lookback: int) -> Tuple[List[Tuple], List[Tuple]]:
    """
    Extract the event type pairs from the single inverted index.

    Args:
        single: The complete single inverted index.
        last: The list with all the last timestamps that correspond to this event.
        lookback: The parameter that describes the maximum time difference between 
            two events in a pair.

    Returns:
        tuple: A tuple where the first element is the extracted event type pairs 
            and the second element is the last timestamps for each event type.
    """
    trace_id: Trace_ID = single[0]
    events: Iterable[Event] = single[1]
    
    # Group events by event_name
    single_map = defaultdict(list)
    for event_name, timestamp, position in events:
        single_map[event_name].append((timestamp, position))
    
    # Build lastMap from last_checked
    last_map = {}
    if last:
        for eventA, eventB, timestamp in last:
            last_map[(eventA, eventB)] = timestamp
    
    # Get all unique event types
    all_events = list(single_map.keys())
    
    # Get all combinations
    combinations = _findCombinations(all_events)
    
    results = []
    new_last_checked = []
    
    for key1, key2 in combinations:
        ts1 = single_map.get(key1, [])
        ts2 = single_map.get(key2, [])
        
        if not ts1 or not ts2:
            continue
        
        last_checked_ts = last_map.get((key1, key2), None)
        
        # Detect all occurrences of this event type pair
        nres = createTuples(key1, key2, ts1, ts2, lookback, last_checked_ts, trace_id)
        
        # If there are any, append them and keep the last timestamp
        if nres:
            new_last_checked.append(nres[-1])
            results.extend(nres)
    
    # Convert to last_checked format: (eventA, eventB, trace_id, timestamp)
    last_checked_list = [
        (x[2], x[0], x[1], x[4])  # trace_id, eventA, eventB, timestampB 
        for x in new_last_checked
    ]
    
    return results, last_checked_list

    
def createTuples(
    key1: str, 
    key2: str, 
    ts1: List[Tuple[int, int]], 
    ts2: List[Tuple[int, int]],
    lookback: int, 
    last_checked: Optional[int],
    trace_id: str
) -> List[Tuple[str, str, str, str, str, int, int]]:
    """
    Creates event type pair tuples following Skip-till-next-match policy.
    
    Returns:
        List of tuples: (eventA, eventB, trace_id, timestampA, timestampB, posA, posB)
    """
    pairs = []
    lookback_delta_millis = lookback * 24 * 60 * 60 * 1000  # Convert lookback from days to milliseconds
    prev = None
    i = 0
    
    for ea_ts, ea_pos in ts1:
        # Evaluate based on previous and last_checked
        if ((prev is None or ea_ts >= prev) and 
            (last_checked is None or ea_ts >= last_checked)):
            
            stop = False
            while i < len(ts2) and not stop:
                eb_ts, eb_pos = ts2[i]
                
                if ea_pos >= eb_pos:
                    # Event a is not before event b, remove it
                    i += 1
                else:
                    # Event a is before event b 2025-01-01T09:00:00
                    if eb_ts - ea_ts <= lookback_delta_millis:
                        # Lookback satisfied, create pair
                        pairs.append((key1, key2, trace_id, ea_ts, eb_ts, ea_pos, eb_pos))
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