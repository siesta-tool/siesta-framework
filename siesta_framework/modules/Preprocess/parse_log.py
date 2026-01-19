from siesta_framework.core.sparkManager import get_spark_session
from siesta_framework.model.DataModel import Event, Activity
from pyspark.sql import SparkSession, DataFrame
from pyspark import RDD
import pm4py
from datetime import datetime


def parse_xml() -> RDD:
    """
    Parse_xml: Parses XES log file and creates an RDD of Event objects.
    
    Returns:
        RDD containing Event objects (as dicts for serialization)
    """
    spark = get_spark_session()
    if spark is None:
        raise RuntimeError("Spark session is not initialized.")

    # Parse the XES file
    log = pm4py.objects.log.importer.xes.importer.apply("/mnt/datasets/bpic2017.xes")
    
    # Convert pm4py log to list of Event dicts
    # We convert to dicts because Event objects can't be pickled directly
    events_as_dicts = []
    for trace in log:
        trace_id = trace.attributes.get('concept:name', str(id(trace)))
        
        for position, event in enumerate(trace):

            timestamp = event.get('time:timestamp', None)
            start_ts = None
            end_ts = None
            if timestamp and isinstance(timestamp, datetime):
                start_ts = timestamp
                end_ts = timestamp
            
            # Collect additional attributes
            attributes = {}
            for k, v in event.items():
                if k not in ['concept:name', 'time:timestamp']:
                    attributes[k] = str(v)
            
            # Create Activity instance
            activity = Activity()
            activity.name = event.get('concept:name', 'Unknown')
            activity.attributes = attributes if attributes else None
            
            # Create Event instance
            event_obj = Event()
            event_obj.activity = activity
            event_obj.trace_id = trace_id
            event_obj.position = position
            event_obj.start_timestamp = start_ts
            event_obj.end_timestamp = end_ts    
             
            # Collect converted-to-dict for serialization
            events_as_dicts.append(event_obj.to_dict())
    
    total_events = len(events_as_dicts)
    num_traces = len(log)
    
    # Free the pm4py log from memory
    del log
    
    # Create RDD using batched parallelize to avoid OOM on large datasets
    events_rdd = _batched_parallelize(spark, events_as_dicts, batch_size=50000)
    
    print(f"Created RDD with {total_events} events from {num_traces} traces")
    
    return events_rdd


def _batched_parallelize(spark: SparkSession, data: list, batch_size: int = 50000) -> RDD:
    """
    Parallelize a large list in batches to avoid driver OOM.

    Args:
        spark: SparkSession instance
        data: List of items to parallelize
        batch_size: Number of items per batch (default 50000)
        
    Returns:
        Combined RDD containing all items
    """
    if len(data) <= batch_size:
        # Small enough to parallelize directly
        return spark.sparkContext.parallelize(data, numSlices=max(10, len(data) // 1000))
    
    # Create RDDs in batches and union them
    rdds = []
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        num_partitions = max(10, len(batch) // 1000)
        rdd = spark.sparkContext.parallelize(batch, numSlices=num_partitions)
        rdds.append(rdd)
        # Clear the batch from memory
        del batch
    
    # Clear original data
    data.clear()
    
    # Union all RDDs
    combined_rdd = rdds[0]
    for rdd in rdds[1:]:
        combined_rdd = combined_rdd.union(rdd)
    
    return combined_rdd
