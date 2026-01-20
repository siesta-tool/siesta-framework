from siesta_framework.core.sparkManager import get_spark_session
from siesta_framework.core.storageFactory import get_storage_manager
from siesta_framework.model.DataModel import Event, Activity
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType
from pyspark import RDD
from datetime import datetime


def parse_xml() -> RDD:
    """
    Parse_xml: Parses XES log file using Spark-XML and creates an RDD of Event objects.
    This approach is scalable and handles large files without loading everything into driver memory.
    
    Returns:
        RDD containing Event objects (as dicts for serialization)
    """
    spark = get_spark_session()
    if spark is None:
        raise RuntimeError("Spark session is not initialized.")
    
    storage = get_storage_manager()
    if not storage:
        raise RuntimeError("Storage manager is not initialized.")

    local_xes_path = "/home/balaktsis/Projects/siesta-framework/siesta_framework/modules/Preprocess/test.xes"
    xes_filename = local_xes_path.split("/")[-1]
    
    print(f"Uploading {local_xes_path} to storage...")
    s3_path = storage.upload_file(local_xes_path, xes_filename)
    print(f"File uploaded to: {s3_path}")
    
    traces_df = spark.read.format("xml") \
        .option("rowTag", "trace") \
        .load(s3_path)
    
    
    # Transform traces DataFrame to events RDD
    def process_trace(row):
        """Process a single trace row and yield Event dicts"""
        
        events = []
        
        # Extract trace ID from trace attributes
        trace_attrs = {}
        if hasattr(row, 'string'):
            for attr in (row.string if isinstance(row.string, list) else [row.string]):
                if attr and hasattr(attr, '_key') and attr._key == 'concept:name':
                    trace_attrs['concept:name'] = getattr(attr, '_value', None)


        trace_id = trace_attrs.get('concept:name', f'trace_{id(row)}')
        
        # Extract events from trace
        if not hasattr(row, 'event') or row.event is None:
            return events
            
        event_list = row.event if isinstance(row.event, list) else [row.event]
        
        for position, event in enumerate(event_list):
            # Extract event attributes
            event_attrs = {}
            activity_name = 'Unknown'
            timestamp = None
            
            # Parse event attributes (string, date, etc.)
            for attr_type in ['string', 'date', 'int', 'float', 'boolean']:
                if hasattr(event, attr_type):
                    attrs = getattr(event, attr_type)
                    attr_list = attrs if isinstance(attrs, list) else [attrs] if attrs else []
                    
                    for attr in attr_list:
                        if not attr:
                            continue
                        key = getattr(attr, '_key', None)
                        value = getattr(attr, '_value', None)
                        
                        if key and value:
                            if key == 'concept:name':
                                activity_name = str(value)
                            elif key == 'time:timestamp':
                                # Parse timestamp
                                if isinstance(value, str):
                                    try:
                                        timestamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
                                    except:
                                        timestamp = None
                                else:
                                    timestamp = value
                            else:
                                event_attrs[key] = str(value)
            
            event_dict = {
                "activity": activity_name,
                "trace_id": trace_id,
                "position": position,
                "start_timestamp": timestamp.isoformat() if timestamp else None,
                "end_timestamp": timestamp.isoformat() if timestamp else None
            }
            
            # Add attributes as a Map to preserve schema consistency
            if event_attrs:
                event_dict["attributes"] = event_attrs
            else:
                event_dict["attributes"] = {}
                
            events.append(event_dict)
        
        return events
    
    # Process traces in parallel and flatten to events
    events_rdd = traces_df.rdd.flatMap(process_trace)
    
    # Get statistics
    total_events = events_rdd.count()
    num_traces = traces_df.count()
    
    print(f"Created RDD with {total_events} events from {num_traces} traces")
    
    return events_rdd
