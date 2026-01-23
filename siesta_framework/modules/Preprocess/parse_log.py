from siesta_framework.core.sparkManager import get_spark_session
from siesta_framework.core.storageFactory import get_storage_manager
from siesta_framework.core.config import get_config
from siesta_framework.model.DataModel import Event, EventConfig
from pyspark.sql import SparkSession
from pyspark import RDD
from datetime import datetime
import os

def parse_log_file() -> RDD:
    """
    Generic parsing function that determines the format and path from config.
    """
    config = get_config()
    log_path = config.get("log_path")
    
    
    if not log_path:
        raise ValueError("Log path not specified in configuration")

    if not os.path.exists(log_path):
        raise FileNotFoundError(f"Log file not found at path: {log_path}")

    spark = get_spark_session()
    if spark is None:
        raise RuntimeError("Spark session is not initialized.")
    
    storage = get_storage_manager()
    if not storage:
        raise RuntimeError("Storage manager is not initialized.")

    filename = os.path.basename(log_path)
    
    print(f"Uploading {log_path} to storage...")
    storage_path = storage.upload_file(log_path, filename)
    print(f"File uploaded to: {storage_path}")

    _, ext = os.path.splitext(log_path)
    log_format = ext.lower().lstrip('.')
    
    if log_format == 'xes':
        return parse_xml(storage_path, spark, config)
    elif log_format == 'csv':
        return parse_csv(storage_path, spark, config)
    else:
        raise ValueError(f"Unsupported log format: {log_format}")


def parse_xml(storage_path: str, spark: SparkSession, system_config: dict) -> RDD:
    """
    Parse_xml: Parses XES log file using Spark-XML and creates an RDD of Event objects.
    This approach is scalable and handles large files without loading everything into driver memory.
    
    Args:
        storage_path: Path to the log file in storage
        spark: Active Spark Session
        system_config: System configuration dictionary
    
    Returns:
        RDD containing Event objects
    """
    config = EventConfig.from_system_config(system_config, "xes")
    
    traces_df = spark.read.format("xml") \
        .option("rowTag", "trace") \
        .load(storage_path)
    
    
    # Transform traces DataFrame to events RDD
    def process_trace(row):
        """Process a single trace row and yield Event dicts"""

        events = []
        
        # Extract trace-level fields
        trace_field_values = {}
        trace_fields_config = config.get_trace_fields()
        
        """
        Check if there's string columns -> Turn them into an iterable list, which we call attributes.
        -> For each attribute, check if its key matches any trace-level field mapping. (eg. 'trace_id': 'concept:name')
         
        """
        if hasattr(row, 'string'):
            for attr in (row.string if isinstance(row.string, list) else [row.string]):
                if attr and hasattr(attr, '_key'):
                    attr_key = getattr(attr, '_key', None)
                    attr_value = getattr(attr, '_value', None)
                    
                    # Check if this attribute maps to any trace-level field
                    for field_name, source_key in trace_fields_config.items():
                        if source_key and attr_key == source_key:
                            trace_field_values[field_name] = attr_value
        
        # Set default for trace fields not found
        for field_name in trace_fields_config.keys():
            if field_name not in trace_field_values:
                trace_field_values[field_name] = f'{field_name}_{id(row)}'
        
        # Extract events from trace
        if not hasattr(row, 'event') or row.event is None:
            return events
            
        event_list = row.event if isinstance(row.event, list) else [row.event]
        
        for position, event in enumerate(event_list):
            # Initialize event field values with trace-level fields
            event_field_values = trace_field_values.copy()
            
            # Get event-level field mappings (excludes trace-level fields)
            event_fields_config = config.get_event_fields()
            
            # Storage for unmapped attributes
            extra_attributes = {}
            
            # Parse event attributes using configured mappings
            for attr_type in ['string', 'date', 'int', 'float', 'boolean']:
                if hasattr(event, attr_type):
                    attrs = getattr(event, attr_type)
                    attr_list = attrs if isinstance(attrs, list) else [attrs] if attrs else []
                    
                    for attr in attr_list:
                        if not attr:
                            continue
                        
                        attr_key = getattr(attr, '_key', None) or getattr(attr, 'key', None)
                        attr_value = getattr(attr, '_value', None) or getattr(attr, 'value', None)
                        
                        if not attr_key or attr_value is None:
                            continue
                        
                        # Check if this attribute maps to any configured Event field
                        mapped = False
                        matching_field = next(
                            ((event_field_name, source_key) for event_field_name, source_key in event_fields_config.items() 
                             if source_key and attr_key == source_key),
                            None
                        )
                        
                        if matching_field:
                            mapped = True
                            event_field_name, source_key = matching_field
                            # Parse timestamps if needed
                            if config.is_timestamp_field(event_field_name):
                                if isinstance(attr_value, str):
                                    try:
                                        attr_value = datetime.fromisoformat(attr_value.replace('Z', '+00:00'))
                                    except:
                                        attr_value = None
                            else:
                                attr_value = str(attr_value)
                            
                            event_field_values[event_field_name] = attr_value
                        
                        # If not mapped to a field, store in extra attributes
                        if not mapped:
                            if config.attributes_mapping and ("*" in config.attributes_mapping or attr_key in config.attributes_mapping):
                                extra_attributes[attr_key] = str(attr_value)
            
            # Handle computed fields (those with None as source_key)
            for event_field_name, source_key in event_fields_config.items():
                if config.is_computed_field(event_field_name):
                    # Handle position specially
                    if event_field_name == 'position':
                        event_field_values[event_field_name] = position
                    # Other computed fields get None by default
                    elif event_field_name not in event_field_values:
                        event_field_values[event_field_name] = None
            
            # Add extra attributes to the event
            event_field_values['attributes'] = extra_attributes
            
            # Create Event object dynamically using from_dict
            event_obj = Event.from_dict(event_field_values)
                
            events.append(event_obj)
        
        return events

    # Process traces in parallel and flatten to events
    events_rdd = traces_df.rdd.flatMap(process_trace)
    
    return events_rdd


def parse_csv(storage_path: str, spark: SparkSession, system_config: dict) -> RDD:
    """
    Parse_csv: Parses CSV log file using Spark CSV reader and creates an RDD of Event objects.
    
    Args:
        storage_path: Path to the log file in storage
        spark: Active Spark Session
        system_config: System configuration dictionary
    
    Returns:
        RDD containing Event objects
    """
    config = EventConfig.from_system_config(system_config, "csv")
    
    # Read CSV into DataFrame
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(storage_path)
    
    fields = config.get_event_fields().items() | config.get_trace_fields().items()
    source_keys = [source_key for _, source_key in fields if source_key]

    # Convert each row to Event object
    def row_to_event(row):
        event_field_values = {}
        extra_attributes = {}
        
        for field_name, source_key in fields:
            if source_key and source_key in row.__fields__:
                value = row[source_key]
                # Parse timestamps if needed
                if config.is_timestamp_field(field_name):
                    if isinstance(value, str):
                        try:
                            value = datetime.fromisoformat(value.replace('Z', '+00:00'))
                        except:
                            value = None
                event_field_values[field_name] = value
            elif config.is_computed_field(field_name):
                # Handle computed fields if needed
                # "position" field should not be set as computed in csv (arbitrary order in file)
                # but should be included as field (read on input file)
                event_field_values[field_name] = None
                        
        # Store unmapped columns as extra attributes
        for col in row.__fields__:
            if col not in source_keys:
                if config.attributes_mapping and ("*" in config.attributes_mapping or col in config.attributes_mapping):
                    extra_attributes[col] = str(row[col])
        
        event_field_values['attributes'] = extra_attributes
        
        return Event.from_dict(event_field_values)
    
    # Process event rows to RDD of Event objects
    events_rdd = df.rdd.map(row_to_event)

    return events_rdd