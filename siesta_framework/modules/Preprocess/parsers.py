from typing import Any, Dict
import pandas as pd
from lxml import etree
import tempfile
from siesta_framework.core.sparkManager import get_spark_session
from siesta_framework.core.storageFactory import get_storage_manager
from siesta_framework.model.DataModel import Event, EventConfig
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import StringType, IntegerType, MapType, ArrayType, TimestampType
from pyspark.sql.functions import monotonically_increasing_id, lit
from datetime import datetime
import os
import logging
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

from siesta_framework.model.StorageModel import MetaData


    
def process_events_batch(preprocess_config: Dict, batch_df, batch_id=None, metadata: MetaData = None) -> DataFrame | None:
    """
    Core processing logic for event batches; parses and stores events in Sequence Table.
    Args:
        batch_df: DataFrame containing the batch of events
        batch_id: Optional batch identifier for logging
    """
    if batch_df.isEmpty():
        return
    try:
        event_config = EventConfig.from_preprocess_config(preprocess_config, "json")
        events_df = _parse_rows(event_config, batch_df)
        
        get_storage_manager().write_sequence_table(events_df, metadata)
        # return events_df
    except Exception as e:
        batch_info = f"batch {batch_id}" if batch_id is not None else "batch"
        print(f"Error processing {batch_info}: {e}")



def _cast_value_by_schema(field_name: str, value, config: EventConfig):
    """Cast a value to the appropriate type based on Event schema.
    
    Args:
        field_name: Name of the Event field
        value: Raw value to cast
        config: EventConfig instance for timestamp field detection
    
    Returns:
        Casted value according to Event schema
    """
    if value is None:
        return None
    
    schema = Event.get_schema()
    field_type = None
    
    # Find the field type in schema
    for field in schema.fields:
        if field.name == field_name:
            field_type = field.dataType
            break
    
    if field_type is None:
        # Field not in schema, return as string
        return str(value)
    
    # Cast based on type
    if isinstance(field_type, IntegerType):
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    elif isinstance(field_type, StringType):
        # For timestamp fields, convert to ISO format string if needed
        if config.is_timestamp_field(field_name):
            if isinstance(value, datetime):
                return value.isoformat(timespec="seconds")
            elif isinstance(value, str):
                try:
                    dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
                    return dt.isoformat()
                except:
                    return value
        return str(value)
    elif isinstance(field_type, MapType):
        # For attributes, ensure it's a dict with string values
        if isinstance(value, dict):
            return {str(k): str(v) for k, v in value.items()}
        return {}
    
    return value


def process_event_log(preprocess_config: dict, metadata: MetaData) -> DataFrame:
    """
    Generic parsing function that determines the format and path from config.
    """
    log_path = preprocess_config.get("log_path")
    if not log_path:
        raise ValueError("Log path not specified in configuration")
    filename = os.path.basename(log_path)
    spark = get_spark_session()
    if spark is None:
        raise RuntimeError("Spark session is not initialized.")
    
    storage = get_storage_manager()
    if not storage:
        raise RuntimeError("Storage manager is not initialized.")
    
    # If local, verify file exists and upload
    if os.path.exists(log_path):
        print(f"Uploading {log_path} to storage...")
        log_path = storage.upload_file(preprocess_config, log_path, filename)
        print(f"File uploaded to: {log_path}")
    
    # Else, assume log_path is already in storage (e.g., s3a://...)

    _, ext = os.path.splitext(filename)
    log_format = ext.lower().lstrip('.')
    
    if log_format == 'xes':
        events_df = parse_xml(log_path, spark, preprocess_config)
    elif log_format == 'csv':
        events_df = parse_csv(log_path, spark, preprocess_config)
    else:
        raise ValueError(f"Unsupported log format: {log_format}")
    storage.write_sequence_table(events_df, metadata)
    return events_df


def parse_xml(storage_path: str, spark: SparkSession, preprocess_config: dict) -> DataFrame:
    """
    Parse XES log file using lxml's iterparse for streaming, memory-efficient parsing.

    XML files are not splittable, so Spark's XML reader processes them in a single
    partition anyway while adding schema-inference overhead and expensive nested-array
    operations. lxml (a C library) streams through the same file 10-50x faster and
    produces a flat list of events that converts to a Spark DataFrame via Arrow.

    Args:
        storage_path: Path to the XES file (local path or s3a:// URI)
        spark: Active Spark Session
        preprocess_config: Preprocess configuration dictionary

    Returns:
        DataFrame matching Event.get_schema()
    """
    logger = logging.getLogger("Preprocess.parse_xml")
    eventConfig = EventConfig.from_preprocess_config(preprocess_config, "xes")

    # --- Resolve file to a local path (lxml needs a local file or file-like object) ---
    local_path = _resolve_to_local_path(storage_path, spark)

    # --- Build field-to-XES-key lookups from EventConfig ---
    trace_fields = eventConfig.get_trace_fields()   # e.g. {'trace_id': 'concept:name'}
    event_fields = eventConfig.get_event_fields()    # e.g. {'activity': 'concept:name', 'start_timestamp': 'time:timestamp', 'position': None}
    timestamp_fields = eventConfig.timestamp_fields  # e.g. {'start_timestamp'}

    # Invert: XES key -> (field_name, is_trace_level)
    trace_key_map = {v: k for k, v in trace_fields.items() if v is not None}
    event_key_map = {v: k for k, v in event_fields.items() if v is not None}

    # Mapped source keys to exclude from attributes
    mapped_source_keys = frozenset(
        v for v in eventConfig.field_mappings.values() if v is not None
    )
    collect_attrs = bool(eventConfig.attributes_mapping)
    attrs_wildcard = collect_attrs and "*" in eventConfig.attributes_mapping
    attrs_keep_keys = (
        frozenset(eventConfig.attributes_mapping) - mapped_source_keys
        if collect_attrs and not attrs_wildcard
        else frozenset()
    )

    # --- XES tag names that carry key/value attributes ---
    XES_ATTR_TAGS = frozenset(('string', 'date', 'int', 'float', 'boolean'))

    # --- Streaming parse with lxml iterparse ---
    logger.info(f"Parsing XES file with lxml iterparse: {storage_path}")

    activities = []
    trace_ids = []
    positions = []
    timestamps = []
    attr_dicts = []

    cur_trace_vals: dict = {}  # trace-level field values
    cur_position = 0


    # Bypassing namespace for lxml - This may be jank
    def iter_no_ns(path):
        for event, elem in etree.iterparse(path, events=('start', 'end')):
            # This is the "Face Value" trick: Remove the namespace prefix from the tag
            if '}' in elem.tag:
                elem.tag = elem.tag.split('}', 1)[1] 
            yield event, elem

    # --- Streaming parse ---
    context = iter_no_ns(local_path)

    for action, elem in context:
        
        if action == 'start' and elem.tag == 'trace':
            cur_trace_vals = {}
            cur_position = 0
            continue

        if action == 'end' and elem.tag == 'event':
            evt_vals: dict = {}
            attrs: dict = {}

            for child in elem:
                if child.tag in XES_ATTR_TAGS:
                    k = child.get('key')
                    v = child.get('value')

                    if k in event_key_map:
                        field_name = event_key_map[k]
                        if field_name in timestamp_fields and v:
                            evt_vals[field_name] = v[:19]  # trim to seconds ISO
                        else:
                            evt_vals[field_name] = v
                    elif collect_attrs:
                        if attrs_wildcard and k not in mapped_source_keys:
                            attrs[k] = str(v) if v is not None else None
                        elif k in attrs_keep_keys:
                            attrs[k] = str(v) if v is not None else None

            activities.append(evt_vals.get('activity'))
            trace_ids.append(cur_trace_vals.get('trace_id'))
            positions.append(cur_position)
            timestamps.append(evt_vals.get('start_timestamp'))
            attr_dicts.append(attrs)
            cur_position += 1
            elem.clear()

        elif action == 'end' and elem.tag == 'trace':
            # Extract trace-level fields from trace's direct children
            if not cur_trace_vals:
                for child in elem:
                    if child.tag in XES_ATTR_TAGS:
                        k = child.get('key')
                        if k in trace_key_map:
                            cur_trace_vals[trace_key_map[k]] = child.get('value')
                    if len(cur_trace_vals) == len(trace_key_map):
                        break  # found all trace fields, stop scanning

                # Backfill trace-level values for all events in this trace
                trace_id = cur_trace_vals.get('trace_id')
                start = len(trace_ids) - cur_position
                for i in range(start, len(trace_ids)):
                    trace_ids[i] = trace_id

            elem.clear()

    logger.info(f"Parsed {len(activities)} events from XES file.")

    # --- Build pandas DataFrame and convert to Spark via Arrow ---
    pdf = pd.DataFrame({
        'activity': activities,
        'trace_id': trace_ids,
        'position': positions,
        'start_timestamp': timestamps,
        'attributes': attr_dicts,
    })

    # Free memory from lists immediately
    del activities, trace_ids, positions, timestamps, attr_dicts

    # Convert to Spark DataFrame (uses Arrow serialization automatically)
    schema = Event.get_schema()
    events_df = spark.createDataFrame(pdf, schema=schema)

    logger.info("Spark DataFrame created from parsed XES data.")
    return events_df


def _resolve_to_local_path(storage_path: str, spark: SparkSession) -> str:
    """Download a remote file (e.g. s3a://) to a local temp file for lxml, or
    return as-is if already local."""
    if os.path.exists(storage_path):
        return storage_path

    # Remote path — use Hadoop FileSystem API to download
    logger = logging.getLogger("Preprocess.parse_xml")
    logger.info(f"Downloading remote file to local temp: {storage_path}")

    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(storage_path)
    fs = path.getFileSystem(hadoop_conf)

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xes")
    local_path = tmp.name
    tmp.close()

    fs.copyToLocalFile(False, path, jvm.org.apache.hadoop.fs.Path(local_path))
    logger.info(f"Downloaded to {local_path}")
    return local_path


def parse_csv(storage_path: str, spark: SparkSession, system_config: dict) -> DataFrame:
    """
    Parse_csv: Parses CSV log file using Spark CSV reader and creates a DataFrame of Event objects.
    
    Args:
        storage_path: Path to the log file in storage
        spark: Active Spark Session
        system_config: System configuration dictionary
    
    Returns:
        DataFrame containing Event objects
    """
    config = EventConfig.from_preprocess_config(system_config, "csv")
    
    # Read CSV into DataFrame
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(storage_path)
    
    return _parse_rows(config, df)

def parse_json(storage_path: str, spark: SparkSession, system_config: dict) -> DataFrame:
    """
    Parse_json: Parses JSON log file using Spark JSON reader and creates an DataFrame of Event objects.
    
    Args:
        storage_path: Path to the log file in storage
        spark: Active Spark Session
        system_config: System configuration dictionary
    
    Returns:
        DataFrame containing Event objects
    """
    config = EventConfig.from_preprocess_config(system_config, "json")
    
    # Read JSON into DataFrame
    df = spark.read.format("json") \
        .option("multiline", "true") \
        .load(storage_path) 
    
    return _parse_rows(config, df)

def _parse_rows(config: EventConfig, df: DataFrame) -> DataFrame:
    """Parse raw DataFrame rows into Event schema using pure DataFrame operations.
    Avoids RDD operations entirely for optimal PySpark performance."""
    fields = dict(config.get_event_fields().items() | config.get_trace_fields().items())
    source_keys = {v for v in fields.values() if v is not None}

    # Get the source column name for trace_id to use in partitioning
    trace_id_source = config.field_mappings.get('trace_id')

    # Add position column using window function
    if trace_id_source and trace_id_source in df.columns:
        df = df.withColumn("position", row_number().over(
            Window.partitionBy(trace_id_source).orderBy(trace_id_source)
        ))
    else:
        df = df.withColumn("position", row_number().over(
            Window.orderBy(monotonically_increasing_id())
        ))

    schema = Event.get_schema()
    schema_type_map = {f.name: f.dataType for f in schema.fields}
    result_df = df

    # Map source columns to Event field names with appropriate type casts
    for field_name, source_key in fields.items():
        if source_key and source_key in df.columns:
            target_type = schema_type_map.get(field_name)
            if config.is_timestamp_field(field_name):
                # Normalize timestamps to ISO-8601 string representation
                result_df = result_df.withColumn(
                    field_name,
                    F.date_format(
                        F.col(source_key).cast("timestamp"),
                        "yyyy-MM-dd'T'HH:mm:ss"
                    )
                )
            elif target_type and isinstance(target_type, IntegerType):
                result_df = result_df.withColumn(field_name, F.col(source_key).cast("int"))
            else:
                result_df = result_df.withColumn(field_name, F.col(source_key).cast("string"))
        elif config.is_computed_field(field_name):
            if field_name != 'position':  # position already assigned above
                result_df = result_df.withColumn(field_name, F.lit(None))

    # Build attributes map from unmapped columns
    _EMPTY_MAP = F.create_map().cast(MapType(StringType(), StringType()))
    original_cols = set(df.columns) - {'position'}
    unmapped_cols = sorted(original_cols - source_keys)

    if config.attributes_mapping and unmapped_cols:
        if "*" in config.attributes_mapping:
            attr_cols = unmapped_cols
        else:
            attr_cols = [c for c in unmapped_cols if c in config.attributes_mapping]

        if attr_cols:
            kv_pairs = []
            for c in attr_cols:
                kv_pairs.extend([F.lit(c), F.col(c).cast("string")])
            result_df = result_df.withColumn("attributes", F.create_map(*kv_pairs))
        else:
            result_df = result_df.withColumn("attributes", _EMPTY_MAP)
    else:
        result_df = result_df.withColumn("attributes", _EMPTY_MAP)

    # Final projection matching Event schema
    return result_df.select(*[
        F.col(f.name).cast(f.dataType).alias(f.name) if f.name in result_df.columns
        else F.lit(None).cast(f.dataType).alias(f.name)
        for f in schema.fields
    ])


def upload_log_file_object(preprocess_config: dict, file: Any, destination_path: str) -> str:
    """
    Uploads an in-memory log file (UploadFile) to storage and returns the S3 path.
    """
    storage = get_storage_manager()
    if not storage:
        raise RuntimeError("Storage manager is not initialized.")

    print(f"Uploading file object to storage as {destination_path}...")
    s3_path = storage.upload_file_object(preprocess_config, file, destination_path)
    print(f"Parser: File uploaded to: {s3_path}")
    #TODO: handle s3 path
    return s3_path

