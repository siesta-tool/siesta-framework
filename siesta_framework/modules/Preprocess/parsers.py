import struct
from typing import Any, Dict
from pyspark import RDD
from siesta_framework.core.logger import timed
from siesta_framework.core.sparkManager import get_spark_session
from siesta_framework.core.storageFactory import get_storage_manager
from siesta_framework.model.DataModel import Event, EventConfig
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import StringType, IntegerType, MapType, ArrayType, TimestampType
from pyspark.sql.functions import monotonically_increasing_id, lit
from datetime import datetime
import os
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
    # timed(events_df.collect, "ParseXML & upload: ")
    # Updating positions in case of pre-existing data in sequence table
    pre_existing_seq_table = storage.read_sequence_table(metadata)
    print("[ABCD] A")
    # max_position = pre_existing_seq_table.agg({"position": "max"}).collect()[0][0] if pre_existing_seq_table.count() > 0 else 0
    # events_df = events_df.withColumn("position", col("position") + max_position)
    print("[ABCD] B")
    # timed(events_df.collect, "Read & update positions: ")
    storage.write_sequence_table(events_df, metadata)
    return events_df


def parse_xml(storage_path: str, spark: SparkSession, preprocess_config: dict) -> DataFrame:
    """
    Parse XES log file using pure Spark DataFrame operations for optimal performance.
    Avoids RDD operations and Python UDFs by leveraging Spark's native higher-order
    functions (filter, transform, posexplode, map_from_entries) which execute entirely
    in the JVM without Python serialization overhead.

    Args:
        storage_path: Path to the log file in storage
        spark: Active Spark Session
        preprocess_config: Preprocess configuration dictionary

    Returns:
        DataFrame containing Event objects
    """
    eventConfig = EventConfig.from_preprocess_config(preprocess_config, "xes")

    # --- Read XES file ---
    traces_df = spark.read.format("xml") \
        .option("rowTag", "trace") \
        .load(storage_path)

    # --- Schema introspection helpers ---
    XES_ATTR_TYPES = ('string', 'date', 'int', 'float', 'boolean')

    def _available_attr_types(schema):
        """Return XES attribute-type column names present in the given schema."""
        return [f.name for f in schema.fields if f.name in XES_ATTR_TYPES]

    def _value_data_type(schema, attr_type_name):
        """Resolve the Spark data type of `_value` inside an attribute-type column."""
        field = schema[attr_type_name]
        elem = field.dataType.elementType if isinstance(field.dataType, ArrayType) else field.dataType
        for f in elem.fields:
            if f.name == '_value':
                return f.dataType
        return StringType()

    def _extract_as_string(arr_col, key, value_type):
        """Extract `_value` from array<{_key, _value}> where `_key == key`.
        Executes entirely in the JVM via Spark's higher-order `filter` function."""
        filtered = F.filter(arr_col, lambda x: x["_key"] == F.lit(key))
        raw = F.when(F.size(filtered) > 0, F.element_at(filtered, 1)["_value"])
        if isinstance(value_type, TimestampType):
            return F.date_format(raw, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
        return raw.cast("string")

    def _normalize_to_kv(arr_col, value_type):
        """Transform an attribute array to array<struct<key:string, value:string>>."""
        if isinstance(value_type, TimestampType):
            return F.transform(arr_col, lambda x: F.struct(
                x["_key"].alias("key"),
                F.date_format(x["_value"], "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("value")
            ))
        return F.transform(arr_col, lambda x: F.struct(
            x["_key"].alias("key"),
            x["_value"].cast("string").alias("value")
        ))

    # --- Ensure columns are ArrayType (spark-xml may infer structs for single items) ---
    trace_schema = traces_df.schema
    trace_attr_types = _available_attr_types(trace_schema)

    for at in trace_attr_types:
        if not isinstance(trace_schema[at].dataType, ArrayType):
            traces_df = traces_df.withColumn(at, F.array(F.col(at)))

    if "event" in [f.name for f in trace_schema.fields]:
        if not isinstance(trace_schema["event"].dataType, ArrayType):
            traces_df = traces_df.withColumn("event", F.array(F.col("event")))

    # --- Extract trace-level fields as top-level columns ---
    trace_fields = eventConfig.get_trace_fields()  # e.g. {'trace_id': 'concept:name'}
    for field_name, source_key in trace_fields.items():
        if source_key:
            extractions = [
                _extract_as_string(F.col(at), source_key, _value_data_type(trace_schema, at))
                for at in trace_attr_types
            ]
            traces_df = traces_df.withColumn(
                f"__trace_{field_name}",
                F.coalesce(*extractions) if extractions else F.lit(None).cast("string")
            )

    # --- Explode events with position index (posexplode is 0-based, matching original enumerate) ---
    trace_cols = [F.col(f"__trace_{f}") for f in trace_fields if trace_fields[f]]
    events_df = traces_df.select(*trace_cols, F.posexplode("event").alias("position", "evt"))

    # --- Introspect the event-level nested schema ---
    event_col_type = traces_df.schema["event"].dataType
    event_struct = event_col_type.elementType if isinstance(event_col_type, ArrayType) else event_col_type
    event_attr_types = _available_attr_types(event_struct)

    # Flatten event attribute sub-arrays into top-level columns (ensures ArrayType)
    for at in event_attr_types:
        evt_field = event_struct[at]
        if not isinstance(evt_field.dataType, ArrayType):
            events_df = events_df.withColumn(f"__evt_{at}", F.array(F.col(f"evt.{at}")))
        else:
            events_df = events_df.withColumn(f"__evt_{at}", F.col(f"evt.{at}"))

    # --- Extract event-level fields ---
    event_fields = eventConfig.get_event_fields()  # e.g. {'activity': 'concept:name', 'position': None, ...}
    for field_name, source_key in event_fields.items():
        if source_key:
            extractions = [
                _extract_as_string(F.col(f"__evt_{at}"), source_key, _value_data_type(event_struct, at))
                for at in event_attr_types
            ]
            events_df = events_df.withColumn(
                field_name,
                F.coalesce(*extractions) if extractions else F.lit(None).cast("string")
            )
        elif eventConfig.is_computed_field(field_name):
            if field_name != 'position':  # position already comes from posexplode
                events_df = events_df.withColumn(field_name, F.lit(None))

    # Promote trace-level columns to their final names
    for field_name, source_key in trace_fields.items():
        if source_key:
            events_df = events_df.withColumnRenamed(f"__trace_{field_name}", field_name)

    # --- Build the attributes map column ---
    mapped_source_keys = [v for v in eventConfig.field_mappings.values() if v is not None]
    empty_kv_array = F.from_json(F.lit("[]"), "array<struct<key:string,value:string>>")

    if eventConfig.attributes_mapping:
        # Normalise every attribute-type array to a uniform [{key, value}] schema
        norm_parts = []
        for at in event_attr_types:
            vtype = _value_data_type(event_struct, at)
            norm = F.coalesce(_normalize_to_kv(F.col(f"__evt_{at}"), vtype), empty_kv_array)
            norm_parts.append(norm)

        if norm_parts:
            all_attrs = F.concat(*norm_parts) if len(norm_parts) > 1 else norm_parts[0]

            if "*" in eventConfig.attributes_mapping:
                # Keep all attributes whose key is NOT already mapped to a named field
                filtered_attrs = F.filter(all_attrs, lambda x: ~x["key"].isin(mapped_source_keys))
            else:
                keep_keys = list(set(eventConfig.attributes_mapping) - set(mapped_source_keys))
                filtered_attrs = F.filter(all_attrs, lambda x: x["key"].isin(keep_keys))

            attributes_col = F.when(
                F.size(filtered_attrs) > 0, F.map_from_entries(filtered_attrs)
            ).otherwise(F.map_from_entries(empty_kv_array))
        else:
            attributes_col = F.map_from_entries(empty_kv_array)
    else:
        attributes_col = F.map_from_entries(empty_kv_array)

    events_df = events_df.withColumn("attributes", attributes_col)

    # --- Timestamp normalisation: replace trailing Z with +00:00 for ISO 8601 ---
    for field_name in eventConfig.timestamp_fields:
        if field_name in events_df.columns:
            events_df = events_df.withColumn(
                field_name,
                F.regexp_replace(F.col(field_name), "Z$", "+00:00")
            )

    # --- Final projection matching Event schema ---
    schema = Event.get_schema()
    return events_df.select(*[
        F.col(f.name).cast(f.dataType).alias(f.name) if f.name in events_df.columns
        else F.lit(None).cast(f.dataType).alias(f.name)
        for f in schema.fields
    ])


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
    fields = config.get_event_fields().items() | config.get_trace_fields().items()
    source_keys = [source_key for _, source_key in fields if source_key]

    # Get the source column name for trace_id to use in partitioning
    trace_id_source = config.field_mappings.get('trace_id')
    
    # Convert each row to Event object
    def row_to_event(row):
        event_field_values = {}
        extra_attributes = {}
        
        for field_name, source_key in fields:
            if source_key and source_key in row.__fields__:
                value = row[source_key]
                # Cast value according to Event schema
                value = _cast_value_by_schema(field_name, value, config)
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

    # Use the source field name for trace_id when partitioning
    if trace_id_source and trace_id_source in df.columns:
        df = df.withColumn("position", row_number().over(Window.partitionBy(trace_id_source).orderBy(trace_id_source)))
    else:
        # Fallback: assign global position if no trace_id available
        df = df.withColumn("position", row_number().over(Window.orderBy(monotonically_increasing_id())))
    
    events_rdd = df.rdd.map(row_to_event)
    event_dicts_rdd = events_rdd.map(lambda event: event.to_dict())
    events_df = get_spark_session().createDataFrame(event_dicts_rdd, schema=Event.get_schema())
    
    return events_df


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

