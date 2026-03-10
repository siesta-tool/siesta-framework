from datetime import datetime
from typing import Any, Dict
import boto3
from botocore.exceptions import ClientError
from fastapi import UploadFile
from pyspark import RDD
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.streaming import StreamingQuery
from siesta_framework.core.interfaces import StorageManager
from siesta_framework.model.StorageModel import MetaData, hash_str
from siesta_framework.model.DataModel import Event, EventConfig, Last_Checked_table_schema, EventPair, count_table_schema, Trace_metadata_table_schema
from siesta_framework.core.config import get_system_config
import siesta_framework.core.sparkManager as SparkManager
from delta.tables import DeltaTable

def _parse_timestamp(ts: str) -> datetime:
    """Parse a timestamp string, accepting both with and without milliseconds."""
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(ts, fmt)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse timestamp: {ts!r}")

import logging
logger = logging.getLogger("S3Manager")

class S3Manager(StorageManager):
    
    """
    S3-based implementation of the StorageManager interface.
    
    This class handles all I/O operations with S3 storage using Spark for distributed processing.
    It manages the various tables (Pairs Index, Sequence, activity_index, Active pairs, Count, Metadata) stored
    as Parquet files in S3.
    """
    
    name = "S3 Storage Manager"
    version = "1.0.0"
    type = "s3"
    
    spark: SparkSession
    config: Dict[str, Any]
    s3_client: boto3.Session.client
   

    def __init__(self):
        """Initialize the S3Manager with a spark manager instance and configuration."""
        
        self.spark = SparkManager.get_spark_session()
        self.config = get_system_config()
        
        # Initialize boto3 S3 client
        try:
            self.s3_client = self._create_s3_client()
        except Exception as e:
            logger.info(f"Error initializing S3 client: {e}")
            raise
        
        # Initialize Spark based on system configuration
        self.initialize_spark()
    
    def initialize_spark(self) -> None:
        """
        Initialize the Spark session configuration for S3 access.
        Assumes spark manager has already been started.

        """

        if self.spark is None:
            raise RuntimeError("Spark session not available. Ensure spark manager is started before initializing S3Manager.")
        
        if self.config.get("s3_access_key") and self.config.get("s3_secret_key"):
            self.spark.conf.set("spark.hadoop.fs.s3a.access.key", self.config["s3_access_key"])
            self.spark.conf.set("spark.hadoop.fs.s3a.secret.key", self.config["s3_secret_key"])
        
        if self.config.get("s3_endpoint"):
            self.spark.conf.set("spark.hadoop.fs.s3a.endpoint", self.config["s3_endpoint"])
 
        logger.info("Spark session configured for S3 access.")
    
    def initialize_db(self, preprocess_config: Dict[str, Any] = {}) -> None:
        """
        Create the appropriate bucket in S3.
        
        This method can optionally clear previous data based on configuration.

        """

        # Ensure bucket exists
        try:
            self.s3_client.head_bucket(Bucket=preprocess_config.get("storage_namespace", "siesta"))
            logger.info(f"Using existing bucket '{preprocess_config.get('storage_namespace', 'siesta')}'")

            # If overwrite_data is True, delete all objects of the specified log
            if preprocess_config.get("overwrite_data", False):
                prefix = f"{preprocess_config.get('log_name', 'default_log')}/"
                try:
                    paginator = self.s3_client.get_paginator('list_objects_v2')
                    pages = paginator.paginate(Bucket=preprocess_config.get("storage_namespace", "siesta"), Prefix=prefix)
                    
                    for page in pages:
                        if 'Contents' in page:
                            objects = [{'Key': obj['Key']} for obj in page['Contents']]
                            if objects:
                                self.s3_client.delete_objects(
                                    Bucket=preprocess_config.get("storage_namespace", "siesta"),
                                    Delete={'Objects': objects}
                                )
                    logger.info(f"Cleared existing log data in '{prefix}'")
                except ClientError as e:
                    logger.info(f"Error clearing existing data: {e}")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                # Bucket doesn't exist, create it
                try:
                    self.s3_client.create_bucket(Bucket=preprocess_config.get("storage_namespace", "siesta"))
                    logger.info(f"Created bucket '{preprocess_config.get('storage_namespace', 'siesta')}'")
                except ClientError as create_error:
                    logger.info(f"Error creating bucket: {create_error}")
                    raise
            else:
                logger.info(f"Error checking bucket: {e}")
                raise
        

        metadata = MetaData(
            storage_namespace=preprocess_config.get("storage_namespace", "siesta"),
            log_name=preprocess_config.get("log_name", "default_log"),
            storage_type=preprocess_config.get("storage_type", "s3")
        )

        # Check if sequence table already exists before creating
        try:
            self.spark.read.format("delta").load(metadata.sequence_table_path)
            logger.info(f"Sequence table already exists at {metadata.sequence_table_path}")
        except Exception:
            logger.info(f"Sequence table does not exist, will create new one")

            empty_seq_df = self.spark.createDataFrame([], schema=Event.get_schema())
            empty_seq_df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(metadata.sequence_table_path)
        
        # Check if the trace metadata table exists before creating
        try:
            self.spark.read.format("delta").load(metadata.trace_metadata_table_path)
            logger.info(f"Trace Metadata table already exists at {metadata.trace_metadata_table_path}")
        except Exception:
            logger.info(f"Trace Metadata table does not exist, will create new one")

            empty_trace_mtd_df = self.spark.createDataFrame([], schema=Trace_metadata_table_schema)
            empty_trace_mtd_df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(metadata.trace_metadata_table_path)

        
        # Check if Last Checked table already exists before creating
        try:
            self.spark.read.format("delta").load(metadata.last_checked_table_path)
            logger.info(f"Last Checked table already exists at {metadata.last_checked_table_path}")
        except Exception:
            logger.info(f"Last Checked table does not exist, will create new one")

            empty_last_checked_df = self.spark.createDataFrame([], schema=Last_Checked_table_schema)
            empty_last_checked_df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(metadata.last_checked_table_path)
        

        # Check if Pairs index table already exists before creating
        try:
            self.spark.read.format("delta").load(metadata.pairs_index_path)
            logger.info(f"Pairs Index table already exists at {metadata.pairs_index_path}")
        except Exception:
            logger.info(f"Pairs Index table does not exist, will create new one")

            empty_index_pairs = self.spark.createDataFrame([], schema=EventPair.get_schema())
            empty_index_pairs.write \
                .format("delta") \
                .partitionBy("source", "target") \
                .mode("overwrite") \
                .save(metadata.pairs_index_path)
        

        # Check if Count table already exists before creating
        try:
            self.spark.read.format("delta").load(metadata.count_table_path)
            logger.info(f"Count table already exists at {metadata.count_table_path}")
        except Exception:
            logger.info(f"Count table does not exist, will create new one")

            empty_count_df = self.spark.createDataFrame([], schema=count_table_schema)
            empty_count_df.write \
                .format("delta") \
                .mode("overwrite") \
                .partitionBy("source") \
                .save(metadata.count_table_path)

        logger.info(f"Database structure initialized at {metadata.count_table_path}")

    def _create_s3_client(self):
        """Create and configure boto3 S3 client.
        
        Args:
            config: Configuration dictionary containing S3 credentials and endpoint
            
        Returns:
            boto3 S3 client
        """
        s3_config = {}
        
        if self.config.get("s3_access_key") and self.config.get("s3_secret_key"):
            s3_config['aws_access_key_id'] = self.config["s3_access_key"]
            s3_config['aws_secret_access_key'] = self.config["s3_secret_key"]
        
        if self.config.get("s3_endpoint"):
            s3_config['endpoint_url'] = self.config["s3_endpoint"]
        
        return boto3.client('s3', **s3_config)


    def _resolve_kafka_servers(self) -> str:
        # Resolve Kafka address based on Spark deployment mode
        kafka_servers = self.config.get("kafka_bootstrap_servers", "localhost:9092")
        spark_master = self.config.get("spark_master", "local[*]")
        
        # If using remote Spark cluster (Docker), use Docker bridge IP for Kafka
        # This IP is accessible from BOTH the host (driver) and Docker containers (executors)
        if spark_master.startswith("spark://"):
            if "localhost" in kafka_servers or "127.0.0.1" in kafka_servers:
                try:
                    bridge_ip = SparkManager.get_docker_bridge_ip()
                    kafka_servers = kafka_servers.replace("localhost", bridge_ip).replace("127.0.0.1", bridge_ip)
                    logger.info(f"Using Docker bridge IP for Kafka: {kafka_servers}")
                except Exception as e:
                    logger.info(f"Warning - could not get bridge IP: {e}")
        else:
            # Local mode - localhost works fine
            logger.info(f"Using local Kafka address: {kafka_servers}")
        return kafka_servers

    def initialize_streaming_collector(self, preprocess_config: Dict[str, Any] = {}) -> StreamingQuery | None:
        """
        Set up streaming from Kafka to S3 if enabled in configuration.
        
        Args:
            preprocess_config: Configuration dictionary containing event settings
        """
        # Begin listening to kafka
        logger.info("Setting up streaming from Kafka for log " + preprocess_config.get("log_name", "default_log") + "...")
        from pyspark.sql.functions import col, from_json
        
        # Define schema for incoming JSON events using source field names
        schema = EventConfig.from_preprocess_config(preprocess_config, "json").get_source_schema()
        
        kafka_servers = self._resolve_kafka_servers()

        # Read streaming data from Kafka
        raw_events_streaming_df = (self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_servers)
            .option("subscribe", preprocess_config.get("kafka_topic", "default_log"))
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load())

        # Parse JSON from Kafka value field and write as JSON Lines
        parsed_events_df = raw_events_streaming_df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Store parsed events as JSON lines by 1-min triggering to bundle rows into file
        streaming_query = (parsed_events_df
            .writeStream
            .format("json")
            .option("path", self.get_steaming_collector_path(preprocess_config))
            .option("checkpointLocation", f"s3a://{self.config.get('storage_namespace', 'siesta')}/{preprocess_config.get('log_name', 'default_log')}/{self.config.get('checkpoint_dir', 'checkpoints')}/")
            .outputMode("append")
            .trigger(processingTime='10 seconds')
            .start())

        logger.info(f"Started streaming query from Kafka topic '{preprocess_config.get('kafka_topic', 'default_log')}' to S3.")
        return streaming_query
    
    def get_steaming_collector_path(self, preprocess_config: Dict[str, Any]) -> str:
        """
        Get the S3 path where the streaming collector stores data.
        
        Args:
            preprocess_config: Configuration dictionary containing event settings
            
        Returns:
            Path as a string
        """
        return f"s3a://{preprocess_config.get('storage_namespace', 'siesta')}/{preprocess_config.get('log_name', 'default_log')}/{preprocess_config.get('raw_events_dir', 'raw_events')}/"
    
    def get_checkpoint_location(self, metadata: MetaData, checkpoint_table: str = "example_table") -> str:
        """
        Get the S3 path for streaming checkpoint location.
        
        Args:
            log_name: Name of the log
            checkpoint_table: Name of the checkpoint table (e.g., 'index', 'collector')
            
        Returns:
            Path as a string
        """
        namespace = metadata.storage_namespace
        log_name = metadata.log_name
        return f"s3a://{namespace}/{log_name}/{self.config.get('checkpoint_dir', 'checkpoints')}/{checkpoint_table}/"


    def upload_file(self, preprocess_config: Dict[str, Any], local_path: str, destination_path: str) -> str:
        """
        Upload a local file to S3.
        
        Args:
            preprocess_config: Configuration dictionary passed during execution
            local_path: Path to the local file
            destination_path: Path/Name for the file in storage (key)
            
        Returns:
            The S3A URI to access the uploaded file
        """
        
        key = destination_path
        if key.startswith("/"):
            key = key[1:]
        key = f"{preprocess_config.get('log_name', 'default_log')}/batches/{key}"
            
        logger.info(f"Uploading '{local_path}' to bucket '{preprocess_config.get('storage_namespace', 'siesta')}' with key '{key}'...")
        try:
            self.s3_client.upload_file(local_path, preprocess_config.get('storage_namespace', 'siesta'), key)
            logger.info("Upload successful.")
            
            # Construct S3A URI for Spark
            return f"s3a://{preprocess_config.get('storage_namespace', 'siesta')}/{key}"
        except Exception as e:
            logger.info(f"Upload failed: {e}")
            raise
    
    def upload_file_object(self, preprocess_config: Dict[str, Any], file_obj: UploadFile, destination_path: str) -> str:
        """
        Upload an in-memory file (UploadFile) to S3.
        
        Args:
            preprocess_config: Configuration dictionary passed during execution
            file_obj: UploadFile object containing the file data
            destination_path: Path/Name for the file in storage (key)
        Returns:
            The S3A URI to access the uploaded file
        """
        
        key = destination_path
        if key.startswith("/"):
            key = key[1:]
        key = f"{preprocess_config.get('log_name', 'default_log')}/batches/{key}"

        logger.info(f"Uploading in-memory file to bucket '{preprocess_config.get('storage_namespace', 'siesta')}' with key '{key}'...")
        try:
            self.s3_client.upload_fileobj(file_obj.file, preprocess_config.get('storage_namespace', 'siesta'), key)
            logger.info("Upload successful.")
            
            # Construct S3A URI for Spark
            return f"s3a://{preprocess_config.get('storage_namespace', 'siesta')}/{key}"
        except Exception as e:
            logger.info(f"Upload failed: {e}")
            raise
    
    def close_spark(self) -> None:
        """Close the Spark connection using sparkManager."""
        SparkManager.shutdown()
        self.spark = None
        # Note: boto3 clients don't need explicit closing
        logger.info("Spark session closed.")
    
    
    def read_activity_index(self, metadata: MetaData) -> DataFrame:
        """
        Load the Activity index from S3 (stored in Activity index table).
        
        Args:
            metadata: MetaData object containing the metadata
            
        Returns:
            DataFrame containing Event objects
        """
        try:
            df = self.spark.read.parquet(metadata.activity_index_path) # type: ignore
            logger.info(f"Read {df.count()} records from the Activity index table")
            return df
        except Exception as e:
            logger.info(f"Error reading Activity Index: {e}")
            return self.spark.createDataFrame([], schema=Event.get_schema()) # type: ignore

    
    ###########################################
    ########### Pairs index Methods ###########
    ###########################################

    def read_pairs_index(self, metadata: Any) -> DataFrame:
        return None

    def write_pairs_index(self, new_pairs: DataFrame, metadata: MetaData) -> None:
        """
        Write the combined pairs to S3 IndexTable, grouped by interval and first event.
        
        Args:
            new_pairs: RDD containing newly generated pairs
            metadata: MetaData object containing the metadata
        """
        try:
            new_pairs.write.partitionBy("source", "target").format("delta").mode("append").save(metadata.pairs_index_path)
            
            # Update metadata
            # metadata.pair_count = new_pairs.count()
            logger.info(f"Wrote new pairs to {metadata.pairs_index_path}")
        except Exception as e:
            logger.info(f"Error writing IndexTable: {e}")
    
    ###########################################
    ########## MetaData Table Methods #########
    ###########################################

    def read_metadata_table(self, preprocess_config: Dict[str, Any], metadata: MetaData) -> MetaData:
        """
        Construct metadata by loading existing metadata from S3 or creating new.
        
        Args:
            preprocess_config: Configuration dictionary containing storage settings
            
        Returns:
            MetaData object containing the current stored metadata
        """

        log_name = preprocess_config.get("log_name", "default_log")
        namespace = preprocess_config.get("storage_namespace", "siesta")

        # Initialize MetaData object with defaults
        metadata.trace_count = 0
        metadata.event_count = 0
        metadata.pair_count = 0
        metadata.first_timestamp = None
        metadata.last_timestamp = None
        metadata.last_mined_timestamp = None
        metadata.approx_unique_traces = set()
        metadata.approx_unique_activities = set()
        try:
            # Try to read the Delta table directly - if it doesn't exist, an exception will be raised
            metadata_df = self.spark.read.format("delta").load(metadata.metadata_table_path)
            
            row = metadata_df.head(1)
            if row:
                r = row[0]
                metadata.trace_count = r.trace_count or 0
                metadata.event_count = r.event_count or 0
                metadata.pair_count = r.pair_count or 0
                metadata.first_timestamp = _parse_timestamp(r.first_timestamp) if r.first_timestamp is not None else None
                metadata.last_timestamp = _parse_timestamp(r.last_timestamp) if r.last_timestamp is not None else None
                metadata.last_mined_timestamp = _parse_timestamp(r.last_mined_timestamp) if r.last_mined_timestamp is not None else None
                metadata.approx_unique_traces = set(r.approx_unique_traces) if r.approx_unique_traces is not None else set()
                metadata.approx_unique_activities = set(r.approx_unique_activities) if r.approx_unique_activities is not None else set()
                metadata.storage_type = "s3"
                logger.info(f"Loaded existing metadata for {log_name}")
        except Exception as e:
            # If table doesn't exist or can't be read, use defaults
            logger.info(f"Metadata does not exist or failed to load for {log_name}. Initialized defaults.")
        return metadata
    
    def write_metadata_table(self, metadata: MetaData) -> None:
        """
        Persist metadata to S3 in Delta format.
        
        Args:
            metadata: MetaData object containing the metadata
        """
        # Convert MetaData object to dictionary
        metadata_dict = metadata.to_dict()

        # Create DataFrame and store 
        metadata_df = self.spark.createDataFrame([metadata_dict], schema=MetaData.get_schema())

        metadata_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(metadata.metadata_table_path)
        
        logger.info(f"Metadata written to {metadata.metadata_table_path}")



    ###########################################
    ########## Sequence Table Methods #########
    ###########################################

    def write_sequence_table(self, events_df: DataFrame, metadata: MetaData) -> None:
        """
        Write the Sequence Table to S3 in Delta format.

        Args:
            events_df: DataFrame containing processed events
            metadata: MetaData object containing the metadata
        """        
        try:
            delta_table = DeltaTable.forPath(self.spark, metadata.sequence_table_path)
            delta_table.alias("existing").merge(
                events_df.alias("new"),
                "existing.trace_id = new.trace_id "
                "AND existing.activity = new.activity "
                "AND existing.position = new.position"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()

            logger.info(f"S3 Manager wrote to sequence table")

            # Update metadata object
            metadata.first_timestamp = datetime(2026, 11, 10, 5)
            metadata.last_timestamp = datetime(2026, 11, 10, 5)
            metadata.event_count += events_df.count()
            metadata.trace_count += events_df.select(F.col("position") == 0).count()
        except Exception as e:
            logger.info(f"Error writing on {metadata.sequence_table_path}: {e}")
            raise


    def read_sequence_table(self, metadata: MetaData) -> DataFrame:
        """
        Read data as a DataFrame from the SequenceTable stored in S3.
        
        Args:
            metadata: MetaData object containing the metadata
        Returns:
            DataFrame containing Event objects
        """
        try:
            df = self.spark.read.format("delta").load(metadata.sequence_table_path)
            return df
        except Exception as e:
            logger.info(f"Error reading from {metadata.sequence_table_path}: {e}")
            return self.spark.createDataFrame([], schema=Event.get_schema())
    
    ####################################################
    ############## Trace Metadata Methods ##############
    ####################################################

    def read_trace_metadata_table(self, metadata: Any) -> DataFrame:
        """
        Read the trace metadata table
        """
        try:
            df = self.spark.read.format("delta").load(metadata.trace_metadata_table_path)
            return df
        except Exception as e:
            logger.info(f"Error reading from {metadata.trace_metadata_table_path}: {e}")
            return self.spark.createDataFrame([], schema=Trace_metadata_table_schema)
    
    def write_trace_metadata_table(self, trace_metadata_df: DataFrame, metadata: Any) -> None:
        """
        Write the Trace metadata table to the DB
        """
        try:
            trace_metadata_df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(metadata.trace_metadata_table_path)  
        except Exception as e:
            logger.info(f"Error writing on {metadata.trace_metadata_table_path}: {e}")
            raise


    ###########################################
    ######### Activity index Methods ##########
    ###########################################
    def write_activity_index(self, events_df: DataFrame, metadata: MetaData) -> None:
        """
        Write processed events to S3 Activity index in Delta format.
        
        Args:
            events_df: DataFrame containing processed events
            metadata: MetaData object containing the metadata
        """        
        try:
            events_df.write \
                .format("delta") \
                .partitionBy("activity") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(metadata.activity_index_path)
            
            # unique_activities = events_df.select("activity").distinct().rdd.map(lambda row: hash_str(row.activity)).collect()
            # globally_uninque_activities = set(unique_activities) - metadata.approx_unique_activities
            # logger.info(f"Wrote {len(globally_uninque_activities)} new activities to {metadata.activity_index_path}.")

            # Update metadata object
            # metadata.approx_unique_activities.update(globally_uninque_activities)            
        except Exception as e:
            logger.info(f"Error writing on {metadata.activity_index_path}: {e}")
            raise

    #################################################
    ########## Last Checked Table Methods ###########
    #################################################
    def write_last_checked_table(self, last_checked: DataFrame, metadata: MetaData) -> None:
        """
        Store new records for last checked timestamps to S3.
        
        Args:
            last_checked: DataFrame containing timestamp of last completion for each event type pair per trace
            metadata: MetaData object containing the metadata
        """
        try:
            
            last_checked.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(metadata.last_checked_table_path)
            logger.info(f"Wrote last checked data to {metadata.last_checked_table_path}")
        except Exception as e:
            logger.info(f"Error writing LastCheckedTable: {e}")
    
        
    def read_last_checked_table(self, metadata: MetaData) -> DataFrame:
        """
        Load data from the LastCheckedTable in S3.
        
        Args:
            metadata: MetaData object containing the metadata
            
        Returns:
            DataFrame with last timestamps per event type pair per trace
        """
        try:
            df = self.spark.read.format("delta").load(metadata.last_checked_table_path)
            return df
        except Exception as e:
            logger.info(f"Error reading LastCheckedTable: {e}")
            return self.spark.createDataFrame([], schema=Last_Checked_table_schema) # type: ignore
        
    #################################################
    ############## Count Table Methods ##############
    #################################################

    def read_count_table(self, metadata: MetaData) -> DataFrame:
        try:
            df = self.spark.read.format("delta").load(metadata.count_table_path)
            return df
        except Exception as e:
            logger.info(f"Error reading Count table: {e}")
            return self.spark.createDataFrame([], schema=count_table_schema)

    def write_count_table(self, count_df:DataFrame, metadata:MetaData) -> None:

        try:
            count_df.write\
                .format("delta")\
                .partitionBy("source")\
                .mode("overwrite")\
                .option("mergeSchema", "true")\
                .save(metadata.count_table_path)
        
        except Exception as e:
            logger.info(f"Error writing Count Table: {e}")
