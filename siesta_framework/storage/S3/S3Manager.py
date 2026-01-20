from typing import Any, Dict
import boto3
from botocore.exceptions import ClientError
from pyspark import RDD
from pyspark.sql import DataFrame
from siesta_framework.core.interfaces import StorageManager
from siesta_framework.model.StorageModel import MetaData
from siesta_framework.model.DataModel import Event


class S3Manager(StorageManager):
    
    """
    S3-based implementation of the StorageManager interface.
    
    This class handles all I/O operations with S3 storage using Spark for distributed processing.
    It manages the various tables (Index, Sequence, Single, LastChecked, Count, Metadata) stored
    as Parquet files in S3.
    """
    
    name = "S3 Storage Manager"
    version = "1.0.0"
   
    def __init__(self, spark_manager, config: Dict[str, Any]):
        """Initialize the S3Manager with a spark manager instance and configuration.
        
        Args:
            spark_manager: The spark manager module that provides get_spark_session()
            config: Configuration dictionary containing Spark and S3 settings
        """
        self.spark_manager = spark_manager
        self.spark = None
        self.config = config
        self.storage_namespace = config.get("storage_namespace", "siesta")
        
        # Initialize boto3 S3 client
        try:
            self.s3_client = self._create_s3_client(config)
        except Exception as e:
            print(f"Error initializing S3 client: {e}")
            raise
        
        # Initialize Spark and database
        self.initialize_spark(config)
        self.initialize_db(config)
    
    def initialize_spark(self, config: Dict[str, Any]) -> None:
        """
        Initialize the Spark session configuration for S3 access.
        Assumes spark manager has already been started.
        
        Args:
            config: Configuration dictionary containing Spark and S3 settings
        """
        self.spark = self.spark_manager.get_spark_session()
        
        if self.spark is None:
            raise RuntimeError("Spark session not available. Ensure spark manager is started before initializing S3Manager.")
        
        if config.get("s3_access_key") and config.get("s3_secret_key"):
            self.spark.conf.set("spark.hadoop.fs.s3a.access.key", config["s3_access_key"])
            self.spark.conf.set("spark.hadoop.fs.s3a.secret.key", config["s3_secret_key"])
        
        if config.get("s3_endpoint"):
            self.spark.conf.set("spark.hadoop.fs.s3a.endpoint", config["s3_endpoint"])
        else:
            self.spark.conf.se
        
        print("S3Manager: Spark session configured for S3 access.")
    
    def _create_s3_client(self, config: Dict[str, Any]):
        """Create and configure boto3 S3 client.
        
        Args:
            config: Configuration dictionary containing S3 credentials and endpoint
            
        Returns:
            boto3 S3 client
        """
        s3_config = {}
        
        if config.get("s3_access_key") and config.get("s3_secret_key"):
            s3_config['aws_access_key_id'] = config["s3_access_key"]
            s3_config['aws_secret_access_key'] = config["s3_secret_key"]
        
        if config.get("s3_endpoint"):
            s3_config['endpoint_url'] = config["s3_endpoint"]
        
        return boto3.client('s3', **s3_config)
    
    def initialize_db(self, config: Dict[str, Any]) -> None:
        """
        Create the appropriate table structure in S3.
        
        This method can optionally clear previous data based on configuration.
        
        Args:
            config: Configuration dictionary containing database settings
        """
        log_name = config.get("log_name", "default")
        
        # Ensure bucket exists
        try:
            self.s3_client.head_bucket(Bucket=self.storage_namespace)
            print(f"S3Manager: Using existing bucket '{self.storage_namespace}'")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                # Bucket doesn't exist, create it
                try:
                    self.s3_client.create_bucket(Bucket=self.storage_namespace)
                    print(f"S3Manager: Created bucket '{self.storage_namespace}'")
                except ClientError as create_error:
                    print(f"S3Manager: Error creating bucket: {create_error}")
                    raise
            else:
                print(f"S3Manager: Error checking bucket: {e}")
                raise
        
        # If clear_existing is True, delete all objects under the log_name prefix
        if config.get("clear_existing", False):
            prefix = f"{log_name}/"
            try:
                # List and delete all objects with this prefix
                paginator = self.s3_client.get_paginator('list_objects_v2')
                pages = paginator.paginate(Bucket=self.storage_namespace, Prefix=prefix)
                
                for page in pages:
                    if 'Contents' in page:
                        objects = [{'Key': obj['Key']} for obj in page['Contents']]
                        if objects:
                            self.s3_client.delete_objects(
                                Bucket=self.storage_namespace,
                                Delete={'Objects': objects}
                            )
                print(f"S3Manager: Cleared existing data for log '{log_name}'")
            except ClientError as e:
                print(f"S3Manager: Error clearing existing data: {e}")
        
        print(f"S3Manager: Database structure initialized at s3://{self.storage_namespace}/{log_name}")
    
    def get_metadata(self, config: Dict[str, Any]) -> MetaData:
        """
        Construct metadata by loading existing metadata from S3 or creating new.
        
        Args:
            config: Configuration dictionary passed during execution
            
        Returns:
            MetaData object containing the metadata
        """
        log_name = config.get("log_name", "default")
        metadata = MetaData(storage_namespace=self.storage_namespace, log_name=log_name)
        
        # Try to load existing metadata
        try:
            metadata_df = self.spark.read.parquet(metadata.metadata_table_path)
            if metadata_df.count() > 0:
                row = metadata_df.first()
                # Populate MetaData object from DataFrame row
                metadata.trace_count = row.get("trace_count", 0)
                metadata.event_count = row.get("event_count", 0)
                metadata.pair_count = row.get("pair_count", 0)
                metadata.is_continued = row.get("is_continued", False)
                metadata.is_streaming = row.get("is_streaming", False)
                metadata.start_timestamp = row.get("start_timestamp")
                metadata.end_timestamp = row.get("end_timestamp")
                metadata.last_mined_timestamp = row.get("last_mined_timestamp")
                print(f"S3Manager: Loaded existing metadata for {log_name}")
        except Exception as e:
            print(f"S3Manager: No existing metadata found, creating new. Error: {e}")
            # Initialize with defaults from config
            metadata.is_continued = config.get("is_continued", False)
            metadata.is_streaming = config.get("is_streaming", False)
            metadata.trace_count = 0
            metadata.event_count = 0
            metadata.pair_count = 0
        
        return metadata
    
    def write_metadata(self, metadata: MetaData) -> None:
        """
        Persist metadata to S3 as a Parquet file.
        
        Args:
            metadata: MetaData object containing the metadata
        """
        # Convert MetaData object to dictionary
        metadata_dict = {
            "log_name": metadata.log_name,
            "trace_count": metadata.trace_count,
            "event_count": metadata.event_count,
            "pair_count": metadata.pair_count,
            "is_continued": metadata.is_continued,
            "is_streaming": metadata.is_streaming,
            "start_timestamp": metadata.start_timestamp,
            "end_timestamp": metadata.end_timestamp,
            "last_mined_timestamp": metadata.last_mined_timestamp,
        }
        
        # Create DataFrame and write to S3
        metadata_df = self.spark.createDataFrame([metadata_dict])
        metadata_df.write.mode("overwrite").parquet(metadata.metadata_table_path)
        print(f"S3Manager: Metadata written to {metadata.metadata_table_path}")

    def upload_file(self, local_path: str, destination_path: str) -> str:
        """
        Upload a local file to S3.
        
        Args:
            local_path: Path to the local file
            destination_path: Path/Name for the file in storage (key)
            
        Returns:
            The S3A URI to access the uploaded file
        """
        import os
        
        key = destination_path
        if key.startswith("/"):
            key = key[1:]
            
        print(f"S3Manager: Uploading '{local_path}' to bucket '{self.storage_namespace}' with key '{key}'...")
        try:
            self.s3_client.upload_file(local_path, self.storage_namespace, key)
            print("S3Manager: Upload successful.")
            
            # Construct S3A URI for Spark
            return f"s3a://{self.storage_namespace}/{key}"
        except Exception as e:
            print(f"S3Manager: Upload failed: {e}")
            raise
    
    def close_spark(self) -> None:
        """Close the Spark connection using sparkManager."""
        self.spark_manager.shutdown()
        self.spark = None
        # Note: boto3 clients don't need explicit closing
        print("S3Manager: Spark session closed.")
    
    def _object_exists(self, key: str) -> bool:
        """Check if an object exists in S3.
        
        Args:
            key: The S3 object key
            
        Returns:
            True if object exists, False otherwise
        """
        try:
            self.s3_client.head_object(Bucket=self.storage_namespace, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise
    
    def read_sequence_table(self, metadata: MetaData) -> RDD:
        """
        Read data as an RDD from the SequenceTable stored in S3.
        
        Args:
            metadata: MetaData object containing the metadata
            detailed: Whether to include detailed information
            
        Returns:
            RDD containing EventTrait objects (Trace objects)
        """
        try:
            df = self.spark.read.parquet(metadata.sequence_table_path)
            rdd = df.rdd
            print(f"S3Manager: Read {rdd.count()} records from SequenceTable")
            return rdd
        except Exception as e:
            print(f"S3Manager: Error reading SequenceTable: {e}")
            return self.spark.sparkContext.emptyRDD()
    
    def read_single_table(self, metadata: MetaData) -> RDD:
        """
        Load the single inverted index from S3 (stored in SingleTable).
        
        Args:
            metadata: MetaData object containing the metadata
            
        Returns:
            RDD containing Event objects
        """
        try:
            df = self.spark.read.parquet(metadata.single_table_path)
            rdd = df.rdd
            print(f"S3Manager: Read {rdd.count()} records from SingleTable")
            return rdd
        except Exception as e:
            print(f"S3Manager: Error reading SingleTable: {e}")
            return self.spark.sparkContext.emptyRDD()
    
    def read_last_checked_table(self, metadata: MetaData) -> RDD:
        """
        Load data from the LastCheckedTable in S3.
        
        Args:
            metadata: MetaData object containing the metadata
            
        Returns:
            RDD with last timestamps per event type pair per trace
        """
        try:
            df = self.spark.read.parquet(metadata.last_checked_table_path)
            rdd = df.rdd
            print(f"S3Manager: Read {rdd.count()} records from LastCheckedTable")
            return rdd
        except Exception as e:
            print(f"S3Manager: Error reading LastCheckedTable: {e}")
            return self.spark.sparkContext.emptyRDD()
    
    def write_last_checked_table(self, last_checked: RDD, metadata: MetaData) -> None:
        """
        Store new records for last checked timestamps to S3.
        
        Args:
            last_checked: RDD containing timestamp of last completion for each event type pair per trace
            metadata: MetaData object containing the metadata
        """
        try:
            # Convert RDD to DataFrame
            df = self.spark.createDataFrame(last_checked)
            df.write.mode("append").parquet(metadata.last_checked_table_path)
            print(f"S3Manager: Wrote last checked data to {metadata.last_checked_table_path}")
        except Exception as e:
            print(f"S3Manager: Error writing LastCheckedTable: {e}")
    
    def write_count_table(self, counts: RDD, metadata: MetaData) -> None:
        """
        Write count statistics to the CountTable in S3.
        
        Args:
            counts: RDD containing calculated basic statistics per event type pair
            metadata: MetaData object containing the metadata
        """
        try:
            # Convert RDD to DataFrame
            df = self.spark.createDataFrame(counts)
            df.write.mode("overwrite").parquet(metadata.count_table_path)
            print(f"S3Manager: Wrote count data to {metadata.count_table_path}")
        except Exception as e:
            print(f"S3Manager: Error writing CountTable: {e}")
    
    def write_sequence_table(self, sequence_rdd: RDD, metadata: MetaData) -> None:
        """
        Write traces to the SequenceTable in S3.
        
        The RDD should already be persisted and should not be modified.
        Updates the metadata object.
        
        Args:
            sequence_rdd: RDD containing traces with new events (Event objects or dicts)
            metadata: MetaData object containing the metadata
        """
        try:
            if sequence_rdd.isEmpty():
                print("S3Manager: Sequence RDD is empty, skipping write.")
                return

            from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType
            
            schema = StructType([
                StructField("activity", StringType(), True),
                StructField("trace_id", StringType(), True),
                StructField("position", IntegerType(), True),
                StructField("start_timestamp", StringType(), True),
                StructField("end_timestamp", StringType(), True),
                StructField("attributes", MapType(StringType(), StringType()), True)
            ])
            
            first_element = sequence_rdd.first()
            if not isinstance(first_element, dict):
                event_dicts_rdd = sequence_rdd.map(lambda event: event.to_dict())
            else:
                event_dicts_rdd = sequence_rdd

            df = self.spark.createDataFrame(event_dicts_rdd, schema=schema)
            df.write.mode("append").parquet(metadata.sequence_table_path)
            
            print(f"S3Manager: Wrote traces to {metadata.sequence_table_path}")
        except Exception as e:
            print(f"S3Manager: Error writing SequenceTable: {e}")
    
    def write_single_table(self, sequence_rdd: RDD, metadata: MetaData) -> None:
        """
        Write traces to the SingleTable in S3.
        
        The RDD is not persisted and should be persisted before storing and unpersisted at the end.
        Updates the metadata object.
        
        Args:
            sequence_rdd: RDD containing newly indexed events in single inverted index form
            metadata: MetaData object containing the metadata
        """
        try:
            # Persist the RDD before processing
            sequence_rdd.persist()
            
            # Convert RDD to DataFrame
            df = self.spark.createDataFrame(sequence_rdd.map(lambda event: event.to_dict()))
            df.write.mode("append").parquet(metadata.single_table_path)
            
            # Update metadata
            metadata.event_count = df.count()
            print(f"S3Manager: Wrote {metadata.event_count} events to {metadata.single_table_path}")
            
            # Unpersist the RDD
            sequence_rdd.unpersist()
        except Exception as e:
            print(f"S3Manager: Error writing SingleTable: {e}")
            # Make sure to unpersist even on error
            if sequence_rdd.is_cached:
                sequence_rdd.unpersist()
    
    def write_index_table(self, new_pairs: RDD, metadata: MetaData) -> None:
        """
        Write the combined pairs to S3 IndexTable, grouped by interval and first event.
        
        Args:
            new_pairs: RDD containing newly generated pairs
            metadata: MetaData object containing the metadata
        """
        try:
            # Convert RDD to DataFrame
            df = self.spark.createDataFrame(new_pairs)
            df.write.mode("append").parquet(metadata.index_table_path)
            
            # Update metadata
            metadata.pair_count = df.count()
            print(f"S3Manager: Wrote {metadata.pair_count} pairs to {metadata.index_table_path}")
        except Exception as e:
            print(f"S3Manager: Error writing IndexTable: {e}")
