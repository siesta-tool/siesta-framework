import argparse
from pathlib import Path
from typing import Annotated, Any, Dict, Optional
from fastapi import Form, UploadFile
from siesta_framework.model.SystemModel import DEFAULT_PREPROCESS_CONFIG
from siesta_framework.modules.Example.main import Example
from siesta_framework.core.interfaces import SiestaModule, StorageManager
from siesta_framework.core.config import get_system_config
from siesta_framework.core.storageFactory import get_storage_manager, get_metadata
from siesta_framework.model.DataModel import EventConfig, Event
from siesta_framework.core.sparkManager import get_spark_session
from siesta_framework.modules.Preprocess.parse_log import parse_log_file, _parse_rows, parse_log_file_object, upload_log_file_object, parse_local_log_file
from pyspark.sql import SparkSession
import siesta_framework as siesta_framework_package
import json


class Preprocessor(SiestaModule):
        
    name = "preprocess"
    version = "1.2.0"
    spark: SparkSession
    storage: StorageManager
    preprocess_config: Dict[str, Any]
    siesta_config: Dict[str, Any]

    def __init__(self):
        super().__init__()

    def register_routes(self) -> SiestaModule.ApiRoutes|None:
        return {"run": ('POST', self.run_preprocess)}

    def startup(self):
        self.siesta_config = get_system_config()
        print("Preprocessor: Spark session initialized.")

    
    def run_preprocess(self, preprocess_config: Annotated[str, Form()], log: UploadFile | None = None) -> str:
        parsed_config = json.loads(preprocess_config)
        self.load_preprocess_config(parsed_config)
        parsed_config = self.preprocess_config

        if log is None:
            if self.preprocess_config.get("enable_streaming", False):
                self.storage = get_storage_manager()
                self.storage.initialize_streaming_collector(self.preprocess_config)
                self.build_sequence_table()
            return "Streaming collector initialized."
        else:
            if not log.filename:
                raise ValueError("Raw bytes uploaded.")
            print(f"Running preprocess with args: {log.filename}")
            print(f"File first bytes: {log.file.read(100)}")
            self.log_path = upload_log_file_object(parsed_config, log, log.filename)
            #TODO: store or parse the s3 path
            events_rdd = parse_log_file_object(parsed_config, self.log_path)
            event_dicts_rdd = events_rdd.map(lambda event: event.to_dict())
            events_df = get_spark_session().createDataFrame(event_dicts_rdd, schema=Event.get_schema())
                
            self._process_events_batch(events_df)
            return self.log_path

    
    def _process_events_batch(self, batch_df, batch_id=None):
        """
        Core processing logic for event batches; parses and stores events in Sequence Table.
        Args:
            batch_df: DataFrame containing the batch of events
            batch_id: Optional batch identifier for logging
        """
        if batch_df.isEmpty():
            return
        try:
            event_config = EventConfig.from_preprocess_config(self.preprocess_config, "json")
            events_rdd = _parse_rows(event_config, batch_df)
            
            event_dicts_rdd = events_rdd.map(lambda event: event.to_dict())
            
            events_df = get_spark_session().createDataFrame(event_dicts_rdd, schema=Event.get_schema())
        
            get_storage_manager().write_sequence_table(events_df, self.preprocess_config)
        except Exception as e:
            batch_info = f"batch {batch_id}" if batch_id is not None else "batch"
            print(f"Error processing {batch_info}: {e}")
    

    def build_sequence_table(self):
        """
        Build the Sequence Table from the log file, supporting both batch and streaming modes.
        """
        print("Preprocessor: Building Sequence Table...")
        if self.preprocess_config.get("enable_streaming", False):

            schema = EventConfig.from_preprocess_config(self.preprocess_config, "json").get_source_schema()
        
            event_stream_agg = (get_spark_session().readStream
            .format("json")     
            .schema(schema)
            .option("schemaInference", "true")
            .option("columnNameOfCorruptRecord", "_corrupt_record") 
            .load(self.storage.get_steaming_collector_path(self.preprocess_config))) 

            config = get_system_config()
            storage = get_storage_manager()

            def process_microbatch(batch_df, batch_id):
                self._process_events_batch(batch_df, batch_id)

            query = (event_stream_agg.writeStream
                .foreachBatch(process_microbatch)
                .outputMode("append")
                .option("checkpointLocation", storage.get_checkpoint_location(self.preprocess_config, "sequence_table"))
                .start())
        
        else:
            
            events_rdd = parse_log_file(self.preprocess_config)
            event_dicts_rdd = events_rdd.map(lambda event: event.to_dict())
            events_df = get_spark_session().createDataFrame(event_dicts_rdd, schema=Event.get_schema())
            
            self._process_events_batch(events_df)

    def run(self, args: Any, **kwargs: Any) -> Any:
        print(f"{self.name} is running with args: {args} and kwargs: {kwargs}")

        parser = argparse.ArgumentParser(description="Siesta Preprocess module")
        parser.add_argument('--preprocess_config', type=str, help='Path to configuration JSON file', required=False)
        # Add optional arguments ...

        parsed_args, unknown_args = parser.parse_known_args(args)
        
        # Check if a config path is provided
        if parsed_args.preprocess_config:
            config_path = parsed_args.preprocess_config
            # Check if the provided path exists
            if not Path(config_path).exists():
                raise FileNotFoundError(f"Config file {config_path} not found.")

            # Load configuration
            try:
                with open(config_path, 'r') as f:
                    user_preprocess_config = json.load(f)
                    # Merge user config with defaults
                    self.load_preprocess_config(user_preprocess_config)
                    print(f"Configuration loaded from {config_path}")
            except Exception as e:
                raise RuntimeError(f"Error loading config from {config_path}: {e}")
                
        print("Begin preprocessing...")
        events_rdd = parse_local_log_file(self.preprocess_config)
        event_dicts_rdd = events_rdd.map(lambda event: event.to_dict())
        events_df = get_spark_session().createDataFrame(event_dicts_rdd, schema=Event.get_schema())
        
        storage = get_storage_manager()
        metadata = get_metadata()
        if storage and metadata:
            storage.write_sequence_table(events_df, preprocess_config=self.preprocess_config)
    

    def load_preprocess_config(self, config: Dict[str, Any]):
        self.preprocess_config = DEFAULT_PREPROCESS_CONFIG.copy()
        self.preprocess_config.update(config)