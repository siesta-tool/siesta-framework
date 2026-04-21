import argparse
from pathlib import Path
from typing import Annotated, Any, Dict
from fastapi import Form, UploadFile
from siesta.core.sparkManager import get_spark_session, cleanup as spark_cleanup
from siesta.model.StorageModel import MetaData
from siesta.model.SystemModel import DEFAULT_INDEX_CONFIG
from siesta.core.interfaces import SiestaModule, StorageManager
from siesta.core.config import get_system_config
from siesta.core.logger import timed
from siesta.core.storageFactory import get_storage_manager
from siesta.modules.index.parsers import upload_log_file_object
from siesta.modules.index.builders import *
from pyspark.sql import SparkSession
import json
import logging
logger = logging.getLogger(__name__)


class Indexing(SiestaModule):
        
    name = "indexing"
    version = "1.2.0"
    spark: SparkSession
    storage: StorageManager
    siesta_config: Dict[str, Any]

    index_config: Dict[str, Any]

    metadata: MetaData | None

    def __init__(self):
        super().__init__()
        self.index_config = {}
        self.metadata = None

    def register_routes(self) -> SiestaModule.ApiRoutes|None:
        return {"run": ('POST', self.api_run)}

    def startup(self):
        logger.info("Startup complete.")

    
    def api_run(self, index_config: Annotated[str, Form()], log_file: UploadFile | None = None) -> Any:
        logger.info(f"{self.name} is running via API request.")

        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()
        self._load_index_config(json.loads(index_config))
        self.storage.initialize_db(self.index_config)

        if log_file is None:
            if not self.index_config.get("enable_streaming", False):
                return "Indexing: No log file uploaded for batch processing and streaming not enabled. Aborting."    
            self.storage.initialize_streaming_collector(self.index_config)
            self.begin_builders(caller="api")
            return "Indexing: Streaming collector initialized."
        else:
            if not log_file.filename:
                return "Indexing: Uploaded log file has no filename. Aborting."
            # Ensure batch mode in case of file upload
            self.index_config["enable_streaming"] = False          
            logger.info(f"Indexing: Running indexing with args: {log_file.filename}")
            self.index_config["log_path"] = upload_log_file_object(self.index_config, log_file, log_file.filename)
            self.begin_builders(caller="api")
            return "Indexing: Batch processing completed."


    def cli_run(self, args: Any, **kwargs: Any) -> Any:
        """
        Entry point for Indexing via the command line.
        """
        logger.info(f"{self.name} is running with args: {args} and kwargs: {kwargs}")

        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        parser = argparse.ArgumentParser(description="Siesta Indexing module")
        parser.add_argument('--index_config', type=str, help='Path to configuration JSON file', required=False)
        # Add optional arguments ...

        parsed_args, unknown_args = parser.parse_known_args(args)
        
        # Check if a config path is provided
        if parsed_args.index_config:
            config_path = parsed_args.index_config
            # Check if the provided path exists
            if not Path(config_path).exists():
                raise FileNotFoundError(f"Config file {config_path} not found.")

            # Load configuration
            try:
                with open(config_path, 'r') as f:
                    user_index_config = json.load(f)
                    self._load_index_config(user_index_config)
                    self.storage.initialize_db(self.index_config)

                    logger.info(f"Configuration loaded from {config_path}")
            except Exception as e:
                raise RuntimeError(f"Error loading config from {config_path}: {e}")

        if self.index_config.get("enable_streaming", False):
            self.storage.initialize_streaming_collector(self.index_config)
                
        self.begin_builders(caller="cli")

        if self.index_config.get("enable_streaming", False):
            from siesta.core.sparkManager import get_spark_session
            logger.info("Awaiting streaming termination... Press Ctrl+C to stop.")
            get_spark_session().streams.awaitAnyTermination()
    

    def _load_index_config(self, config: Dict[str, Any]):
        self.index_config = DEFAULT_INDEX_CONFIG.copy()
        self.index_config.update(config)

    def begin_builders(self, caller: str = "cli"):
        # Create a metadata object that will overwrite existing metadata 
        # according to new indexing task (based on the index_config)
        self.metadata = MetaData(
            storage_namespace=self.index_config.get("storage_namespace", "siesta"),
            log_name=self.index_config.get("log_name", "default_log"),
            storage_type=self.index_config.get("storage_type", "s3")
        )
        

        # # Load existing metadata from storage if available
        self.storage.read_metadata_table(self.metadata) 

        seq_df = timed(build_sequence_table, "Indexing.", index_config=self.index_config, metadata=self.metadata)
        activity_index_df = timed(build_activity_index, "Indexing.", events_df=seq_df, metadata=self.metadata)
        
        # seq_df (checkpointed in update_event_positions) is no longer needed in batch mode
        if not isinstance(seq_df, StreamingQuery):
            seq_df.unpersist()

        if isinstance(activity_index_df, StreamingQuery):
            build_last_checked_index_and_count_streamed(self.index_config, self.metadata, batch_activity_index_df=activity_index_df)
        else:
            pairs_df, _ = timed(build_last_checked_table, "Indexing.", self.index_config, self.metadata, batch_activity_index_df=activity_index_df)
            # pairs_df is cached inside build_last_checked_table
            
            timed(build_pairs_index, "Indexing.", self.index_config, self.metadata, pairs_df)

            timed(build_count_table, "Indexing.", self.index_config, self.metadata, pairs_df)

            # Release the memory occupied by pairs_df now it's not needed
            pairs_df.unpersist()

            # Release cached data and Delta metadata after batch processing
            spark_cleanup()

            # In CLI mode, we want to keep streaming jobs alive until termination
            if caller == "cli" and self.index_config.get("enable_streaming", False):
                get_spark_session().streams.awaitAnyTermination()

 