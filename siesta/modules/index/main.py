import argparse
from pathlib import Path
from typing import Annotated, Any, Dict
from fastapi import Form, UploadFile
from siesta.core.sparkManager import get_spark_session, cleanup as spark_cleanup
from siesta.model.StorageModel import MetaData
from pydantic import BaseModel, ConfigDict, Field
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


class IndexConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    log_name: str = Field("example_log", description="Name for the log in storage")
    log_path: str = Field("../datasets/test.xes", description="Path to the local log file (XES, CSV, or JSON)")
    storage_namespace: str = Field("siesta", description="Storage namespace / bucket prefix")
    clear_existing: bool = Field(False, description="Drop and rebuild the existing index")
    enable_streaming: bool = Field(False, description="Start a Kafka streaming collector instead of batch processing")
    kafka_topic: str = Field("example_log", description="Kafka topic to consume in streaming mode")
    lookback: str = Field("7d", description="Lookback window for incremental indexing, e.g. '7d', '30d'")
    field_mappings: dict = Field(default_factory=lambda: {
        "xes": {
            "activity": "concept:name",
            "trace_id": "concept:name",
            "position": None,
            "start_timestamp": "time:timestamp",
            "attributes": ["*"],
        },
        "csv": {
            "activity": "activity",
            "trace_id": "trace_id",
            "position": "position",
            "start_timestamp": "timestamp",
            "attributes": ["resource", "cost"],
        },
        "json": {
            "activity": "activity",
            "trace_id": "caseID",
            "position": "position",
            "start_timestamp": "Timestamp",
        },
    }, description="Per-format mapping of Event fields to source column names")
    trace_level_fields: list[str] = Field(default_factory=lambda: ["trace_id"], description="Fields extracted at trace level")
    timestamp_fields: list[str] = Field(default_factory=lambda: ["start_timestamp"], description="Fields to parse as Unix epoch seconds")


DEFAULT_INDEX_CONFIG: Dict[str, Any] = IndexConfig().model_dump()


class Indexing(SiestaModule):
        
    name = "indexer"
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

    
    def api_run(
        self,
        index_config: Annotated[str, Form(
            description="JSON configuration object - see endpoint description for all supported fields.",
            openapi_examples={
                "batch": {
                    "summary": "Batch indexing",
                    "value": '{"log_name": "example_log", "storage_namespace": "siesta", "clear_existing": false, "field_mappings": {"csv": {"trace_id": "case:concept:name", "activity": "concept:name", "start_timestamp": "time:timestamp"}}}',
                },
                "streaming": {
                    "summary": "Kafka streaming",
                    "value": '{"log_name": "example_log", "storage_namespace": "siesta", "enable_streaming": true, "kafka_topic": "example_log"}',
                },
            },
        )],
        log_file: UploadFile | None = None,
    ) -> Any:
        """Index an event log from a file upload or start a Kafka streaming collector.

        Parses the uploaded log (XES, CSV, or JSON) and builds the sequence table,
        activity index, pairs index, and count table in storage. When no file is
        provided and `enable_streaming` is `true`, starts a Kafka-based streaming
        collector instead.

        **Form fields:**
        - `log_file` *(file, optional)* - log file to index (XES, CSV, or JSON). Omit for streaming mode.
        - `index_config` *(JSON string)* - configuration; fields below.

        **Config fields (`index_config`):**
        - `log_name` *(str, default: `"example_log"`)* - name for the log in storage.
        - `storage_namespace` *(str, default: `"siesta"`)* - storage namespace / bucket prefix.
        - `clear_existing` *(bool, default: `false`)* - drop and rebuild the existing index.
        - `enable_streaming` *(bool, default: `false`)* - start a Kafka streaming collector instead of batch processing.
        - `kafka_topic` *(str, default: `"example_log"`)* - Kafka topic to consume in streaming mode.
        - `lookback` *(str, default: `"7d"`)* - lookback window for incremental indexing (e.g. `"7d"`, `"30d"`).
        - `field_mappings` *(object)* - per-format mapping of Event fields to source column names.
          Formats: `xes`, `csv`, `json`. Use `"*"` in `attributes` list to capture all unmapped fields.
        - `trace_level_fields` *(list, default: `["trace_id"]`)* - fields extracted at trace level.
        - `timestamp_fields` *(list, default: `["start_timestamp"]`)* - fields to parse as Unix epoch seconds.
        """
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
        parser.add_argument('--indexer_config', type=str, help='Path to configuration JSON file', required=False)
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

 