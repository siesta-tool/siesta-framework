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
from siesta_framework.modules.Preprocess.parse_log import _parse_rows, parse_log_file, upload_log_file_object, _process_events_batch, build_sequence_table
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

        if log is None:
            if self.preprocess_config.get("enable_streaming", False):
                self.storage = get_storage_manager()
                self.storage.initialize_streaming_collector(self.preprocess_config)
                build_sequence_table(self.preprocess_config)
            return "Streaming collector initialized."
        else:
            if not log.filename:
                raise ValueError("Raw bytes uploaded.")
            print(f"Running preprocess with args: {log.filename}")
            print(f"File first bytes: {log.file.read(100)}")
            self.preprocess_config["log_path"] = upload_log_file_object(parsed_config, log, log.filename)
            
            # events_df = parse_log_file_object(parsed_config, self.log_path)
            events_df = parse_log_file(self.preprocess_config, local=False)
            _process_events_batch(self.preprocess_config, events_df)
            return self.preprocess_config["log_path"]


    

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
        events_df = parse_log_file(self.preprocess_config, local=True)
        
        storage = get_storage_manager()
        metadata = get_metadata()
        if storage and metadata:
            storage.write_sequence_table(events_df, preprocess_config=self.preprocess_config)
    

    def load_preprocess_config(self, config: Dict[str, Any]):
        self.preprocess_config = DEFAULT_PREPROCESS_CONFIG.copy()
        self.preprocess_config.update(config)