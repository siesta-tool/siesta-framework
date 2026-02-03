import argparse
from pathlib import Path
from typing import Annotated, Any, Dict
from fastapi import Form, UploadFile
from siesta_framework.model.StorageModel import MetaData
from siesta_framework.model.SystemModel import DEFAULT_PREPROCESS_CONFIG
from siesta_framework.core.interfaces import SiestaModule, StorageManager
from siesta_framework.core.config import get_system_config
from siesta_framework.core.logger import timed
from siesta_framework.core.storageFactory import get_storage_manager
from siesta_framework.modules.Preprocess.parsers import upload_log_file_object
from siesta_framework.modules.Preprocess.builders import build_sequence_table
from pyspark.sql import SparkSession
import timeit
import json
import logging
logger = logging.getLogger("Preprocess")


class Preprocessor(SiestaModule):
        
    name = "preprocess"
    version = "1.2.0"
    spark: SparkSession
    storage: StorageManager
    siesta_config: Dict[str, Any]

    preprocess_config: Dict[str, Any]

    metadata: MetaData

    def __init__(self):
        super().__init__()

    def register_routes(self) -> SiestaModule.ApiRoutes|None:
        return {"run": ('POST', self.api_run)}

    def startup(self):
        self.siesta_config = get_system_config()
        logger.info("Preprocessor: Startup complete.")

    
    def api_run(self, preprocess_config: Annotated[str, Form()], log_file: UploadFile | None = None) -> Any:
        print(f"{self.name} is running via API request.")

        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()
        self._load_preprocess_config(json.loads(preprocess_config))
        self.storage.initialize_db(self.preprocess_config)

        if log_file is None:
            if not self.preprocess_config.get("enable_streaming", False):
                return "Preprocess: No log file uploaded for batch processing and streaming not enabled. Aborting."    
            self.storage.initialize_streaming_collector(self.preprocess_config)
            self.begin_builders()
            return "Preprocess: Streaming collector initialized."
        else:
            if not log_file.filename:
                return "Preprocess: Uploaded log file has no filename. Aborting."
            # Ensure batch mode in case of file upload
            self.preprocess_config["enable_streaming"] = False          
            logger.info(f"Preprocess: Running preprocess with args: {log_file.filename}")
            self.preprocess_config["log_path"] = upload_log_file_object(self.preprocess_config, log_file, log_file.filename)
            self.begin_builders()
            return "Preprocess: Batch processing completed."


    def cli_run(self, args: Any, **kwargs: Any) -> Any:
        """
        Entry point for Preprocess via the command line.
        """
        print(f"{self.name} is running with args: {args} and kwargs: {kwargs}")

        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

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
                    
                    self._load_preprocess_config(user_preprocess_config)
                    self.storage.initialize_db(self.preprocess_config)

                    print(f"Preprocess: Configuration loaded from {config_path}")
            except Exception as e:
                raise RuntimeError(f"Error loading config from {config_path}: {e}")
                
        self.begin_builders()
    

    def _load_preprocess_config(self, config: Dict[str, Any]):
        self.preprocess_config = DEFAULT_PREPROCESS_CONFIG.copy()
        self.preprocess_config.update(config)

    def begin_builders(self):
        # Create a metadata object that will overwrite existing metadata 
        # according to new preprocessing task (based on the preprocess_config)
        self.metadata = self.storage.read_metadata_table(self.preprocess_config)
        timed(build_sequence_table, "Preprocess: ", self.preprocess_config, self.metadata)
        
        self.storage.write_metadata_table(self.metadata)
