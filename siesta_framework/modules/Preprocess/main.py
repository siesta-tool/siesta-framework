import argparse
from pathlib import Path
from typing import Any, Dict

from fastapi import File, UploadFile
import siesta_framework as siesta_framework_package
from siesta_framework.core.interfaces import SiestaModule
from siesta_framework.core.storageFactory import get_storage_manager, get_metadata
from siesta_framework.modules.Preprocess.parse_log import parse_local_log_file, upload_log_file_object, parse_log_file_object
import json


class Preprocessor(SiestaModule):
    
    name = "preprocess"
    version = "1.2.0"
    __DEFAULT_CONFIG_PATH = str(Path(siesta_framework_package.__file__).parent / 'config' / 'siesta.config.json')
    config: Dict[str, Any] = {}

    def startup(self):
        # print(dir(example_module))
        pass
    
    def register_routes(self) -> SiestaModule.ApiRoutes|None:
        return {"upload": ('POST', self.upload),"run": ('POST', self.run_preprocess)}
    
    def upload(self, log: UploadFile) -> str:
        print(f"Running preprocess with args: {log.filename}")
        print(f"File first bytes: {log.file.read(100)}")
        self.s3_path = upload_log_file_object(log, log.filename)
        #TODO: store or parse the s3 path
        return self.s3_path
    
    def run_preprocess(self, config: Dict[str, Any]) -> str:
        print(f"Running preprocess with args: {config}")
        self.load_config(config)
        events_rdd = parse_log_file_object(self.config, self.s3_path)
        #TODO: cleanup
        return "Run preprocess route called"

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
        else:
            config_path = self.__DEFAULT_CONFIG_PATH

        # Load configuration
        try:
            with open(config_path, 'r') as f:
                user_config = json.load(f)
                # Merge user config with defaults
                self.config.update(user_config)
                print(f"Configuration loaded from {config_path}")
        except Exception as e:
            raise RuntimeError(f"Error loading config from {config_path}: {e}")
        
        self.load_config(self.config)
        
        print("Running testparse...")
        events_rdd = parse_local_log_file(self.config)
        
        storage = get_storage_manager()
        metadata = get_metadata()
        if storage and metadata:
            storage.write_sequence_table(events_rdd, metadata)
    
    def load_config(self, config: Dict[str, Any]) -> None:
        DEFAULT_CONFIG: Dict[str, Any] = {
            # Log information
            "log_name": "default_log",
            "log_path": "datasets/test.xes",
            # Log parsing configuration
            "field_mappings": {
                "xes": {
                    "activity": "concept:name",
                    "trace_id": "concept:name",
                    "position": None,  # Computed from sequence
                    "start_timestamp": "time:timestamp",
                    "end_timestamp": "time:timestamp",
                },
                "csv": {
                    "activity": "Activity",
                    "trace_id": "CaseID",
                    "position": None,
                    "start_timestamp": "Timestamp",
                    "end_timestamp": "Timestamp",
                }
            },
            "trace_level_fields": ["trace_id"],
            "timestamp_fields": ["start_timestamp", "end_timestamp"],
        }

        # Merge provided config with defaults
        DEFAULT_CONFIG.update(config)
        self.config = DEFAULT_CONFIG