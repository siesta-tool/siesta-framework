import argparse
from pathlib import Path
from fastapi import Form, UploadFile
from typing import Any, Dict, Tuple, Annotated
from siesta_framework.core.interfaces import SiestaModule, StorageManager
from pyspark.sql import SparkSession
from siesta_framework.core.storageFactory import get_storage_manager
from siesta_framework.core.sparkManager import get_spark_session, cleanup as spark_cleanup
from siesta_framework.core.config import get_system_config
from siesta_framework.core.logger import timed
from siesta_framework.model.StorageModel import MetaData
from siesta_framework.model.SystemModel import DEFAULT_QUERY_CONFIG, Query_Config
import json
import logging
from siesta_framework.modules.Query.query_processors import process_stats_query


logger = logging.getLogger("Query")


class Example(SiestaModule):
    def __init__(self):
        super().__init__()

    name = "query"
    version = "1.0.0"
    spark: SparkSession
    storage: StorageManager
    siesta_config: Dict[str, Any]
    query_config: Query_Config
    metadata: MetaData | None

    type exampleSimpleType = int | str
    type exampleComplicatedType = Dict[str, Tuple[int, exampleSimpleType]]

    def startup(self):
        print(f"{self.name} v{self.version} initialized.")

    def register_routes(self) -> SiestaModule.ApiRoutes:
        return {
            "example_endpoint": ("GET", self.example_endpoint),
            "log_info": ("GET", self.get_log_info)
        }
    
    def cli_run(self, args: Any, **kwargs: Any) -> Any:
        """
        Entry point for Query via the command line.
        """
        logger.info(f"{self.name} is running with args: {args} and kwargs: {kwargs}")

        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        parser = argparse.ArgumentParser(description="Siesta Query module")
        parser.add_argument('--query_config', type=str, help='Path to configuration JSON file', required=False)
        # Add optional arguments ...

        parsed_args, unknown_args = parser.parse_known_args(args)
        
        # Check if a config path is provided
        if parsed_args.query_config:
            config_path = parsed_args.query_config
            # Check if the provided path exists
            if not Path(config_path).exists():
                raise FileNotFoundError(f"Config file {config_path} not found.")

            # Load configuration
            try:
                with open(config_path, 'r') as f:
                    user_query_config = json.load(f)
                    
                    self._load_query_config(user_query_config)

                    logger.info(f"Configuration loaded from {config_path}")
            except Exception as e:
                raise RuntimeError(f"Error loading config from {config_path}: {e}")

        else:
            raise RuntimeError(f"Config not provided")
        
        self.metadata = MetaData(
            storage_namespace=self.query_config.get("storage_namespace", "siesta"),
            log_name=self.query_config.get("log_name", "default_log"),
            storage_type=self.query_config.get("storage_type", "s3")
        )

        self._dissect_query(self.query_config, self.metadata)

    def _load_query_config(self, config: Query_Config):
        self.query_config = DEFAULT_QUERY_CONFIG.copy()
        self.query_config = self.query_config | config
        logger.info(self.query_config)

    def _dissect_query(self, config: Query_Config, metadata: MetaData):
        match config.get("method", "").lower():
            case "stats":
                process_stats_query(config, metadata)
            case "patterns":
                pass
            case "detection":
                pass
            case "explore":
                pass
            case "violations":
                pass



    def example_endpoint(self, request_data: str = "default") -> exampleComplicatedType|None:
        return None
    
    def get_log_info(self, log_name: str) -> Dict[str, Any]|None:
        """Example API endpoint that works with a specific log.
        
        Args:
            log_name: The log/dataset name from the request.
            
        Returns:
            Dict with log metadata info.
        """
        pass