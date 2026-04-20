import argparse
from pathlib import Path
from typing import Any, Dict, Tuple
from siesta.core.interfaces import SiestaModule, StorageManager
from pyspark.sql import SparkSession
from siesta.core.storageFactory import get_storage_manager
from siesta.core.config import get_system_config
from siesta.core.logger import timed
from siesta.model.StorageModel import MetaData
from siesta.model.SystemModel import DEFAULT_QUERY_CONFIG, Query_Config
import json
import logging
from siesta.modules.Query.processors.detection_query import process_detection_query
from siesta.modules.Query.processors.exploration_query import process_exploration_query
from siesta.modules.Query.processors.stats_query import process_stats_query


logger = logging.getLogger(__name__)


class Query(SiestaModule):
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
        logger.info(f"{self.name} v{self.version} initialized.")

    def register_routes(self) -> SiestaModule.ApiRoutes:
        return {
            "run": ("POST", self.api_run)
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


        result = self._dissect_query(self.query_config)
        if result is not None:
            logger.info(f"Query result: {result}")
        return result


    def api_run(self, query_config: Query_Config) -> Any | None:
        """
        Entry point for Query via the API.
        """

        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        self._load_query_config(query_config)

        return self._dissect_query(self.query_config)


    def _load_query_config(self, config: Query_Config):
        #TODO: Add schema validation w/ explainability
        self.query_config = DEFAULT_QUERY_CONFIG.copy()
        self.query_config = self.query_config | config
        logger.info(self.query_config)

    def _dissect_query(self, config: Query_Config):
                
        self.metadata = MetaData(
            storage_namespace=self.query_config.get("storage_namespace", "siesta"),
            log_name=self.query_config.get("log_name", "default_log"),
            storage_type=self.query_config.get("storage_type", "s3")
        )

        self.metadata = self.storage.read_metadata_table(self.metadata)

        match config.get("method", "").lower():
            case "statistics":
                return timed(process_stats_query, "Stats Query: ", config, self.metadata)
            case "detection":
                return timed(process_detection_query, "Detection Query: ", config, self.metadata)
            case "exploration":
                return timed(process_exploration_query, "Exploration Query: ", config, self.metadata)