import argparse
from pathlib import Path
from typing import Annotated, Any, Dict
from fastapi import Body
from pydantic import BaseModel, ConfigDict, Field
from siesta.core.interfaces import SiestaModule, StorageManager
from pyspark.sql import SparkSession
from siesta.core.storageFactory import get_storage_manager
from siesta.core.config import get_system_config
from siesta.core.logger import timed
from siesta.model.StorageModel import MetaData
import json
import logging
from siesta.modules.query.processors.detection_query import process_detection_query
from siesta.modules.query.processors.exploration_query import process_exploration_query
from siesta.modules.query.processors.stats_query import process_stats_query
from siesta.modules.eclair.trees import load_response_rules


logger = logging.getLogger(__name__)



DEFAULT_ECLAIR_CONFIG: Dict[str, Any] = {
    "log_name": "example_log",
    "traces_path": "",
    "S_r": 0.0,
    "S_a": 0.0,
    "cell_size": 0.01,
    "group_by": "day_of_week",
    "max_trees": 10,
    "prune_support": 0.5,
}


class Eclair(SiestaModule):
    def __init__(self):
        super().__init__()

    name = "eclair"
    version = "1.0.0"
    spark: SparkSession
    storage: StorageManager
    siesta_config: Dict[str, Any]
    eclair_config: Dict[str, Any]
    metadata: MetaData | None

    def startup(self):
        logger.info(f"{self.name} v{self.version} initialized.")

    def register_routes(self) -> SiestaModule.ApiRoutes:
        return {}
        return {
            "statistics":  ("POST", self.api_statistics),
            "detection":   ("POST", self.api_detection),
            "exploration": ("POST", self.api_exploration),
        }

    # ------------------------------------------------------------------
    # CLI entry point
    # ------------------------------------------------------------------

    def cli_run(self, args: Any, **kwargs: Any) -> Any:
        logger.info(f"{self.name} is running with args: {args} and kwargs: {kwargs}")

        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        parser = argparse.ArgumentParser(description="Siesta Query module")
        parser.add_argument('--eclair_config', type=str, help='Path to configuration JSON file', required=False)

        parsed_args, _ = parser.parse_known_args(args)

        if not parsed_args.eclair_config:
            raise RuntimeError("Config not provided. Use --eclair_config <path>")

        config_path = parsed_args.eclair_config
        if not Path(config_path).exists():
            raise FileNotFoundError(f"Config file {config_path} not found.")

        try:
            with open(config_path, 'r') as f:
                self._load_eclair_config(json.load(f))
                logger.info(f"Configuration loaded from {config_path}")
        except Exception as e:
            raise RuntimeError(f"Error loading config from {config_path}: {e}")

        self._load_metadata()

        return load_response_rules(self.eclair_config, self.metadata)



    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _load_eclair_config(self, config: Dict[str, Any]):
        #TODO: Add schema validation w/ explainability
        self.eclair_config = DEFAULT_ECLAIR_CONFIG.copy()
        self.eclair_config = self.eclair_config | config
        logger.info(self.eclair_config)

    def _load_metadata(self):
        self.metadata = MetaData(
            storage_namespace=self.eclair_config.get("storage_namespace", "siesta"),
            log_name=self.eclair_config.get("log_name", "default_log"),
            storage_type=self.eclair_config.get("storage_type", "s3"),
        )
        self.metadata = self.storage.read_metadata_table(self.metadata)

    def _dissect_query(self, config: Dict[str, Any]):
        self._load_metadata()
        match config.get("method", "").lower():
            case "statistics":
                return timed(process_stats_query, "Stats Query: ", config, self.metadata)
            case "detection":
                return timed(process_detection_query, "Detection Query: ", config, self.metadata)
            case "exploration":
                return timed(process_exploration_query, "Exploration Query: ", config, self.metadata)
