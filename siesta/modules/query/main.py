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


logger = logging.getLogger(__name__)


class QueryMethodInput(BaseModel):
    model_config = ConfigDict(extra="allow")
    pattern: str = Field("", description="Pattern string, e.g. 'A B* C' or 'A[pos=?1]+ B[pos=?1+5]'")
    explore_mode: str = Field("accurate", description="'accurate', 'fast', or 'hybrid'. Used by exploration only")
    explore_k: int = Field(10, description="Candidates for hybrid exploration (0 = fast mode)")


class QueryConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    log_name: str = Field("example_log", description="Name of the indexed log")
    storage_namespace: str = Field("siesta", description="Storage namespace")
    method: str = Field("statistics", description="'statistics', 'detection', or 'exploration'")
    query: QueryMethodInput = Field(default_factory=QueryMethodInput, description="Query-specific parameters")
    support_threshold: float = Field(0.0, description="Minimum support fraction [0,1] for results")


DEFAULT_QUERY_CONFIG: Dict[str, Any] = QueryConfig().model_dump()


class Querying(SiestaModule):
    def __init__(self):
        super().__init__()

    name = "executor"
    version = "1.0.0"
    spark: SparkSession
    storage: StorageManager
    siesta_config: Dict[str, Any]
    query_config: Dict[str, Any]
    metadata: MetaData | None

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

        if parsed_args.query_config:
            config_path = parsed_args.query_config
            if not Path(config_path).exists():
                raise FileNotFoundError(f"Config file {config_path} not found.")

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


    def api_run(self, query_config: Annotated[QueryConfig, Body(
        openapi_examples={
            "statistics": {
                "summary": "Statistics query",
                "value": {"log_name": "example_log", "method": "statistics", "query": {"pattern": "A B"}, "support_threshold": 0.0},
            },
            "detection": {
                "summary": "Detection query",
                "value": {"log_name": "example_log", "method": "detection", "query": {"pattern": "A B* C"}, "support_threshold": 0.1},
            },
            "exploration": {
                "summary": "Exploration query",
                "value": {"log_name": "example_log", "method": "exploration", "query": {"pattern": "A B", "explore_mode": "accurate", "explore_k": 10}},
            },
        }
    )]) -> Any | None:
        """Run a pattern query over an indexed event log.

        Three methods selected via the `method` field:
        - **`statistics`**: Aggregate occurrence counts and support for activity pairs or a given pattern.
        - **`detection`**: Return traces that satisfy a temporal / positional pattern.
        - **`exploration`**: Find the most likely continuations for a partial pattern.

        **Request body (`QueryConfig`):**
        - `log_name` *(str, default: `"example_log"`)* — name of the indexed log.
        - `storage_namespace` *(str, default: `"siesta"`)* — storage namespace.
        - `method` *(str, default: `"statistics"`)* — `"statistics"`, `"detection"`, or `"exploration"`.
        - `query.pattern` *(str)* — pattern string, e.g. `"A B* C"` or `"A[pos=?1]+ B[pos=?1+5]"`.
        - `query.explore_mode` *(str, default: `"accurate"`)* — `"accurate"`, `"fast"`, or `"hybrid"`. Used by `exploration` only.
        - `query.explore_k` *(int, default: `10`)* — candidates for hybrid exploration (`0` = fast mode).
        - `support_threshold` *(float [0,1], default: `0.0`)* — minimum support fraction for results.
        """

        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        self._load_query_config(query_config.model_dump())

        return self._dissect_query(self.query_config)

    def _load_query_config(self, config: Dict[str, Any]):
        #TODO: Add schema validation w/ explainability
        self.query_config = DEFAULT_QUERY_CONFIG.copy()
        self.query_config = self.query_config | config
        logger.info(self.query_config)

    def _dissect_query(self, config: Dict[str, Any]):

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
