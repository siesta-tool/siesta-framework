import argparse
import datetime
from pathlib import Path
from typing import Annotated, Any, Dict
from fastapi import Form
from siesta_framework.model.StorageModel import MetaData
from siesta_framework.model.SystemModel import DEFAULT_COMPARATOR_CONFIG
from siesta_framework.core.interfaces import SiestaModule, StorageManager
from siesta_framework.core.config import get_system_config
from siesta_framework.core.logger import timed
from siesta_framework.core.storageFactory import get_storage_manager
from pyspark.sql import SparkSession, DataFrame, functions as F

import csv
import json
import logging

from siesta_framework.modules.Compare import ngrams
logger = logging.getLogger(__name__)


class Comparator(SiestaModule):
        
    name = "comparator"
    version = "1.0.0"
    spark: SparkSession
    storage: StorageManager
    siesta_config: Dict[str, Any]

    comparator_config: Dict[str, Any]

    metadata: MetaData | None

    def __init__(self):
        super().__init__()
        self.comparator_config = {}
        self.metadata = None

    def register_routes(self) -> SiestaModule.ApiRoutes|None:
        return {"run": ('POST', self.api_run)}

    def startup(self):
        logger.info("Startup complete.")

    def api_run(self, comparator_config: Annotated[str, Form()]) -> Any:
        logger.info(f"{self.name} is running via API request.")

        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()
        self._load_comparator_config(json.loads(comparator_config))

        logger.info(f"Running comparator with args: {self.comparator_config}")
        
        self.compare(caller="api")

        logger.info(f"Completed. Results available at {self.comparator_config['output_path']}.")
        
        with open(self.comparator_config["output_path"], 'r', newline="") as f:
            try:
                return list(csv.DictReader(f))
            except Exception:
                logger.error(f"Failed to parse comparator results from {self.comparator_config['output_path']}. Check if the file is a valid CSV and inspect logs for details.")
                return f"Cannot parse comparator results. Check logs and {self.comparator_config['output_path']} for details."


    def cli_run(self, args: Any, **kwargs: Any) -> Any:
        """
        Entry point for Mining via the command line.
        """
        logger.info(f"{self.name} is running with args: {args} and kwargs: {kwargs}")

        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        parser = argparse.ArgumentParser(description="Siesta Mining module")
        parser.add_argument('--mining_config', type=str, help='Path to configuration JSON file', required=False)

        parsed_args, _ = parser.parse_known_args(args)
        
        # Check if a config path is provided
        if parsed_args.mining_config:
            config_path = parsed_args.mining_config
            # Check if the provided path exists
            if not Path(config_path).exists():
                raise FileNotFoundError(f"Config file {config_path} not found.")

            # Load configuration
            try:
                with open(config_path, 'r') as f:
                    user_comparator_config = json.load(f)
                    
                    self._load_comparator_config(user_comparator_config)
                    self.storage.initialize_db(self.comparator_config)

                    logger.info(f"Loaded config from {config_path}: {user_comparator_config}")
            except Exception as e:
                raise RuntimeError(f"Error loading config from {config_path}: {e}")

        self.compare(caller="cli")

        logger.info(f"Completed. Results available at {self.comparator_config['output_path']}.")
        
        return self.comparator_config["output_path"]

    def _load_comparator_config(self, config: Dict[str, Any]):
        # Validate that the specified log exists in storage before proceeding with comparator. 
        if not self.storage.log_exists(config):
            log_name = config.get("log_name", "default_log")
            raise ValueError(f"Log '{log_name}' does not exist in storage. Run preprocessing first.")

        self.comparator_config = DEFAULT_COMPARATOR_CONFIG.copy()
        self.comparator_config.update(config)

        # Ensure output_path is unique for each run to avoid overwriting results
        given_output_path = config.get("output_path", "../output/" + config.get("log_name", "comparator_results"))
        Path(given_output_path).parent.mkdir(parents=True, exist_ok=True)
        self.comparator_config["output_path"] = given_output_path + "_" + str(datetime.datetime.now().timestamp()) + ".csv"

    
    def compare(self, caller: str):
        logger.info(f"Beginning comparator process initiated by {caller}.")

        # Load metadata if available, and evolved traces since last comparator from storage
        self.metadata = MetaData(
            storage_namespace=self.comparator_config.get("storage_namespace", "siesta"),
            log_name=self.comparator_config.get("log_name", "default_log"),
            storage_type=self.comparator_config.get("storage_type", "s3")
        )

        self.metadata = self.storage.read_metadata_table(self.comparator_config, self.metadata) 
        all_events_df = self.storage.read_sequence_table(self.metadata)
        all_events_df.cache()  # Cache evolved traces as they will be used multiple times

        # Based on the defined value-groups, create groups of events based on the separating key
        separating_key = self.comparator_config.get("separating_key", "activity")
        separating_groups = self.comparator_config.get("separating_groups", [])

        # grouped_dfs will contain a list of tuples: (group_values_on_separating_key, group_events_df)
        # e.g. [ (["fail_1", "fail_2"], df_of_fail_events), (["success_1", "success_2"], df_of_success_events) ]
        # or if only one group is defined: [ (["fail_1", "fail_2"], df_of_fail_events), (["not_fail_1", "not_fail_2"], df_of_non_fail_events) ]
        grouped_dfs = []
        if len(separating_groups) < 2:
            # We consider as second group all values of the separating key that are not in the first group, 
            # to ensure we have at least 2 groups to compare.
            group_1_df = all_events_df.filter(F.col(separating_key).isin(separating_groups[0]))
            group_2_df = all_events_df.filter(~F.col(separating_key).isin(separating_groups[0]))
            grouped_dfs.append((separating_groups[0], group_1_df))
            grouped_dfs.append((f"not_{separating_groups[0]}", group_2_df))
        else:
            for group in separating_groups:
                group_df = all_events_df.filter(F.col(separating_key).isin(group))
                grouped_dfs.append((group, group_df))

        if self.comparator_config.get("method", "ngrams") == "ngrams":
            ngrams(grouped_dfs, n = self.comparator_config.get("method_params", {}).get("n", 4))
        else: # TODO: Implement other comparison methods
            pass
