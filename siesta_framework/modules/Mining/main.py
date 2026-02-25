import argparse
import datetime
from pathlib import Path
from typing import Annotated, Any, Dict
from fastapi import Form
from siesta_framework.model.StorageModel import MetaData
from siesta_framework.model.SystemModel import DEFAULT_MINING_CONFIG
from siesta_framework.core.interfaces import SiestaModule, StorageManager
from siesta_framework.core.config import get_system_config
from siesta_framework.core.logger import timed
from siesta_framework.core.storageFactory import get_storage_manager
from siesta_framework.modules.Mining.existential import discover_existential
from siesta_framework.modules.Mining.positional import discover_positional
from siesta_framework.modules.Mining.ordered import discover_ordered
from siesta_framework.modules.Mining.unordered import discover_unordered
from pyspark.sql import SparkSession, DataFrame, functions as F

import csv
import io
import json
import logging
logger = logging.getLogger("Mining")


class Miner(SiestaModule):
        
    name = "mining"
    version = "1.0.0"
    spark: SparkSession
    storage: StorageManager
    siesta_config: Dict[str, Any]

    mining_config: Dict[str, Any]

    metadata: MetaData | None

    def __init__(self):
        super().__init__()
        self.mining_config = {}
        self.metadata = None

    def register_routes(self) -> SiestaModule.ApiRoutes|None:
        return {"run": ('POST', self.api_run)}

    def startup(self):
        logger.info("Startup complete.")

    def api_run(self, mining_config: Annotated[str, Form()]) -> Any:
        print(f"{self.name} is running via API request.")

        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()
        self._load_mining_config(json.loads(mining_config))

        logger.info(f"Mining: Running mining with args: {mining_config}")
        
        self.mine(caller="api")

        logger.info(f"Mining: Completed. Results available at {self.mining_config['output_path']}.")
        
        # with open(self.mining_config["output_path"], 'r') as f:
        #     try:
        #         return json.load(f)
        #     except json.JSONDecodeError:
        #         return f"Mining: Cannot parse mining results. Check logs and {self.mining_config['output_path']} for details."


    def cli_run(self, args: Any, **kwargs: Any) -> Any:
        """
        Entry point for Mining via the command line.
        """
        print(f"{self.name} is running with args: {args} and kwargs: {kwargs}")

        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        parser = argparse.ArgumentParser(description="Siesta Mining module")
        parser.add_argument('--mining_config', type=str, help='Path to configuration JSON file', required=False)
        # Add optional arguments ...

        parsed_args, unknown_args = parser.parse_known_args(args)
        
        # Check if a config path is provided
        if parsed_args.mining_config:
            config_path = parsed_args.mining_config
            # Check if the provided path exists
            if not Path(config_path).exists():
                raise FileNotFoundError(f"Config file {config_path} not found.")

            # Load configuration
            try:
                with open(config_path, 'r') as f:
                    user_mining_config = json.load(f)
                    
                    self._load_mining_config(user_mining_config)
                    self.storage.initialize_db(self.mining_config)

                    logger.info(f"Mining: Loaded config from {config_path}: {user_mining_config}")
            except Exception as e:
                raise RuntimeError(f"Error loading config from {config_path}: {e}")

        self.mine(caller="cli")

        logger.info(f"Mining: Completed. Results available at {self.mining_config['output_path']}.")
        
        return self.mining_config["output_path"]

    def _load_mining_config(self, config: Dict[str, Any]):
        self.mining_config = DEFAULT_MINING_CONFIG.copy()
        self.mining_config.update(config)

        if not self.storage.log_exists(self.mining_config):
            log_name = self.mining_config.get("log_name", "default_log")
            raise ValueError(f"Log '{log_name}' does not exist in storage. Run preprocessing first.")

        # Ensure output_path is unique for each run to avoid overwriting results
        given_output_path = self.mining_config.get("output_path", self.mining_config.get("log_name", "mining_results"))
        Path(given_output_path).parent.mkdir(parents=True, exist_ok=True)
        self.mining_config["output_path"] = given_output_path + "_" + str(datetime.datetime.now().timestamp()) + ".csv"


    def mine(self, caller: str):
        """
        Permorms incremental mining on the log data based on the provided mining configuration and metadata.
        The method loads evolved traces since the last mining, discovers new constraints and keeps only valid old ones and new ones in storage (by overwrite mode), and outputs the results to a CSV file on the driver's local filesystem.

        :param caller: a string indicating the caller of the mining process (e.g. "cli", "api") for logging purposes.
        """

        logger.info(f"Beginning mining process initiated by {caller}.")

        # Load metadata if available, and evolved traces since last mining from storage
        self.metadata = MetaData(
            storage_namespace=self.mining_config.get("storage_namespace", "siesta"),
            log_name=self.mining_config.get("log_name", "default_log"),
            storage_type=self.mining_config.get("storage_type", "s3")
        )

        self.storage.read_metadata_table(self.mining_config, self.metadata) 
        evolved_df = self.storage.read_sequence_table(self.metadata, filter_out="mined" if not self.mining_config.get("force_recompute", False) else None)
        evolved_df.cache()  # Cache evolved traces as they will be used multiple times during mining

        # Perform mining
        positional_constraints = discover_positional(evolved_df, self.metadata)
        existential_constraints = discover_existential(evolved_df, self.metadata)
        ordered_constraints = discover_ordered(evolved_df, self.metadata)
        unordered_constraints = discover_unordered(evolved_df, self.metadata)

        constraints = positional_constraints \
            .unionByName(existential_constraints, allowMissingColumns=True) \
            .unionByName(ordered_constraints, allowMissingColumns=True) \
            .unionByName(unordered_constraints, allowMissingColumns=True)
        constraints.show(70,truncate=False)

        # Update metadata with new last mining timestamp based on the max timestamp of the evolved traces
        self.metadata.last_mined_timestamp = evolved_df.agg({"start_timestamp": "max"}).collect()[0][0] if not evolved_df.rdd.isEmpty() else self.metadata.last_mined_timestamp
        self.storage.write_metadata_table(self.metadata)

        # self._output_constraints(constraints)


    def _output_constraints(self, constraints: DataFrame):
        """
        Outputs the discovered constraints to a CSV file on the driver's local filesystem 
        based on the specified output path in the mining configuration.
        This method collects the results from the Spark executors and writes them incrementally to avoid driver memory issues.
        
        :param constraints: DataFrame containing the discovered constraints with columns like 'category', 'template', 'sources', 'targets', 'occurrences', 'support', 'confidence', and optionally 'trace_ids'.
        """
        # Prepare a CSV-friendly DataFrame: array columns are serialised to
        # pipe-delimited strings on the executors so the driver only receives
        # flat string rows.
        select_cols = [
            F.col("category"),
            F.col("template"),
            F.concat_ws("|", F.col("sources")).alias("sources"),
            F.concat_ws("|", F.col("targets")).alias("targets"),
            F.col("occurrences").cast("string"),
            F.col("support").cast("string"),
            F.col("confidence").cast("string"),
        ]
        
        # Optionally include the list of trace_ids supporting each constraint, serialized as a pipe-delimited string. 
        # This can be large, so it's controlled by a config flag.
        if self.mining_config.get("include_trace_lists", False):
            select_cols.append(F.concat_ws("|", F.col("trace_ids")).alias("trace_ids"))
        
        constraints_csv = constraints.select(*select_cols)

        # Stream rows partition-by-partition from executors to the driver and write
        # them incrementally into a single CSV file on the driver's local filesystem.
        output_path = self.mining_config["output_path"]
        col_names = constraints_csv.columns

        with open(output_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(col_names)
            for row in constraints_csv.toLocalIterator(prefetchPartitions=True):
                writer.writerow([row[c] for c in col_names])
