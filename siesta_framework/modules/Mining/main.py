import argparse
import datetime
from pathlib import Path
from typing import Annotated, Any, Dict
from fastapi import Form, UploadFile
from siesta_framework.core.sparkManager import get_spark_session
from siesta_framework.model.StorageModel import MetaData
from siesta_framework.model.SystemModel import DEFAULT_MINING_CONFIG
from siesta_framework.core.interfaces import SiestaModule, StorageManager
from siesta_framework.core.config import get_system_config
from siesta_framework.core.logger import timed
from siesta_framework.core.storageFactory import get_storage_manager
from siesta_framework.modules.Mining.positional import discover_positional
from siesta_framework.modules.Preprocess.parsers import upload_log_file_object
from siesta_framework.modules.Preprocess.builders import build_sequence_table, build_single_table
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery
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
        
        self.begin_miners(caller="api")

        logger.info(f"Mining: Completed. Results available at {self.mining_config['output_path']}.")
        
        with open(self.mining_config["output_path"], 'r') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return f"Mining: Cannot parse mining results. Check logs and {self.mining_config['output_path']} for details."


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

        self.begin_miners(caller="cli")

        logger.info(f"Mining: Completed. Results available at {self.mining_config['output_path']}.")
        
        return self.mining_config["output_path"]

    def _load_mining_config(self, config: Dict[str, Any]):
        self.mining_config = DEFAULT_MINING_CONFIG.copy()
        self.mining_config.update(config)
        # Ensure output_path is unique for each run to avoid overwriting results
        self.mining_config["output_path"] = self.mining_config.get("output_path",self.mining_config.get("log_name","mining_results")) + str(datetime.datetime.now().timestamp())

    # Returns the output location of the mining results (e.g. S3 path or local path)
    def begin_miners(self, caller: str):
        # Placeholder for mining logic
        logger.info(f"Beginning mining process initiated by {caller}.")
        # Here you would implement the actual mining logic, e.g.:
        # - Load preprocessed data from storage
        # - Apply mining algorithms (e.g. process discovery, conformance checking)
        # - Store results back to storage or output them as needed
       
       
       
       
        self.metadata = MetaData(
            storage_namespace=self.mining_config.get("storage_namespace", "siesta"),
            log_name=self.mining_config.get("log_name", "default_log"),
            storage_type=self.mining_config.get("storage_type", "s3")
        )

        # Load existing metadata from storage if available
        self.storage.read_metadata_table(self.mining_config, self.metadata) 
        
        evolved = self.storage.read_sequence_table(self.metadata)
        positional_constraints = discover_positional(evolved, self.metadata)
        positional_constraints.show(truncate=False)  # Show sample results for demonstration

        pass