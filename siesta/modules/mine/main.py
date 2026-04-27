import argparse
import datetime
import time
from pathlib import Path
from typing import Annotated, Any, Dict
from fastapi import Body
from pydantic import BaseModel, ConfigDict, Field
from siesta.model.StorageModel import MetaData
from siesta.core.interfaces import SiestaModule, StorageManager
from siesta.core.config import get_system_config
from siesta.core.logger import timed
from siesta.core.storageFactory import get_storage_manager
from siesta.core.sparkManager import cleanup as spark_cleanup
from siesta.modules.mine.existential import discover_existential
from siesta.modules.mine.positional import discover_positional
from siesta.modules.mine.ordered import discover_ordered
from siesta.modules.mine.unordered import discover_unordered
from siesta.modules.mine.negations import discover_negations
from pyspark.sql import SparkSession, DataFrame, functions as F

import csv
import json
import logging
logger = logging.getLogger(__name__)


class MiningConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    log_name: str = Field("example_log", description="Name of the indexed log")
    storage_namespace: str = Field("siesta", description="Storage namespace")
    categories: list[str] = Field(["*"], description="Constraint categories: 'positional', 'existential', 'ordered', 'unordered', 'negation', or '*' for all")
    grouping: str = Field("trace", description="Grouping strategy: 'trace' or 'window'")
    window_size: int = Field(30, description="Position-based window size when grouping='window'")
    support_threshold: float = Field(0.0, description="Minimum support fraction [0,1] to retain constraints")
    include_trace_lists: bool = Field(False, description="Append a pipe-delimited trace_ids column per constraint")
    force_recompute: bool = Field(False, description="Remine all traces ignoring previous mining state")
    output_path: str = Field("output/example_log", description="Local path prefix for the output CSV")


DEFAULT_MINING_CONFIG: Dict[str, Any] = MiningConfig().model_dump()


class Mining(SiestaModule):
        
    name = "miner"
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

    def api_run(self, mining_config: Annotated[MiningConfig, Body(openapi_examples={
        "default": {
            "summary": "Mine all constraint categories with default settings",
            "value": {
                "log_name": "example_log",
                "storage_namespace": "siesta",
                "categories": ["*"],
                "grouping": "trace",
                "window_size": 30,
                "support_threshold": 0.0,
                "include_trace_lists": False,
                "force_recompute": False,
            },
        },
    })]) -> Any:
        """Mine declarative constraints from an indexed event log.

        Performs incremental constraint discovery across the selected categories. Only
        traces that evolved since the last mining run are processed unless
        `force_recompute` is set. Results are written to a CSV file and returned as a
        list of rows.

        **Config fields:**
        - `log_name` *(str, default: `"example_log"`)* - name of the indexed log. **Required.**
        - `storage_namespace` *(str, default: `"siesta"`)* - storage namespace.
        - `categories` *(list, default: `["*"]`)* - constraint categories to mine.
            `"*"` = all. Options: `"positional"`, `"existential"`, `"ordered"`, `"unordered"`, `"negation"`.
        - `grouping` *(str, default: `"trace"`)* - grouping strategy: `"trace"` or `"window"`.
        - `window_size` *(int, default: `30`)* - position-based window size when `grouping="window"`.
        - `support_threshold` *(float [0,1], default: `0.0`)* - minimum support fraction to retain constraints.
        - `include_trace_lists` *(bool, default: `false`)* - append a pipe-delimited `trace_ids` column per constraint.
        - `force_recompute` *(bool, default: `false`)* - remine all traces ignoring previous mining state.
        """
        logger.info(f"{self.name} is running via API request.")

        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()
        
        try:
            self._load_mining_config(mining_config.model_dump())
        except ValueError as e:
            logger.error(f"Invalid mining config: {e}")
            return {"code": 400, "message": str(e)}

        logger.info(f"Running mining with args: {mining_config}")
        
        start_time = time.time()
        self.mine(caller="api")
        end_time = time.time()

        logger.info(f"Completed in {end_time - start_time} seconds. Results available at {self.mining_config['output_path']}.")
        
        with open(self.mining_config["output_path"], 'r', newline="") as f:
            try:
                return {"code": 200, "mined": list(csv.DictReader(f)), "time": end_time - start_time}
            except Exception:
                logger.error(f"Failed to parse mining results from {self.mining_config['output_path']}. Check if the file is a valid CSV and inspect logs for details.")
                return {"code": 500, "message": f"Cannot parse mining results. Check logs and {self.mining_config['output_path']} for details."}


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
                    user_mining_config = json.load(f)
                    
                    self._load_mining_config(user_mining_config)
                    self.storage.initialize_db(self.mining_config)

                    logger.info(f"Loaded config from {config_path}: {user_mining_config}")
            except Exception as e:
                logger.error(f"Failed to load mining config from {config_path}: {e}")
                raise ValueError(f"Invalid config file: {e}")

        start_time = time.time()
        self.mine(caller="cli")
        end_time = time.time()
        logger.info(f"Completed in {end_time - start_time:.2f} seconds. Results available at {self.mining_config['output_path']}.")
        
        return self.mining_config["output_path"]

    def _load_mining_config(self, config: Dict[str, Any]):
        # Validate that the specified log exists in storage before proceeding with mining. 
        if not self.storage.log_exists(config):
            log_name = config.get("log_name", "default_log")
            logger.exception(f"Log '{log_name}' does not exist in storage. Run preprocessing first.")
            raise ValueError(f"Log '{log_name}' does not exist in storage. Run preprocessing first.")
        
        self.mining_config = DEFAULT_MINING_CONFIG.copy()
        self.mining_config.update(config)

        # Ensure that the specified categories are valid before proceeding with mining.
        valid_categories = {"positional", "existential", "ordered", "unordered", "negation", "*"}
        if not set(self.mining_config["categories"]).issubset(valid_categories):
            raise ValueError(f"Invalid categories specified in mining_config: {self.mining_config['categories']}. Valid options are: {valid_categories}.")

        # Ensure output_path is unique for each run to avoid overwriting results
        given_output_path = config.get("output_path", "../output/" + config.get("log_name", "mining_results"))
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

        self.metadata = self.storage.read_metadata_table(self.metadata) 
        evolved_df = self.storage.read_sequence_table(self.metadata, filter_out="mined" if not self.mining_config.get("force_recompute", False) else None)
        evolved_df.cache()  # Cache evolved traces as they will be used multiple times during mining

        # Perform mining based on the specified categories in the mining configuration. 
        # Each miner function returns a DataFrame with a common schema, and we union them together 
        # while adding a "category" column to identify the source of each constraint.
        miners = []
        include_trace_lists = self.mining_config.get("include_trace_lists", False)
        for category in self.mining_config["categories"]:
            if category in ["positional", "*"]:
                miners.append(("positional", discover_positional))
            if category in ["existential", "*"]:
                miners.append(("existential", discover_existential))
            if category in ["ordered", "*"]:
                miners.append(("ordered", discover_ordered))
            if category in ["unordered", "*"]:
                miners.append(("unordered", discover_unordered))
            if category in ["negation", "*"]:
                miners.append(("negation", lambda e, m: discover_negations(e, m, include_trace_lists)))
        
        raw_miner_results = []
        constraints_df_list = []
        for category, miner_func in miners:
            result = miner_func(evolved_df, self.metadata)
            raw_miner_results.append(result)
            constraints_df_list.append(result.withColumn("category", F.lit(category)))

        constraints_df = constraints_df_list[0]
        for constaint_df in constraints_df_list[1:]:
            constraints_df = constraints_df.unionByName(constaint_df, allowMissingColumns=True)

        # Update metadata with new last mining timestamp based on the max timestamp of the evolved traces
        self.metadata.last_mined_timestamp = evolved_df.agg({"start_timestamp": "max"}).collect()[0][0] if not evolved_df.rdd.isEmpty() else self.metadata.last_mined_timestamp
        evolved_df.unpersist()
        self.storage.write_metadata_table(self.metadata)

        # Output the discovered constraints to a CSV file on the driver's local filesystem 
        # based on the specified output path in the mining configuration.
        self._output_constraints(constraints_df, self.metadata.trace_count)

        # Release cached miner results now that output is written
        for df in raw_miner_results:
            df.unpersist(blocking=False)

        # Release Delta metadata and any remaining cached data
        spark_cleanup()


    def _output_constraints(self, constraints_df: DataFrame, trace_count: int):
        """
        Outputs the discovered constraints to a CSV file on the driver's local filesystem 
        based on the specified output path in the mining configuration.
        This method collects the results from the Spark executors and writes them incrementally to avoid driver memory issues.
        
        :param constraints: DataFrame based on ConstraintEntry schema (template, source, target, occurrences, trace_id)
        """
        # Aggregate trace_ids for the same (template, source, target, occurrences) tuples
        # Handle pre-aggregated constraints (support-only negation mode) separately
        has_precomputed = "_support_count" in constraints_df.columns

        if has_precomputed:
            precomputed = constraints_df.filter(F.col("_support_count").isNotNull())
            trace_level = constraints_df.filter(F.col("_support_count").isNull()).drop("_support_count")
        else:
            trace_level = constraints_df
            precomputed = None

        grouped_constraints = trace_level.groupBy(
            "category", "template", "source", "target", "occurrences"
        ).agg(
            F.collect_list("trace_id").alias("trace_ids")
        )

        # Calculate support: len(trace_ids) / trace_count
        grouped_constraints = grouped_constraints.withColumn(
            "support",
            (F.size(F.col("trace_ids")) / F.lit(trace_count))
        )

        if precomputed is not None:
            grouped_pre = precomputed.select(
                F.col("category"), F.col("template"), F.col("source"),
                F.col("target"), F.col("occurrences"),
                (F.col("_support_count") / F.lit(trace_count)).alias("support"),
                F.array().cast("array<string>").alias("trace_ids"),
            )
            grouped_constraints = grouped_constraints.unionByName(grouped_pre)

        grouped_constraints = grouped_constraints.filter(F.col("support") >= self.mining_config.get("support_threshold", 0.0))

        # Prepare a CSV-friendly DataFrame
        select_cols = [
            F.col("category"),
            F.col("template"),
            F.col("source"),
            F.col("target"),
            F.col("occurrences").cast("string"),
            F.col("support").cast("string"),
        ]
        
        # Optionally include the list of trace_ids supporting each constraint, serialized as a pipe-delimited string. 
        if self.mining_config.get("include_trace_lists", False):
            select_cols.append(F.concat_ws("|", F.col("trace_ids")).alias("trace_ids"))
        
        constraints_csv = grouped_constraints.select(*select_cols)

        # Stream rows partition-by-partition from executors to the driver and write
        # them incrementally into a single CSV file on the driver's local filesystem.
        output_path = self.mining_config["output_path"]
        col_names = constraints_csv.columns

        with open(output_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(col_names)
            for row in constraints_csv.toLocalIterator(prefetchPartitions=True):
                writer.writerow([row[c] for c in col_names])
