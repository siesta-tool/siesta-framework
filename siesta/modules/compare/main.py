import argparse
import datetime
import csv
import json
from pathlib import Path
from typing import Annotated, Any, Dict
from fastapi import Body
from pydantic import BaseModel, ConfigDict, Field
from siesta.model.StorageModel import MetaData
from siesta.core.interfaces import SiestaModule, StorageManager
from siesta.core.config import get_system_config
from siesta.core.logger import timed
from siesta.core.storageFactory import get_storage_manager
from pyspark.sql import SparkSession, functions as F
from siesta.modules.compare.ngrams import discover_ngrams, save_ngram_results, create_network
from siesta.modules.compare.dm import discover_rare_rules, discover_targeted_rules, save_dm_results
from siesta.modules.mine.ordered import discover_ordered
import logging

logger = logging.getLogger(__name__)


class ComparatorConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    log_name: str = Field("example_log", description="Name of the indexed log")
    storage_namespace: str = Field("siesta", description="Storage namespace")
    method: str = Field("ngrams", description="Comparison method: 'ngrams', 'rare_rules', or 'targeted_rules'. Ignored by API endpoints - set automatically.")
    method_params: dict = Field(default_factory=lambda: {"n": 2}, description="Method-specific params. ngrams: {n}. targeted_rules: {target_label, filtering_support}")
    separating_key: str = Field("activity", description="Column used to label traces into groups")
    separating_groups: list[list[str]] = Field(default_factory=list, description="Group definitions, e.g. [['fail','error']] splits into listed values vs. all others")
    support_threshold: float = Field(0.0, description="Minimum support fraction [0,1] for results")
    output_path: str = Field("output/example_log", description="Local path prefix for the output file")


DEFAULT_COMPARATOR_CONFIG: Dict[str, Any] = ComparatorConfig().model_dump()


class Comparing(SiestaModule):

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

    def register_routes(self) -> SiestaModule.ApiRoutes | None:
        return {
            "ngrams":         ("POST", self.api_ngrams),
            "rare_rules":     ("POST", self.api_rare_rules),
            "targeted_rules": ("POST", self.api_targeted_rules),
        }

    def startup(self):
        logger.info("Startup complete.")

    # ------------------------------------------------------------------
    # CLI entry point
    # ------------------------------------------------------------------

    def cli_run(self, args: Any, **kwargs: Any) -> Any:
        logger.info(f"{self.name} is running with args: {args} and kwargs: {kwargs}")

        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        parser = argparse.ArgumentParser(description="Siesta Comparator module")
        parser.add_argument('--mining_config', type=str, help='Path to configuration JSON file', required=False)

        parsed_args, _ = parser.parse_known_args(args)

        if parsed_args.mining_config:
            config_path = parsed_args.mining_config
            if not Path(config_path).exists():
                raise FileNotFoundError(f"Config file {config_path} not found.")

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

    # ------------------------------------------------------------------
    # API entry points
    # ------------------------------------------------------------------

    def api_ngrams(self, comparator_config: Annotated[ComparatorConfig, Body(
        openapi_examples={
            "default": {
                "summary": "Compare n-gram frequencies with default settings",
                "value": {
                    "log_name": "example_log",
                    "storage_namespace": "siesta",
                    "method_params": {"n": 2},
                    "separating_key": "activity",
                    "separating_groups": [],
                    "support_threshold": 0.0,
                },
            },
        }
    )]) -> Any | None:
        """Compare n-gram frequency distributions between two trace groups.

        Splits traces into two groups based on `separating_groups` and computes
        n-gram frequency differences. Results are written to a CSV file.

        **Request body (`ComparatorConfig`):**
        - `log_name` *(str, default: `"example_log"`)* - name of the indexed log.
        - `storage_namespace` *(str, default: `"siesta"`)* - storage namespace.
        - `method_params` *(object, default: `{"n": 2}`)* - `n` (int) = gram length; `vis` (bool) = generate HTML network.
        - `separating_key` *(str, default: `"activity"`)* - column used to label traces into groups.
        - `separating_groups` *(list[list[str]])* - group definitions, e.g. `[["fail", "error"]]`.
        - `support_threshold` *(float [0,1], default: `0.0`)* - minimum support fraction for results.
        """
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        config = comparator_config.model_dump()
        config["method"] = "ngrams"
        try:
            self._load_comparator_config(config)
        except Exception as e:
            logger.exception(f"Error loading comparator config: {e}")
            return {"code": 400, "message": f"Invalid config: {e}"}

        self.compare(caller="api")

        logger.info(f"Completed. Results available at {self.comparator_config['output_path']}.")
        with open(self.comparator_config["output_path"], 'r', newline="") as f:
            try:
                return list(csv.DictReader(f))
            except Exception:
                logger.error(f"Failed to parse ngrams results from {self.comparator_config['output_path']}.")
                return f"Cannot parse results. Check logs and {self.comparator_config['output_path']} for details."

    def api_rare_rules(self, comparator_config: Annotated[ComparatorConfig, Body(
        openapi_examples={
            "default": {
                "summary": "Discover rare rules with default settings",
                "value": {
                    "log_name": "example_log",
                    "storage_namespace": "siesta",
                    "method_params": {"n": 2},
                    "separating_key": "activity",
                    "separating_groups": [],
                    "support_threshold": 0.0,
                },
            },
        }
    )]) -> Any | None:
        """Discover directly-following rules that are rare in one group but frequent in another.

        Compares ordered constraints across the two trace groups and returns rules
        whose support differs significantly. Results are written to a JSON file.

        **Request body (`ComparatorConfig`):**
        - `log_name` *(str, default: `"example_log"`)* - name of the indexed log.
        - `storage_namespace` *(str, default: `"siesta"`)* - storage namespace.
        - `separating_key` *(str, default: `"activity"`)* - column used to label traces into groups.
        - `separating_groups` *(list[list[str]])* - group definitions, e.g. `[["fail", "error"]]`.
        - `support_threshold` *(float [0,1], default: `0.0`)* - minimum support fraction threshold.
        """
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        config = comparator_config.model_dump()
        config["method"] = "rare_rules"
        
        try:
            self._load_comparator_config(config)
        except Exception as e:
            logger.exception(f"Error loading comparator config: {e}")
            return {"code": 400, "message": f"Invalid config: {e}"}
        
        self.compare(caller="api")

        logger.info(f"Completed. Results available at {self.comparator_config['output_path']}.")
        with open(self.comparator_config["output_path"], 'r') as f:
            try:
                return json.load(f)
            except Exception:
                logger.error(f"Failed to parse rare_rules results from {self.comparator_config['output_path']}.")
                return f"Cannot parse results. Check logs and {self.comparator_config['output_path']} for details."

    def api_targeted_rules(self, comparator_config: Annotated[ComparatorConfig, Body(
        openapi_examples={
            "default": {
                "summary": "Discover targeted rules with default settings",
                "value": {
                    "log_name": "example_log",
                    "storage_namespace": "siesta",
                    "method_params": {"n": 2},
                    "separating_key": "activity",
                    "separating_groups": [],
                    "support_threshold": 0.0,
                },
            },
        }
    )]) -> Any | None:
        """Discover directly-following rules strongly associated with a target trace group.

        Evaluates which rules are characteristic of the target group (label `1`)
        using a support-based filter. Results are written to a JSON file.

        **Request body (`ComparatorConfig`):**
        - `log_name` *(str, default: `"example_log"`)* - name of the indexed log.
        - `storage_namespace` *(str, default: `"siesta"`)* - storage namespace.
        - `method_params` *(object)* - `target_label` (int, default: `1`), `filtering_support` (float, default: `1`).
        - `separating_key` *(str, default: `"activity"`)* - column used to label traces into groups.
        - `separating_groups` *(list[list[str]])* - group definitions, e.g. `[["fail", "error"]]`.
        - `support_threshold` *(float [0,1], default: `0.0`)* - minimum support fraction for results.
        """
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        config = comparator_config.model_dump()
        config["method"] = "targeted_rules"
       
        try:
            self._load_comparator_config(config)
        except Exception as e:
            logger.exception(f"Error loading comparator config: {e}")
            return {"code": 400, "message": f"Invalid config: {e}"}

        self.compare(caller="api")

        logger.info(f"Completed. Results available at {self.comparator_config['output_path']}.")
        with open(self.comparator_config["output_path"], 'r') as f:
            try:
                return json.load(f)
            except Exception:
                logger.error(f"Failed to parse targeted_rules results from {self.comparator_config['output_path']}.")
                return f"Cannot parse results. Check logs and {self.comparator_config['output_path']} for details."

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _load_comparator_config(self, config: Dict[str, Any]):
        if not self.storage.log_exists(config):
            logger.exception(f"Log '{config.get('log_name')}' does not exist in storage. Run preprocessing first.")
            raise ValueError(f"Log '{config.get('log_name')}' does not exist in storage. Run preprocessing first.")
        

        self.comparator_config = DEFAULT_COMPARATOR_CONFIG.copy()
        self.comparator_config.update(config)
        if self.comparator_config.get("output_path") is not None and self.comparator_config.get("output_path") == "output/example_log":
            self.comparator_config["output_path"] = "output/" + config.get("log_name", "comparator_results")
            
        given_output_path = config.get("output_path", "../../../output/" + config.get("log_name", "comparator_results"))
        Path(given_output_path).parent.mkdir(parents=True, exist_ok=True)
        self.comparator_config["output_path"] = given_output_path + "_" + str(datetime.datetime.now().timestamp())

    def compare(self, caller: str):
        logger.info(f"Beginning comparator process initiated by {caller}.")

        self.metadata = MetaData(
            storage_namespace=self.comparator_config.get("storage_namespace", "siesta"),
            log_name=self.comparator_config.get("log_name", "default_log"),
            storage_type=self.comparator_config.get("storage_type", "s3")
        )

        self.metadata = self.storage.read_metadata_table(self.metadata)
        all_events_df = self.storage.read_sequence_table(self.metadata).dropDuplicates(["trace_id", "activity", "start_timestamp"])
        all_events_df.cache()

        method = self.comparator_config.get("method", "ngrams")
        params = self.comparator_config.get("method_params", {})

        target_activities = self.comparator_config.get("separating_groups", [[]])[0]
        trace_labels = (
            all_events_df
            .withColumn("label", F.when(F.col("activity").isin(target_activities), 1).otherwise(0))
            .groupBy("trace_id")
            .agg(F.max("label").alias("label"))
        )

        if method == "ngrams":
            results = discover_ngrams(
                events=all_events_df,
                trace_labels=trace_labels,
                n=params.get("n", 2)
            )
            self.comparator_config["output_path"] += ".csv"
            save_ngram_results(results, self.comparator_config["output_path"], fmt="csv")
            if params.get("vis", False):
                with open(self.comparator_config["output_path"].replace(".csv", ".html"), 'w') as f:
                    f.write(create_network(self.comparator_config["output_path"]))

        elif method == "rare_rules":
            ordered_constraints_df = discover_ordered(all_events_df, self.metadata)

            result_list = discover_rare_rules(
                ordered_constraints_df=ordered_constraints_df,
                trace_labels=trace_labels,
                trace_count=self.metadata.trace_count,
                support_pct=self.comparator_config.get("support_threshold", 0.1),
            )

            self.comparator_config["output_path"] += ".json"
            save_dm_results(result_list, self.comparator_config["output_path"])
            logger.info(f"DM results written to {self.comparator_config['output_path']}.")

        elif method == "targeted_rules":
            ordered_constraints_df = discover_ordered(all_events_df, self.metadata)

            result_list = discover_targeted_rules(
                ordered_constraints_df=ordered_constraints_df,
                trace_labels=trace_labels,
                target_label=params.get("target_label", 1),
                support_threshold=self.comparator_config.get("support_threshold", 0.8),
                filtering_support=params.get("filtering_support", 1),
            )

            self.comparator_config["output_path"] += ".json"
            save_dm_results(result_list, self.comparator_config["output_path"])
            logger.info(f"DM_2 results written to {self.comparator_config['output_path']}.")

        all_events_df.unpersist()
