import argparse
import datetime
import csv
import json
from pathlib import Path
from typing import Any, Dict
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
    method: str = Field("ngrams", description="Comparison method: 'ngrams', 'rare_rules', or 'targeted_rules'")
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

    def register_routes(self) -> SiestaModule.ApiRoutes|None:
        return {"run": ('POST', self.api_run)}

    def startup(self):
        logger.info("Startup complete.")

    def api_run(self, comparator_config: ComparatorConfig) -> Any:
        """Compare groups of traces using statistical or rule-based methods.

        Three methods available via the `method` field:
        - **`ngrams`**: Compare n-gram frequency distributions between the defined groups.
        - **`rare_rules`**: Discover directly-following rules that are rare in one group but frequent in another.
        - **`targeted_rules`**: Discover rules strongly associated with a target group.

        Results are written to a local file (CSV for `ngrams`, JSON for rule methods) and returned.

        **Config fields:**
        - `log_name` *(str, default: `"example_log"`)* - name of the indexed log. **Required.**
        - `storage_namespace` *(str, default: `"siesta"`)* - storage namespace.
        - `method` *(str, default: `"ngrams"`)* - `"ngrams"`, `"rare_rules"`, or `"targeted_rules"`.
        - `method_params` *(object, default: `{"n": 2}`)* - method-specific parameters.
          `ngrams`: `n` (int) = gram length.
          `targeted_rules`: `target_label` (int, default: `1`), `filtering_support` (float, default: `1`).
        - `separating_key` *(str, default: `"activity"`)* - column used to label traces into groups.
        - `separating_groups` *(list[list[str]])* - group definitions, e.g. `[["fail", "error"]]`
          splits into the listed values vs. all remaining traces.
        - `support_threshold` *(float [0,1], default: `0.0`)* - minimum support fraction for results.
        - `output_path` *(str, default: `"../output/example_log"`)* - local path prefix for the output file.
        """
        logger.info(f"{self.name} is running via API request.")

        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()
        self._load_comparator_config(comparator_config.model_dump())

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
        self.comparator_config["output_path"] = given_output_path + "_" + str(datetime.datetime.now().timestamp())

    
    def compare(self, caller: str):
        logger.info(f"Beginning comparator process initiated by {caller}.")

        # Load metadata if available otherwise initialize it based on the comparator config.
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

        # For now, we consider only TWO groups for comparison, defined by the one list in "separating_groups" config parameter.
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



        # The code below will be used for MORE than 2 groups, when we generalize

        # # Based on the defined value-groups, create groups of events based on the separating key
        # separating_key = self.comparator_config.get("separating_key", "activity")
        # separating_groups = self.comparator_config.get("separating_groups", [])

        # # grouped_dfs will contain a list of tuples: (group_values_on_separating_key, group_events_df)
        # # e.g. [ (["fail_1", "fail_2"], df_of_fail_events), (["success_1", "success_2"], df_of_success_events) ]
        # # or if only one group is defined: [ (["fail_1", "fail_2"], df_of_fail_events), (["not_fail_1", "not_fail_2"], df_of_non_fail_events) ]
        # grouped_dfs = []
        # if len(separating_groups) < 2:
        #     # We consider as second group all values of the separating key that are not in the first group, 
        #     # to ensure we have at least 2 groups to compare.
        #     group_1_df = all_events_df.filter(F.col(separating_key).isin(separating_groups[0]))
        #     group_2_df = all_events_df.filter(~F.col(separating_key).isin(separating_groups[0]))
        #     grouped_dfs.append((separating_groups[0], group_1_df))
        #     grouped_dfs.append((f"not_{separating_groups[0]}", group_2_df))
        # else:
        #     for group in separating_groups:
        #         group_df = all_events_df.filter(F.col(separating_key).isin(group))
        #         grouped_dfs.append((group, group_df))

        all_events_df.unpersist()