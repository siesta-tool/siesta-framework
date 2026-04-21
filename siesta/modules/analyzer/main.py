import argparse
import csv
import datetime
import json
from pathlib import Path
from typing import Any, Dict

from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict, Field
from pyspark.sql import SparkSession, functions as F

from siesta.core.config import get_system_config
from siesta.core.interfaces import SiestaModule, StorageManager
from siesta.core.storageFactory import get_storage_manager
from siesta.model.StorageModel import MetaData
from siesta.modules.analyzer.directly_follows import compute_directly_follows
from siesta.modules.analyzer.loop_detection import compute_loop_detection
from siesta.modules.analyzer.durations import compute_activity_durations, compute_group_durations
from siesta.modules.analyzer.attribute_deviations import (
    compute_attribute_deviations, render_html, ALL_STEPS,
)
from siesta.modules.mine.ordered import discover_ordered


class DirectlyFollowsConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    log_name: str = Field("example_log", description="Name of the indexed log")
    storage_namespace: str = Field("siesta", description="Storage namespace")
    end_time: str | None = Field(None, description="Attribute key for event end timestamp. null = transition time (next_start - start)")
    support_threshold: float | None = Field(None, description="Min support fraction [0,1]; null = no filtering")
    filter_out: bool = Field(False, description="When true, keeps pairs with support ≤ threshold instead")
    include_traces: bool = Field(False, description="Append a trace_ids column listing traces that contain each pair")
    return_csv: bool = Field(False, description="Return a CSV file download instead of a JSON list")
    output_path: str = Field("output/example_log", description="Local path prefix for the output file")


class LoopDetectionConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    log_name: str = Field("example_log", description="Name of the indexed log")
    storage_namespace: str = Field("siesta", description="Storage namespace")
    grouping_key: str | list[str] | None = Field(None, description="Attribute key(s) to group by; null = trace_id")
    grouping_value: str | list[str] | dict | None = Field(None, description="Restrict to groups with matching key value(s)")
    min_timestamp: int | None = Field(None, description="Lower bound on start_timestamp (epoch seconds)")
    support_threshold: float | None = Field(None, description="Min support fraction [0,1]; null = no filtering")
    filter_out: bool = Field(False, description="When true, keeps rare loops (support ≤ threshold)")
    top_k: int | None = Field(None, description="Keep only the k most-supported loops; null = all")
    trace_based: bool = Field(False, description="Add trace_ids list to each loop entry (only when grouping by trace_id)")
    output_path: str = Field("output/example_log", description="Local path prefix for the output file")


class DurationsConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    log_name: str = Field("example_log", description="Name of the indexed log")
    storage_namespace: str = Field("siesta", description="Storage namespace")
    duration_mode: str = Field("activity", description="'activity' (per activity type) or 'group' (per group instance)")
    end_time: str | None = Field(None, description="Attribute key for event end timestamp. null = transition / span time")
    grouping_key: str | list[str] | None = Field(None, description="Attribute key(s) defining groups; null = trace_id")
    grouping_value: str | list[str] | None = Field(None, description="Restrict to groups with matching key value(s)")
    per_group: bool = Field(False, description="Activity mode only: produce one row per (group, activity) instead of globally")
    return_csv: bool = Field(False, description="Return a CSV file download instead of a JSON list")
    output_path: str = Field("output/example_log", description="Local path prefix for the output file")


class AttributeDeviationsConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    log_name: str = Field("example_log", description="Name of the indexed log. **Required.**")
    storage_namespace: str = Field("siesta", description="Storage namespace.")
    steps: list[int] = Field(
        list(ALL_STEPS),
        description=(
            "Which analysis steps to run. Subset of [0,1,2,3,4]. "
            "0=value frequency (inter+intra-trace), 1=activity×attribute, "
            "2=position-conditioned, 3=n-gram context, 4=value transitions."
        ),
    )
    excluded_attributes: list[str] | None = Field(
        None,
        description="Attribute keys to skip in addition to auto-detected timestamp keys.",
    )
    surprise_threshold: float = Field(
        4.0,
        description="Anomaly threshold for categorical attributes (−log₂ score). Default 4.0 ≈ support ≤ 6.25 %.",
    )
    zscore_threshold: float = Field(
        3.5,
        description="Anomaly threshold for numeric attributes (|robust z-score| via MAD).",
    )
    ngram_n: int = Field(2, description="N-gram length for step 3 (window of activities ending at the event).")
    min_group_size: int = Field(5, description="Minimum observations required to score a group in steps 3 and 4.")
    n_buckets: int = Field(5, description="Number of relative-position buckets for step 2.")
    support_threshold: float | None = Field(
        None,
        description=(
            "Filter output by inter-trace support of the flagged value [0,1]. "
            "null = no filter. Combined with filter_out."
        ),
    )
    filter_out: bool = Field(
        False,
        description="When true, keep deviations where support ≤ threshold (rare values). "
                    "When false (default), keep deviations where support ≥ threshold.",
    )
    on_rare: float | None = Field(
        None,
        ge=0.0,
        le=1.0,
        description=(
            "Support threshold [0,1] used to enable rare-mode. When set, discover ordered constraints "
            "and run deviations only on traces that violate at least one ordered constraint with "
            "support >= on_rare. null disables rare-mode."
        ),
    )
    output_format: str = Field("json", description="Output format: 'json' (default), 'csv', or 'html'.")
    output_path: str = Field("output/example_log", description="Local path prefix for csv/html output files.")

import logging

logger = logging.getLogger(__name__)

DEFAULT_ANALYZER_CONFIG: Dict[str, Any] = {
    **DirectlyFollowsConfig().model_dump(),
    **LoopDetectionConfig().model_dump(),
    **DurationsConfig().model_dump(),
    **AttributeDeviationsConfig().model_dump(),
    "method": "directly_follows"
}


class Analyzing(SiestaModule):

    name = "analyzer"
    version = "1.0.0"
    spark: SparkSession
    storage: StorageManager
    siesta_config: Dict[str, Any]
    analyzer_config: Dict[str, Any]
    metadata: MetaData | None

    def __init__(self):
        super().__init__()
        self.analyzer_config = {}
        self.metadata = None

    def startup(self):
        logger.info("Analyzer startup complete.")

    def register_routes(self) -> SiestaModule.ApiRoutes | None:
        return {
            "directly_follows":      ("POST", self.api_directly_follows),
            "loop_detection":        ("POST", self.api_loop_detection),
            "durations":             ("POST", self.api_durations),
            "attribute_deviations":  ("POST", self.api_attribute_deviations),
        }

    # ------------------------------------------------------------------
    # API entry points
    # ------------------------------------------------------------------

    def api_directly_follows(self, analyzer_config: DirectlyFollowsConfig) -> Any:
        """Find directly-following activity pairs in an indexed event log.

        Returns pairs of consecutive activities with support (fraction of traces where A
        is directly followed by B) and duration statistics in seconds.

        **Config fields:**
        - `log_name` *(str)* - name of the indexed log. **Required.**
        - `storage_namespace` *(str, default: `"siesta"`)* - storage namespace.
        - `end_time` *(str | null, default: `null`)* - attribute key for event end timestamp.
          If set, duration = `end_time - start_timestamp` (activity duration).
          If null, duration = `next_start - start_timestamp` (transition time).
        - `support_threshold` *(float [0,1] | null, default: `null`)* - keep pairs with support ≥ threshold. `null` = no filtering.
        - `filter_out` *(bool, default: `false`)* - when `true`, keeps pairs with support ≤ threshold instead.
        - `include_traces` *(bool, default: `false`)* - append a `trace_ids` column to the output.
        - `return_csv` *(bool, default: `false`)* - return a CSV file download instead of a JSON list.
        - `output_path` *(str, default: `"output/<log_name>"`)* - local path prefix for the output file.
        """
        logger.info(f"{self.name} running directly_follows via API.")
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        config = analyzer_config.model_dump()
        config["method"] = "directly_follows"
        self._load_analyzer_config(config)

        return self._run_directly_follows(caller="api")

    def api_durations(self, analyzer_config: DurationsConfig) -> Any:
        """Compute duration statistics for activities or groups in an indexed event log.

        Two modes controlled by `duration_mode`:
        - **`activity`**: avg / min / max duration per activity type; optionally per group when `per_group=true`.
        - **`group`**: total duration per group instance (e.g. per trace).

        **Config fields:**
        - `log_name` *(str)* - name of the indexed log. **Required.**
        - `storage_namespace` *(str, default: `"siesta"`)* - storage namespace.
        - `duration_mode` *(str, default: `"activity"`)* - `"activity"` or `"group"`.
        - `end_time` *(str | null, default: `null`)* - attribute key for event end timestamp.
          Activity mode: `end_time - start_timestamp`; group mode: sum of per-event durations.
          If null - activity mode uses transition time; group mode uses `last_start - first_start`.
        - `grouping_key` *(str | list | null, default: `null`)* - attribute key(s) defining groups. `null` = `trace_id`.
        - `grouping_value` *(str | list | null, default: `null`)* - restrict to groups with matching key value(s).
        - `per_group` *(bool, default: `false`)* - `activity` mode only: produce one row per (group, activity).
        - `return_csv` *(bool, default: `false`)* - return a CSV file download instead of a JSON list.
        - `output_path` *(str, default: `"output/<log_name>"`)* - local path prefix for the output file.
        """
        logger.info(f"{self.name} running durations via API.")
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        config = analyzer_config.model_dump()
        config["method"] = "durations"
        self._load_analyzer_config(config)

        return self._run_durations(caller="api")

    def api_loop_detection(self, analyzer_config: LoopDetectionConfig) -> Any:
        """Detect self-loops and non-self-loops in an indexed event log.

        A **self-loop** is an activity immediately followed by itself.
        A **non-self-loop** is a minimal cycle A → … → A where A does not appear in the body.

        Returns JSON with `self_loops` and `non_self_loops` arrays. Each entry contains the
        activity pattern and its support fraction across groups.

        **Config fields:**
        - `log_name` *(str)* - name of the indexed log. **Required.**
        - `storage_namespace` *(str, default: `"siesta"`)* - storage namespace.
        - `grouping_key` *(str | list | null, default: `null`)* - attribute key(s) to group by. `null` = `trace_id`.
        - `grouping_value` *(str | list | dict | null, default: `null`)* - restrict to specific group values.
        - `min_timestamp` *(int | null, default: `null`)* - lower bound on `start_timestamp` (epoch seconds).
        - `support_threshold` *(float [0,1] | null, default: `null`)* - keep loops with support ≥ threshold. `null` = no filtering.
        - `filter_out` *(bool, default: `false`)* - when `true`, keeps loops with support ≤ threshold (rare loops).
        - `top_k` *(int | null, default: `null`)* - keep only the k most-supported loops. `null` = all.
        - `trace_based` *(bool, default: `false`)* - add a `trace_ids` list to each loop entry (only when grouping by `trace_id`).
        - `output_path` *(str, default: `"output/<log_name>"`)* - local path prefix for the output file.
        """
        logger.info(f"{self.name} running loop_detection via API.")
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        config = analyzer_config.model_dump()
        config["method"] = "loop_detection"
        self._load_analyzer_config(config)

        return self._run_loop_detection(caller="api")

    def api_attribute_deviations(self, analyzer_config: AttributeDeviationsConfig) -> Any:
        """Detect anomalous attribute values in an indexed event log using a multi-step pipeline.

        Five steps (all on by default; select via `steps`):
        - **Step 0** - Value frequency: flags values that are globally rare across traces
          (`value_freq_inter`) or appear unusually often within a single trace (`value_freq_intra`).
        - **Step 1** - Activity × Attribute: flags values whose distribution within an activity type
          is anomalous (Laplace surprise for categorical, MAD z-score for numeric).
        - **Step 2** - Position-conditioned: same as step 1 but conditioned on relative position
          within the trace (bucketed).
        - **Step 3** - N-gram context: same as step 1 but conditioned on the n-gram of activities
          ending at this event.
        - **Step 4** - Value transitions: flags rare (prev_value → curr_value) transitions
          within a trace (categorical attributes only).

        **Config fields:**
        - `log_name` *(str)* - name of the indexed log. **Required.**
        - `storage_namespace` *(str, default: `"siesta"`)* - storage namespace.
        - `steps` *(list[int], default: `[0,1,2,3,4]`)* - which steps to run.
        - `excluded_attributes` *(list[str] | null)* - attribute keys to skip (auto-excludes timestamp keys).
        - `surprise_threshold` *(float, default: `4.0`)* - categorical anomaly threshold (−log₂ score).
        - `zscore_threshold` *(float, default: `3.5`)* - numeric anomaly threshold (|robust z-score|).
        - `ngram_n` *(int, default: `2`)* - n-gram length for step 3.
        - `min_group_size` *(int, default: `5`)* - minimum group size for steps 3 and 4.
        - `n_buckets` *(int, default: `5`)* - position buckets for step 2.
        - `support_threshold` *(float | null)* - filter output by inter-trace support [0,1].
                - `filter_out` *(bool, default: `false`)* - when true, keep deviations with support ≤ threshold.
                - `on_rare` *(float [0,1] | null, default: `null`)* - rare-mode threshold.
                    When set, run analysis only on traces violating at least one ordered constraint
                    whose support is ≥ `on_rare`.
        - `output_format` *(str, default: `"json"`)* - `"json"`, `"csv"`, or `"html"`.
        - `output_path` *(str)* - path prefix for csv/html file output.
        """
        logger.info(f"{self.name} running attribute_deviations via API.")
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        config = analyzer_config.model_dump()
        config["method"] = "attribute_deviations"
        self._load_analyzer_config(config)

        return self._run_attribute_deviations(caller="api")

    # ------------------------------------------------------------------
    # CLI entry point
    # ------------------------------------------------------------------

    def cli_run(self, args: Any, **kwargs: Any) -> Any:
        logger.info(f"{self.name} is running with args: {args}")
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        parser = argparse.ArgumentParser(description="Siesta Analyzer module")
        parser.add_argument("--analyzer_config", type=str, required=False,
                            help="Path to analyzer configuration JSON file")
        parsed_args, _ = parser.parse_known_args(args)

        if not parsed_args.analyzer_config:
            raise RuntimeError("Config not provided. Use --analyzer_config <path>")

        config_path = parsed_args.analyzer_config
        if not Path(config_path).exists():
            raise FileNotFoundError(f"Config file {config_path} not found.")

        with open(config_path, "r") as f:
            user_config = json.load(f)

        self._load_analyzer_config(user_config)
        self.storage.initialize_db(self.analyzer_config)

        method = self.analyzer_config.get("method", "directly_follows")
        match method:
            case "directly_follows":
                return self._run_directly_follows(caller="cli")
            case "loop_detection":
                return self._run_loop_detection(caller="cli")
            case "durations":
                return self._run_durations(caller="cli")
            case "attribute_deviations":
                return self._run_attribute_deviations(caller="cli")
            case _:
                raise ValueError(f"Unknown analyzer method: '{method}'")

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _load_analyzer_config(self, config: Dict[str, Any]):
        if not self.storage.log_exists(config):
            log_name = config.get("log_name", "default_log")
            logger.error(
                f"Log '{log_name}' does not exist in storage. Run indexing first."
            )
            return f"Log '{log_name}' does not exist in storage. Run indexing first."

        self.analyzer_config = DEFAULT_ANALYZER_CONFIG.copy()
        self.analyzer_config.update(config)

        given_output = config.get(
            "output_path",
            "output/" + config.get("log_name", "analyzer_results"),
        )
        Path(given_output).parent.mkdir(parents=True, exist_ok=True)
        self.analyzer_config["output_path"] = (
            given_output + "_" + str(datetime.datetime.now().timestamp())
        )

    def _load_metadata(self):
        self.metadata = MetaData(
            storage_namespace=self.analyzer_config.get("storage_namespace", "siesta"),
            log_name=self.analyzer_config.get("log_name", "default_log"),
            storage_type=self.analyzer_config.get("storage_type", "s3"),
        )
        self.metadata = self.storage.read_metadata_table(self.metadata)

    # ------------------------------------------------------------------
    # Method implementations
    # ------------------------------------------------------------------

    def _run_directly_follows(self, caller: str) -> Any:
        logger.info(f"Running directly_follows initiated by {caller}.")
        self._load_metadata()

        events_df = self.storage.read_sequence_table(self.metadata)
        events_df.cache()

        result_df = compute_directly_follows(
            events_df=events_df,
            trace_count=self.metadata.trace_count,
            end_time=self.analyzer_config.get("end_time"),
            support_threshold=self.analyzer_config.get("support_threshold"),
            filter_out=self.analyzer_config.get("filter_out", False),
            include_traces=self.analyzer_config.get("include_traces", False),
        )

        output_path = self.analyzer_config["output_path"] + ".csv"
        self.analyzer_config["output_path"] = output_path

        result_pd = result_df.toPandas()
        if self.analyzer_config.get("include_traces", False) and "trace_ids" in result_pd.columns:
            result_pd["trace_ids"] = result_pd["trace_ids"].apply(
                lambda x: ",".join(sorted(x)) if x else ""
            )
        result_pd.to_csv(output_path, index=False)

        events_df.unpersist()
        logger.info(f"Completed. Results written to {output_path}.")

        if caller == "api":
            if self.analyzer_config.get("return_csv", False):
                return FileResponse(
                    output_path,
                    media_type="text/csv",
                    filename=Path(output_path).name,
                )
            with open(output_path, "r", newline="") as f:
                try:
                    return list(csv.DictReader(f))
                except Exception:
                    logger.error(f"Failed to parse results from {output_path}.")
                    return f"Results written to {output_path}. Check logs for details."

        return output_path

    def _run_loop_detection(self, caller: str) -> Any:
        logger.info(f"Running loop_detection initiated by {caller}.")
        self._load_metadata()

        events_df = self.storage.read_sequence_table(self.metadata)
        events_df.cache()

        result = compute_loop_detection(
            events_df=events_df,
            grouping_key=self.analyzer_config.get("grouping_key"),
            grouping_value=self.analyzer_config.get("grouping_value"),
            min_timestamp=self.analyzer_config.get("min_timestamp"),
            support_threshold=self.analyzer_config.get("support_threshold"),
            filter_out=self.analyzer_config.get("filter_out", False),
            top_k=self.analyzer_config.get("top_k"),
            trace_based=self.analyzer_config.get("trace_based", False),
        )

        events_df.unpersist()
        logger.info(
            f"Completed. Found {len(result['self_loops'])} self-loop type(s) and "
            f"{len(result['non_self_loops'])} non-self-loop type(s)."
        )

        if caller == "cli":
            output_path = self.analyzer_config["output_path"] + ".json"
            self.analyzer_config["output_path"] = output_path
            with open(output_path, "w") as f:
                json.dump(result, f, indent=2)
            logger.info(f"Results written to {output_path}.")
            return output_path

        return result

    def _run_durations(self, caller: str) -> Any:
        logger.info(f"Running durations initiated by {caller}.")
        self._load_metadata()

        events_df = self.storage.read_sequence_table(self.metadata)
        events_df.cache()

        mode         = self.analyzer_config.get("duration_mode", "activity")
        end_time     = self.analyzer_config.get("end_time")
        grouping_key = self.analyzer_config.get("grouping_key")
        grouping_val = self.analyzer_config.get("grouping_value")

        if mode == "group":
            result_df = compute_group_durations(
                events_df=events_df,
                end_time=end_time,
                grouping_key=grouping_key,
                grouping_value=grouping_val,
            )
        else:
            result_df = compute_activity_durations(
                events_df=events_df,
                end_time=end_time,
                grouping_key=grouping_key,
                grouping_value=grouping_val,
                per_group=self.analyzer_config.get("per_group", False),
            )

        output_path = self.analyzer_config["output_path"] + ".csv"
        self.analyzer_config["output_path"] = output_path

        result_df.toPandas().to_csv(output_path, index=False)

        events_df.unpersist()
        logger.info(f"Completed. Results written to {output_path}.")

        if caller == "api":
            if self.analyzer_config.get("return_csv", False):
                return FileResponse(
                    output_path,
                    media_type="text/csv",
                    filename=Path(output_path).name,
                )
            with open(output_path, "r", newline="") as f:
                try:
                    return list(csv.DictReader(f))
                except Exception:
                    logger.error(f"Failed to parse results from {output_path}.")
                    return f"Results written to {output_path}. Check logs for details."

        return output_path

    def _run_attribute_deviations(self, caller: str) -> Any:
        logger.info(f"Running attribute_deviations initiated by {caller}.")
        self._load_metadata()

        events_df = self.storage.read_sequence_table(self.metadata)
        events_df.cache()
        total_traces = self.metadata.trace_count

        rare_support_threshold = self.analyzer_config.get("on_rare")
        if rare_support_threshold is not None:
            logger.info(
                "on_rare set: discovering ordered constraints and filtering to non-satisfying traces "
                f"for support >= {rare_support_threshold}."
            )

            ordered_constraints = discover_ordered(events_df, self.metadata).select(
                "template", "source", "target", "trace_id"
            ).cache()

            high_supported = (
                ordered_constraints
                .groupBy("template", "source", "target")
                .agg(F.countDistinct("trace_id").alias("support_count"))
                .withColumn("support", F.col("support_count") / F.lit(self.metadata.trace_count))
                .filter(F.col("support") >= F.lit(float(rare_support_threshold)))
                .select("template", "source", "target")
            )

            high_constraint_count = high_supported.count()
            if high_constraint_count > 0:
                satisfied_counts = (
                    ordered_constraints
                    .join(F.broadcast(high_supported), on=["template", "source", "target"], how="inner")
                    .select("trace_id", "template", "source", "target")
                    .distinct()
                    .groupBy("trace_id")
                    .agg(F.count(F.lit(1)).alias("satisfied_high_constraints"))
                )

                rare_trace_ids = (
                    events_df
                    .select("trace_id")
                    .distinct()
                    .join(satisfied_counts, on="trace_id", how="left")
                    .fillna({"satisfied_high_constraints": 0})
                    .filter(F.col("satisfied_high_constraints") < F.lit(high_constraint_count))
                    .select("trace_id")
                )

                filtered_events_df = events_df.join(F.broadcast(rare_trace_ids), on="trace_id", how="inner")
                filtered_events_df.cache()
                events_df.unpersist()
                events_df = filtered_events_df

                total_traces = rare_trace_ids.count()
                logger.info(
                    f"on_rare set: selected {total_traces} non-satisfying trace(s) "
                    f"out of {self.metadata.trace_count}."
                )
            else:
                logger.info(
                    "on_rare set: no high-support ordered constraints found; "
                    "running deviations on all traces."
                )

            ordered_constraints.unpersist()

        records, active_steps = compute_attribute_deviations(
            events_df=events_df,
            total_traces=total_traces,
            steps=self.analyzer_config.get("steps", list(ALL_STEPS)),
            excluded_keys=self.analyzer_config.get("excluded_attributes"),
            surprise_threshold=self.analyzer_config.get("surprise_threshold", 4.0),
            zscore_threshold=self.analyzer_config.get("zscore_threshold", 3.5),
            n_buckets=self.analyzer_config.get("n_buckets", 5),
            ngram_n=self.analyzer_config.get("ngram_n", 2),
            min_group_size=self.analyzer_config.get("min_group_size", 5),
            support_threshold=self.analyzer_config.get("support_threshold"),
            filter_out=self.analyzer_config.get("filter_out", False),
        )

        events_df.unpersist()
        logger.info(f"Completed. {len(records)} deviation records found.")

        output_format = self.analyzer_config.get("output_format", "json")
        log_name = self.analyzer_config.get("log_name", "log")

        if output_format == "csv":
            import csv as _csv
            output_path = self.analyzer_config["output_path"] + "_deviations.csv"
            with open(output_path, "w", newline="", encoding="utf-8") as f:
                if records:
                    all_step_keys: set = set()
                    for rec in records:
                        all_step_keys.update(rec.get("scores", {}).keys())
                    flat = []
                    for rec in records:
                        row = {k: v for k, v in rec.items() if k not in ("flagged_by", "scores")}
                        row["flagged_by"] = "|".join(rec.get("flagged_by", []))
                        for sk in sorted(all_step_keys):
                            row[f"score_{sk}"] = rec["scores"].get(sk, "")
                        flat.append(row)
                    writer = _csv.DictWriter(f, fieldnames=list(flat[0].keys()))
                    writer.writeheader()
                    writer.writerows(flat)
            if caller == "api":
                return FileResponse(output_path, media_type="text/csv", filename=Path(output_path).name)
            return output_path

        if output_format == "html":
            output_path = self.analyzer_config["output_path"] + "_deviations.html"
            html_content = render_html(records, log_name, active_steps)
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(html_content)
            if caller == "api":
                return FileResponse(output_path, media_type="text/html", filename=Path(output_path).name)
            return output_path

        # Default: JSON
        result = {
            "log_name": log_name,
            "total_deviations": len(records),
            "deviations": records,
        }
        if caller == "cli":
            output_path = self.analyzer_config["output_path"] + "_deviations.json"
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2)
            logger.info(f"Results written to {output_path}.")
            return output_path
        return result
