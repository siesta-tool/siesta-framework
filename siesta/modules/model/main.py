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
from siesta.modules.model.discover import discover_process_model
import shutil
import os


class ModelerConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    log_name: str = Field("example_log", description="Name of the indexed log")
    storage_namespace: str = Field("siesta", description="Storage namespace")
    end_time: str | None = Field(None, description="Attribute key for event end timestamp. null = transition time (next_start - start)")
    output_format: str = Field("xml", description="Output format: 'xml' (default), 'png'")
    output_path: str = Field("output/example_log", description="Local path prefix for the output file")


import logging

logger = logging.getLogger(__name__)

DEFAULT_MODELER_CONFIG: Dict[str, Any] = {
    **ModelerConfig().model_dump(),
    "method": "directly_follows"
}


class Modeling(SiestaModule):

    name = "modeler"
    version = "1.0.0"
    spark: SparkSession
    storage: StorageManager
    siesta_config: Dict[str, Any]
    modeler_config: Dict[str, Any]
    metadata: MetaData | None

    def __init__(self):
        super().__init__()
        self.modeler_config = {}
        self.metadata = None

    def startup(self):
        logger.info("Analyzer startup complete.")

    def register_routes(self) -> SiestaModule.ApiRoutes | None:
        return {
            "directly_follows":   ("POST", self.api_directly_follows),
            "bpmn":               ("POST", self.api_bpmn)
        }

    # ------------------------------------------------------------------
    # API entry points
    # ------------------------------------------------------------------

    def api_directly_follows(self, modeler_config: ModelerConfig) -> Any:
        """Find directly-following activity pairs in an indexed event log.

        Returns pairs of consecutive activities with support (fraction of traces where A
        is directly followed by B) and duration statistics in seconds.

        **Config fields:**
        - `log_name` *(str)* - name of the indexed log. **Required.**
        - `storage_namespace` *(str, default: `"siesta"`)* - storage namespace.
        - `end_time` *(str | null, default: `null`)* - attribute key for event end timestamp.
          If set, duration = `end_time - start_timestamp` (activity duration).
          If null, duration = `next_start - start_timestamp` (transition time).
        - `output_format` *(str, default: `"xml"`)* - output format: `"xml"` (default) or `"json"`.
        - `output_path` *(str, default: `"output/<log_name>"`)* - local path prefix for the output file.
        """
        logger.info(f"{self.name} running directly_follows via API.")
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        config = modeler_config.model_dump()
        config["method"] = "directly_follows"
        self._load_modeler_config(config)

        return self._run_directly_follows(caller="api")

    def api_bpmn(self, modeler_config: ModelerConfig) -> Any:
        """Find directly-following activity pairs in an indexed event log.

        Returns pairs of consecutive activities with support (fraction of traces where A
        is directly followed by B) and duration statistics in seconds.

        **Config fields:**
        - `log_name` *(str)* - name of the indexed log. **Required.**
        - `storage_namespace` *(str, default: `"siesta"`)* - storage namespace.
        - `end_time` *(str | null, default: `null`)* - attribute key for event end timestamp.
          If set, duration = `end_time - start_timestamp` (activity duration).
          If null, duration = `next_start - start_timestamp` (transition time).
        - `output_format` *(str, default: `"xml"`)* - output format: `"xml"` (default) or `"json"`.
        - `output_path` *(str, default: `"output/<log_name>"`)* - local path prefix for the output file.
        """
        logger.info(f"{self.name} running bpmn via API.")
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        config = modeler_config.model_dump()
        config["method"] = "bpmn"
        self._load_modeler_config(config)

        return self._run_bpmn(caller="api")
    

    # ------------------------------------------------------------------
    # CLI entry point
    # ------------------------------------------------------------------

    def cli_run(self, args: Any, **kwargs: Any) -> Any:
        logger.info(f"{self.name} is running with args: {args}")
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        parser = argparse.ArgumentParser(description="Siesta Modeler module")
        parser.add_argument("--modeler_config", type=str, required=False,
                            help="Path to modeler configuration JSON file")
        parsed_args, _ = parser.parse_known_args(args)

        if not parsed_args.modeler_config:
            raise RuntimeError("Config not provided. Use --modeler_config <path>")

        config_path = parsed_args.modeler_config
        if not Path(config_path).exists():
            raise FileNotFoundError(f"Config file {config_path} not found.")

        with open(config_path, "r") as f:
            user_config = json.load(f)

        self._load_modeler_config(user_config)
        self.storage.initialize_db(self.modeler_config)

        method = self.modeler_config.get("method", "directly_follows")
        match method:
            case "directly_follows":
                return self._run_directly_follows(caller="cli")
            case "bpmn":
                return self._run_bpmn(caller="cli")
            case _:
                raise ValueError(f"Unknown modeler method: '{method}'")

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _load_modeler_config(self, config: Dict[str, Any]):
        if not self.storage.log_exists(config):
            log_name = config.get("log_name", "default_log")
            logger.error(
                f"Log '{log_name}' does not exist in storage. Run indexing first."
            )
            return f"Log '{log_name}' does not exist in storage. Run indexing first."

        self.modeler_config = DEFAULT_MODELER_CONFIG.copy()
        self.modeler_config.update(config)

        given_output = config.get(
            "output_path",
            "output/" + config.get("log_name", "analyzer_results"),
        )
        Path(given_output).parent.mkdir(parents=True, exist_ok=True)
        self.modeler_config["output_path"] = (
            given_output + "_" + str(datetime.datetime.now().timestamp())
        )

    def _load_metadata(self):
        self.metadata = MetaData(
            storage_namespace=self.modeler_config.get("storage_namespace", "siesta"),
            log_name=self.modeler_config.get("log_name", "default_log"),
            storage_type=self.modeler_config.get("storage_type", "s3"),
        )
        self.metadata = self.storage.read_metadata_table(self.metadata)

    # ------------------------------------------------------------------
    # Method implementations
    # ------------------------------------------------------------------

    def _run_directly_follows(self, caller: str) -> Any:
        logger.info(f"Running model discovery (dfg) initiated by {caller}.")
        self._load_metadata()

        events_df = self.storage.read_sequence_table(self.metadata)
        events_df.cache()

        end_time = self.modeler_config.get("end_time")
        activity_durations = None

        # If an end_time attribute is provided, try to parse it defensively and
        # compute average durations per activity for visualization.
        if end_time:
            try:
                end_raw = F.col("attributes").getItem(end_time)
                # If numeric (epoch seconds) use as-is, otherwise try to parse timestamp
                end_ts_expr = F.when(end_raw.isNull(), None).when(
                    end_raw.rlike('^[0-9]+$'), end_raw.cast('long')
                ).otherwise(F.unix_timestamp(F.to_timestamp(end_raw)))

                dur_df = (
                    events_df
                    .withColumn("end_ts_parsed", end_ts_expr)
                    .withColumn("duration", F.col("end_ts_parsed") - F.col("start_timestamp"))
                    .filter(F.col("duration").isNotNull())
                )

                dur_agg = dur_df.groupBy("activity").agg(F.avg(F.col("duration")).alias("avg_duration"))
                dur_pd = dur_agg.toPandas()
                activity_durations = {
                    row["activity"]: float(row["avg_duration"]) for _, row in dur_pd.dropna(subset=["avg_duration"]).iterrows()
                }
            except Exception:
                logger.exception("Failed to compute activity durations from attributes; continuing without durations.")
                activity_durations = None

        # Prepare DataFrame for PM4Py: rename columns to expected names
        pm_df = (
            events_df
            .withColumnRenamed("trace_id", "case:concept:name")
            .withColumnRenamed("activity", "concept:name")
            .withColumnRenamed("start_timestamp", "time:timestamp")
        )

        # Discover model using DFG
        try:
            model_path, fmt, vis_path = discover_process_model(
                pm_df,
                algo="dfg",
                output_type="petrinet",
                noise_threshold=self.modeler_config.get("noise_threshold", 0.0),
                filter_percentile=self.modeler_config.get("filter_percentile", 0.0),
                activity_durations=activity_durations,
            )
        except Exception:
            events_df.unpersist()
            logger.exception("Model discovery (dfg) failed.")
            raise

        # Copy returned files to configured output path prefix
        model_output = self.modeler_config["output_path"] + "." + fmt
        vis_output = self.modeler_config["output_path"] + ".png"
        try:
            if os.path.exists(model_path):
                shutil.copy(model_path, model_output)
            if vis_path and os.path.exists(vis_path):
                shutil.copy(vis_path, vis_output)
        except Exception:
            logger.exception("Failed to copy discovered model files to output location.")

        events_df.unpersist()
        logger.info(f"Completed. Model written to {model_output} (visualization: {vis_output}).")

        self.modeler_config["output_path"] = model_output

        if caller == "api":
            # Prefer returning visualization PNG when available
            if os.path.exists(vis_output):
                return FileResponse(vis_output, media_type="image/png", filename=Path(vis_output).name)
            return FileResponse(model_output, media_type="application/octet-stream", filename=Path(model_output).name)

        return model_output

    def _run_bpmn(self, caller: str) -> Any:
        logger.info(f"Running model discovery (bpmn) initiated by {caller}.")
        self._load_metadata()

        events_df = self.storage.read_sequence_table(self.metadata)
        events_df.cache()

        end_time = self.modeler_config.get("end_time")
        activity_durations = None

        if end_time:
            try:
                end_raw = F.col("attributes").getItem(end_time)
                end_ts_expr = F.when(end_raw.isNull(), None).when(
                    end_raw.rlike('^[0-9]+$'), end_raw.cast('long')
                ).otherwise(F.unix_timestamp(F.to_timestamp(end_raw)))

                dur_df = (
                    events_df
                    .withColumn("end_ts_parsed", end_ts_expr)
                    .withColumn("duration", F.col("end_ts_parsed") - F.col("start_timestamp"))
                    .filter(F.col("duration").isNotNull())
                )

                dur_agg = dur_df.groupBy("activity").agg(F.avg(F.col("duration")).alias("avg_duration"))
                dur_pd = dur_agg.toPandas()
                activity_durations = {
                    row["activity"]: float(row["avg_duration"]) for _, row in dur_pd.dropna(subset=["avg_duration"]).iterrows()
                }
            except Exception:
                logger.exception("Failed to compute activity durations from attributes; continuing without durations.")
                activity_durations = None

        pm_df = (
            events_df
            .withColumnRenamed("trace_id", "case:concept:name")
            .withColumnRenamed("activity", "concept:name")
            .withColumnRenamed("start_timestamp", "time:timestamp")
        )

        try:
            model_path, fmt, vis_path = discover_process_model(
                pm_df,
                algo="inductive",
                output_type="bpmn",
                noise_threshold=self.modeler_config.get("noise_threshold", 0.0),
                filter_percentile=self.modeler_config.get("filter_percentile", 0.0),
                activity_durations=activity_durations,
            )
        except Exception:
            events_df.unpersist()
            logger.exception("Model discovery (bpmn) failed.")
            raise

        model_output = self.modeler_config["output_path"] + "." + fmt
        vis_output = self.modeler_config["output_path"] + ".png"
        try:
            if os.path.exists(model_path):
                shutil.copy(model_path, model_output)
            if vis_path and os.path.exists(vis_path):
                shutil.copy(vis_path, vis_output)
        except Exception:
            logger.exception("Failed to copy discovered model files to output location.")

        events_df.unpersist()
        logger.info(f"Completed. BPMN model written to {model_output} (visualization: {vis_output}).")

        self.modeler_config["output_path"] = model_output

        if caller == "api":
            if os.path.exists(vis_output):
                return FileResponse(vis_output, media_type="image/png", filename=Path(vis_output).name)
            return FileResponse(model_output, media_type="application/xml", filename=Path(model_output).name)

        return model_output