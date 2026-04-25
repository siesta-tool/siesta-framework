import argparse
import csv
import datetime
import json
from pathlib import Path
from typing import Annotated, Any, Dict
from fastapi import Body
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
    output_format: str = Field("xml", description="Output format: 'xml' (default), 'png', 'html'")
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
        logger.info("Modeler startup complete.")

    def register_routes(self) -> SiestaModule.ApiRoutes | None:
        return {
            "directly_follows":   ("POST", self.api_directly_follows),
            "bpmn":               ("POST", self.api_bpmn)
        }

    # ------------------------------------------------------------------
    # API entry points
    # ------------------------------------------------------------------

    def api_directly_follows(self, modeler_config: Annotated[ModelerConfig, Body(openapi_examples={
        "default": {
            "summary": "Discover DFG with default settings",
            "value": {
                "log_name": "example_log",
                "storage_namespace": "siesta",
                "end_time": None,
                "output_format": "xml",
            },
        },
    })]) -> Any:
        """Discover a Directly-Follows Graph (DFG) from an indexed event log.

        Returns the DFG model file. Format and content depend on `output_format`.

        **Config fields:**
        - `log_name` *(str)* - name of the indexed log. **Required.**
        - `storage_namespace` *(str, default: `"siesta"`)* - storage namespace.
        - `end_time` *(str | null, default: `null`)* - event attribute key for the activity end
            timestamp. If set, average activity duration is computed and shown inside each node.
            If null, no duration annotation is added.
        - `noise_threshold` *(float, default: `0.0`)* - fraction of infrequent paths to remove
            (0.0 = keep all, 1.0 = keep only the most frequent path).
        - `filter_percentile` *(float, default: `0.0`)* - pre-filter log variants by frequency
            percentile before discovery (0.0 = keep all variants).
        - `output_format` *(str, default: `"xml"`)* - file returned by the endpoint:
            `"xml"` — DFG model as XML, `"png"` — process map image, `"html"` — interactive graph.
        """
        logger.info(f"{self.name} running directly_follows via API.")
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        config = modeler_config.model_dump()
        config["method"] = "directly_follows"
        self._load_modeler_config(config)

        return self._run_directly_follows(caller="api")

    def api_bpmn(self, modeler_config: Annotated[ModelerConfig, Body(openapi_examples={
        "default": {
            "summary": "Discover BPMN model with default settings",
            "value": {
                "log_name": "example_log",
                "storage_namespace": "siesta",
                "end_time": None,
                "output_format": "xml",
            },
        },
    })]) -> Any:
        """Discover a BPMN process model via the Inductive Miner from an indexed event log.

        Returns the BPMN model file. Format and content depend on `output_format`.

        **Config fields:**
        - `log_name` *(str)* - name of the indexed log. **Required.**
        - `storage_namespace` *(str, default: `"siesta"`)* - storage namespace.
        - `end_time` *(str | null, default: `null`)* - event attribute key for the activity end
            timestamp. If set, average activity duration is computed and shown inside each node.
            If null, no duration annotation is added.
        - `noise_threshold` *(float, default: `0.0`)* - noise threshold for the Inductive Miner IMf
            variant (0.0 = standard IM, >0 enables IMf with that threshold).
        - `filter_percentile` *(float, default: `0.0`)* - pre-filter log variants by frequency
            percentile before discovery (0.0 = keep all variants).
        - `output_format` *(str, default: `"xml"`)* - file returned by the endpoint:
            `"xml"` — BPMN 2.0 model as XML, `"png"` — process map image, `"html"` — interactive graph.
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

    def _compute_activity_durations(self, events_df) -> dict | None:
        end_time = self.modeler_config.get("end_time")
        if not end_time:
            return None
        try:
            end_raw = F.col("attributes").getItem(end_time)
            end_ts_expr = F.when(end_raw.isNull(), None).when(
                end_raw.rlike('^[0-9]+$'), end_raw.cast('long')
            ).otherwise(F.unix_timestamp(F.to_timestamp(end_raw)))
            dur_pd = (
                events_df
                .withColumn("_end", end_ts_expr)
                .withColumn("_dur", F.col("_end") - F.col("start_timestamp"))
                .filter(F.col("_dur").isNotNull())
                .groupBy("activity")
                .agg(F.avg("_dur").alias("avg_duration"))
                .toPandas()
            )
            return {
                row["activity"]: float(row["avg_duration"])
                for _, row in dur_pd.dropna(subset=["avg_duration"]).iterrows()
            }
        except Exception:
            logger.exception("Failed to compute activity durations; continuing without.")
            return None

    def _copy_outputs(self, model_path, fmt, png_path, html_path):
        base = self.modeler_config["output_path"]
        model_out = base + "." + fmt
        png_out   = base + ".png"
        html_out  = base + ".html"
        try:
            for src, dst in [(model_path, model_out), (png_path, png_out), (html_path, html_out)]:
                if src and os.path.exists(src):
                    shutil.copy(src, dst)
        except Exception:
            logger.exception("Failed to copy model files to output location.")
        return model_out, png_out, html_out

    def _api_response(self, model_out, png_out, html_out):
        fmt = self.modeler_config.get("output_format", "xml")
        if fmt == "png" and os.path.exists(png_out):
            return FileResponse(png_out, media_type="image/png", filename=Path(png_out).name)
        if fmt == "html" and os.path.exists(html_out):
            return FileResponse(html_out, media_type="text/html", filename=Path(html_out).name)
        return FileResponse(model_out, media_type="application/octet-stream",
                            filename=Path(model_out).name)

    def _run_directly_follows(self, caller: str) -> Any:
        logger.info(f"Running model discovery (dfg) initiated by {caller}.")
        self._load_metadata()

        events_df = self.storage.read_sequence_table(self.metadata)
        events_df.cache()
        activity_durations = self._compute_activity_durations(events_df)

        pm_df = (
            events_df
            .withColumnRenamed("trace_id", "case:concept:name")
            .withColumnRenamed("activity", "concept:name")
            .withColumnRenamed("start_timestamp", "time:timestamp")
        )

        try:
            model_path, fmt, png_path, html_path = discover_process_model(
                pm_df,
                algo="dfg",
                noise_threshold=self.modeler_config.get("noise_threshold", 0.0),
                filter_percentile=self.modeler_config.get("filter_percentile", 0.0),
                activity_durations=activity_durations,
            )
        except Exception:
            events_df.unpersist()
            logger.exception("Model discovery (dfg) failed.")
            raise

        model_out, png_out, html_out = self._copy_outputs(model_path, fmt, png_path, html_path)
        events_df.unpersist()
        logger.info(f"DFG model -> {model_out}  PNG -> {png_out}  HTML -> {html_out}")
        self.modeler_config["output_path"] = model_out

        if caller == "api":
            return self._api_response(model_out, png_out, html_out)
        return model_out

    def _run_bpmn(self, caller: str) -> Any:
        logger.info(f"Running model discovery (bpmn) initiated by {caller}.")
        self._load_metadata()

        events_df = self.storage.read_sequence_table(self.metadata)
        events_df.cache()
        activity_durations = self._compute_activity_durations(events_df)

        pm_df = (
            events_df
            .withColumnRenamed("trace_id", "case:concept:name")
            .withColumnRenamed("activity", "concept:name")
            .withColumnRenamed("start_timestamp", "time:timestamp")
        )

        try:
            model_path, fmt, png_path, html_path = discover_process_model(
                pm_df,
                algo="inductive",
                noise_threshold=self.modeler_config.get("noise_threshold", 0.0),
                filter_percentile=self.modeler_config.get("filter_percentile", 0.0),
                activity_durations=activity_durations,
            )
        except Exception:
            events_df.unpersist()
            logger.exception("Model discovery (bpmn) failed.")
            raise

        model_out, png_out, html_out = self._copy_outputs(model_path, fmt, png_path, html_path)
        events_df.unpersist()
        logger.info(f"BPMN model -> {model_out}  PNG -> {png_out}  HTML -> {html_out}")
        self.modeler_config["output_path"] = model_out

        if caller == "api":
            return self._api_response(model_out, png_out, html_out)
        return model_out