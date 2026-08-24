import argparse
import csv
import datetime
import json
import os
import shutil
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

from siesta.modules.analyser.directly_follows import compute_directly_follows
from siesta.modules.analyser.loop_detection import compute_loop_detection
from siesta.modules.analyser.durations import compute_activity_durations, compute_group_durations
from siesta.modules.analyser.attribute_deviations import (
    compute_attribute_deviations, render_html, ALL_STEPS,
)
from siesta.modules.analyser.ngrams import discover_ngrams, save_ngram_results, create_network
from siesta.modules.analyser.dm import discover_rare_rules, discover_targeted_rules, save_dm_results
from siesta.modules.analyser.trace_labels import resolve_trace_labels
from siesta.modules.analyser.process_model import discover_process_model, compute_activity_durations_map
from siesta.modules.analyser.bottlenecks import compute_bottlenecks
from siesta.modules.analyser.temporal_deviations import compute_temporal_deviations
from siesta.modules.mine.ordered import discover_ordered


class DirectlyFollowsConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    log_name: str = Field("example_log", description="Name of the indexed log")
    storage_namespace: str = Field("siesta", description="Storage namespace")
    min_timestamp: str | None = Field(None, description="Lower bound on start_timestamp as ISO 8601 datetime with millisecond precision (e.g. '2024-01-15T10:30:45.123Z')")
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
    min_timestamp: str | None = Field(None, description="Lower bound on start_timestamp as ISO 8601 datetime with millisecond precision (e.g. '2024-01-15T10:30:45.123Z')")
    support_threshold: float | None = Field(None, description="Min support fraction [0,1]; null = no filtering")
    filter_out: bool = Field(False, description="When true, keeps rare loops (support ≤ threshold)")
    top_k: int | None = Field(None, description="Keep only the k most-supported loops; null = all")
    trace_based: bool = Field(False, description="Add trace_ids list to each loop entry (only when grouping by trace_id)")
    output_path: str = Field("output/example_log", description="Local path prefix for the output file")


class DurationsConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    log_name: str = Field("example_log", description="Name of the indexed log")
    storage_namespace: str = Field("siesta", description="Storage namespace")
    min_timestamp: str | None = Field(None, description="Lower bound on start_timestamp as ISO 8601 datetime with millisecond precision (e.g. '2024-01-15T10:30:45.123Z')")
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
    min_timestamp: str | None = Field(None, description="Lower bound on start_timestamp as ISO 8601 datetime with millisecond precision (e.g. '2024-01-15T10:30:45.123Z')")
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


class ComparisonConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    log_name: str = Field("example_log", description="Name of the indexed log")
    storage_namespace: str = Field("siesta", description="Storage namespace")
    method_params: dict = Field(default_factory=lambda: {"n": 2}, description="Method-specific params. ngrams: {n, vis}. targeted_rules: {target_label, filtering_support}")
    separating_key: str = Field("activity", description="Column or attribute key used to label traces into groups")
    separating_groups: list[list[str]] = Field(default_factory=list, description="Group definitions, e.g. [['fail','error']] splits into listed values vs. all others")
    support_threshold: float = Field(0.0, description="Minimum support fraction [0,1] for results")
    output_path: str = Field("output/example_log", description="Local path prefix for the output file")


class ProcessModelConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    log_name: str = Field("example_log", description="Name of the indexed log")
    storage_namespace: str = Field("siesta", description="Storage namespace")
    min_timestamp: str | None = Field(None, description="Lower bound on start_timestamp as ISO 8601 datetime with millisecond precision (e.g. '2024-01-15T10:30:45.123Z')")
    end_time: str | None = Field(None, description="Attribute key for event end timestamp, used only to annotate visualizations with average per-activity durations. null = no duration annotation")
    noise_threshold: float = Field(0.0, description="Fraction of infrequent DFG paths to prune before discovery (0.0 = keep all, 1.0 = keep only the most frequent path)")
    output_format: str = Field("model", description="File returned by the endpoint: 'model' (native file - .xml/.bpmn/.pnml), 'png' (process map image), or 'html' (interactive graph)")
    output_path: str = Field("output/example_log", description="Local path prefix for the output file")


class BottlenecksConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    log_name: str = Field("example_log", description="Name of the indexed log")
    storage_namespace: str = Field("siesta", description="Storage namespace")
    min_timestamp: str | None = Field(None, description="Lower bound on start_timestamp as ISO 8601 datetime with millisecond precision (e.g. '2024-01-15T10:30:45.123Z')")
    end_time: str | None = Field(None, description="Attribute key for event end timestamp. null = transition time (next_start - start)")
    grouping_key: str | list[str] | None = Field(None, description="Attribute key(s) defining what counts as 'adjacent' events; null = trace_id")
    grouping_value: str | list[str] | None = Field(None, description="Restrict to groups with matching key value(s)")
    zscore_threshold: float = Field(3.5, description="Robust (MAD-based) z-score threshold above which a pair's average duration is flagged as anomalous relative to the rest of the process")
    top_k: int | None = Field(None, description="Keep only the k highest-impact pairs; null = all")
    return_csv: bool = Field(False, description="Return a CSV file download instead of a JSON list")
    output_path: str = Field("output/example_log", description="Local path prefix for the output file")


class TemporalDeviationsConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    log_name: str = Field("example_log", description="Name of the indexed log")
    storage_namespace: str = Field("siesta", description="Storage namespace")
    min_timestamp: str | None = Field(None, description="Lower bound on start_timestamp as ISO 8601 datetime with millisecond precision (e.g. '2024-01-15T10:30:45.123Z')")
    separating_key: str = Field("activity", description="Column or attribute key used to label traces into groups (e.g. a 'decision' attribute)")
    separating_groups: list[list[str]] = Field(default_factory=list, description="Group definitions, e.g. [['rejected']] splits into listed values (label 1) vs. all others (label 0)")
    activity_pairs: list[list[str]] | None = Field(None, description="Explicit [source, target] activity pairs to check for duration-based discrimination. null = auto-derive the top max_auto_pairs most frequent pairs")
    max_auto_pairs: int = Field(50, description="Upper bound on auto-derived candidate pairs when activity_pairs is null")
    min_group_size: int = Field(5, description="Minimum combined (count_1 + count_0) required to keep a (pair, threshold) result")
    top_k_per_pair: int | None = Field(None, description="Keep only the k best (by |balance|) thresholds per activity pair; null = keep all decile candidates")
    return_csv: bool = Field(False, description="Return a CSV file download instead of a JSON list")
    output_path: str = Field("output/example_log", description="Local path prefix for the output file")


import logging

logger = logging.getLogger(__name__)


def _parse_min_timestamp(iso_str: str | None) -> int | None:
    """Parse an ISO 8601 datetime string to epoch milliseconds, or return None."""
    if iso_str is None:
        return None
    parsed = datetime.datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    return int(parsed.timestamp() * 1000)


DEFAULT_ANALYSER_CONFIG: Dict[str, Any] = {
    **DirectlyFollowsConfig().model_dump(),
    **LoopDetectionConfig().model_dump(),
    **DurationsConfig().model_dump(),
    **AttributeDeviationsConfig().model_dump(),
    **ComparisonConfig().model_dump(),
    **ProcessModelConfig().model_dump(),
    **BottlenecksConfig().model_dump(),
    **TemporalDeviationsConfig().model_dump(),
    "method": "directly_follows"
}


class Analyser(SiestaModule):

    name = "analyser"
    version = "1.0.0"
    spark: SparkSession
    storage: StorageManager
    siesta_config: Dict[str, Any]
    analyser_config: Dict[str, Any]
    metadata: MetaData | None

    def __init__(self):
        super().__init__()
        self.analyser_config = {}
        self.metadata = None

    def startup(self):
        logger.info("Analyser startup complete.")

    def register_routes(self) -> SiestaModule.ApiRoutes | None:
        return {
            "directly_follows":      ("POST", self.api_directly_follows),
            "loop_detection":        ("POST", self.api_loop_detection),
            "durations":             ("POST", self.api_durations),
            "attribute_deviations":  ("POST", self.api_attribute_deviations),
            "ngrams":                ("POST", self.api_ngrams),
            "rare_rules":            ("POST", self.api_rare_rules),
            "targeted_rules":        ("POST", self.api_targeted_rules),
            "dfg_model":             ("POST", self.api_dfg_model),
            "bpmn":                  ("POST", self.api_bpmn),
            "petri_net":             ("POST", self.api_petri_net),
            "bottlenecks":           ("POST", self.api_bottlenecks),
            "temporal_deviations":   ("POST", self.api_temporal_deviations),
        }

    # ------------------------------------------------------------------
    # API entry points - core analysis (unchanged from the former `analyse` module)
    # ------------------------------------------------------------------

    def api_directly_follows(self, analyser_config: Annotated[DirectlyFollowsConfig, Body(openapi_examples={
        "default": {
            "summary": "Find directly-following pairs with default settings",
            "value": {
                "log_name": "example_log",
                "storage_namespace": "siesta",
                "min_timestamp": None,
                "end_time": None,
                "support_threshold": None,
                "filter_out": False,
                "include_traces": False,
                "return_csv": False,
            },
        },
    })]) -> Any:
        """Find directly-following activity pairs in an indexed event log.

        Returns pairs of consecutive activities with support (fraction of traces where A
        is directly followed by B) and duration statistics in seconds.

        **Config fields:**
        - `log_name` *(str)* - name of the indexed log. **Required.**
        - `storage_namespace` *(str, default: `"siesta"`)* - storage namespace.
        - `min_timestamp` *(str | null, default: `null`)* - lower bound on `start_timestamp` as ISO 8601 with millisecond precision.
        - `end_time` *(str | null, default: `null`)* - attribute key for event end timestamp.
            If set, duration = `end_time - start_timestamp` (activity duration).
            If null, duration = `next_start - start_timestamp` (transition time).
        - `support_threshold` *(float [0,1] | null, default: `null`)* - keep pairs with support ≥ threshold. `null` = no filtering.
        - `filter_out` *(bool, default: `false`)* - when `true`, keeps pairs with support ≤ threshold instead.
        - `include_traces` *(bool, default: `false`)* - append a `trace_ids` column to the output.
        - `return_csv` *(bool, default: `false`)* - return a CSV file download instead of a JSON list.
            """
        logger.info(f"{self.name} running directly_follows via API.")
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        config = analyser_config.model_dump()
        config["method"] = "directly_follows"
        try:
            self._load_analyser_config(config)
        except Exception as e:
            logger.exception(f"Error loading analyser config: {e}")
            return {"code": 400, "message": f"Invalid config: {e}"}

        return self._run_directly_follows(caller="api")

    def api_durations(self, analyser_config: Annotated[DurationsConfig, Body(openapi_examples={
        "default": {
            "summary": "Compute duration statistics with default settings",
            "value": {
                "log_name": "example_log",
                "storage_namespace": "siesta",
                "min_timestamp": None,
                "duration_mode": "activity",
                "end_time": None,
                "grouping_key": None,
                "grouping_value": None,
                "per_group": False,
                "return_csv": False,
            },
        },
    })]) -> Any:
        """Compute duration statistics for activities or groups in an indexed event log.

        Two modes controlled by `duration_mode`:
        - **`activity`**: avg / min / max duration per activity type; optionally per group when `per_group=true`.
        - **`group`**: total duration per group instance (e.g. per trace).

        **Config fields:**
        - `log_name` *(str)* - name of the indexed log. **Required.**
        - `storage_namespace` *(str, default: `"siesta"`)* - storage namespace.
        - `min_timestamp` *(str | null, default: `null`)* - lower bound on `start_timestamp`.
        - `duration_mode` *(str, default: `"activity"`)* - `"activity"` or `"group"`.
        - `end_time` *(str | null, default: `null`)* - attribute key for event end timestamp.
            Activity mode: `end_time - start_timestamp`; group mode: sum of per-event durations.
            If null - activity mode uses transition time; group mode uses `last_start - first_start`.
        - `grouping_key` *(str | list | null, default: `null`)* - attribute key(s) defining groups. `null` = `trace_id`.
        - `grouping_value` *(str | list | null, default: `null`)* - restrict to groups with matching key value(s).
        - `per_group` *(bool, default: `false`)* - `activity` mode only: produce one row per (group, activity).
        - `return_csv` *(bool, default: `false`)* - return a CSV file download instead of a JSON list.
        """
        logger.info(f"{self.name} running durations via API.")
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        config = analyser_config.model_dump()
        config["method"] = "durations"
        try:
            self._load_analyser_config(config)
        except Exception as e:
            logger.exception(f"Error loading analyser config: {e}")
            return {"code": 400, "message": f"Invalid config: {e}"}

        return self._run_durations(caller="api")

    def api_loop_detection(self, analyser_config: Annotated[LoopDetectionConfig, Body(openapi_examples={
        "default": {
            "summary": "Detect loops with default settings",
            "value": {
                "log_name": "example_log",
                "storage_namespace": "siesta",
                "grouping_key": None,
                "grouping_value": None,
                "min_timestamp": None,
                "support_threshold": None,
                "filter_out": False,
                "top_k": None,
                "trace_based": False,
            },
        },
    })]) -> Any:
        """Detect self-loops and non-self-loops in an indexed event log.

        A **self-loop** is an activity immediately followed by itself.
        A **non-self-loop** is a minimal cycle A -> … -> A where A does not appear in the body.

        Returns JSON with `self_loops` and `non_self_loops` arrays. Each entry contains the
        activity pattern and its support fraction across groups.

        **Config fields:**
        - `log_name` *(str)* - name of the indexed log. **Required.**
        - `storage_namespace` *(str, default: `"siesta"`)* - storage namespace.
        - `grouping_key` *(str | list | null, default: `null`)* - attribute key(s) to group by. `null` = `trace_id`.
        - `grouping_value` *(str | list | dict | null, default: `null`)* - restrict to specific group values.
        - `min_timestamp` *(str | null, default: `null`)* - lower bound on `start_timestamp`.
        - `support_threshold` *(float [0,1] | null, default: `null`)* - keep loops with support ≥ threshold. `null` = no filtering.
        - `filter_out` *(bool, default: `false`)* - when `true`, keeps loops with support ≤ threshold (rare loops).
        - `top_k` *(int | null, default: `null`)* - keep only the k most-supported loops. `null` = all.
        - `trace_based` *(bool, default: `false`)* - add a `trace_ids` list to each loop entry (only when grouping by `trace_id`).
        """
        logger.info(f"{self.name} running loop_detection via API.")
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        config = analyser_config.model_dump()
        config["method"] = "loop_detection"
        try:
            self._load_analyser_config(config)
        except Exception as e:
            logger.exception(f"Error loading analyser config: {e}")
            return {"code": 400, "message": f"Invalid config: {e}"}

        return self._run_loop_detection(caller="api")

    def api_attribute_deviations(self, analyser_config: Annotated[AttributeDeviationsConfig, Body(openapi_examples={
        "default": {
            "summary": "Detect attribute deviations with default settings",
            "value": {
                "log_name": "example_log",
                "storage_namespace": "siesta",
                "min_timestamp": None,
                "steps": [0, 1, 2, 3, 4],
                "excluded_attributes": None,
                "surprise_threshold": 4.0,
                "zscore_threshold": 3.5,
                "ngram_n": 2,
                "min_group_size": 5,
                "n_buckets": 5,
                "support_threshold": None,
                "filter_out": False,
                "on_rare": None,
                "output_format": "json",
            },
        },
    })]) -> Any:
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
        - **Step 4** - Value transitions: flags rare (prev_value -> curr_value) transitions
            within a trace (categorical attributes only).

        **Config fields:**
        - `log_name` *(str)* - name of the indexed log. **Required.**
        - `storage_namespace` *(str, default: `"siesta"`)* - storage namespace.
        - `min_timestamp` *(str | null, default: `null`)* - lower bound on `start_timestamp`.
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
        """
        logger.info(f"{self.name} running attribute_deviations via API.")
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        config = analyser_config.model_dump()
        config["method"] = "attribute_deviations"
        try:
            self._load_analyser_config(config)
        except Exception as e:
            logger.exception(f"Error loading analyser config: {e}")
            return {"code": 400, "message": f"Invalid config: {e}"}

        return self._run_attribute_deviations(caller="api")

    # ------------------------------------------------------------------
    # API entry points - comparison (from the former `compare` module)
    # ------------------------------------------------------------------

    def api_ngrams(self, analyser_config: Annotated[ComparisonConfig, Body(
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
    )]) -> Any:
        """Compare n-gram frequency distributions between two trace groups.

        Splits traces into two groups based on `separating_key`/`separating_groups` and
        computes n-gram frequency differences (balance/confidence/support statistics).

        **Config fields:**
        - `log_name` *(str, default: `"example_log"`)* - name of the indexed log.
        - `storage_namespace` *(str, default: `"siesta"`)* - storage namespace.
        - `method_params` *(object, default: `{"n": 2}`)* - `n` (int) = gram length; `vis` (bool) = generate HTML network.
        - `separating_key` *(str, default: `"activity"`)* - column or attribute key used to label traces into groups.
        - `separating_groups` *(list[list[str]])* - group definitions, e.g. `[["fail", "error"]]`.
        - `support_threshold` *(float [0,1], default: `0.0`)* - minimum support fraction for results.
        """
        logger.info(f"{self.name} running ngrams via API.")
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        config = analyser_config.model_dump()
        config["method"] = "ngrams"
        try:
            self._load_analyser_config(config)
        except Exception as e:
            logger.exception(f"Error loading analyser config: {e}")
            return {"code": 400, "message": f"Invalid config: {e}"}

        return self._run_ngrams(caller="api")

    def api_rare_rules(self, analyser_config: Annotated[ComparisonConfig, Body(
        openapi_examples={
            "default": {
                "summary": "Discover rare rules with default settings",
                "value": {
                    "log_name": "example_log",
                    "storage_namespace": "siesta",
                    "separating_key": "activity",
                    "separating_groups": [],
                    "support_threshold": 0.1,
                },
            },
        }
    )]) -> Any:
        """Discover directly-following rules that are rare overall but appear in target traces.

        Compares ordered constraints across the two trace groups and returns rules
        whose support is low globally (≤ `support_threshold`) but present in target-group traces.

        **Config fields:**
        - `log_name` *(str, default: `"example_log"`)* - name of the indexed log.
        - `storage_namespace` *(str, default: `"siesta"`)* - storage namespace.
        - `separating_key` *(str, default: `"activity"`)* - column or attribute key used to label traces into groups.
        - `separating_groups` *(list[list[str]])* - group definitions, e.g. `[["fail", "error"]]`.
        - `support_threshold` *(float [0,1], default: `0.1`)* - global-support ceiling defining "rare".
        """
        logger.info(f"{self.name} running rare_rules via API.")
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        config = analyser_config.model_dump()
        config["method"] = "rare_rules"
        try:
            self._load_analyser_config(config)
        except Exception as e:
            logger.exception(f"Error loading analyser config: {e}")
            return {"code": 400, "message": f"Invalid config: {e}"}

        return self._run_rare_rules(caller="api")

    def api_targeted_rules(self, analyser_config: Annotated[ComparisonConfig, Body(
        openapi_examples={
            "default": {
                "summary": "Discover targeted rules with default settings",
                "value": {
                    "log_name": "example_log",
                    "storage_namespace": "siesta",
                    "method_params": {"target_label": 1, "filtering_support": 1.0},
                    "separating_key": "activity",
                    "separating_groups": [],
                    "support_threshold": 0.8,
                },
            },
        }
    )]) -> Any:
        """Discover directly-following rules strongly associated with a target trace group.

        Evaluates which rules are characteristic of the target group using a support-based filter.

        **Config fields:**
        - `log_name` *(str, default: `"example_log"`)* - name of the indexed log.
        - `storage_namespace` *(str, default: `"siesta"`)* - storage namespace.
        - `method_params` *(object)* - `target_label` (int, default: `1`), `filtering_support` (float, default: `1`).
        - `separating_key` *(str, default: `"activity"`)* - column or attribute key used to label traces into groups.
        - `separating_groups` *(list[list[str]])* - group definitions, e.g. `[["fail", "error"]]`.
        - `support_threshold` *(float [0,1], default: `0.8`)* - min fraction of target-group traces a rule must cover.
        """
        logger.info(f"{self.name} running targeted_rules via API.")
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        config = analyser_config.model_dump()
        config["method"] = "targeted_rules"
        try:
            self._load_analyser_config(config)
        except Exception as e:
            logger.exception(f"Error loading analyser config: {e}")
            return {"code": 400, "message": f"Invalid config: {e}"}

        return self._run_targeted_rules(caller="api")

    # ------------------------------------------------------------------
    # API entry points - process model discovery (from the former `model` module)
    # ------------------------------------------------------------------

    def api_dfg_model(self, analyser_config: Annotated[ProcessModelConfig, Body(openapi_examples={
        "default": {
            "summary": "Discover DFG model with default settings",
            "value": {
                "log_name": "example_log",
                "storage_namespace": "siesta",
                "end_time": None,
                "noise_threshold": 0.0,
                "output_format": "model",
            },
        },
    })]) -> Any:
        """Discover a Directly-Follows Graph model from an indexed event log.

        Computes the DFG with our own distributed Spark aggregation (same statistics as
        `directly_follows`, but exported as a process-map model file/visualization
        instead of a stats table).

        **Config fields:**
        - `log_name` *(str)* - name of the indexed log. **Required.**
        - `storage_namespace` *(str, default: `"siesta"`)* - storage namespace.
        - `min_timestamp` *(str | null, default: `null`)* - lower bound on `start_timestamp`.
        - `end_time` *(str | null, default: `null`)* - attribute key for activity end timestamp;
            if set, average activity duration is computed and shown inside each node.
        - `noise_threshold` *(float, default: `0.0`)* - fraction of infrequent paths to remove.
        - `output_format` *(str, default: `"model"`)* - `"model"` (native `.xml`), `"png"`, or `"html"`.
        """
        logger.info(f"{self.name} running dfg_model via API.")
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        config = analyser_config.model_dump()
        config["method"] = "dfg_model"
        try:
            self._load_analyser_config(config)
        except ValueError as e:
            logger.error(f"Invalid config for dfg_model: {e}")
            return {"code": 400, "message": str(e)}

        return self._run_process_model(caller="api", algo="dfg")

    def api_bpmn(self, analyser_config: Annotated[ProcessModelConfig, Body(openapi_examples={
        "default": {
            "summary": "Discover BPMN model with default settings",
            "value": {
                "log_name": "example_log",
                "storage_namespace": "siesta",
                "end_time": None,
                "noise_threshold": 0.0,
                "output_format": "model",
            },
        },
    })]) -> Any:
        """Discover a BPMN process model via pm4py's Inductive Miner, fed by our own
        scalable Spark DFG (no full-log materialization).

        **Config fields:** same as `dfg_model`. `output_format`: `"model"` (native `.bpmn`), `"png"`, or `"html"`.
        """
        logger.info(f"{self.name} running bpmn via API.")
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        config = analyser_config.model_dump()
        config["method"] = "bpmn"
        try:
            self._load_analyser_config(config)
        except ValueError as e:
            logger.error(f"Invalid config for bpmn: {e}")
            return {"code": 400, "message": str(e)}

        return self._run_process_model(caller="api", algo="bpmn")

    def api_petri_net(self, analyser_config: Annotated[ProcessModelConfig, Body(openapi_examples={
        "default": {
            "summary": "Discover Petri net model with default settings",
            "value": {
                "log_name": "example_log",
                "storage_namespace": "siesta",
                "end_time": None,
                "noise_threshold": 0.0,
                "output_format": "model",
            },
        },
    })]) -> Any:
        """Discover a (sound, block-structured) Petri net process model via pm4py's
        Inductive Miner, fed by our own scalable Spark DFG (no full-log materialization).

        **Config fields:** same as `dfg_model`. `output_format`: `"model"` (native `.pnml`), `"png"`, or `"html"`.
        """
        logger.info(f"{self.name} running petri_net via API.")
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        config = analyser_config.model_dump()
        config["method"] = "petri_net"
        try:
            self._load_analyser_config(config)
        except ValueError as e:
            logger.error(f"Invalid config for petri_net: {e}")
            return {"code": 400, "message": str(e)}

        return self._run_process_model(caller="api", algo="petri_net")

    # ------------------------------------------------------------------
    # API entry points - new analytics
    # ------------------------------------------------------------------

    def api_bottlenecks(self, analyser_config: Annotated[BottlenecksConfig, Body(openapi_examples={
        "default": {
            "summary": "Detect bottleneck activity pairs with default settings",
            "value": {
                "log_name": "example_log",
                "storage_namespace": "siesta",
                "end_time": None,
                "grouping_key": None,
                "grouping_value": None,
                "zscore_threshold": 3.5,
                "top_k": None,
            },
        },
    })]) -> Any:
        """Surface activity pairs with anomalously high inter-event durations relative
        to the rest of the process, ranked by their contribution to overall cycle time.

        **Config fields:**
        - `log_name` *(str)* - name of the indexed log. **Required.**
        - `storage_namespace` *(str, default: `"siesta"`)* - storage namespace.
        - `min_timestamp` *(str | null, default: `null`)* - lower bound on `start_timestamp`.
        - `end_time` *(str | null, default: `null`)* - attribute key for event end timestamp.
        - `grouping_key` *(str | list | null, default: `null`)* - attribute key(s) defining what counts as
            "adjacent" events; `null` = `trace_id`.
        - `grouping_value` *(str | list | null, default: `null`)* - restrict to groups with matching key value(s).
        - `zscore_threshold` *(float, default: `3.5`)* - robust (MAD-based) z-score threshold for flagging.
        - `top_k` *(int | null, default: `null`)* - keep only the k highest-impact pairs.
        - `return_csv` *(bool, default: `false`)* - return a CSV file download instead of a JSON list.
        """
        logger.info(f"{self.name} running bottlenecks via API.")
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        config = analyser_config.model_dump()
        config["method"] = "bottlenecks"
        try:
            self._load_analyser_config(config)
        except Exception as e:
            logger.exception(f"Error loading analyser config: {e}")
            return {"code": 400, "message": f"Invalid config: {e}"}

        return self._run_bottlenecks(caller="api")

    def api_temporal_deviations(self, analyser_config: Annotated[TemporalDeviationsConfig, Body(openapi_examples={
        "default": {
            "summary": "Discover duration-based discriminating rules with default settings",
            "value": {
                "log_name": "example_log",
                "storage_namespace": "siesta",
                "separating_key": "activity",
                "separating_groups": [["rejected"]],
                "activity_pairs": None,
                "max_auto_pairs": 50,
                "min_group_size": 5,
                "top_k_per_pair": 3,
            },
        },
    })]) -> Any:
        """Discover duration-based discriminating rules between groups of traces.

        For instance, this may reveal that applications in which a manual review and a
        credit reassessment occur within five minutes of each other are significantly
        more likely to be rejected - a pattern conditioned on timing, not just sequence.

        **Config fields:**
        - `log_name` *(str)* - name of the indexed log. **Required.**
        - `storage_namespace` *(str, default: `"siesta"`)* - storage namespace.
        - `min_timestamp` *(str | null, default: `null`)* - lower bound on `start_timestamp`.
        - `separating_key` *(str, default: `"activity"`)* - column or attribute key used to label traces into groups.
        - `separating_groups` *(list[list[str]])* - group definitions, e.g. `[["rejected"]]`.
        - `activity_pairs` *(list[[str,str]] | null)* - explicit pairs to check; `null` = auto-derive top `max_auto_pairs`.
        - `max_auto_pairs` *(int, default: `50`)* - cap on auto-derived candidate pairs.
        - `min_group_size` *(int, default: `5`)* - minimum combined trace count required to keep a result.
        - `top_k_per_pair` *(int | null, default: `null`)* - keep only the k best thresholds per pair.
        - `return_csv` *(bool, default: `false`)* - return a CSV file download instead of a JSON list.
        """
        logger.info(f"{self.name} running temporal_deviations via API.")
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        config = analyser_config.model_dump()
        config["method"] = "temporal_deviations"
        try:
            self._load_analyser_config(config)
        except Exception as e:
            logger.exception(f"Error loading analyser config: {e}")
            return {"code": 400, "message": f"Invalid config: {e}"}

        return self._run_temporal_deviations(caller="api")

    # ------------------------------------------------------------------
    # CLI entry point
    # ------------------------------------------------------------------

    def cli_run(self, args: Any, **kwargs: Any) -> Any:
        logger.info(f"{self.name} is running with args: {args}")
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        parser = argparse.ArgumentParser(description="Siesta Analyser module")
        parser.add_argument("--analyser_config", type=str, required=False,
                            help="Path to analyser configuration JSON file")
        parsed_args, _ = parser.parse_known_args(args)

        if not parsed_args.analyser_config:
            raise RuntimeError("Config not provided. Use --analyser_config <path>")

        config_path = parsed_args.analyser_config
        if not Path(config_path).exists():
            raise FileNotFoundError(f"Config file {config_path} not found.")

        with open(config_path, "r") as f:
            user_config = json.load(f)

        try:
            self._load_analyser_config(user_config)
        except Exception as e:
            raise ValueError(f"Error loading analyser config: {e}")

        self.storage.initialize_db(self.analyser_config)

        method = self.analyser_config.get("method", "directly_follows")
        match method:
            case "directly_follows":
                return self._run_directly_follows(caller="cli")
            case "loop_detection":
                return self._run_loop_detection(caller="cli")
            case "durations":
                return self._run_durations(caller="cli")
            case "attribute_deviations":
                return self._run_attribute_deviations(caller="cli")
            case "ngrams":
                return self._run_ngrams(caller="cli")
            case "rare_rules":
                return self._run_rare_rules(caller="cli")
            case "targeted_rules":
                return self._run_targeted_rules(caller="cli")
            case "dfg_model":
                return self._run_process_model(caller="cli", algo="dfg")
            case "bpmn":
                return self._run_process_model(caller="cli", algo="bpmn")
            case "petri_net":
                return self._run_process_model(caller="cli", algo="petri_net")
            case "bottlenecks":
                return self._run_bottlenecks(caller="cli")
            case "temporal_deviations":
                return self._run_temporal_deviations(caller="cli")
            case _:
                raise ValueError(f"Unknown analyser method: '{method}'")

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _load_analyser_config(self, config: Dict[str, Any]):
        if not self.storage.log_exists(config):
            logger.error(
                f"Log '{config.get("log_name")}' does not exist in storage. Run indexing first."
            )
            raise ValueError(f"Log '{config.get("log_name")}' not found in storage.")

        if config.get("log_name") is None:
            raise ValueError("Log name not specified in config.")

        self.analyser_config = DEFAULT_ANALYSER_CONFIG.copy()
        self.analyser_config.update(config)

        if config.get("output_path") is not None and config.get("output_path") == "output/example_log":
            config["output_path"] = "output/" + config.get("log_name", "analyser_results")

        given_output = config.get("output_path")

        Path(given_output).parent.mkdir(parents=True, exist_ok=True)
        self.analyser_config["output_path"] = (
            given_output + "_" + str(datetime.datetime.now().timestamp())
        )

    def _load_metadata(self):
        self.metadata = MetaData(
            storage_namespace=self.analyser_config.get("storage_namespace", "siesta"),
            log_name=self.analyser_config.get("log_name", "default_log"),
            storage_type=self.analyser_config.get("storage_type", "s3"),
        )
        self.metadata = self.storage.read_metadata_table(self.metadata)

    # ------------------------------------------------------------------
    # Method implementations - core analysis
    # ------------------------------------------------------------------

    def _run_directly_follows(self, caller: str) -> Any:
        logger.info(f"Running directly_follows initiated by {caller}.")
        self._load_metadata()

        events_df = self.storage.read_sequence_table(self.metadata)
        min_ts = _parse_min_timestamp(self.analyser_config.get("min_timestamp"))
        if min_ts is not None:
            events_df = events_df.filter(F.col("start_timestamp") >= min_ts)
        events_df.cache()

        result_df = compute_directly_follows(
            events_df=events_df,
            trace_count=self.metadata.trace_count,
            end_time=self.analyser_config.get("end_time"),
            support_threshold=self.analyser_config.get("support_threshold"),
            filter_out=self.analyser_config.get("filter_out", False),
            include_traces=self.analyser_config.get("include_traces", False),
        )

        output_path = self.analyser_config["output_path"] + ".csv"
        self.analyser_config["output_path"] = output_path

        result_pd = result_df.toPandas()
        if self.analyser_config.get("include_traces", False) and "trace_ids" in result_pd.columns:
            result_pd["trace_ids"] = result_pd["trace_ids"].apply(
                lambda x: ",".join(sorted(x)) if x else ""
            )
        result_pd.to_csv(output_path, index=False)

        events_df.unpersist()
        logger.info(f"Completed. Results written to {output_path}.")

        if caller == "api":
            if self.analyser_config.get("return_csv", False):
                return FileResponse(
                    output_path,
                    media_type="text/csv",
                    filename=Path(output_path).name,
                )
            with open(output_path, "r", newline="") as f:
                try:
                    return {"code": 200, "data": list(csv.DictReader(f))}
                except Exception:
                    logger.error(f"Failed to parse results from {output_path}.")
                    return {"code": 500, "message": f"Failed to parse results from {output_path}."}

        return output_path

    def _run_loop_detection(self, caller: str) -> Any:
        logger.info(f"Running loop_detection initiated by {caller}.")
        self._load_metadata()

        events_df = self.storage.read_sequence_table(self.metadata)
        events_df.cache()

        min_ts = _parse_min_timestamp(self.analyser_config.get("min_timestamp"))
        if min_ts is not None:
            events_df = events_df.filter(F.col("start_timestamp") >= min_ts)

        result = compute_loop_detection(
            events_df=events_df,
            grouping_key=self.analyser_config.get("grouping_key"),
            grouping_value=self.analyser_config.get("grouping_value"),
            support_threshold=self.analyser_config.get("support_threshold"),
            filter_out=self.analyser_config.get("filter_out", False),
            top_k=self.analyser_config.get("top_k"),
            trace_based=self.analyser_config.get("trace_based", False),
        )

        events_df.unpersist()
        logger.info(
            f"Completed. Found {len(result['self_loops'])} self-loop type(s) and "
            f"{len(result['non_self_loops'])} non-self-loop type(s)."
        )

        if caller == "cli":
            output_path = self.analyser_config["output_path"] + ".json"
            self.analyser_config["output_path"] = output_path
            with open(output_path, "w") as f:
                json.dump(result, f, indent=2)
            logger.info(f"Results written to {output_path}.")
            return output_path

        return {"code": 200, **result}

    def _run_durations(self, caller: str) -> Any:
        logger.info(f"Running durations initiated by {caller}.")
        self._load_metadata()

        events_df = self.storage.read_sequence_table(self.metadata)
        min_ts = _parse_min_timestamp(self.analyser_config.get("min_timestamp"))
        if min_ts is not None:
            events_df = events_df.filter(F.col("start_timestamp") >= min_ts)
        events_df.cache()

        mode         = self.analyser_config.get("duration_mode", "activity")
        end_time     = self.analyser_config.get("end_time")
        grouping_key = self.analyser_config.get("grouping_key")
        grouping_val = self.analyser_config.get("grouping_value")

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
                per_group=self.analyser_config.get("per_group", False),
            )

        output_path = self.analyser_config["output_path"] + ".csv"
        self.analyser_config["output_path"] = output_path

        result_df.toPandas().to_csv(output_path, index=False)

        events_df.unpersist()
        logger.info(f"Completed. Results written to {output_path}.")

        if caller == "api":
            if self.analyser_config.get("return_csv", False):
                return FileResponse(
                    output_path,
                    media_type="text/csv",
                    filename=Path(output_path).name,
                )
            with open(output_path, "r", newline="") as f:
                try:
                    return {"code": 200, "data": list(csv.DictReader(f))}
                except Exception:
                    logger.error(f"Failed to parse results from {output_path}.")
                    return {"code": 500, "message": f"Failed to parse results from {output_path}."}

        return output_path

    def _run_attribute_deviations(self, caller: str) -> Any:
        logger.info(f"Running attribute_deviations initiated by {caller}.")
        self._load_metadata()

        events_df = self.storage.read_sequence_table(self.metadata)
        min_ts = _parse_min_timestamp(self.analyser_config.get("min_timestamp"))
        if min_ts is not None:
            events_df = events_df.filter(F.col("start_timestamp") >= min_ts)
        events_df.cache()
        total_traces = self.metadata.trace_count

        rare_support_threshold = self.analyser_config.get("on_rare")
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
            steps=self.analyser_config.get("steps", list(ALL_STEPS)),
            excluded_keys=self.analyser_config.get("excluded_attributes"),
            surprise_threshold=self.analyser_config.get("surprise_threshold", 4.0),
            zscore_threshold=self.analyser_config.get("zscore_threshold", 3.5),
            n_buckets=self.analyser_config.get("n_buckets", 5),
            ngram_n=self.analyser_config.get("ngram_n", 2),
            min_group_size=self.analyser_config.get("min_group_size", 5),
            support_threshold=self.analyser_config.get("support_threshold"),
            filter_out=self.analyser_config.get("filter_out", False),
        )

        events_df.unpersist()
        logger.info(f"Completed. {len(records)} deviation records found.")

        output_format = self.analyser_config.get("output_format", "json")
        log_name = self.analyser_config.get("log_name", "log")

        if output_format == "csv":
            import csv as _csv
            output_path = self.analyser_config["output_path"] + "_deviations.csv"
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
            output_path = self.analyser_config["output_path"] + "_deviations.html"
            html_content = render_html(records, log_name, active_steps)
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(html_content)
            if caller == "api":
                return FileResponse(output_path, media_type="text/html", filename=Path(output_path).name)
            return output_path

        # Default: JSON
        result = {
            "code": 200,
            "log_name": log_name,
            "total_deviations": len(records),
            "deviations": records,
        }
        if caller == "cli":
            output_path = self.analyser_config["output_path"] + "_deviations.json"
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2)
            logger.info(f"Results written to {output_path}.")
            return output_path
        return result

    # ------------------------------------------------------------------
    # Method implementations - comparison
    # ------------------------------------------------------------------

    def _run_ngrams(self, caller: str) -> Any:
        logger.info(f"Running ngrams initiated by {caller}.")
        self._load_metadata()

        events_df = self.storage.read_sequence_table(self.metadata).dropDuplicates(
            ["trace_id", "activity", "start_timestamp"]
        )
        min_ts = _parse_min_timestamp(self.analyser_config.get("min_timestamp"))
        if min_ts is not None:
            events_df = events_df.filter(F.col("start_timestamp") >= min_ts)
        events_df.cache()

        trace_labels = resolve_trace_labels(
            events_df,
            self.analyser_config.get("separating_key", "activity"),
            self.analyser_config.get("separating_groups", []),
        )
        params = self.analyser_config.get("method_params", {})

        results = discover_ngrams(events=events_df, trace_labels=trace_labels, n=params.get("n", 2))

        output_path = self.analyser_config["output_path"] + ".csv"
        self.analyser_config["output_path"] = output_path
        save_ngram_results(results, output_path, fmt="csv")

        if params.get("vis", False):
            with open(output_path.replace(".csv", ".html"), "w") as f:
                f.write(create_network(output_path))

        events_df.unpersist()
        logger.info(f"Completed. Results written to {output_path}.")

        if caller == "api":
            with open(output_path, "r", newline="") as f:
                try:
                    return {"code": 200, "data": list(csv.DictReader(f))}
                except Exception:
                    logger.error(f"Failed to parse ngrams results from {output_path}.")
                    return {"code": 500, "message": f"Failed to parse results from {output_path}."}
        return output_path

    def _run_rare_rules(self, caller: str) -> Any:
        logger.info(f"Running rare_rules initiated by {caller}.")
        self._load_metadata()

        events_df = self.storage.read_sequence_table(self.metadata).dropDuplicates(
            ["trace_id", "activity", "start_timestamp"]
        )
        min_ts = _parse_min_timestamp(self.analyser_config.get("min_timestamp"))
        if min_ts is not None:
            events_df = events_df.filter(F.col("start_timestamp") >= min_ts)
        events_df.cache()

        trace_labels = resolve_trace_labels(
            events_df,
            self.analyser_config.get("separating_key", "activity"),
            self.analyser_config.get("separating_groups", []),
        )

        ordered_constraints_df = discover_ordered(events_df, self.metadata)
        result_list = discover_rare_rules(
            ordered_constraints_df=ordered_constraints_df,
            trace_labels=trace_labels,
            trace_count=self.metadata.trace_count,
            support_pct=self.analyser_config.get("support_threshold", 0.1),
        )

        output_path = self.analyser_config["output_path"] + ".json"
        self.analyser_config["output_path"] = output_path
        save_dm_results(result_list, output_path)

        events_df.unpersist()
        logger.info(f"Completed. Results written to {output_path}.")

        if caller == "api":
            return {"code": 200, "data": result_list}
        return output_path

    def _run_targeted_rules(self, caller: str) -> Any:
        logger.info(f"Running targeted_rules initiated by {caller}.")
        self._load_metadata()

        events_df = self.storage.read_sequence_table(self.metadata).dropDuplicates(
            ["trace_id", "activity", "start_timestamp"]
        )
        min_ts = _parse_min_timestamp(self.analyser_config.get("min_timestamp"))
        if min_ts is not None:
            events_df = events_df.filter(F.col("start_timestamp") >= min_ts)
        events_df.cache()

        trace_labels = resolve_trace_labels(
            events_df,
            self.analyser_config.get("separating_key", "activity"),
            self.analyser_config.get("separating_groups", []),
        )
        params = self.analyser_config.get("method_params", {})

        ordered_constraints_df = discover_ordered(events_df, self.metadata)
        result_list = discover_targeted_rules(
            ordered_constraints_df=ordered_constraints_df,
            trace_labels=trace_labels,
            target_label=params.get("target_label", 1),
            support_threshold=self.analyser_config.get("support_threshold", 0.8),
            filtering_support=params.get("filtering_support", 1),
        )

        output_path = self.analyser_config["output_path"] + ".json"
        self.analyser_config["output_path"] = output_path
        save_dm_results(result_list, output_path)

        events_df.unpersist()
        logger.info(f"Completed. Results written to {output_path}.")

        if caller == "api":
            return {"code": 200, "data": result_list}
        return output_path

    # ------------------------------------------------------------------
    # Method implementations - process model discovery
    # ------------------------------------------------------------------

    def _copy_outputs(self, model_path, fmt, png_path, html_path):
        base = self.analyser_config["output_path"]
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

    def _model_api_response(self, model_out, png_out, html_out):
        fmt = self.analyser_config.get("output_format", "model")
        if fmt == "png" and os.path.exists(png_out):
            return FileResponse(png_out, media_type="image/png", filename=Path(png_out).name)
        if fmt == "html" and os.path.exists(html_out):
            return FileResponse(html_out, media_type="text/html", filename=Path(html_out).name)
        return FileResponse(model_out, media_type="application/octet-stream",
                            filename=Path(model_out).name)

    def _run_process_model(self, caller: str, algo: str) -> Any:
        logger.info(f"Running process model discovery ({algo}) initiated by {caller}.")
        self._load_metadata()

        events_df = self.storage.read_sequence_table(self.metadata)
        min_ts = _parse_min_timestamp(self.analyser_config.get("min_timestamp"))
        if min_ts is not None:
            events_df = events_df.filter(F.col("start_timestamp") >= min_ts)
        events_df.cache()

        try:
            model_path, fmt, png_path, html_path = discover_process_model(
                events_df,
                algo=algo,
                end_time=self.analyser_config.get("end_time"),
                noise_threshold=self.analyser_config.get("noise_threshold", 0.0),
            )
        except Exception:
            events_df.unpersist()
            logger.exception(f"Process model discovery ({algo}) failed.")
            raise

        model_out, png_out, html_out = self._copy_outputs(model_path, fmt, png_path, html_path)
        events_df.unpersist()
        logger.info(f"{algo} model -> {model_out}  PNG -> {png_out}  HTML -> {html_out}")
        self.analyser_config["output_path"] = model_out

        if caller == "api":
            return self._model_api_response(model_out, png_out, html_out)
        return model_out

    # ------------------------------------------------------------------
    # Method implementations - new analytics
    # ------------------------------------------------------------------

    def _run_bottlenecks(self, caller: str) -> Any:
        logger.info(f"Running bottlenecks initiated by {caller}.")
        self._load_metadata()

        events_df = self.storage.read_sequence_table(self.metadata)
        min_ts = _parse_min_timestamp(self.analyser_config.get("min_timestamp"))
        if min_ts is not None:
            events_df = events_df.filter(F.col("start_timestamp") >= min_ts)
        events_df.cache()

        result_df = compute_bottlenecks(
            events_df=events_df,
            trace_count=self.metadata.trace_count,
            end_time=self.analyser_config.get("end_time"),
            grouping_key=self.analyser_config.get("grouping_key"),
            grouping_value=self.analyser_config.get("grouping_value"),
            zscore_threshold=self.analyser_config.get("zscore_threshold", 3.5),
            top_k=self.analyser_config.get("top_k"),
        )

        output_path = self.analyser_config["output_path"] + ".csv"
        self.analyser_config["output_path"] = output_path
        result_df.toPandas().to_csv(output_path, index=False)

        events_df.unpersist()
        logger.info(f"Completed. Results written to {output_path}.")

        if caller == "api":
            if self.analyser_config.get("return_csv", False):
                return FileResponse(output_path, media_type="text/csv", filename=Path(output_path).name)
            with open(output_path, "r", newline="") as f:
                try:
                    return {"code": 200, "data": list(csv.DictReader(f))}
                except Exception:
                    logger.error(f"Failed to parse results from {output_path}.")
                    return {"code": 500, "message": f"Failed to parse results from {output_path}."}
        return output_path

    def _run_temporal_deviations(self, caller: str) -> Any:
        logger.info(f"Running temporal_deviations initiated by {caller}.")
        self._load_metadata()

        events_df = self.storage.read_sequence_table(self.metadata)
        min_ts = _parse_min_timestamp(self.analyser_config.get("min_timestamp"))
        if min_ts is not None:
            events_df = events_df.filter(F.col("start_timestamp") >= min_ts)
        events_df.cache()

        trace_labels = resolve_trace_labels(
            events_df,
            self.analyser_config.get("separating_key", "activity"),
            self.analyser_config.get("separating_groups", []),
        )

        result_df = compute_temporal_deviations(
            events_df=events_df,
            trace_labels=trace_labels,
            trace_count=self.metadata.trace_count,
            activity_pairs=self.analyser_config.get("activity_pairs"),
            max_auto_pairs=self.analyser_config.get("max_auto_pairs", 50),
            min_group_size=self.analyser_config.get("min_group_size", 5),
            top_k_per_pair=self.analyser_config.get("top_k_per_pair"),
        )

        output_path = self.analyser_config["output_path"] + ".csv"
        self.analyser_config["output_path"] = output_path
        result_df.toPandas().to_csv(output_path, index=False)

        events_df.unpersist()
        logger.info(f"Completed. Results written to {output_path}.")

        if caller == "api":
            if self.analyser_config.get("return_csv", False):
                return FileResponse(output_path, media_type="text/csv", filename=Path(output_path).name)
            with open(output_path, "r", newline="") as f:
                try:
                    return {"code": 200, "data": list(csv.DictReader(f))}
                except Exception:
                    logger.error(f"Failed to parse results from {output_path}.")
                    return {"code": 500, "message": f"Failed to parse results from {output_path}."}
        return output_path
