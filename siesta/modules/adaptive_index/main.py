import argparse
import json
import time
from pathlib import Path
from typing import Annotated, Any, Dict, List, Optional

from fastapi import Form, UploadFile
from pydantic import BaseModel, ConfigDict, Field
from pyspark.sql import SparkSession
from pyspark.sql.streaming.query import StreamingQuery

from siesta.core.config import get_system_config
from siesta.core.interfaces import SiestaModule, StorageManager
from siesta.core.logger import timed
from siesta.core.sparkManager import cleanup as spark_cleanup, get_spark_session
from siesta.core.storageFactory import get_storage_manager
from siesta.model.PerspectiveModel import PairStatus, PerspectiveLevel
from siesta.model.StorageModel import MetaData
from siesta.modules.adaptive_index.builders import (
    build_pair_persistent,
    incremental_update_persistent_pairs,
    promote_to_l1,
    promote_to_l2,
)
from siesta.modules.adaptive_index.catalog import evict_catalog, get_catalog
from siesta.modules.adaptive_index.retention import RetentionPolicy
from siesta.modules.adaptive_query.lru_cache import get_lru_cache
from siesta.modules.index.builders import (
    build_activity_index,
    build_sequence_table,
)
from siesta.modules.index.parsers import upload_log_file_object

import logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration model
# ---------------------------------------------------------------------------

class AdaptiveIndexConfig(BaseModel):
    """
    Configuration for one adaptive-indexing run.

    All fields shared with the eager IndexConfig use the same names and
    defaults so that existing tooling can pass the same JSON body to both
    endpoints without modification.

    The only new fields are:
      - perspectives: list of grouping-key dicts to pre-declare at L0.
      - retention_half_life_seconds: half-life parameter for the retention cost function.

    Example
    -------
    {
        "log_name": "bpic_2017",
        "storage_namespace": "siesta",
        "lookback": "30d",
        "perspectives": [
            {"grouping_keys": ["org:resource"], "lookback": "30d"},
            {"grouping_keys": ["concept:name", "org:group"], "lookback": "14d"}
        ],
        "retention_half_life_seconds": 3600
    }
    """

    model_config = ConfigDict(extra="allow")

    log_name: str = Field(
        "example_log",
        description="Name for the log in storage.",
    )
    log_path: str = Field(
        "../datasets/test.xes",
        description="Path to the local log file (XES, CSV, or JSON).",
    )
    storage_namespace: str = Field(
        "siesta",
        description="Storage namespace / bucket prefix.",
    )
    clear_existing: bool = Field(
        False,
        description="Drop and rebuild the existing index.",
    )
    enable_streaming: bool = Field(
        False,
        description="Start a Kafka streaming collector instead of batch processing.",
    )
    kafka_topic: str = Field(
        "example_log",
        description="Kafka topic to consume in streaming mode.",
    )
    lookback: str = Field(
        "7d",
        description="Default lookback window, e.g. '7d', '30d', '255i'.",
    )
    field_mappings: dict = Field(
        default_factory=lambda: {
            "xes": {
                "activity": "concept:name",
                "trace_id": "concept:name",
                "position": None,
                "start_timestamp": "time:timestamp",
                "attributes": ["*"],
            },
            "csv": {
                "activity": "activity",
                "trace_id": "trace_id",
                "position": "position",
                "start_timestamp": "timestamp",
                "attributes": ["resource", "cost"],
            },
            "json": {
                "activity": "activity",
                "trace_id": "caseID",
                "position": "position",
                "start_timestamp": "Timestamp",
            },
        },
        description="Per-format mapping of Event fields to source column names.",
    )
    trace_level_fields: list[str] = Field(
        default_factory=lambda: ["trace_id"],
        description="Fields extracted at trace level.",
    )
    timestamp_fields: list[str] = Field(
        default_factory=lambda: ["start_timestamp"],
        description="Fields to parse as Unix epoch seconds.",
    )
    perspectives: List[Dict[str, Any]] = Field(
        default_factory=list,
        description=(
            "Optional list of perspectives to pre-declare at L0 after "
            "ingest.  Each entry needs at least 'grouping_keys' (list[str]) "
            "and optionally 'lookback' (str) and "
            "'lookback_mode' ('time'|'position')."
        ),
    )
    half_life_seconds: float = Field(
        3600.0,
        description=(
            "Half-life (seconds) for exponential decay of perspective counters. "
            "Shorter values make the policy react faster to workload shifts; "
            "longer values amortise build costs over more queries."
        ),
    )


DEFAULT_ADAPTIVE_INDEX_CONFIG: Dict[str, Any] = (
    AdaptiveIndexConfig().model_dump()
)


# ---------------------------------------------------------------------------
# Perspective-registration request model
# ---------------------------------------------------------------------------

class PerspectiveRegistrationRequest(BaseModel):
    """Body for POST /adaptive-index/register-perspective."""

    log_name: str
    storage_namespace: str = "siesta"
    storage_type: str = "s3"
    grouping_keys: List[str]
    lookback: str = "7d"
    lookback_mode: str = "time"


# ---------------------------------------------------------------------------
# Module
# ---------------------------------------------------------------------------

class Adaptive_Indexing(SiestaModule):
    """
    Adaptive, workload-driven indexer for the attribute-aware
    pattern-query framework.

    State that persists across calls
    ---------------------------------
    self._catalog   : PerspectiveCatalog obtained via get_catalog().
                      Points to the process-wide singleton for the
                      currently active (namespace, log_name).

    self._retention : RetentionPolicy.  Stateless (pure predicate
                      evaluator); recreated only when the configured
                      horizon changes.

    self.metadata   : MetaData for the currently active log. 
    """

    name    = "adaptive_indexer"
    version = "1.0.0"

    storage: StorageManager
    siesta_config: Dict[str, Any]
    index_config: Dict[str, Any]
    metadata: MetaData

    _catalog:        Any   # PerspectiveCatalog | None
    _retention:      Any   # RetentionPolicy | None

    def __init__(self):
        super().__init__()
        self.index_config    = {}
        self._catalog        = None
        self._retention      = None

    # ------------------------------------------------------------------
    # Framework hooks
    # ------------------------------------------------------------------

    def register_routes(self) -> SiestaModule.ApiRoutes | None:
        return {
            "run":                   ("POST", self.api_run),
            "register-perspective":  ("POST", self.api_register_perspective),
        }

    def startup(self) -> None:
        logger.info(f"{self.name} v{self.version}: startup complete.")

    # ------------------------------------------------------------------
    # API entry points
    # ------------------------------------------------------------------

    def api_run(
        self,
        index_config: Annotated[
            str,
            Form(
                description=(
                    "JSON configuration object.  Accepts the same fields "
                    "as the eager indexer plus the optional 'perspectives' "
                    "list and 'retention_half_life_seconds'."
                ),
                openapi_examples={
                    "batch_with_perspective": {
                        "summary": "Batch ingest + declare a perspective",
                        "value": json.dumps({
                            "log_name":           "bpic_2017",
                            "storage_namespace":  "siesta",
                            "lookback":           "30d",
                            "perspectives": [
                                {"grouping_keys": ["org:resource"]}
                            ],
                        }),
                    },
                    "streaming": {
                        "summary": "Kafka streaming",
                        "value": json.dumps({
                            "log_name":          "bpic_2017",
                            "storage_namespace": "siesta",
                            "enable_streaming":  True,
                            "kafka_topic":       "bpic_2017",
                        }),
                    },
                },
            ),
        ],
        log_file: UploadFile | None = None,
    ) -> Any:
        """
        Ingest an event log and update the adaptive index.

        The shared SequenceTable and ActivityIndex are updated exactly
        as the eager indexer does.  Additionally, all L3 (persistent)
        pair indices under every established perspective are maintained
        incrementally, and the retention policy is evaluated.
        """
        logger.info(f"{self.name}: received API run request.")

        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()
        self._load_index_config(json.loads(index_config))
        self.storage.initialize_db(self.index_config)

        # If clear_existing is set, evict the stale in-memory catalog
        # so that begin_builders() starts from a clean slate.
        if self.index_config.get("clear_existing", False):
            candidate = MetaData(
                storage_namespace=self.index_config.get(
                    "storage_namespace", "siesta"
                ),
                log_name=self.index_config.get("log_name", "default_log"),
                storage_type=self.index_config.get("storage_type", "s3"),
            )
            evict_catalog(candidate)
            self._catalog        = None
            self._retention      = None
            logger.info(
                f"{self.name}: evicted catalog for "
                f"{candidate.storage_namespace}/{candidate.log_name} "
                "(clear_existing=True)."
            )

        if log_file is None:
            if not self.index_config.get("enable_streaming", False):
                return {
                    "code": 400,
                    "message": (
                        "AdaptiveIndexing: no log file uploaded and "
                        "streaming not enabled.  Aborting."
                    ),
                }
            self.storage.initialize_streaming_collector(self.index_config)
            self.begin_builders(caller="api")
            return {
                "code": 200,
                "message": "AdaptiveIndexing: streaming collector initialised.",
            }

        if not log_file.filename:
            logger.error(f"{self.name}: uploaded file has no filename.")
            return {
                "code": 400,
                "message": (
                    "AdaptiveIndexing: uploaded log file has no filename."
                ),
            }

        self.index_config["enable_streaming"] = False
        self.index_config["log_path"] = upload_log_file_object(
            self.index_config, log_file, log_file.filename
        )

        t0 = time.time()
        self.begin_builders(caller="api")
        elapsed = time.time() - t0

        logger.info(
            f"{self.name}: batch processing completed in {elapsed:.3f}s."
        )
        return {
            "code":    200,
            "message": "AdaptiveIndexing: batch processing completed.",
            "time":    elapsed,
        }

    def api_register_perspective(
        self,
        request: PerspectiveRegistrationRequest,
    ) -> Any:
        """
        Declare a new grouping perspective at L0.

        No Spark computation is performed.  The perspective will be
        promoted to L1 on the first query that references it (via the
        QueryPlanner), or during the next ingest cycle if the retention
        predicates warrant it.
        """
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        if self._catalog is None:
            meta = MetaData(
                storage_namespace=request.storage_namespace,
                log_name=request.log_name,
                storage_type=request.storage_type,
            )
            self._catalog = get_catalog(meta, self.storage or get_storage_manager())

        pid, _ = self._catalog.get_or_declare(
            grouping_keys=request.grouping_keys,
            lookback=request.lookback,
            lookback_mode=request.lookback_mode,
        )

        logger.info(
            f"{self.name}: registered perspective '{pid}' "
            f"(keys={request.grouping_keys}) at L0 via API."
        )
        return {
            "code":           200,
            "perspective_id": pid,
            "message":        f"Perspective '{pid}' registered at L0.",
        }

    # ------------------------------------------------------------------
    # CLI entry point
    # ------------------------------------------------------------------

    def cli_run(self, args: Any, **kwargs: Any) -> Any:
        logger.info(f"{self.name}: running via CLI with args={args}.")

        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        parser = argparse.ArgumentParser(
            description="Siesta Adaptive Indexing module"
        )
        parser.add_argument(
            "--index_config",
            type=str,
            help="Path to configuration JSON file.",
            required=False,
        )
        parsed_args, _ = parser.parse_known_args(args)

        if parsed_args.index_config:
            config_path = parsed_args.index_config
            if not Path(config_path).exists():
                raise FileNotFoundError(
                    f"Config file not found: {config_path}"
                )
            try:
                with open(config_path) as f:
                    user_config = json.load(f)
                self._load_index_config(user_config)
                self.storage.initialize_db(self.index_config)
                logger.info(
                    f"{self.name}: config loaded from {config_path}."
                )
            except Exception as exc:
                raise RuntimeError(
                    f"Error loading config from {config_path}: {exc}"
                ) from exc


        if self.index_config.get("enable_streaming", False):
            self.storage.initialize_streaming_collector(self.index_config)

        self.begin_builders(caller="cli")

        if self.index_config.get("enable_streaming", False):
            logger.info(
                f"{self.name}: awaiting streaming termination.  "
                "Press Ctrl+C to stop."
            )
            get_spark_session().streams.awaitAnyTermination()

    # ------------------------------------------------------------------
    # Core build orchestration
    # ------------------------------------------------------------------

    def begin_builders(self, caller: str = "cli") -> None:
        """
        Orchestrate one ingest cycle.

        Step 1  Sequence Table + Activity Index
            Delegated to the existing builders from modules/index/builders.py.
            The shared tables remain fully compatible with the eager indexer
            and the existing query module.

        Step 2  Declare proactive perspectives (L0)
            Perspectives listed in index_config["perspectives"] are
            registered in the catalog at L0.  No Spark work.

        Step 3  Incremental maintenance for L3 (persistent) pairs
            For every perspective at L1 or above, run the
            LastChecked-guided pair extraction for every persistent pair
            and append results to the per-perspective PairsIndex.

        Step 4  Retention evaluation
            Check all retention predicates and execute any warranted
            promotions or demotions.  Flush the catalog to Delta.

        Streaming mode
            Steps 2-4 are wired inside _on_batch() so they run on every
            Structured Streaming micro-batch.
        """

        # One-time lazy init - runs exactly once per process lifetime.
        # After this, self._catalog and self._retention are just there.
        if self._catalog is None:
            self.metadata = MetaData(
                storage_namespace=self.index_config.get("storage_namespace", "siesta"),
                log_name=self.index_config.get("log_name", "default_log"),
                storage_type=self.index_config.get("storage_type", "s3"),
            )
            self.storage.read_metadata_table(self.metadata)
            self._catalog = get_catalog(self.metadata, self.storage)
            self._retention = RetentionPolicy(
                half_life_seconds=self.index_config.get(
                    "retention_half_life_seconds",
                    DEFAULT_ADAPTIVE_INDEX_CONFIG["retention_half_life_seconds"],
                ),
            )
        # ----------------------------------------------------------------
        # Step 1a  Sequence Table
        # ----------------------------------------------------------------
        seq_df = timed(
            build_sequence_table,
            f"{self.name}.",
            index_config=self.index_config,
            metadata=self.metadata,
        )

        # ----------------------------------------------------------------
        # Step 1b  Activity Index (streaming path diverges here)
        # ----------------------------------------------------------------
        if isinstance(seq_df, StreamingQuery):
            self._begin_builders_streaming(seq_df)
            if caller == "cli":
                logger.info(
                    f"{self.name}: streaming jobs running.  "
                    "Press Ctrl+C to stop."
                )
                get_spark_session().streams.awaitAnyTermination()
            return

        # ---- Batch path -----------------------------------------------
        activity_index_df = timed(
            build_activity_index,
            f"{self.name}.",
            events_df=seq_df,
            metadata=self.metadata,
        )
        seq_df.unpersist()

        # ----------------------------------------------------------------
        # Steps 2-4
        # ----------------------------------------------------------------
        batch_min_ts = (
            activity_index_df
            .agg({"start_timestamp": "min"})
            .collect()[0][0]
        )

        self._declare_proactive_perspectives()
        self._run_incremental_maintenance(activity_index_df, batch_min_ts)
        self._evaluate_retention()   # flush() is called inside here

        activity_index_df.unpersist()
        spark_cleanup()

    # ------------------------------------------------------------------
    # Streaming path
    # ------------------------------------------------------------------

    def _begin_builders_streaming(
        self, seq_streaming_query: StreamingQuery
    ) -> None:
        """
        Attach adaptive maintenance as a foreachBatch listener on top of
        the SequenceTable streaming query.
        """
        sequence_stream = (
            get_spark_session()
            .readStream
            .format("delta")
            .load(self.metadata.sequence_table_path)
        )

        storage  = self.storage
        metadata = self.metadata
        module   = self   # capture for closure

        def _foreach_batch(micro_batch_df, batch_id: int):
            if micro_batch_df.isEmpty():
                return
            storage.write_activity_index(micro_batch_df, metadata)
            storage.write_metadata_table(metadata)
            module._on_batch(micro_batch_df)

        (
            sequence_stream
            .writeStream
            .queryName("adaptive_index_maintenance")
            .foreachBatch(_foreach_batch)
            .outputMode("append")
            .option(
                "checkpointLocation",
                storage.get_checkpoint_location(
                    metadata, "adaptive_index_maintenance"
                ),
            )
            .start()
        )

    def _on_batch(self, micro_batch_df) -> None:
        """
        Called for every micro-batch in streaming mode.  Runs steps
        2-4 of begin_builders() on the supplied batch DataFrame.

        Can also be called directly in unit tests to simulate a batch.
        """
        batch_min_ts = (
            micro_batch_df
            .agg({"start_timestamp": "min"})
            .collect()[0][0]
        )
        self._declare_proactive_perspectives()
        self._run_incremental_maintenance(micro_batch_df, batch_min_ts)
        self._evaluate_retention()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_index_config(self, config: Dict[str, Any]) -> None:
        self.index_config = DEFAULT_ADAPTIVE_INDEX_CONFIG.copy()
        self.index_config.update(config)

    def _declare_proactive_perspectives(self) -> None:
        """
        Register any perspectives listed in index_config['perspectives']
        at L0 in the catalog.  Pure catalog write, no Spark work.
        """
        for spec in self.index_config.get("perspectives", []):
            grouping_keys = spec.get("grouping_keys")
            if not grouping_keys:
                logger.warning(
                    f"{self.name}: perspective spec missing 'grouping_keys'"
                    f", skipping: {spec}"
                )
                continue

            pid, stats = self._catalog.get_or_declare(
                grouping_keys=grouping_keys,
                lookback=spec.get(
                    "lookback",
                    self.index_config.get("lookback", "7d"),
                ),
                lookback_mode=spec.get("lookback_mode", "time"),
            )

            if stats.level == PerspectiveLevel.L0_DECLARED:
                logger.info(
                    f"{self.name}: proactively declared '{pid}' "
                    f"(keys={grouping_keys}) at L0."
                )

    def _run_incremental_maintenance(self, batch_activity_df, batch_min_ts: int) -> None:
        for pid, stats in self._catalog.all_established_perspectives():

            persistent_pairs = [
                (a, b)
                for (a, b), ps in stats.pairs.items()
                if ps.status == PairStatus.PERSISTENT
            ]

            t0 = time.time()
            try:
                pair_elapsed = incremental_update_persistent_pairs(
                    pid=pid,
                    grouping_keys=stats.grouping_keys,
                    batch_activity_df=batch_activity_df,
                    batch_min_ts=batch_min_ts,
                    persistent_pairs=persistent_pairs,
                    lookback=stats.lookback,
                    lookback_mode=stats.lookback_mode,
                    has_pos=(
                        stats.level >= PerspectiveLevel.L2_POS_ESTABLISHED
                    ),
                    metadata=self.metadata,
                    storage=self.storage,
                )
            except Exception as exc:
                logger.error(
                    f"{self.name}: maintenance failed for '{pid}': {exc}",
                    exc_info=True,
                )
                continue

            # Invalidate the LRU cache for this perspective so that the
            # query planner does not serve pre-ingest transient pairs.
            # This must happen after the Delta write completes so that
            # the next query that rebuilds a transient pair reads the
            # updated sequence table.
            get_lru_cache(self.metadata).invalidate_perspective(pid)

            total_ms = (time.time() - t0) * 1000
            if pair_elapsed:
                self._catalog.record_batch_maintenance(
                    pid=pid,
                    l1_ms=0.0,
                    l2_ms=0.0,
                    pair_ms=pair_elapsed,
                )
            logger.info(
                f"{self.name}: maintenance for '{pid}' done in {total_ms:.1f}ms."
            )

    def _evaluate_retention(self) -> None:
        """
        Evaluate retention predicates for all perspectives and pairs and
        execute warranted promotions / demotions.

        Flushes the catalog to Delta at the end so that all write-back
        mutations accumulated during the ingest cycle are persisted.

        Promotion rules
        ---------------
        L0 -> L1 (backstop)  : if the query planner has fed enough
                               query-touch evidence via record_query_touch,
                               the retention policy may warrant promotion
                               even before the next query.  This is a
                               safety net; the query planner promotes
                               synchronously on first touch.
        L1 -> L2             : triggered when pos-referencing query
                               frequency * mean savings exceeds threshold.
        ABSENT -> PERSISTENT : triggered per pair when accumulated
                               savings cross the L3 threshold.

        Demotion rules
        --------------
        PERSISTENT -> TRANSIENT : triggered when utility falls below
                               (1 - hysteresis) * threshold.  The pair
                               stops receiving incremental maintenance
                               but remains in the LRU cache for reuse.
                               The LRU eviction policy handles the
                               final TRANSIENT -> ABSENT transition.
        """
        for pid, stats in self._catalog.all_perspectives():

            # ---- L0 -> L1 backstop ------------------------------------
            if (
                stats.level == PerspectiveLevel.L0_DECLARED
                and self._retention.should_promote_l1(stats)
            ):
                logger.info(
                    f"{self.name}: promoting '{pid}' L0->L1 "
                    "(retention backstop)."
                )
                t0 = time.time()
                try:
                    promote_to_l1(
                        pid=pid,
                        grouping_keys=stats.grouping_keys,
                        metadata=self.metadata,
                        storage=self.storage,
                    )
                    stats.l1_build_cost_ms = (time.time() - t0) * 1000
                    self._catalog.promote(
                        pid, PerspectiveLevel.L1_POS_FREE
                    )
                except Exception as exc:
                    logger.error(
                        f"{self.name}: L1 promotion failed for "
                        f"'{pid}': {exc}",
                        exc_info=True,
                    )

            # ---- L1 -> L2 ---------------------------------------------
            if (
                stats.level == PerspectiveLevel.L1_POS_FREE
                and self._retention.should_promote_l2(stats)
            ):
                logger.info(
                    f"{self.name}: promoting '{pid}' L1->L2."
                )
                t0 = time.time()
                try:
                    promote_to_l2(
                        pid=pid,
                        grouping_keys=stats.grouping_keys,
                        metadata=self.metadata,
                        storage=self.storage,
                    )
                    stats.l2_build_cost_ms = (time.time() - t0) * 1000
                    self._catalog.promote(
                        pid, PerspectiveLevel.L2_POS_ESTABLISHED
                    )
                except Exception as exc:
                    logger.error(
                        f"{self.name}: L2 promotion failed for "
                        f"'{pid}': {exc}",
                        exc_info=True,
                    )

            # ---- L1 -> L0 demotion ----
            if (
                stats.level == PerspectiveLevel.L1_POS_FREE
                and self._retention.should_demote_l1(stats)
            ):
                logger.info(f"{self.name}: demoting '{pid}' L1->L0.")
                # Drop the per-perspective sequence table. Future queries that
                # touch this perspective will rebuild it via L0->L1 promotion.
                try:
                    from siesta.modules.adaptive_index.builders import _perspective_sequence_path
                    seq_path = _perspective_sequence_path(self.metadata, pid)
                    spark = get_spark_session()
                    spark.sql(f"DROP TABLE IF EXISTS delta.`{seq_path}`")
                    # Reset L1-specific stats so subsequent re-promotion starts clean.
                    stats.l1_build_cost_ms = 0.0
                    stats.l1_maintenance_ms_per_batch = 0.0
                    # Note: we do NOT reset l1_query_count - that's the workload signal
                    # that drives re-promotion.
                    self._catalog.set_level(pid, PerspectiveLevel.L0_DECLARED)
                except Exception as exc:
                    logger.error(
                        f"{self.name}: L1 demotion failed for '{pid}': {exc}",
                        exc_info=True,
                    )

            # ---- L2 -> L1 demotion ----
            elif (
                stats.level == PerspectiveLevel.L2_POS_ESTABLISHED
                and self._retention.should_demote_l2(stats)
            ):
                logger.info(f"{self.name}: demoting '{pid}' L2->L1.")
                # Stop maintaining pos. Existing per-pair indices keep their pos
                # data but it goes stale; the query planner falls back to ts-sort.
                try:
                    from siesta.modules.adaptive_index.builders import _perspective_seq_metadata_path
                    meta_path = _perspective_seq_metadata_path(self.metadata, pid)
                    spark = get_spark_session()
                    spark.sql(f"DROP TABLE IF EXISTS delta.`{meta_path}`")
                    stats.l2_build_cost_ms = 0.0
                    stats.l2_maintenance_ms_per_batch = 0.0
                    self._catalog.set_level(pid, PerspectiveLevel.L1_POS_FREE)
                except Exception as exc:
                    logger.error(
                        f"{self.name}: L2 demotion failed for '{pid}': {exc}",
                        exc_info=True,
                    )

            # ---- Per-pair retention -----------------------------------
            for (act_a, act_b), pair_stats in list(stats.pairs.items()):

                if pair_stats.status == PairStatus.ABSENT:
                    if self._retention.should_persist_pair(pair_stats):
                        logger.info(
                            f"{self.name}: persisting pair "
                            f"({act_a},{act_b}) under '{pid}'."
                        )
                        t0 = time.time()
                        try:
                            build_pair_persistent(
                                pid=pid,
                                act_a=act_a,
                                act_b=act_b,
                                metadata=self.metadata,
                                storage=self.storage,
                                lookback=stats.lookback,
                                lookback_mode=stats.lookback_mode,
                                has_pos=(
                                    stats.level
                                    >= PerspectiveLevel.L2_POS_ESTABLISHED
                                ),
                            )
                            pair_stats.build_cost_ms = (
                                (time.time() - t0) * 1000
                            )
                            self._catalog.promote_pair(
                                pid, act_a, act_b, PairStatus.PERSISTENT
                            )
                        except Exception as exc:
                            logger.error(
                                f"{self.name}: pair persistence failed "
                                f"for ({act_a},{act_b}) under "
                                f"'{pid}': {exc}",
                                exc_info=True,
                            )

                elif pair_stats.status == PairStatus.PERSISTENT:
                    if self._retention.should_demote_pair(pair_stats):
                        logger.info(
                            f"{self.name}: demoting pair "
                            f"({act_a},{act_b}) under '{pid}' — "
                            "utility below threshold, moving to "
                            "TRANSIENT (LRU-cached)."
                        )
                        # Demote to TRANSIENT (L3-), not ABSENT.
                        # The pair stops receiving incremental
                        # maintenance but stays in the query planner's
                        # LRU cache for immediate reuse.  The LRU
                        # eviction policy handles the final
                        # TRANSIENT -> ABSENT transition based on
                        # memory pressure.
                        self._catalog.promote_pair(
                            pid, act_a, act_b, PairStatus.TRANSIENT
                        )
            
        # Flush all write-back mutations accumulated during this cycle.
        self._catalog.flush()