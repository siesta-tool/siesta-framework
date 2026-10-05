"""
Lifecycle of a single query
---------------------------
1.  The API endpoint receives a QueryConfig with an optional
    `grouping_keys` field.  If absent, the query falls through to
    the existing eager query module via _fallback_to_eager().

2.  The planner consults the PerspectiveCatalog to resolve the
    perspective.  If the perspective is at L0, the planner promotes
    it to L1 (and to L2 if the pattern references pos) on the spot.
    This is the synchronous first-touch promotion from the design
    notes.

3.  For each (A, B) pair required by the pattern, the planner checks
    the pair's status:
      - PERSISTENT (L3): read the per-pair Delta table.
      - TRANSIENT  (L3-): serve from the LRU cache if present,
        otherwise rebuild transiently and cache.
      - ABSENT: scan the per-perspective sequence table and extract
        pairs on the fly.  Time the extraction; record the cost as
        the cold-start build baseline for the retention policy.

4.  The assembled pairs DataFrame is passed to the detection or
    exploration processor, which runs the same pruning + CEP
    validation logic as the eager module but keyed on the group
    value v instead of trace_id, and sorting pseudo-sequences by
    ts (L1) or pos (L2) according to the plan.

5.  After the query completes, the planner records workload
    statistics (pairs touched, savings, pos reference) into the
    catalog and flushes.

LRU cache
---------
The L3- cache is a bounded dict on the driver, keyed by
(pid, act_a, act_b) and valued by the collected pairs list.
Eviction is LRU by last_accessed_ts.  The cache lives on the
module instance and survives across API requests.

Fallback
--------
When no grouping_keys are provided, the module delegates entirely
to the existing eager query processors.  This means a deployment
can expose both /query/* (eager) and /adaptive-query/* (adaptive)
endpoints simultaneously, with no interference.
"""

import argparse
import json
import threading
import time
from collections import OrderedDict
from pathlib import Path
from typing import Annotated, Any, Dict, List, Optional, Tuple
import queue

from fastapi import Body
from pydantic import BaseModel, ConfigDict, Field
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col

from siesta.core.config import get_system_config
from siesta.core.interfaces import SiestaModule, StorageManager
from siesta.core.logger import timed
from siesta.core.sparkManager import get_spark_session
from siesta.core.storageFactory import get_storage_manager
from siesta.model.PerspectiveModel import (
    PairStats,
    PairStatus,
    PerspectiveLevel,
)
from siesta.model.StorageModel import MetaData
from siesta.modules.adaptive_index.builders import (
    build_pair_transient,
    build_pairs_transient_batched,
    promote_to_l1,
    promote_to_l2,
    _get_perspective_seq_df,
    _grouping_col,
    _perspective_pair_path,
)
from siesta.modules.adaptive_index.catalog import get_catalog
from siesta.modules.adaptive_index.retention import RetentionPolicy
from siesta.modules.adaptive_query.lru_cache import PairLRUCache, get_lru_cache
from siesta.modules.adaptive_query.state import (
    GROUP_COUNTS,
    drain_promotions,
    pair_table_files,
    reset_log_state,
    set_log_option,
    submit_promotion,
)
from siesta.modules.index.pair_attributes import (
    has_attribute_constraints,
    join_back_attributes,
    strip_pair_attributes,
)
from siesta.modules.query.parse_seql import (
    can_match_single_event,
    extract_info_pairs,
    extract_responded_pairs,
    pattern_labels,
    split_pattern_to_list,
    Quantifier as SeqlQuantifier,
)
from siesta.modules.query.processors.detection_query import (
    build_exact_pair_predicate,
    detect as eager_detect,
    process_detection_query as eager_process_detection,
)
from siesta.modules.query.processors.predicates import build_event_keep_predicate
from siesta.modules.query.processors.exploration_query import (
    process_exploration_query as eager_process_exploration,
)
from siesta.modules.query.processors.stats_query import (
    process_stats_query as eager_process_stats,
)
from siesta.modules.query.CEP_adapter import find_occurrences_dsl
from siesta.model.DataModel import EventPair

import pandas as pd

import logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config model
# ---------------------------------------------------------------------------

class AdaptiveQueryMethodInput(BaseModel):
    model_config = ConfigDict(extra="allow")
    pattern: str = Field(
        "", description="Pattern string, e.g. 'A B* C'"
    )
    explore_mode: str = Field(
        "accurate",
        description="'accurate', 'fast', or 'hybrid'.",
    )
    explore_k: int = Field(
        10, description="Candidates for hybrid exploration."
    )


class AdaptiveQueryConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    log_name: str = Field("example_log")
    storage_namespace: str = Field("siesta")
    method: str = Field(
        "detection",
        description="'statistics', 'detection', or 'exploration'.",
    )
    query: AdaptiveQueryMethodInput = Field(
        default_factory=AdaptiveQueryMethodInput,
    )
    support_threshold: float = Field(0.0)

    # ---- Adaptive-specific fields ----
    grouping_keys: Optional[List[str]] = Field(
        None,
        description=(
            "Attribute keys defining the grouping perspective phi_G. "
            "If None, the query falls through to the eager module."
        ),
    )
    lookback: str = Field(
        "7d", description="Lookback window for pair extraction."
    )
    lookback_mode: str = Field(
        "time", description="'time' or 'position'."
    )


DEFAULT_ADAPTIVE_QUERY_CONFIG: Dict[str, Any] = (
    AdaptiveQueryConfig().model_dump()
)


# ---------------------------------------------------------------------------
# Module
# ---------------------------------------------------------------------------

class Adaptive_Querying(SiestaModule):
    """
    Adaptive query executor with workload-driven index selection.

    Exposes the same three endpoints as the eager query module
    (detection, exploration, statistics) with the addition of an
    optional `grouping_keys` field that activates the adaptive
    planner.
    """

    name = "adaptive_executor"
    version = "1.0.0"

    storage: StorageManager
    siesta_config: Dict[str, Any]
    query_config: Dict[str, Any]
    metadata: MetaData
    _lru: PairLRUCache
    _retention: RetentionPolicy | None
    _retention_params: tuple | None    


    def __init__(self):
        super().__init__()
        self.query_config = {}
        self._retention = None
        self._retention_params = None
        # Promotion workers and group counts live in
        # siesta.modules.adaptive_query.state so the indexer can reset
        # them and evaluation code can wait for pending promotions.


    # ------------------------------------------------------------------
    # Framework hooks
    # ------------------------------------------------------------------

    def startup(self) -> None:
        logger.info(f"{self.name} v{self.version}: startup complete.")

    def register_routes(self) -> SiestaModule.ApiRoutes:
        return {
            "detection": ("POST", self.api_detection),
            "exploration": ("POST", self.api_exploration),
            "statistics": ("POST", self.api_statistics),
            "pair_coverage": ("POST", self.api_pair_coverage),
            "eval_catalog": ("POST", self.api_eval_catalog),
            "eval_reset": ("POST", self.api_eval_reset),
            "eval_drain": ("POST", self.api_eval_drain),
            "eval_force_persist": ("POST", self.api_eval_force_persist),
            "eval_pair_digest": ("POST", self.api_eval_pair_digest),
        }

    # ------------------------------------------------------------------
    # API entry points
    # ------------------------------------------------------------------
    
    def api_detection(
        self,
        query_config: Annotated[
            AdaptiveQueryConfig,
            Body(
                openapi_examples={
                    "adaptive_detection": {
                        "summary": "Detection under a custom perspective",
                        "value": {
                            "log_name": "bpic_2017",
                            "query": {"pattern": "A B C"},
                            "grouping_keys": ["org:resource"],
                            "lookback": "30d",
                        },
                    },
                    "eager_fallback": {
                        "summary": "No grouping keys — falls through to eager",
                        "value": {
                            "log_name": "bpic_2017",
                            "query": {"pattern": "A B C"},
                        },
                    },
                },
            ),
        ],
    ) -> Any:
        """
        Retrieve all groups satisfying a pattern, optionally under a
        custom grouping perspective.

        If `grouping_keys` is provided, the query runs against the
        adaptive per-perspective PairsIndex.  Otherwise it falls through
        to the eager detection processor.
        """
        self._bootstrap(query_config.model_dump(), method="detection")

        if not self.query_config.get("grouping_keys"):
            return self._fallback_to_eager("detection")

        return self._run_adaptive_detection()

    def api_exploration(
        self,
        query_config: Annotated[
            AdaptiveQueryConfig,
            Body(
                openapi_examples={
                    "adaptive_exploration": {
                        "summary": "Exploration under a custom perspective",
                        "value": {
                            "log_name": "bpic_2017",
                            "query": {
                                "pattern": "A B",
                                "explore_mode": "accurate",
                            },
                            "grouping_keys": ["org:resource"],
                        },
                    },
                },
            ),
        ],
    ) -> Any:
        """
        Find activity continuations, optionally under a custom perspective.
        """
        self._bootstrap(query_config.model_dump(), method="exploration")

        if not self.query_config.get("grouping_keys"):
            return self._fallback_to_eager("exploration")

        return self._run_adaptive_exploration()

    def api_statistics(
        self,
        query_config: Annotated[AdaptiveQueryConfig, Body()],
    ) -> Any:
        """
        Pair statistics.  Always falls through to the eager module
        since count tables are not maintained per perspective.
        """
        self._bootstrap(query_config.model_dump(), method="statistics")
        return self._fallback_to_eager("statistics")

    def api_pair_coverage(
        self,
        body: Annotated[dict, Body(...)],
    ) -> Any:
        """
        For a given log + perspective, compute how many groups each
        ordered activity pair (A, B) co-occurs in (A precedes B at
        least once within the group).

        This is the input the warm-up workload-builder uses to stratify
        queries by result density.  The same operation can be done
        offline, but doing it inside the API keeps the Spark session and
        storage manager singletons in scope.

        Request body
        ------------
        {
            "log_name":          "bpic2020",
            "storage_namespace": "siesta",
            "grouping_keys":     ["org:resource"],
            "activities":        ["A", "B", ...]   # optional whitelist;
                                                # if absent uses all
                                                # observed activities
        }

        Response
        --------
        {
            "log_name":     "...",
            "perspective":  "org:resource",
            "group_count":  123,
            "pairs": [
                {"source": "A", "target": "B", "groups": 87},
                ...
            ]
        }
        """
        from siesta.model.StorageModel import MetaData

        log_name      = body["log_name"]
        namespace     = body.get("storage_namespace", "siesta")
        grouping_keys = body["grouping_keys"]
        activity_whitelist: list[str] | None = body.get("activities")

        metadata = MetaData(
            storage_namespace=namespace,
            log_name=log_name,
            storage_type="s3",
        )
        storage = get_storage_manager()
        metadata = storage.read_metadata_table(metadata)

        seq_df = storage.read_sequence_table(metadata)

        # Compute the perspective group value for every event using the
        # same helper the indexer uses — keeps the grouping definition
        # consistent end-to-end.
        from siesta.modules.adaptive_index.builders import _grouping_col

        grouped = (
            seq_df
            .withColumn("group_value", _grouping_col(grouping_keys))
            .filter(col("group_value").isNotNull())
        )

        if activity_whitelist:
            grouped = grouped.filter(col("activity").isin(activity_whitelist))

        # For each group, compute the (earliest, latest) timestamp of each
        # activity within that group.  An ordered pair (A, B) co-occurs
        # in a group iff min_ts(A) < max_ts(B), with strict inequality
        # when A != B and at least one A event precedes some B event.
        from pyspark.sql.functions import min as F_min, max as F_max, count_distinct

        per_group_activity = (
            grouped
            .groupBy("group_value", "activity")
            .agg(
                F_min("start_timestamp").alias("first_ts"),
                F_max("start_timestamp").alias("last_ts"),
            )
        )

        # Self-join on group_value, alias as A and B sides.
        a_side = per_group_activity.alias("a")
        b_side = per_group_activity.alias("b")

        pair_cooc = (
            a_side.join(b_side, col("a.group_value") == col("b.group_value"))
            .filter(col("a.activity") != col("b.activity"))
            .filter(col("a.first_ts") < col("b.last_ts"))
            .select(
                col("a.activity").alias("source"),
                col("b.activity").alias("target"),
                col("a.group_value").alias("group_value"),
            )
            .dropDuplicates(["source", "target", "group_value"])
        )

        coverage = (
            pair_cooc
            .groupBy("source", "target")
            .agg(count_distinct("group_value").alias("groups"))
            .collect()
        )

        group_count = grouped.select("group_value").distinct().count()

        pairs = sorted(
            [{"source": r.source, "target": r.target, "groups": int(r.groups)}
            for r in coverage],
            key=lambda x: (-x["groups"], x["source"], x["target"]),
        )

        pid = ":".join(sorted(grouping_keys))
        return {
            "log_name":    log_name,
            "perspective": pid,
            "group_count": int(group_count),
            "pairs":       pairs,
        }

    # ------------------------------------------------------------------
    # Evaluation endpoints
    # ------------------------------------------------------------------
    # Instrumentation for benchmarks: inspect the lifecycle state, reset
    # the in-memory adaptive state, wait for asynchronous promotions, and
    # persist pairs without a query workload (the all-pairs counterfactual).

    @staticmethod
    def _eval_metadata(body: dict) -> MetaData:
        return MetaData(
            storage_namespace=body.get("storage_namespace", "siesta"),
            log_name=body["log_name"],
            storage_type=body.get("storage_type", "s3"),
        )

    def api_eval_catalog(self, body: Annotated[dict, Body(...)]) -> Any:
        """
        Snapshot of the catalog of a log: per perspective its level and,
        per pair, status, decayed query count, build / savings /
        maintenance costs.  Also reports the LRU contents.

        Body: {log_name, storage_namespace?, grouping_keys?, drain?}.
        With ``drain`` (default true) pending promotions finish first so
        the snapshot reflects them.
        """
        from siesta.modules.adaptive_index.catalog import _make_perspective_id

        metadata = self._eval_metadata(body)
        waited = drain_promotions(metadata) if body.get("drain", True) else 0.0
        catalog = get_catalog(metadata, get_storage_manager())
        pid = (
            _make_perspective_id(body["grouping_keys"])
            if body.get("grouping_keys") else None
        )
        lru = get_lru_cache(metadata)
        lru_keys = [k for k in lru.keys() if pid is None or k[0] == pid]
        return {
            "code": 200,
            "drained_s": waited,
            "perspectives": catalog.snapshot(pid),
            "lru": {
                "capacity": lru.capacity,
                "size": len(lru),
                "evictions": lru.evictions,
                "entries": [f"{k[0]}:{k[1]}->{k[2]}" for k in lru_keys],
            },
        }

    def api_eval_reset(self, body: Annotated[dict, Body(...)]) -> Any:
        """
        Drop the in-memory adaptive state of a log (pending promotions are
        drained first): LRU cache, group-count cache and catalog singleton.
        Storage is untouched; the catalog reloads from Delta on next use.

        Body: {log_name, storage_namespace?}.
        """
        return {"code": 200, **reset_log_state(self._eval_metadata(body))}

    def api_eval_drain(self, body: Annotated[dict, Body(...)]) -> Any:
        """
        Block until the asynchronous post-query promotions of a log (or of
        every log when ``log_name`` is omitted) have finished.

        Body: {log_name?, storage_namespace?, timeout_s?}.
        """
        metadata = self._eval_metadata(body) if body.get("log_name") else None
        waited = drain_promotions(metadata, timeout_s=body.get("timeout_s"))
        return {"code": 200, "drained_s": waited}

    def api_eval_force_persist(self, body: Annotated[dict, Body(...)]) -> Any:
        """
        Build and persist pair indices of a perspective without a query
        workload.  Used for the counterfactual in which every pair of every
        perspective is maintained.

        Body: {log_name, storage_namespace?, grouping_keys, pairs: [[A, B],
        ...] | "all", lookback?, lookback_mode?}.  ``"all"`` persists every
        ordered pair of distinct activities that co-occurs in some group,
        plus the self-pairs of activities that occur twice in a group.
        """
        from siesta.modules.adaptive_index.builders import build_pairs_persistent_batched

        metadata = self._eval_metadata(body)
        storage = get_storage_manager()
        metadata = storage.read_metadata_table(metadata)
        grouping_keys = body["grouping_keys"]
        drain_promotions(metadata)
        set_log_option(metadata, "embed_pair_attributes",
                       body.get("pair_attributes", "embedded") != "join")

        catalog = get_catalog(metadata, storage)
        pid, stats = catalog.get_or_declare(
            grouping_keys=grouping_keys,
            lookback=body.get("lookback", "3650d"),
            lookback_mode=body.get("lookback_mode", "time"),
        )
        if stats.level < PerspectiveLevel.L1_POS_FREE:
            promote_to_l1(pid, grouping_keys, metadata, storage)
            catalog.promote(pid, PerspectiveLevel.L1_POS_FREE)

        requested = body.get("pairs", "all")
        if requested == "all":
            pairs = self._cooccurring_pairs(metadata, storage, grouping_keys)
        else:
            pairs = [tuple(p) for p in requested]
        todo = [
            (a, b) for (a, b) in pairs
            if catalog.get_pair_status(pid, a, b) != PairStatus.PERSISTENT
        ]

        t0 = time.time()
        costs = build_pairs_persistent_batched(
            pid=pid,
            pairs=todo,
            lookback=stats.lookback,
            lookback_mode=stats.lookback_mode,
            grouping_keys=grouping_keys,
            metadata=metadata,
            storage=storage,
            has_pos=stats.level >= PerspectiveLevel.L2_POS_ESTABLISHED,
        )
        for pair, ms in costs.items():
            catalog.record_pair_build_cost(pid, pair[0], pair[1], ms)
        # Pinned: the counterfactual maintains these pairs whatever the
        # workload, so retention must not demote them.
        catalog.promote_pairs(pid, pairs, PairStatus.PERSISTENT, pinned=True)
        return {
            "code": 200,
            "perspective": pid,
            "requested": len(pairs),
            "built": len(todo),
            "time": time.time() - t0,
        }

    def api_eval_pair_digest(self, body: Annotated[dict, Body(...)]) -> Any:
        """
        Row count and an order-independent digest of the persisted pair
        tables of a perspective, to check that incrementally maintained
        pairs equal a from-scratch build.

        Body: {log_name, storage_namespace?, grouping_keys, pairs?}.  Without
        ``pairs`` every PERSISTENT pair of the perspective is digested.
        """
        from siesta.modules.adaptive_index.catalog import _make_perspective_id

        metadata = self._eval_metadata(body)
        drain_promotions(metadata)
        storage = get_storage_manager()
        catalog = get_catalog(metadata, storage)
        pid = _make_perspective_id(body["grouping_keys"])
        stats = catalog.get(pid)
        if body.get("pairs"):
            pairs = [tuple(p) for p in body["pairs"]]
        elif stats is not None:
            pairs = sorted(
                p for p, ps in stats.pairs.items()
                if ps.status == PairStatus.PERSISTENT
            )
        else:
            pairs = []

        spark = get_spark_session()
        digests = {}
        for (a, b) in pairs:
            path = _perspective_pair_path(metadata, pid, a, b)
            try:
                row = (
                    spark.read.format("delta").load(path)
                    .select(F.xxhash64(
                        "trace_id", "source_position", "target_position",
                        "source_timestamp", "target_timestamp",
                    ).alias("h"))
                    .agg(
                        F.count("*").alias("n"),
                        F.sum(col("h").cast("decimal(38,0)")).alias("s"),
                    )
                    .collect()[0]
                )
                digests[f"{a}->{b}"] = {"rows": int(row.n), "digest": str(row.s or 0)}
            except Exception as exc:
                digests[f"{a}->{b}"] = {"error": str(exc)[:200]}
        return {"code": 200, "perspective": pid, "pairs": digests}

    @staticmethod
    def _cooccurring_pairs(metadata, storage, grouping_keys) -> list:
        """Ordered activity pairs (A, B) with an A before a B in some group."""
        from pyspark.sql.functions import min as F_min, max as F_max, count as F_count

        per_group_activity = (
            storage.read_sequence_table(metadata)
            .withColumn("group_value", _grouping_col(grouping_keys))
            .filter(col("group_value").isNotNull())
            .groupBy("group_value", "activity")
            .agg(
                F_min("start_timestamp").alias("first_ts"),
                F_max("start_timestamp").alias("last_ts"),
                F_count("*").alias("n"),
            )
        )
        a_side = per_group_activity.alias("a")
        b_side = per_group_activity.alias("b")
        rows = (
            a_side.join(b_side, col("a.group_value") == col("b.group_value"))
            .filter(
                ((col("a.activity") != col("b.activity"))
                 & (col("a.first_ts") < col("b.last_ts")))
                | ((col("a.activity") == col("b.activity")) & (col("a.n") > 1))
            )
            .select(col("a.activity").alias("source"), col("b.activity").alias("target"))
            .distinct()
            .collect()
        )
        return sorted((r.source, r.target) for r in rows)

    # ------------------------------------------------------------------
    # CLI
    # ------------------------------------------------------------------

    def cli_run(self, args: Any, **kwargs: Any) -> Any:
        logger.info(f"{self.name}: CLI run with args={args}.")

        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        parser = argparse.ArgumentParser(
            description="Siesta Adaptive Query module"
        )
        parser.add_argument(
            "--query_config", type=str, required=True,
            help="Path to configuration JSON file.",
        )
        parsed_args, _ = parser.parse_known_args(args)

        config_path = parsed_args.query_config
        if not Path(config_path).exists():
            raise FileNotFoundError(f"Config not found: {config_path}")

        with open(config_path) as f:
            config = json.load(f)

        method = config.get("method", "detection")
        self._bootstrap(config, method=method)

        if not self.query_config.get("grouping_keys"):
            return self._fallback_to_eager(method)

        if method == "detection":
            return self._run_adaptive_detection()
        elif method == "exploration":
            return self._run_adaptive_exploration()
        elif method == "statistics":
            return self._fallback_to_eager("statistics")
        else:
            raise ValueError(f"Unknown method: {method}")

    # ------------------------------------------------------------------
    # Eager fallback
    # ------------------------------------------------------------------

    def _fallback_to_eager(self, method: str) -> Any:
        """
        Delegate to the existing eager query processor.

        No adaptive machinery is involved.  This path exists so that a
        single deployment can serve both eager and adaptive queries.
        """
        logger.info(
            f"{self.name}: no grouping_keys — falling through to "
            f"eager {method}."
        )
        if method == "detection":
            return timed(
                eager_process_detection, "Eager Detection: ",
                self.query_config, self.metadata,
            )
        elif method == "exploration":
            return timed(
                eager_process_exploration, "Eager Exploration: ",
                self.query_config, self.metadata,
            )
        elif method == "statistics":
            return timed(
                eager_process_stats, "Eager Stats: ",
                self.query_config, self.metadata,
            )

    # ------------------------------------------------------------------
    # Adaptive detection
    # ------------------------------------------------------------------
    @staticmethod
    def _cep_is_redundant(
        pair_branches: set,
        info_pairs: set,
    ) -> bool:
        """
        Return True when the pruning step alone fully answers the pattern, so
        the downstream CEP pass would only re-prove what is already known.

        Conditions:
        1. Exactly one ResponsePair.
        2. Both quantifiers are ONE (no Kleene repetition).
        3. No forbidden-between activities (negation).
        4. No attribute constraints on either endpoint.
        5. No info-pairs (none are needed without constraints/negation).

        Under these conditions the smallest (source, target) STNM row of a
        group is exactly CEP's first match: the earliest source that has a
        later target, together with the first target after it.

        Attribute constraints are *not* allowed: the pairs index follows
        skip-till-next-match, so a valid attribute-satisfying match A -> B
        often has no (A, B) row of its own.  CEP recovers it from the
        (A, A) / (B, B) self-pair rows, which only the CEP path fetches.
        """
        if len(pair_branches) != 1:
            return False
        rp = next(iter(pair_branches))
        if rp.source_quantifier != SeqlQuantifier.ONE:
            return False
        if rp.target_quantifier != SeqlQuantifier.ONE:
            return False
        if rp.forbidden_between:
            return False
        if rp.source.constraints or rp.target.constraints:
            return False
        return not info_pairs

    def _fast_path_mode(self, pattern, pair_branches, info_pairs) -> str:
        """
        Decide how the pattern is answered.

        Returns:
          "single" — exactly one unconstrained responded pair; answered from
                     the pair rows directly (min position-pair per group).
          "cep"    — anything else: the general CEP path.

        A native multi-pair chain join is not used: stitching STNM pair rows
        on a shared position misses matches, because a source that falls
        before the previous pair's target is skipped (e.g. ``A B C`` on
        ``B A B C`` has no (B, C) row starting at the second B).
        """
        if len({rp.branch_id for rp in pair_branches}) > 1:
            return "cep"
        if self._cep_is_redundant(pair_branches, info_pairs):
            return "single"
        return "cep"

    def _run_adaptive_detection(self) -> Any:
        """
        Full adaptive detection pipeline:
        1. Resolve perspective and promote if needed
        2. Determine pair sources via planner
        3. Fetch pairs (L3 / L3- / lazy)
        4. Prune candidate groups
        5. Validate via CEP
        6. Record workload statistics
        """
        t_start = time.time()
        # Stage timings (seconds) and per-pair sources, reported in the
        # response for evaluation: which tier served each pair and where
        # the query time went.
        tm: Dict[str, float] = {}
        pair_sources: Dict[str, str] = {}
        status_before: Dict[str, str] = {}

        pattern = self.query_config.get("query", {}).get("pattern", "")
        grouping_keys = self.query_config["grouping_keys"]
        # "join": pair tables carry no attributes (embed_pair_attributes =
        # False); attributes are joined back from an event table.
        join_attrs = self.query_config.get("pair_attributes", "embedded") == "join"
        set_log_option(self.metadata, "embed_pair_attributes", not join_attrs)
        lookback = self.query_config.get("lookback", "7d")
        lookback_mode = self.query_config.get("lookback_mode", "time")
        support_threshold = self.query_config.get("support_threshold", 0.0)

        # --- Step 1: Resolve perspective --------------------------------
        catalog = get_catalog(self.metadata, self.storage)
        pid, stats = catalog.get_or_declare(
            grouping_keys=grouping_keys,
            lookback=lookback,
            lookback_mode=lookback_mode,
        )

        references_pos = self._pattern_references_pos(pattern)

        if stats.level < PerspectiveLevel.L1_POS_FREE:
            logger.info(f"{self.name}: promoting '{pid}' L0->L1.")
            t0 = time.time()
            promote_to_l1(pid, grouping_keys, self.metadata, self.storage)
            stats.l1_build_cost_ms = (time.time() - t0) * 1000
            catalog.promote(pid, PerspectiveLevel.L1_POS_FREE)

        if references_pos and stats.level < PerspectiveLevel.L2_POS_ESTABLISHED:
            logger.info(f"{self.name}: promoting '{pid}' L1->L2.")
            t0 = time.time()
            promote_to_l2(pid, grouping_keys, self.metadata, self.storage)
            stats.l2_build_cost_ms = (time.time() - t0) * 1000
            catalog.promote(pid, PerspectiveLevel.L2_POS_ESTABLISHED)

        has_pos = stats.level >= PerspectiveLevel.L2_POS_ESTABLISHED
        sort_key = "position" if (references_pos and has_pos) else "start_timestamp"
        tm["resolve"] = time.time() - t_start

        t0 = time.time()
        group_count = self._perspective_group_count(pid, grouping_keys)
        tm["group_count"] = time.time() - t0

        if can_match_single_event(pattern):
            # A single-event match involves no pair, so pair tables cannot
            # deliver it: hand CEP every event of the pattern's activities.
            t0 = time.time()
            result = self._detect_from_group_events(
                pattern, pid, grouping_keys, has_pos, sort_key,
            )
            tm["validate"] = time.time() - t0
            return self._finish_detection(
                catalog, pid, stats, result, set(), {},
                references_pos, support_threshold, group_count, t_start,
                tm, pair_sources, status_before,
            )

        # --- Step 2: Determine required pairs ---------------------------
        pair_branches = set(extract_responded_pairs(pattern))
        info_pairs = extract_info_pairs(pattern)
        all_responded = {
            (rp.source.label, rp.target.label) for rp in pair_branches
        }

        # Decide the execution mode (single-pair skip / multi-pair chain join
        # / CEP) up front so we fetch exactly the pairs each path needs.
        mode = self._fast_path_mode(pattern, pair_branches, info_pairs)
        if mode == "single":
            # An unconstrained single pair has no info-pairs to fetch.
            all_pairs_2d = set(all_responded)
        else:
            all_pairs_2d = all_responded | {(p[0], p[1]) for p in info_pairs}

        # --- Step 3: Fetch pairs (batched) -------------------------------
        # Statuses are resolved in-memory first; I/O is then issued per
        # TIER, not per pair:
        #   PERSISTENT -> per-pair Delta loads in a thread pool.  load()
        #                 is driver-side snapshot resolution (MinIO log
        #                 listing + replay, ~0.3-1 s each); serialising
        #                 C(n,2) of them dominated warm latency.  The
        #                 data scan itself stays lazy in the single
        #                 downstream union job.
        #   TRANSIENT  -> LRU, no Spark action (unchanged).
        #   ABSENT     -> ONE shared SequenceTable scan for all absent
        #                 pairs via build_pairs_transient_batched.
        # All per-pair bookkeeping (LRU put, promotion, build-cost
        # recording) is preserved.
        pair_dfs: list[DataFrame] = []
        lazy_costs: Dict[Tuple[str, str], float] = {}
        spark = get_spark_session()
        lru = get_lru_cache(self.metadata, self.query_config.get("lru_capacity"))
 
        persistent_pairs: list[Tuple[str, str]] = []
        absent_pairs:     list[Tuple[str, str]] = []
        # Rows of LRU hits and freshly scanned pairs: one local DataFrame
        # for all of them (a createDataFrame per pair cost ~1 s each).
        local_rows: list = []
 
        t0 = time.time()
        for (act_a, act_b) in all_pairs_2d:
            pair_status = catalog.get_pair_status(pid, act_a, act_b)
            key = f"{act_a}->{act_b}"
            status_before[key] = pair_status.name if pair_status is not None else "ABSENT"
 
            if pair_status == PairStatus.PERSISTENT:
                persistent_pairs.append((act_a, act_b))
            elif pair_status == PairStatus.TRANSIENT:
                cached = lru.get(pid, act_a, act_b)
                if cached is not None:
                    local_rows.extend(cached)
                    lazy_costs[(act_a, act_b)] = 0.0
                    pair_sources[key] = "LRU"
                else:
                    # Cache miss — treat as absent.
                    absent_pairs.append((act_a, act_b))
            else:
                absent_pairs.append((act_a, act_b))
        tm["fetch_lru"] = time.time() - t0
 
        # ── PERSISTENT: parallel Delta snapshot resolution ─────────────
        t0 = time.time()
        if persistent_pairs:
            from concurrent.futures import ThreadPoolExecutor
 
            def _load_pair(pair: Tuple[str, str]):
                a, b = pair
                path = _perspective_pair_path(self.metadata, pid, a, b)
                try:
                    return pair, pair_table_files(spark, path)
                except Exception:
                    logger.warning(
                        f"{self.name}: L3 read failed for "
                        f"({a},{b}), falling back to lazy."
                    )
                    return pair, None
 
            n_workers = min(16, len(persistent_pairs))
            delta_files: list = []
            with ThreadPoolExecutor(max_workers=n_workers) as ex:
                for pair, files in ex.map(_load_pair, persistent_pairs):
                    if files is not None:
                        delta_files.extend(files)
                        lazy_costs[pair] = 0.0
                        pair_sources[f"{pair[0]}->{pair[1]}"] = "DELTA"
                    else:
                        absent_pairs.append(pair)
            if delta_files:
                # ONE Parquet scan over the live files of all persisted pair
                # tables: a union of per-table reads schedules a task per
                # file of every table.  Pair tables are append / overwrite
                # only (no deletion vectors), so the snapshot's files are
                # exactly its rows.
                pair_dfs.append(
                    spark.read.schema(EventPair.get_schema()).parquet(*delta_files)
                )
        tm["fetch_delta"] = time.time() - t0
 
        # ── ABSENT: one shared scan for all missing pairs ──────────────
        tm["scan"] = 0.0
        tm["catalog_writes"] = 0.0
        if absent_pairs:
            t0 = time.time()
            rows_by_pair = build_pairs_transient_batched(
                pid=pid,
                pairs=absent_pairs,
                lookback=lookback,
                lookback_mode=lookback_mode,
                candidate_group_ids=[],  # full scan
                grouping_keys=grouping_keys,
                metadata=self.metadata,
                storage=self.storage,
                has_pos=has_pos,
            )
            # All absent pairs shared one scan; attribute the amortised
            # per-pair share so the retention policy sees the true
            # per-pair price under batching.
            tm["scan"] = time.time() - t0
            shared_cost_ms = tm["scan"] * 1000 / max(1, len(absent_pairs))
            t0 = time.time()
            cached_pairs = []
            for (act_a, act_b) in absent_pairs:
                pair_sources[f"{act_a}->{act_b}"] = "SCAN"
                lazy_costs[(act_a, act_b)] = shared_cost_ms
                catalog.record_pair_build_cost(
                    pid, act_a, act_b, shared_cost_ms
                )
                collected = rows_by_pair.get((act_a, act_b), [])
                try:
                    lru.put(pid, act_a, act_b, collected)
                    cached_pairs.append((act_a, act_b))
                except Exception as exc:
                    logger.warning(
                        f"{self.name}: failed to cache transient pair "
                        f"({act_a},{act_b}): {exc}"
                    )
                local_rows.extend(collected)
            # One write-through for all newly cached pairs.  A pair that is
            # already PERSISTENT (its Delta read failed) keeps its status.
            catalog.promote_pairs(
                pid,
                [
                    p for p in cached_pairs
                    if catalog.get_pair_status(pid, *p) != PairStatus.PERSISTENT
                ],
                PairStatus.TRANSIENT,
                write_through=False,
            )
            tm["catalog_writes"] = time.time() - t0
 
        if local_rows:
            pair_dfs.append(spark.createDataFrame(local_rows, schema=EventPair.get_schema()))

        t_validate = time.time()
        if not pair_dfs:
            # No pair has an instance in any group: nothing can match.  The
            # query still counts towards the workload statistics.
            tm["validate"] = 0.0
            return self._finish_detection(
                catalog, pid, stats, [], all_pairs_2d, lazy_costs,
                references_pos, support_threshold, group_count, t_start,
                tm, pair_sources, status_before,
            )

        # Union all pair DataFrames
        from functools import reduce
        index_df = reduce(DataFrame.unionByName, pair_dfs)
        logger.info(f"TIMING pair_fetch done: {time.time() - t_start:.2f}s  pattern={pattern!r}")

        # --- Step 4: Prune candidate groups -----------------------------
        # Pruning must see the unfiltered pair rows: the pairs index follows
        # skip-till-next-match, so an attribute-satisfying match may have no
        # (A, B) row of its own (see build_event_keep_predicate).
        branch_required: dict[int, set[tuple[str, str]]] = {}
        for rp in pair_branches:
            if (
                rp.source_quantifier == SeqlQuantifier.STAR
                or rp.target_quantifier == SeqlQuantifier.STAR
            ):
                continue
            branch_required.setdefault(rp.branch_id, set()).add(
                (rp.source.label, rp.target.label)
            )

        branch_pruned_dfs = []
        for bid, required_2d in branch_required.items():
            if not required_2d:
                continue
            branch_pred = build_exact_pair_predicate(required_2d)
            branch_pruned = (
                index_df
                .where(branch_pred)
                .dropDuplicates(["trace_id", "source", "target"])
                .groupBy("trace_id")
                .agg(F.count("*").alias("pair_count"))
                .filter(col("pair_count") == len(required_2d))
                .select("trace_id")
            )
            branch_pruned_dfs.append(branch_pruned)

        if not branch_pruned_dfs:
            pruned_group_ids = index_df.select("trace_id").distinct()
        elif len(branch_pruned_dfs) == 1:
            pruned_group_ids = branch_pruned_dfs[0].distinct()
        else:
            pruned_group_ids = reduce(
                lambda a, b: a.union(b), branch_pruned_dfs
            ).distinct()

        # pruned_count = pruned_group_ids.count()
        # logger.info(f"TIMING prune={time.time()-t_start:.2f}s  pruned_groups={pruned_count}")

        # Attribute pushdown for CEP: drop rows whose endpoints can never be
        # part of a match.  Applied after pruning, never to the prune input.
        rows_df = index_df
        if join_attrs:
            t0 = time.time()
            # LRU / scanned rows still carry their maps in memory; this
            # variant must not use them.
            rows_df = strip_pair_attributes(index_df)
            if has_attribute_constraints(pattern):
                rows_df = join_back_attributes(
                    rows_df.join(pruned_group_ids, on="trace_id", how="left_semi"),
                    self._attribute_events(pid, grouping_keys, has_pos, pattern, pruned_group_ids),
                )
            tm["join_back_plan"] = time.time() - t0

        keep_pred = build_event_keep_predicate(pattern) if mode == "cep" else None
        cep_rows_df = rows_df.where(keep_pred) if keep_pred is not None else rows_df

        pair_positions_df = (
            cep_rows_df
            .join(pruned_group_ids, on="trace_id", how="inner")
            .repartition("trace_id")
        )

        # after .rdd.map (just inspect the count):
        # positions_count = pair_positions_df.count()
        # logger.info(f"TIMING positions_built={time.time()-t_start:.2f}s  row_count={positions_count}")

        # # --- Step 5: Validate via CEP -----------------------------------
        # # Build per-group pseudo-sequences and run OpenCEP, same as the
        # # eager module but sorting by sort_key (ts or pos).
        # matches_rdd = pair_positions_df.rdd.map(
        #     lambda r: (
        #         r.trace_id,
        #         {
        #             "source":             r.source,
        #             "target":             r.target,
        #             "source_position":    r.source_position,
        #             "target_position":    r.target_position,
        #             "source_timestamp":   r.source_timestamp,
        #             "target_timestamp":   r.target_timestamp,
        #             "source_attributes":  r.source_attributes,
        #             "target_attributes":  r.target_attributes,
        #         },
        #     )
        # )

        # sort_field = (
        #     "position" if sort_key == "position" else "timestamp"
        # )

        # def validate_group(group_id_rows):
        #     group_id, rows = group_id_rows
        #     rows = list(rows)

        #     seen_positions = {}
        #     for r in rows:
        #         for side in [
        #             ("source", "source_position", "source_timestamp", "source_attributes"),
        #             ("target", "target_position", "target_timestamp", "target_attributes"),
        #         ]:
        #             name_k, pos_k, ts_k, attr_k = side
        #             pos = r[pos_k]
        #             if pos not in seen_positions:
        #                 seen_positions[pos] = {
        #                     "name":      r[name_k],
        #                     "position":  pos,
        #                     "timestamp": r[ts_k],
        #                 }
        #                 attrs = r[attr_k]
        #                 if attrs:
        #                     for key, value in attrs.items():
        #                         seen_positions[pos][key] = value

        #     events = sorted(
        #         seen_positions.values(),
        #         key=lambda e: (int(e[sort_field]), e.get("name", "")),
        #     )

        #     positions = find_occurrences_dsl(
        #         [e["name"] for e in events],
        #         pattern,
        #         events=events,
        #     )
        #     return (group_id, positions)

        # group_count = (
        #     self.metadata.trace_count
        #     if self.metadata.trace_count
        #     else 0
        # )

        # result = (
        #     matches_rdd
        #     .groupByKey()
        #     .map(validate_group)
        #     .filter(
        #         lambda r: len(r[1]) >= support_threshold * group_count
        #         if group_count
        #         else True
        #     )
        #     .collect()
        # )
        
        # --- Step 5: Validate ------------------------------------------
        # When the prune step provably establishes the answer (single
        # ordered pair, no constraints, no negation, no quantifier
        # repetition) we skip CEP entirely.  Pruning has already kept
        # exactly the groups that contain a (source, target)
        # co-occurrence in the right order, and each surviving row
        # carries the matching positions directly.
        sort_field = "position" if sort_key == "position" else "timestamp"

        if mode == "single":
            rp = next(iter(pair_branches))
            # Restrict to the rows that actually match the pair label
            # (the union upstream may have included extra info-pair
            # rows in principle; here info_pairs is empty so this is
            # a no-op when there is only one pair table, but the
            # filter stays for safety).
            matching_rows = pair_positions_df.where(
                (col("source") == rp.source.label)
                & (col("target") == rp.target.label)
            )


            # Match the CEP path's result shape: (group_id, positions) with
            # the positions of the first match, i.e. the smallest
            # (source, target) row of the group.
            per_group = (
                matching_rows
                .withColumn(
                    "pos_pair",
                    F.array(col("source_position"), col("target_position")),
                )
                .groupBy("trace_id")
                .agg(F.min(col("pos_pair")).alias("positions"))
            )

            collected = per_group.collect()

            result = [(row.trace_id, list(row.positions)) for row in collected]


        else:
            # General CEP path: quantifiers, negation, attribute constraints,
            # multiple pairs requiring cross-pair ordering, etc.
            matches_rdd = pair_positions_df.rdd.map(
                lambda r: (
                    r.trace_id,
                    {
                        "source":             r.source,
                        "target":             r.target,
                        "source_position":    r.source_position,
                        "target_position":    r.target_position,
                        "source_timestamp":   r.source_timestamp,
                        "target_timestamp":   r.target_timestamp,
                        "source_attributes":  r.source_attributes,
                        "target_attributes":  r.target_attributes,
                    },
                )
            )

            def validate_group(group_id_rows):
                group_id, rows = group_id_rows
                rows = list(rows)

                seen_positions = {}
                for r in rows:
                    for side in [
                        ("source", "source_position", "source_timestamp", "source_attributes"),
                        ("target", "target_position", "target_timestamp", "target_attributes"),
                    ]:
                        name_k, pos_k, ts_k, attr_k = side
                        pos = r[pos_k]
                        if pos not in seen_positions:
                            seen_positions[pos] = {
                                "name":      r[name_k],
                                "position":  pos,
                                "timestamp": r[ts_k],
                            }
                            attrs = r[attr_k]
                            if attrs:
                                for key, value in attrs.items():
                                    seen_positions[pos][key] = value

                # Timestamp ties keep the group order, as in the eager engine.
                events = sorted(
                    seen_positions.values(),
                    key=lambda e: (int(e[sort_field]), int(e["position"])),
                )

                positions = find_occurrences_dsl(
                    [e["name"] for e in events],
                    pattern,
                    events=events,
                )
                # CEP returns indices into `events`; report their positions,
                # as the eager engine does.
                return (group_id, [int(events[i]["position"]) for i in positions])

            result = (
                matches_rdd
                .groupByKey()
                .map(validate_group)
                .filter(lambda r: len(r[1]) > 0)
                .collect()
            )
        logger.info(f"TIMING cep_done: {time.time() - t_start:.2f}s  pattern={pattern!r}")
        tm["validate"] = time.time() - t_validate

        return self._finish_detection(
            catalog, pid, stats, result, all_pairs_2d, lazy_costs,
            references_pos, support_threshold, group_count, t_start,
            tm, pair_sources, status_before,
        )

    def _attribute_events(self, pid, grouping_keys, has_pos, pattern, group_ids_df) -> DataFrame:
        """
        (trace_id, position, attributes) of the pattern's activities, with
        the positions the perspective's pair tables use.  Under the case
        perspective group positions are trace positions, so the Activity
        index (partitioned by activity) serves them.
        """
        labels = sorted(pattern_labels(pattern))
        if list(grouping_keys) == ["trace_id"]:
            ev = self.storage.read_activity_events(self.metadata, labels)
            return ev.join(group_ids_df, on="trace_id", how="left_semi")
        return _get_perspective_seq_df(
            pid=pid,
            grouping_keys=grouping_keys,
            metadata=self.metadata,
            storage=self.storage,
            has_pos=has_pos,
            group_ids_df=group_ids_df,
        ).where(col("activity").isin(labels))

    def _detect_from_group_events(self, pattern, pid, grouping_keys, has_pos, sort_key):
        """
        Run CEP over each group's events of the pattern's activities, read
        from the perspective's sequence (group positions as in pair tables).
        """
        sort_field = "position" if sort_key == "position" else "timestamp"
        seq_df = _get_perspective_seq_df(
            pid=pid,
            grouping_keys=grouping_keys,
            metadata=self.metadata,
            storage=self.storage,
            has_pos=has_pos,
        ).where(col("activity").isin(sorted(pattern_labels(pattern))))

        def to_event(r):
            event = {"name": r.activity, "position": r.position, "timestamp": r.start_timestamp}
            if r.attributes:
                event.update(r.attributes)
            return r.trace_id, event

        def validate_group(group_id_events):
            group_id, events = group_id_events
            events = sorted(events, key=lambda e: (int(e[sort_field]), int(e["position"])))
            positions = find_occurrences_dsl([e["name"] for e in events], pattern, events=events)
            return (group_id, [int(events[i]["position"]) for i in positions])

        return (
            seq_df.rdd.map(to_event)
            .groupByKey()
            .map(validate_group)
            .filter(lambda r: len(r[1]) > 0)
            .collect()
        )

    def _perspective_group_count(self, pid, grouping_keys) -> int:
        """
        Number of groups of the perspective: the denominator of a pattern's
        support.  Cached per (log, perspective, event_count), so a new ingest
        recounts.  The trace-level perspective uses ``trace_count`` when the
        log's metadata has it (the adaptive indexer does not maintain it).
        """
        if list(grouping_keys) == ["trace_id"] and self.metadata.trace_count:
            return self.metadata.trace_count
        key = (self.metadata.storage_namespace, self.metadata.log_name, pid,
               getattr(self.metadata, "event_count", None))
        if key not in GROUP_COUNTS:
            GROUP_COUNTS[key] = (
                self.storage.read_sequence_table(self.metadata)
                .select(_grouping_col(grouping_keys).alias("group_value"))
                .where(col("group_value").isNotNull())
                .distinct()
                .count()
            )
        return GROUP_COUNTS[key]

    def _finish_detection(self, catalog, pid, stats, result, all_pairs_2d, lazy_costs,
                          references_pos, support_threshold, group_count, t_start,
                          tm=None, pair_sources=None, status_before=None):
        """
        Step 6 (workload statistics, promotion) and response formatting.

        ``time`` is the query latency: everything up to and including the
        workload-statistics update.  The promotion it may trigger runs on the
        perspective's background worker.  With ``wait_promotion`` in the
        request the response waits for it and reports ``pair_status_after``;
        that wait is not part of ``time``.
        """
        tm = tm if tm is not None else {}
        t0 = time.time()
        retention = self._get_retention()

        # --- Step 6: Record workload statistics -------------------------
        catalog.record_query_touch(
            pid=pid,
            pairs_touched=list(all_pairs_2d),
            references_pos=references_pos,
            total_query_ms=(time.time() - t_start) * 1000,
            pair_savings_ms={
                (a, b): (
                    stats.pairs.get((a, b), PairStats()).build_cost_ms
                    - cost
                )
                for (a, b), cost in lazy_costs.items()
                if cost == 0.0
                and stats.pairs.get((a, b), PairStats()).build_cost_ms > 0
            },
            decay=retention,
        )
        tm["finish"] = time.time() - t0
        t_total = time.time() - t_start

        # Promotion (and the catalog flush) run on the perspective's worker
        # so they do not block the response.
        metadata = self.metadata
        pairs_touched = list(all_pairs_2d)

        def _promote_and_flush():
            try:
                self._maybe_promote_after_query(
                    pid, pairs_touched, references_pos, metadata, retention,
                )
            finally:
                catalog.flush()

        submit_promotion(metadata, pid, _promote_and_flush)

        pair_status_after = None
        promotion_s = None
        if self.query_config.get("wait_promotion", False):
            # The wait is the promotion's own cost (pair builds + catalog
            # flush); it is reported separately and is not part of ``time``.
            promotion_s = drain_promotions(metadata, pid)
            post_stats = catalog.get(pid)
            pair_status_after = {
                f"{a}->{b}": (
                    post_stats.pairs[(a, b)].status.name
                    if post_stats is not None and (a, b) in post_stats.pairs
                    else "ABSENT"
                )
                for (a, b) in pairs_touched
            }


        # Support is the fraction of the perspective's groups that match the
        # pattern; below the threshold the pattern counts as not detected.
        support = len(result) / group_count if group_count else 0.0
        formatted = [] if support < support_threshold else [
            {"group_id": gid, "support": support, "positions": positions}
            for gid, positions in result
        ]

        response = {
            "code": 200,
            "perspective": pid,
            "total": len(formatted),
            "support": support,
            "matched_groups": len(result),
            "group_count": group_count,
            "detected": formatted,
            "time": t_total,
            "timings": tm,
            "pair_sources": pair_sources or {},
            "pair_status_before": status_before or {},
        }
        if pair_status_after is not None:
            response["pair_status_after"] = pair_status_after
            response["promotion_s"] = promotion_s
        return response

    # ------------------------------------------------------------------
    # Adaptive exploration
    # ------------------------------------------------------------------

    def _run_adaptive_exploration(self) -> Any:
        """
        Adaptive exploration: find activity continuations under a
        custom grouping perspective.

        Reuses _run_adaptive_detection for each candidate continuation,
        mirroring Algorithm 7 from the paper.
        """
        t_start = time.time()

        pattern = self.query_config.get("query", {}).get("pattern", "")
        mode = self.query_config.get("query", {}).get(
            "explore_mode", "accurate"
        )
        support_threshold = self.query_config.get("support_threshold", 0.0)
        grouping_keys = self.query_config["grouping_keys"]
        lookback = self.query_config.get("lookback", "7d")
        lookback_mode = self.query_config.get("lookback_mode", "time")

        # Determine the last activity in the pattern to find continuations.
        pattern_data = split_pattern_to_list(pattern)
        activities = [x.get("label") for x in pattern_data]
        pattern_suffix = activities[-1] if activities else ""

        if not pattern_suffix:
            return {
                "code": 400,
                "error": "Cannot explore: empty pattern.",
            }

        # Find candidate continuations from the PairsIndex catalog.
        catalog = get_catalog(self.metadata, self.storage)
        pid, stats = catalog.get_or_declare(
            grouping_keys=grouping_keys,
            lookback=lookback,
            lookback_mode=lookback_mode,
        )

        # Ensure at least L1 so we can read the sequence table.
        if stats.level < PerspectiveLevel.L1_POS_FREE:
            t0 = time.time()
            promote_to_l1(pid, grouping_keys, self.metadata, self.storage)
            stats.l1_build_cost_ms = (time.time() - t0) * 1000
            catalog.promote(pid, PerspectiveLevel.L1_POS_FREE)

        # Read the per-perspective PairsIndex to find which activities
        # follow pattern_suffix.  Check both persisted pairs and the
        # activity index for candidate targets.
        spark = get_spark_session()
        has_pos = stats.level >= PerspectiveLevel.L2_POS_ESTABLISHED

        try:
            seq_df = _get_perspective_seq_df(
                pid=pid,
                grouping_keys=grouping_keys,
                metadata=self.metadata,
                storage=self.storage,
                has_pos=has_pos,
            )
        except Exception:
            return {
                "code": 500,
                "error": (
                    f"Sequence data for perspective '{pid}' not available."
                ),
            }

        # Candidate targets: all activities that appear in this perspective's groups.
        candidates = (
            seq_df
            .filter(col("activity") != pattern_suffix)
            .select("activity")
            .distinct()
            .rdd
            .map(lambda r: r.activity)
            .collect()
        )

        if not candidates:
            return {
                "code": 200,
                "perspective": pid,
                "explored": [],
                "time": time.time() - t_start,
            }

        # For each candidate, run a detection query on the extended
        # pattern and compute the probability.
        propositions = []
        for target in candidates:
            extended_pattern = f"{pattern} {target}"

            # Temporarily override the config pattern
            saved_pattern = self.query_config.get("query", {}).get("pattern")
            self.query_config.setdefault("query", {})["pattern"] = extended_pattern

            try:
                result = self._run_adaptive_detection()
                support = result.get("support", 0.0)
            except Exception as exc:
                logger.warning(
                    f"{self.name}: exploration detection failed for "
                    f"'{target}': {exc}"
                )
                support = 0.0
            finally:
                self.query_config["query"]["pattern"] = saved_pattern

            if support >= support_threshold:
                propositions.append({
                    "next_activity": target,
                    "support": support,
                })

        propositions.sort(key=lambda p: p["support"], reverse=True)

        return {
            "code": 200,
            "perspective": pid,
            "explored": propositions,
            "time": time.time() - t_start,
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _get_retention(self) -> RetentionPolicy:
        """
        The retention policy for the current request's overrides
        (half_life_seconds / min_query_count / hysteresis / cost_scale).  Rebuilt
        whenever they change; RetentionPolicy is stateless, so this is free.
        """
        params = (
            float(self.query_config.get("half_life_seconds", 3600.0)),
            int(self.query_config.get("min_query_count", 3)),
            float(self.query_config.get("hysteresis", 0.15)),
            float(self.query_config.get("cost_scale", 1.0)),
        )
        if self._retention is None or self._retention_params != params:
            self._retention = RetentionPolicy(
                half_life_seconds=params[0],
                min_query_count=params[1],
                hysteresis=params[2],
                cost_scale=params[3],
            )
            self._retention_params = params
        return self._retention

    def _maybe_promote_after_query(self, pid, pairs_touched, references_pos,
                                   metadata, retention):
        """
        Lightweight per-pair retention check after each query.

        Only evaluates the predicates for artefacts this query touched.
        Full sweep over all perspectives still happens at ingest time.
        Runs on the perspective's promotion worker, so it takes the
        metadata and retention policy of the query that submitted it
        rather than reading the (shared, mutable) module state.
        """
        catalog = get_catalog(metadata, self.storage)
        stats = catalog.get(pid)
        if stats is None:
            return

        # L1 promotion is already synchronous on first touch; nothing to do here.
        # L2 promotion can fire here if pos queries cross threshold.
        if (references_pos
            and stats.level == PerspectiveLevel.L1_POS_FREE
            and retention.should_promote_l2(stats)):
            try:
                t0 = time.time()
                promote_to_l2(pid, stats.grouping_keys, metadata, self.storage)
                stats.l2_build_cost_ms = (time.time() - t0) * 1000
                catalog.promote(pid, PerspectiveLevel.L2_POS_ESTABLISHED)
            except Exception as exc:
                logger.error(f"{self.name}: synchronous L2 promotion failed: {exc}")

        # L3 promotion of the touched pairs that pass the retention gate:
        # one batched build (one scan, one grouped extraction) and one
        # catalog write for all of them.
        to_persist = []
        for (a, b) in pairs_touched:
            ps = stats.pairs.get((a, b))
            if ps is None or ps.status == PairStatus.PERSISTENT:
                continue
            if retention.should_persist_pair(ps):
                to_persist.append((a, b))
        if to_persist:
            try:
                from siesta.modules.adaptive_index.builders import build_pairs_persistent_batched
                costs = build_pairs_persistent_batched(
                    pid=pid, pairs=to_persist,
                    lookback=stats.lookback, lookback_mode=stats.lookback_mode,
                    grouping_keys=stats.grouping_keys,
                    metadata=metadata, storage=self.storage,
                    has_pos=stats.level >= PerspectiveLevel.L2_POS_ESTABLISHED,
                )
                for pair, ms in costs.items():
                    stats.pairs[pair].build_cost_ms = ms
                catalog.promote_pairs(pid, to_persist, PairStatus.PERSISTENT)
                # Resolve the new tables' snapshots here, off the query path.
                spark = get_spark_session()
                for (a, b) in to_persist:
                    pair_table_files(spark, _perspective_pair_path(metadata, pid, a, b))
            except Exception as exc:
                logger.error(
                    f"{self.name}: L3 promotion failed for {to_persist}: {exc}"
                )
    def _get_lru(self) -> PairLRUCache:
        return get_lru_cache(self.metadata)

    def _bootstrap(
        self, config: Dict[str, Any], method: str
    ) -> None:
        """
        Validate config, initialise storage and metadata.

        Called at the top of every API endpoint and CLI run.
        """
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        config["method"] = method
        if config.get("log_name") is None:
            raise ValueError("log_name not specified in config.")
        if config.get("query", {}).get("pattern") is None:
            raise ValueError("query.pattern not specified in config.")

        self.query_config = DEFAULT_ADAPTIVE_QUERY_CONFIG.copy()
        self.query_config.update(config)

        self.metadata = MetaData(
            storage_namespace=self.query_config.get(
                "storage_namespace", "siesta"
            ),
            log_name=self.query_config.get("log_name", "default_log"),
            storage_type=self.query_config.get("storage_type", "s3"),
        )
        self.metadata = self.storage.read_metadata_table(self.metadata)

    @staticmethod
    def _pattern_references_pos(pattern: str) -> bool:
        """
        Return True if the pattern contains any positional constraint
        (pos=... syntax).

        This is the signal that drives L2 promotion and the sort-key
        decision.  A simple string check suffices because the parser
        uses [pos=...] syntax exclusively.
        """
        return "pos=" in pattern

    def _get_pair_status(
        self, pid: str, act_a: str, act_b: str
    ) -> PairStatus | None:
        """
        Return the effective pair status, accounting for LRU cache.

        A TRANSIENT pair with no LRU entry is effectively ABSENT
        for query purposes (needs lazy rebuild).
        """
        catalog = get_catalog(self.metadata, self.storage)
        status = catalog.get_pair_status(pid, act_a, act_b)
        if status == PairStatus.TRANSIENT:
            if not get_lru_cache(self.metadata).contains(pid, act_a, act_b):
                return PairStatus.ABSENT
        return status