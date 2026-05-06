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
    promote_to_l1,
    promote_to_l2,
    _perspective_pair_path,
)
from siesta.modules.adaptive_index.catalog import get_catalog
from siesta.modules.adaptive_index.retention import RetentionPolicy
from siesta.modules.adaptive_query.lru_cache import PairLRUCache, get_lru_cache
from siesta.modules.query.parse_seql import (
    extract_info_pairs,
    extract_responded_pairs,
    split_pattern_to_list,
    Quantifier as SeqlQuantifier,
)
from siesta.modules.query.processors.detection_query import (
    build_exact_pair_predicate,
    detect as eager_detect,
    process_detection_query as eager_process_detection,
)
from siesta.modules.query.processors.exploration_query import (
    process_exploration_query as eager_process_exploration,
)
from siesta.modules.query.processors.stats_query import (
    process_stats_query as eager_process_stats,
)
from siesta.modules.query.CEP_adapter import find_occurrences_dsl
from siesta.model.DataModel import EventPair

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

    def __init__(self):
        super().__init__()
        self.query_config = {}
        self._retention = None

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

        pattern = self.query_config.get("query", {}).get("pattern", "")
        grouping_keys = self.query_config["grouping_keys"]
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

        # --- Step 2: Determine required pairs ---------------------------
        pair_branches = set(extract_responded_pairs(pattern))
        info_pairs = extract_info_pairs(pattern)
        all_responded = {
            (rp.source.label, rp.target.label) for rp in pair_branches
        }
        all_pairs_2d = all_responded | {(p[0], p[1]) for p in info_pairs}

        # --- Step 3: Fetch pairs per source -----------------------------
        pair_dfs: list[DataFrame] = []
        lazy_costs: Dict[Tuple[str, str], float] = {}
        spark = get_spark_session()

        for (act_a, act_b) in all_pairs_2d:
            pair_status = catalog.get_pair_status(pid, act_a, act_b)

            if pair_status == PairStatus.PERSISTENT:
                # L3: read from per-pair Delta table
                pair_path = _perspective_pair_path(
                    self.metadata, pid, act_a, act_b
                )
                try:
                    df = spark.read.format("delta").load(pair_path)
                    pair_dfs.append(df)
                    lazy_costs[(act_a, act_b)] = 0.0
                except Exception:
                    logger.warning(
                        f"{self.name}: L3 read failed for "
                        f"({act_a},{act_b}), falling back to lazy."
                    )
                    pair_status = PairStatus.ABSENT

            if pair_status == PairStatus.TRANSIENT:
                # L3-: check LRU cache
                cached = get_lru_cache(self.metadata).get(pid, act_a, act_b)
                if cached is not None:
                    df = spark.createDataFrame(
                        cached, schema=EventPair.get_schema()
                    )
                    pair_dfs.append(df)
                    lazy_costs[(act_a, act_b)] = 0.0
                    continue
                else:
                    # Cache miss — treat as absent
                    pair_status = PairStatus.ABSENT

            if pair_status == PairStatus.ABSENT:
                # Lazy scan: build transiently
                t0 = time.time()
                df = build_pair_transient(
                    pid=pid,
                    act_a=act_a,
                    act_b=act_b,
                    lookback=lookback,
                    lookback_mode=lookback_mode,
                    candidate_group_ids=[],  # full scan
                    metadata=self.metadata,
                    storage=self.storage,
                    has_pos=has_pos,
                )
                cost_ms = (time.time() - t0) * 1000
                lazy_costs[(act_a, act_b)] = cost_ms

                # Record cold-start build cost
                catalog.record_pair_build_cost(
                    pid, act_a, act_b, cost_ms
                )

                # Cache in LRU for future queries
                try:
                    collected = df.collect()
                    get_lru_cache(self.metadata).put(pid, act_a, act_b, collected)
                    catalog.promote_pair(pid, act_a, act_b, PairStatus.TRANSIENT)
                except Exception as exc:
                    logger.warning(
                        f"{self.name}: failed to cache transient pair "
                        f"({act_a},{act_b}): {exc}"
                    )

                pair_dfs.append(df)

        if not pair_dfs:
            return {
                "code": 200,
                "total": 0,
                "detected": [],
                "time": time.time() - t_start,
            }

        # Union all pair DataFrames
        from functools import reduce
        index_df = reduce(DataFrame.unionByName, pair_dfs)

        # --- Step 4: Prune candidate groups -----------------------------
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

        pair_positions_df = (
            index_df
            .join(pruned_group_ids, on="trace_id", how="inner")
            .repartition("trace_id")
        )

        # --- Step 5: Validate via CEP -----------------------------------
        # Build per-group pseudo-sequences and run OpenCEP, same as the
        # eager module but sorting by sort_key (ts or pos).
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

        sort_field = (
            "position" if sort_key == "position" else "timestamp"
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

            events = sorted(
                seen_positions.values(),
                key=lambda e: int(e[sort_field]),
            )

            positions = find_occurrences_dsl(
                [e["name"] for e in events],
                pattern,
                events=events,
            )
            return (group_id, positions)

        group_count = (
            self.metadata.trace_count
            if self.metadata.trace_count
            else 0
        )

        result = (
            matches_rdd
            .groupByKey()
            .map(validate_group)
            .filter(
                lambda r: len(r[1]) >= support_threshold * group_count
                if group_count
                else True
            )
            .collect()
        )

        t_total = time.time() - t_start

        # --- Step 6: Record workload statistics -------------------------
        catalog.record_query_touch(
            pid=pid,
            pairs_touched=list(all_pairs_2d),
            references_pos=references_pos,
            total_query_ms=t_total * 1000,
            pair_savings_ms={
                (a, b): (
                    stats.pairs.get((a, b), PairStats()).build_cost_ms
                    - cost
                )
                for (a, b), cost in lazy_costs.items()
                if cost == 0.0
                and stats.pairs.get((a, b), PairStats()).build_cost_ms > 0
            },
        )
        self._maybe_promote_after_query(pid, all_pairs_2d, references_pos)

        catalog.flush()

        formatted = [
            {
                "group_id": gid,
                "support": (
                    len(positions) / group_count if group_count else 0
                ),
                "positions": positions,
            }
            for gid, positions in result
            if len(positions) >= support_threshold
        ]

        return {
            "code": 200,
            "perspective": pid,
            "total": len(formatted),
            "detected": formatted,
            "time": t_total,
        }

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
        from siesta.modules.adaptive_index.builders import (
            _perspective_sequence_path,
        )

        seq_path = _perspective_sequence_path(self.metadata, pid)
        try:
            seq_df = spark.read.format("delta").load(seq_path)
        except Exception:
            return {
                "code": 500,
                "error": (
                    f"Sequence table for perspective '{pid}' not found."
                ),
            }

        # Candidate targets: all activities that appear after
        # pattern_suffix in any group.
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
                count = result.get("total", 0)
            except Exception as exc:
                logger.warning(
                    f"{self.name}: exploration detection failed for "
                    f"'{target}': {exc}"
                )
                count = 0
            finally:
                self.query_config["query"]["pattern"] = saved_pattern

            group_count = (
                self.metadata.trace_count
                if self.metadata.trace_count
                else 1
            )
            support = count / group_count if group_count else 0.0
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
    def _maybe_promote_after_query(self, pid, pairs_touched, references_pos):
        """
        Lightweight per-pair retention check after each query.

        Only evaluates the predicates for artefacts this query touched.
        Full sweep over all perspectives still happens at ingest time.
        """
        if self._retention is None:
            from siesta.modules.adaptive_index.retention import RetentionPolicy
            self._retention = RetentionPolicy(
                half_life_seconds=self.query_config.get("half_life_seconds", 3600.0),
            )

        catalog = get_catalog(self.metadata, self.storage)
        stats = catalog.get(pid)
        if stats is None:
            return

        # L1 promotion is already synchronous on first touch; nothing to do here.
        # L2 promotion can fire here if pos queries cross threshold.
        if (references_pos
            and stats.level == PerspectiveLevel.L1_POS_FREE
            and self._retention.should_promote_l2(stats)):
            try:
                t0 = time.time()
                promote_to_l2(pid, stats.grouping_keys, self.metadata, self.storage)
                stats.l2_build_cost_ms = (time.time() - t0) * 1000
                catalog.promote(pid, PerspectiveLevel.L2_POS_ESTABLISHED)
            except Exception as exc:
                logger.error(f"{self.name}: synchronous L2 promotion failed: {exc}")

        # Per-pair L3 promotion.
        for (a, b) in pairs_touched:
            ps = stats.pairs.get((a, b))
            if ps is None or ps.status == PairStatus.PERSISTENT:
                continue
            if self._retention.should_persist_pair(ps):
                try:
                    from siesta.modules.adaptive_index.builders import build_pair_persistent
                    t0 = time.time()
                    has_pos = stats.level >= PerspectiveLevel.L2_POS_ESTABLISHED
                    build_pair_persistent(
                        pid=pid, act_a=a, act_b=b, lookback=stats.lookback, lookback_mode=stats.lookback_mode,
                        metadata=self.metadata, storage=self.storage,
                        has_pos=has_pos,
                    )
                    ps.build_cost_ms = (time.time() - t0) * 1000
                    catalog.promote_pair(pid, a, b, PairStatus.PERSISTENT)
                except Exception as exc:
                    logger.error(
                        f"{self.name}: synchronous L3 promotion failed for "
                        f"({a},{b}): {exc}"
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