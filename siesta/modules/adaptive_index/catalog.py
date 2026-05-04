"""
siesta/modules/adaptive_index/catalog.py

The PerspectiveCatalog is the single source of truth for the lifecycle
state of every perspective and every pair index derived from a given
event log.  It is designed around three principles:

1.  Process-wide singleton per (storage_namespace, log_name).
    All components that need a catalog for the same log — the adaptive
    indexer, the query planner, any future analysis module — call
    get_catalog() and receive the same object.  There is never more
    than one PerspectiveCatalog per log per process.

2.  Delta-backed durability with an in-memory write-back cache.
    The catalog is loaded from a small Delta table on first access and
    kept entirely in memory thereafter.  Mutations fall into two tiers:
      - Write-through: perspective declarations and level promotions
        are written to Delta immediately because they are infrequent
        and their loss across a restart would be expensive to recover.
      - Write-back (dirty marking): per-query workload statistics and
        per-batch maintenance costs are accumulated in memory and
        flushed to Delta at explicit flush() call sites (end of each
        ingest batch, end of each query in the query planner).
        Losing a handful of query-touch records between flush points
        is acceptable — the retention policy will simply converge
        slightly more slowly.

3.  Thread safety via a single reentrant lock.
    The Spark driver is single-threaded for Python UDFs, but FastAPI
    may serve concurrent HTTP requests in a thread pool.  All public
    methods acquire self._lock before touching shared state.

Delta table layout
------------------
One row per perspective.  Per-pair statistics are stored as a JSON
blob in the `pairs_json` column so that the schema does not need to
change as new activity types appear.

    perspective_id         STRING  NOT NULL   -- "__".join(sorted(keys))
    grouping_keys          ARRAY<STRING>      -- the raw key list
    level                  INT                -- PerspectiveLevel ordinal
    lookback               STRING             -- e.g. "7d", "255i"
    lookback_mode          STRING             -- "time" | "position"
    l1_build_cost_ms       DOUBLE
    l1_query_count         LONG
    l1_total_savings_ms    DOUBLE
    l1_maintenance_ms_per_batch   DOUBLE
    l2_build_cost_ms       DOUBLE
    l2_pos_query_count     LONG
    l2_total_savings_ms    DOUBLE
    l2_maintenance_ms_per_batch   DOUBLE
    pairs_json             STRING             -- JSON: {"{A}__{B}": {...}}

Path convention (mirrors existing S3Manager paths):
    s3a://{namespace}/{log_name}/adaptive/catalog
"""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import Generator, Optional, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from siesta.core.interfaces import StorageManager
from siesta.core.sparkManager import get_spark_session
from siesta.model.PerspectiveModel import (
    PairStats,
    PairStatus,
    PerspectiveLevel,
    PerspectiveStats,
)
from siesta.model.StorageModel import MetaData

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Delta schema
# ---------------------------------------------------------------------------

CATALOG_SCHEMA = StructType([
    StructField("perspective_id",              StringType(),        False),
    StructField("grouping_keys",               ArrayType(StringType()), False),
    StructField("level",                       IntegerType(),       False),
    StructField("lookback",                    StringType(),        False),
    StructField("lookback_mode",               StringType(),        False),
    StructField("l1_build_cost_ms",            DoubleType(),        True),
    StructField("l1_query_count",              LongType(),          True),
    StructField("l1_total_savings_ms",         DoubleType(),        True),
    StructField("l1_maintenance_ms_per_batch", DoubleType(),        True),
    StructField("l2_build_cost_ms",            DoubleType(),        True),
    StructField("l2_pos_query_count",          LongType(),          True),
    StructField("l2_total_savings_ms",         DoubleType(),        True),
    StructField("l2_maintenance_ms_per_batch", DoubleType(),        True),
    StructField("pairs_json",                  StringType(),        True),
])


# ---------------------------------------------------------------------------
# Process-wide singleton registry
# ---------------------------------------------------------------------------

# Keyed by (storage_namespace, log_name).  Access always goes through the
# module-level lock before touching the dict.
_CATALOG_REGISTRY: dict[tuple[str, str], "PerspectiveCatalog"] = {}
_REGISTRY_LOCK = threading.Lock()


def get_catalog(metadata: MetaData, storage: StorageManager) -> "PerspectiveCatalog":
    """
    Return the process-wide PerspectiveCatalog for the given log,
    creating and loading it from Delta storage on first access.

    This is the only correct way to obtain a PerspectiveCatalog.
    Direct instantiation should be avoided outside of tests.
    """
    key = (metadata.storage_namespace, metadata.log_name)
    with _REGISTRY_LOCK:
        if key not in _CATALOG_REGISTRY:
            logger.info(
                f"PerspectiveCatalog: creating new instance for "
                f"{metadata.storage_namespace}/{metadata.log_name}."
            )
            _CATALOG_REGISTRY[key] = PerspectiveCatalog(storage, metadata)
        return _CATALOG_REGISTRY[key]


def evict_catalog(metadata: MetaData) -> None:
    """
    Remove the catalog for a log from the process registry.

    Intended for use when a log is fully rebuilt (clear_existing=True)
    so that the stale in-memory state is not reused.  The caller is
    responsible for flushing any pending state before calling this.
    """
    key = (metadata.storage_namespace, metadata.log_name)
    with _REGISTRY_LOCK:
        _CATALOG_REGISTRY.pop(key, None)
        logger.info(
            f"PerspectiveCatalog: evicted registry entry for "
            f"{metadata.storage_namespace}/{metadata.log_name}."
        )


# ---------------------------------------------------------------------------
# PerspectiveCatalog
# ---------------------------------------------------------------------------

class PerspectiveCatalog:
    """
    In-memory, Delta-backed catalog of all perspectives and pair-index
    lifecycle state for one event log.

    Do not instantiate directly in production code.  Use get_catalog().
    """

    def __init__(self, storage: StorageManager, metadata: MetaData) -> None:
        self._storage  = storage
        self._metadata = metadata
        self._lock     = threading.RLock()

        # Primary state store: perspective_id -> PerspectiveStats
        self._cache: dict[str, PerspectiveStats] = {}

        # Perspectives whose in-memory state differs from Delta.
        # Only used for write-back mutations (query touches, maintenance).
        # Write-through mutations bypass the dirty set and write immediately.
        self._dirty: set[str] = set()

        self._load_from_delta()

    # ------------------------------------------------------------------
    # Public factory / lookup
    # ------------------------------------------------------------------

    def get_or_declare(
        self,
        grouping_keys: list[str],
        lookback: str,
        lookback_mode: str,
    ) -> tuple[str, PerspectiveStats]:
        """
        Return the existing PerspectiveStats for the given grouping, or
        register a new perspective at L0 and persist it immediately
        (write-through).

        Returns (perspective_id, stats).
        """
        pid = _make_perspective_id(grouping_keys)

        with self._lock:
            if pid not in self._cache:
                stats = PerspectiveStats(
                    level=PerspectiveLevel.L0_DECLARED,
                    grouping_keys=list(grouping_keys),
                    lookback=lookback,
                    lookback_mode=lookback_mode,
                )
                self._cache[pid] = stats
                # Write-through: new declaration is immediately durable.
                self._persist_one(pid)
                logger.info(
                    f"PerspectiveCatalog: declared '{pid}' at L0 "
                    f"(keys={grouping_keys})."
                )
            return pid, self._cache[pid]

    def get(self, pid: str) -> Optional[PerspectiveStats]:
        """Return stats for a known perspective ID, or None."""
        with self._lock:
            return self._cache.get(pid)

    # ------------------------------------------------------------------
    # Lifecycle transitions (write-through)
    # ------------------------------------------------------------------

    def promote(self, pid: str, target_level: PerspectiveLevel) -> None:
        """
        Advance a perspective to target_level.

        Promotes are write-through: the new level is persisted immediately
        so that a subsequent process restart does not re-run the (possibly
        expensive) physical promotion.
        """
        with self._lock:
            stats = self._cache.get(pid)
            if stats is None:
                raise KeyError(f"Unknown perspective '{pid}'.")
            if target_level <= stats.level:
                logger.debug(
                    f"PerspectiveCatalog: '{pid}' already at "
                    f"L{stats.level.value}, ignoring promote to "
                    f"L{target_level.value}."
                )
                return
            stats.level = target_level
            self._persist_one(pid)
            logger.info(
                f"PerspectiveCatalog: '{pid}' promoted to "
                f"L{target_level.value}."
            )

    def promote_pair(
        self,
        pid: str,
        act_a: str,
        act_b: str,
        target_status: PairStatus,
    ) -> None:
        """
        Update the status of a specific pair under a perspective.

        Write-through: pair status changes (persist / evict) are
        written immediately because they gate which pairs receive
        incremental maintenance on every ingest batch.
        """
        with self._lock:
            stats = self._cache.get(pid)
            if stats is None:
                raise KeyError(f"Unknown perspective '{pid}'.")
            pair_stats = stats.pairs.setdefault(
                (act_a, act_b), PairStats()
            )
            pair_stats.status = target_status
            self._persist_one(pid)
            logger.info(
                f"PerspectiveCatalog: pair ({act_a},{act_b}) under "
                f"'{pid}' -> {target_status.name}."
            )

    # ------------------------------------------------------------------
    # Workload statistics (write-back)
    # ------------------------------------------------------------------

    def record_query_touch(
        self,
        pid: str,
        pairs_touched: list[tuple[str, str]],
        references_pos: bool,
        total_query_ms: float,
        pair_savings_ms: dict[tuple[str, str], float],
    ) -> None:
        """
        Record that a query under perspective `pid` completed.

        Called by the QueryPlanner after every query.  This is the
        hot path — writes are in-memory only.  Call flush() after the
        query returns to persist to Delta.

        Parameters
        ----------
        pid              : perspective ID
        pairs_touched    : all (A, B) pairs fetched during this query
        references_pos   : True if the query referenced a[pos]
        total_query_ms   : wall-clock time of the full query
        pair_savings_ms  : per-pair savings vs. lazy scan (may be 0.0
                           for pairs that were scanned lazily this time)
        """
        with self._lock:
            stats = self._cache.get(pid)
            if stats is None:
                logger.warning(
                    f"PerspectiveCatalog.record_query_touch: unknown "
                    f"perspective '{pid}', ignoring."
                )
                return

            stats.l1_query_count += 1
            # Coarse savings estimate for L1: the avoided cost of
            # recomputing phi_G and re-partitioning.  We approximate
            # this as a fixed fraction of the total query time for
            # queries that hit the established perspective.
            # A more accurate estimate is set by the QueryPlanner once
            # it can measure lazy-path cost on first touch.
            stats.l1_total_savings_ms += total_query_ms * 0.1

            if references_pos:
                stats.l2_pos_query_count += 1
                stats.l2_total_savings_ms += total_query_ms * 0.05

            for (a, b) in pairs_touched:
                ps = stats.pairs.setdefault((a, b), PairStats())
                ps.query_count      += 1
                ps.total_savings_ms += pair_savings_ms.get((a, b), 0.0)
                ps.last_accessed_ts  = time.time()

            self._dirty.add(pid)

    def record_batch_maintenance(
        self,
        pid: str,
        l1_ms: float,
        l2_ms: float,
        pair_ms: dict[tuple[str, str], float],
    ) -> None:
        """
        Record the wall-clock cost of one ingest-batch maintenance cycle
        for perspective `pid`.

        Called by AdaptiveIndexing._run_incremental_maintenance after
        each batch.  Write-back: flushed at end of ingest cycle.

        Parameters
        ----------
        pid      : perspective ID
        l1_ms    : cost of L1 grouping-column projection for this batch
        l2_ms    : cost of L2 positional-annotation update for this batch
        pair_ms  : per-pair maintenance cost {(A, B): ms}
        """
        with self._lock:
            stats = self._cache.get(pid)
            if stats is None:
                logger.warning(
                    f"PerspectiveCatalog.record_batch_maintenance: "
                    f"unknown perspective '{pid}', ignoring."
                )
                return

            # Running mean: update with exponential moving average so
            # that recent batch costs are weighted more heavily than
            # historical ones.  Alpha = 0.2 gives roughly a 5-batch
            # effective window.
            alpha = 0.2
            if stats.l1_maintenance_ms_per_batch == 0.0:
                stats.l1_maintenance_ms_per_batch = l1_ms
            else:
                stats.l1_maintenance_ms_per_batch = (
                    (1 - alpha) * stats.l1_maintenance_ms_per_batch
                    + alpha * l1_ms
                )

            if stats.l2_maintenance_ms_per_batch == 0.0:
                stats.l2_maintenance_ms_per_batch = l2_ms
            else:
                stats.l2_maintenance_ms_per_batch = (
                    (1 - alpha) * stats.l2_maintenance_ms_per_batch
                    + alpha * l2_ms
                )

            for (a, b), ms in pair_ms.items():
                ps = stats.pairs.setdefault((a, b), PairStats())
                ps.total_maintenance_ms += ms
                ps.maintenance_batch_count += 1

            self._dirty.add(pid)

    def record_pair_build_cost(
        self,
        pid: str,
        act_a: str,
        act_b: str,
        build_cost_ms: float,
    ) -> None:
        """
        Store the measured first-touch build cost for a pair.

        Called by the QueryPlanner when it materialises a pair
        transiently for the first time.  This measurement becomes the
        cold-start savings baseline: future queries that hit a cached
        or persistent version of the pair are credited with saving
        this cost.  Write-back.
        """
        with self._lock:
            stats = self._cache.get(pid)
            if stats is None:
                return
            ps = stats.pairs.setdefault((act_a, act_b), PairStats())
            # Only record if not already set — first measurement wins.
            if ps.build_cost_ms == 0.0:
                ps.build_cost_ms = build_cost_ms
            self._dirty.add(pid)

    # ------------------------------------------------------------------
    # Iteration helpers
    # ------------------------------------------------------------------

    def all_perspectives(
        self,
    ) -> Generator[tuple[str, PerspectiveStats], None, None]:
        """Yield (pid, stats) for every known perspective."""
        with self._lock:
            # Snapshot the keys so that callers can safely mutate the
            # cache during iteration (e.g. via promote).
            items = list(self._cache.items())
        yield from items

    def all_established_perspectives(
        self,
    ) -> Generator[tuple[str, PerspectiveStats], None, None]:
        """
        Yield (pid, stats) for every perspective at L1 or above.

        These are the perspectives that have a physical grouping column
        on the SequenceTable and may have persistent pair indices that
        need incremental maintenance.
        """
        with self._lock:
            items = list(self._cache.items())
        for pid, stats in items:
            if stats.level >= PerspectiveLevel.L1_POS_FREE:
                yield pid, stats

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def flush(self) -> None:
        """
        Write all dirty perspectives to the Delta catalog table.

        Call this at the end of each ingest batch and at the end of
        each query in the QueryPlanner.  The call is cheap when the
        dirty set is empty (the common case for the query planner after
        a perspective warms up).
        """
        with self._lock:
            if not self._dirty:
                return
            dirty_pids = list(self._dirty)
            self._dirty.clear()

        # Release the lock before doing the Delta write so that other
        # threads can continue recording stats while we write.
        for pid in dirty_pids:
            with self._lock:
                stats = self._cache.get(pid)
            if stats is not None:
                self._persist_one(pid)

        logger.debug(
            f"PerspectiveCatalog: flushed {len(dirty_pids)} "
            f"perspective(s) to Delta."
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _catalog_path(self) -> str:
        return (
            f"s3a://{self._metadata.storage_namespace}"
            f"/{self._metadata.log_name}/adaptive/catalog"
        )

    def _load_from_delta(self) -> None:
        """
        Read the catalog Delta table and populate the in-memory cache.

        If the table does not exist yet (first run for this log), an
        empty cache is retained and the table will be created on the
        first _persist_one call.
        """
        spark: SparkSession = get_spark_session()
        path = self._catalog_path()

        try:
            df = spark.read.format("delta").load(path)
            rows = df.collect()
            with self._lock:
                for row in rows:
                    stats = _row_to_stats(row)
                    pid   = row["perspective_id"]
                    self._cache[pid] = stats
            logger.info(
                f"PerspectiveCatalog: loaded {len(rows)} perspective(s) "
                f"from {path}."
            )
        except Exception:
            # Table does not exist yet — this is the normal first-run path.
            logger.info(
                f"PerspectiveCatalog: no catalog found at {path}, "
                "starting fresh."
            )
            self._initialise_delta_table(spark, path)

    def _initialise_delta_table(
        self, spark: SparkSession, path: str
    ) -> None:
        """Create an empty Delta catalog table at path."""
        empty = spark.createDataFrame([], schema=CATALOG_SCHEMA)
        (
            empty.write
            .format("delta")
            .mode("overwrite")
            .save(path)
        )
        logger.info(
            f"PerspectiveCatalog: initialised empty catalog at {path}."
        )

    def _persist_one(self, pid: str) -> None:
        """
        Upsert one perspective row into the Delta catalog table.

        Uses Delta MERGE (upsert) so that concurrent writers from
        different threads do not truncate each other's data.  The lock
        should be held by the caller when reading from self._cache, but
        the Delta write itself happens outside the lock so that other
        threads are not blocked.
        """
        with self._lock:
            stats = self._cache.get(pid)
        if stats is None:
            return

        spark: SparkSession = get_spark_session()
        path = self._catalog_path()

        row = _stats_to_row(pid, stats)
        new_df = spark.createDataFrame([row], schema=CATALOG_SCHEMA)

        try:
            from delta.tables import DeltaTable
            dt = DeltaTable.forPath(spark, path)
            (
                dt.alias("existing")
                .merge(
                    new_df.alias("incoming"),
                    "existing.perspective_id = incoming.perspective_id",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        except Exception as exc:
            # If the table disappeared between the load and this write
            # (e.g. clear_existing was called), recreate it.
            logger.warning(
                f"PerspectiveCatalog: MERGE failed for '{pid}' "
                f"({exc}), attempting table recreation."
            )
            try:
                self._initialise_delta_table(spark, path)
                new_df.write.format("delta").mode("append").save(path)
            except Exception as exc2:
                logger.error(
                    f"PerspectiveCatalog: could not persist '{pid}': {exc2}",
                    exc_info=True,
                )


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------

def _make_perspective_id(grouping_keys: list[str]) -> str:
    """
    Stable, order-independent identifier for a set of grouping keys.
    Uses sorted join so that ["a","b"] and ["b","a"] resolve to the same id.
    """
    return "__".join(sorted(grouping_keys))


def _stats_to_row(pid: str, stats: PerspectiveStats) -> dict:
    """Serialise PerspectiveStats to a flat dict matching CATALOG_SCHEMA."""
    # Serialise the pairs dict as JSON.
    # Key format: "{A}__{B}" (double underscore), value: PairStats fields.
    pairs_serialised = {
        f"{a}__{b}": {
            "build_cost_ms":       ps.build_cost_ms,
            "query_count":         ps.query_count,
            "total_savings_ms":    ps.total_savings_ms,
            "total_maintenance_ms": ps.total_maintenance_ms,
            "status":              ps.status.value,
            "last_accessed_ts":    ps.last_accessed_ts,
        }
        for (a, b), ps in stats.pairs.items()
    }
    return {
        "perspective_id":              pid,
        "grouping_keys":               stats.grouping_keys,
        "level":                       stats.level.value,
        "lookback":                    stats.lookback,
        "lookback_mode":               stats.lookback_mode,
        "l1_build_cost_ms":            stats.l1_build_cost_ms,
        "l1_query_count":              stats.l1_query_count,
        "l1_total_savings_ms":         stats.l1_total_savings_ms,
        "l1_maintenance_ms_per_batch": stats.l1_maintenance_ms_per_batch,
        "l2_build_cost_ms":            stats.l2_build_cost_ms,
        "l2_pos_query_count":          stats.l2_pos_query_count,
        "l2_total_savings_ms":         stats.l2_total_savings_ms,
        "l2_maintenance_ms_per_batch": stats.l2_maintenance_ms_per_batch,
        "pairs_json":                  json.dumps(pairs_serialised),
    }


def _row_to_stats(row) -> PerspectiveStats:
    """Deserialise a Delta catalog row back to PerspectiveStats."""
    pairs: dict[tuple[str, str], PairStats] = {}

    raw_pairs = json.loads(row["pairs_json"] or "{}")
    for key, v in raw_pairs.items():
        # Key format: "{A}__{B}" — split on first double-underscore only.
        parts = key.split("__", 1)
        if len(parts) != 2:
            logger.warning(
                f"PerspectiveCatalog: malformed pair key '{key}' in "
                "catalog row, skipping."
            )
            continue
        a, b = parts
        ps = PairStats(
            build_cost_ms=       v.get("build_cost_ms",       0.0),
            query_count=         v.get("query_count",         0),
            total_savings_ms=    v.get("total_savings_ms",    0.0),
            total_maintenance_ms=v.get("total_maintenance_ms",0.0),
            status=              PairStatus(v.get("status",   PairStatus.ABSENT.value)),
            last_accessed_ts=    v.get("last_accessed_ts",    0.0),
        )
        pairs[(a, b)] = ps

    return PerspectiveStats(
        level=         PerspectiveLevel(row["level"]),
        grouping_keys= list(row["grouping_keys"]),
        lookback=      row["lookback"],
        lookback_mode= row["lookback_mode"],
        l1_build_cost_ms=            row["l1_build_cost_ms"]            or 0.0,
        l1_query_count=              row["l1_query_count"]              or 0,
        l1_total_savings_ms=         row["l1_total_savings_ms"]         or 0.0,
        l1_maintenance_ms_per_batch= row["l1_maintenance_ms_per_batch"] or 0.0,
        l2_build_cost_ms=            row["l2_build_cost_ms"]            or 0.0,
        l2_pos_query_count=          row["l2_pos_query_count"]          or 0,
        l2_total_savings_ms=         row["l2_total_savings_ms"]         or 0.0,
        l2_maintenance_ms_per_batch= row["l2_maintenance_ms_per_batch"] or 0.0,
        pairs=pairs,
    )