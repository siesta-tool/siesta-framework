"""
Physical materialisation functions for the adaptive index lifecycle.

Each function corresponds to one lifecycle transition or maintenance
operation from the design notes:

    promote_to_l1
        L0 -> L1: read the shared SequenceTable, compute the grouping
        value v = phi_G(event) for every row, write a per-perspective
        grouped sequence table partitioned by v.

    promote_to_l2
        L1 -> L2: add an intra-group position column (assigned by
        ordering events within each group by timestamp via a window
        function), and bootstrap the SequenceMetadata table that tracks
        the last assigned position per group for incremental updates.

    build_pair_transient
        On-demand pair extraction for the query planner.  Reads the
        per-perspective grouped sequence table for a specified set of
        candidate groups, runs STNM extraction for one (A, B) pair, and
        returns the resulting DataFrame without writing to Delta.

    build_pair_persistent
        Full historical build for one (A, B) pair at L3.  Reads the
        entire per-perspective grouped sequence table, extracts all STNM
        pairs, writes to the per-pair PairsIndex Delta table, and
        bootstraps the perspective's LastChecked table.

    incremental_update_perspective
        Per-batch maintenance for the grouped sequence table of an
        established perspective: computes v for each new event, assigns
        intra-group positions if the perspective is at L2 (extending
        from SequenceMetadata), and appends to the per-perspective table.
        Called for every established perspective on every ingest batch,
        even if it has no persistent pairs.

    incremental_update_persistent_pairs
        Per-batch maintenance for all L3 pair indices under one
        perspective.  Calls incremental_update_perspective internally,
        then for each persistent (A, B) pair runs the LastChecked-guided
        STNM extraction and appends new records to the per-pair
        PairsIndex.  Returns a dict {(A, B): elapsed_ms}.

Storage layout (all relative to s3a://{namespace}/{log_name}/adaptive/):

    {pid}/sequence_table/       grouped events, partitioned by trace_id
                                (column name kept as trace_id for reuse
                                of existing STNM machinery; values are
                                group values v, not original trace IDs)
    {pid}/sequence_metadata/    v -> last_pos  (L2 only)
    {pid}/pairs/{A}__{B}/       per-pair PairsIndex, partitioned by source
    {pid}/last_checked/         LastChecked for all L3 pairs of this
                                perspective, partitioned by source

Design notes
------------
Column naming: the per-perspective sequence table uses "trace_id" as
the column name for the group value.  This is intentional: it lets
the existing STNM computation functions (createTuples,
_calculate_pairs_stnm, extract_last_checked_and_all_pairs) be called
directly without modification, since they all key on "trace_id".
The query planner and query processors are aware that for adaptive
perspectives "trace_id" holds the group value, not the original case
identifier.

Position semantics: at L1 (has_pos=False) the pair records carry the
original intra-trace position from the shared SequenceTable.  These
positions are meaningless for cross-trace groupings but are harmless
because the query planner sorts the pseudo-sequence by timestamp at
L1.  At L2 (has_pos=True) positions are intra-group positions computed
by promote_to_l2 and maintained by incremental_update_perspective.
"""

from __future__ import annotations

import time
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    lit,
    row_number,
    when,
)
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

from siesta.core.interfaces import StorageManager
from siesta.core.sparkManager import get_spark_session
from siesta.model.DataModel import EventPair, Last_Checked_table_schema
from siesta.model.StorageModel import MetaData
from siesta.modules.index.computations import (
    _parse_lookback,
    createTuples,
    update_last_checked,
)

import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Top-level columns that may be used as grouping keys directly.
# Everything else is looked up in the attributes map.
# ---------------------------------------------------------------------------
_TOP_LEVEL_COLS = {"trace_id", "activity", "start_timestamp", "position"}


# ===========================================================================
# Path helpers
# ===========================================================================

def _adaptive_root(metadata: MetaData, pid: str) -> str:
    return (
        f"s3a://{metadata.storage_namespace}"
        f"/{metadata.log_name}/adaptive/{pid}"
    )


def _perspective_sequence_path(metadata: MetaData, pid: str) -> str:
    return f"{_adaptive_root(metadata, pid)}/sequence_table"


def _perspective_seq_metadata_path(metadata: MetaData, pid: str) -> str:
    return f"{_adaptive_root(metadata, pid)}/sequence_metadata"


def _perspective_pair_path(
    metadata: MetaData, pid: str, act_a: str, act_b: str
) -> str:
    # Double-underscore separator mirrors the catalog key convention.
    # Activity names may contain single underscores so we use double.
    safe_a = act_a.replace("/", "_")
    safe_b = act_b.replace("/", "_")
    return f"{_adaptive_root(metadata, pid)}/pairs/{safe_a}__{safe_b}"


def _perspective_last_checked_path(metadata: MetaData, pid: str) -> str:
    return f"{_adaptive_root(metadata, pid)}/last_checked"


# ===========================================================================
# Grouping value computation
# ===========================================================================

def _grouping_col(grouping_keys: List[str]) -> "Column":  # noqa: F821
    """
    Return a Spark Column expression that computes the group value
    v = phi_G(event) as a deterministic string.

    Top-level columns (trace_id, activity, start_timestamp, position)
    are referenced directly; all other keys are looked up in the
    attributes MapType column.  Keys are sorted before concatenation so
    that the group value is independent of the order in grouping_keys.

    Returns NULL when any of the requested keys is missing on the event.
    Callers MUST filter null rows before partitioning: events that do
    not carry the perspective's attributes are not part of the
    perspective and must not be indexed under it.
    """
    parts = []
    for key in sorted(grouping_keys):
        if key in _TOP_LEVEL_COLS:
            parts.append(col(key).cast(StringType()))
        else:
            parts.append(col("attributes")[key].cast(StringType()))

    if len(parts) == 1:
        return parts[0].alias("trace_id")

    # All-or-nothing: if any key is missing the whole value is NULL.
    # concat_ws silently skips nulls, so guard explicitly.
    any_null = parts[0].isNull()
    for p in parts[1:]:
        any_null = any_null | p.isNull()
    return (
        when(any_null, lit(None).cast(StringType()))
        .otherwise(concat_ws("|||", *parts))
        .alias("trace_id")
    )


# ===========================================================================
# L0 -> L1 promotion
# ===========================================================================

def promote_to_l1(
    pid: str,
    grouping_keys: List[str],
    metadata: MetaData,
    storage: StorageManager,
) -> float:
    """
    Materialise the per-perspective grouped sequence table at L1.

    Reads the shared SequenceTable, computes v = phi_G(event) for every
    row, and writes a new Delta table partitioned by the group value.
    The new table uses "trace_id" as the column name for v so that the
    existing STNM computation functions can be reused without changes.

    Returns
    -------
    float
        Elapsed wall-clock time in milliseconds.
    """
    logger.info(
        f"AdaptiveBuilders: promote_to_l1 for perspective '{pid}' "
        f"(keys={grouping_keys})."
    )
    t0 = time.time()
    spark = get_spark_session()

    seq_df = storage.read_sequence_table(metadata)

    # Compute grouping value and rename to trace_id.  Drop events that
    # do not carry the perspective's attributes — they are not part of
    # this perspective and must not be indexed under it.
    grouped_df = (
        seq_df
        .withColumn("trace_id", _grouping_col(grouping_keys))
        .filter(col("trace_id").isNotNull())
        .select("trace_id", "activity", "start_timestamp", "attributes")
    )

    path = _perspective_sequence_path(metadata, pid)
    (
        grouped_df.write
        .format("delta")
        .partitionBy("trace_id")
        .mode("overwrite")
        .save(path)
    )

    elapsed = (time.time() - t0) * 1000
    logger.info(
        f"AdaptiveBuilders: L1 promotion for '{pid}' completed in "
        f"{elapsed:.1f}ms — wrote to {path}."
    )
    return elapsed


# ===========================================================================
# L1 -> L2 promotion
# ===========================================================================

def promote_to_l2(
    pid: str,
    grouping_keys: List[str],
    metadata: MetaData,
    storage: StorageManager,
) -> float:
    """
    Add intra-group positions to the per-perspective sequence table.

    Reads the L1 table, assigns a 0-indexed position to each event
    within its group (ordered by start_timestamp), overwrites the table
    with the position column included, and bootstraps SequenceMetadata
    with the last assigned position per group.

    Returns
    -------
    float
        Elapsed wall-clock time in milliseconds.
    """
    logger.info(
        f"AdaptiveBuilders: promote_to_l2 for perspective '{pid}'."
    )
    t0 = time.time()
    spark = get_spark_session()

    path = _perspective_sequence_path(metadata, pid)

    try:
        l1_df = spark.read.format("delta").load(path)
    except Exception as exc:
        raise RuntimeError(
            f"promote_to_l2: perspective '{pid}' sequence table not found "
            f"at {path}. Was promote_to_l1 called first?"
        ) from exc

    # Assign 0-indexed intra-group position ordered by timestamp.
    # row_number() is 1-based so we subtract 1.
    window = Window.partitionBy("trace_id").orderBy("start_timestamp")
    l2_df = l1_df.withColumn(
        "position", (row_number().over(window) - 1).cast("integer")
    )

    # Overwrite with position included.
    (
        l2_df.write
        .format("delta")
        .partitionBy("trace_id")
        .mode("overwrite")
        .save(path)
    )

    # Bootstrap SequenceMetadata: trace_id (group value) -> last_pos.
    meta_df = (
        l2_df
        .groupBy("trace_id")
        .agg(F.max("position").cast("integer").alias("last_pos"))
    )
    meta_path = _perspective_seq_metadata_path(metadata, pid)
    (
        meta_df.write
        .format("delta")
        .mode("overwrite")
        .save(meta_path)
    )

    elapsed = (time.time() - t0) * 1000
    logger.info(
        f"AdaptiveBuilders: L2 promotion for '{pid}' completed in "
        f"{elapsed:.1f}ms."
    )
    return elapsed


# ===========================================================================
# Per-batch perspective sequence table maintenance
# ===========================================================================

def incremental_update_perspective(
    pid: str,
    grouping_keys: List[str],
    batch_activity_df: DataFrame,
    metadata: MetaData,
    storage: StorageManager,
    has_pos: bool,
) -> Tuple[float, float]:
    """
    Append new events from the current ingest batch to the per-perspective
    grouped sequence table, extending intra-group positions if has_pos.

    This function must be called for every established perspective on
    every ingest batch, regardless of whether the perspective has any
    persistent pairs.  It corresponds to the L1 and L2 maintenance costs
    m_est_G(Delta_i) and m_pos_G(Delta_i) in the retention cost function.

    The two phases are timed separately so callers can feed the individual
    costs into record_batch_maintenance for accurate retention accounting.

    Parameters
    ----------
    pid               : perspective ID
    grouping_keys     : attribute keys defining phi_G
    batch_activity_df : activity-index DataFrame for the current batch
                        (schema: activity, trace_id, position,
                         start_timestamp, attributes)
    metadata          : log metadata
    storage           : storage manager
    has_pos           : True if the perspective is at L2

    Returns
    -------
    Tuple[float, float]
        (l1_ms, l2_ms) where l1_ms is the cost of computing the grouping
        value and appending to the sequence table, and l2_ms is the extra
        cost of position extension and SequenceMetadata maintenance.
        l2_ms is always 0.0 when has_pos is False.
    """
    spark = get_spark_session()

    # ------------------------------------------------------------------
    # Phase 1 (L1 cost): compute the grouping value for every new event
    # and append to the per-perspective sequence table.
    # Timed independently of Phase 2.
    # ------------------------------------------------------------------
    t1 = time.time()

    new_events = (
        batch_activity_df
        .withColumn("trace_id", _grouping_col(grouping_keys))
        .filter(col("trace_id").isNotNull())
    )

    if not has_pos:
        append_df = new_events.select(
            "trace_id", "activity", "start_timestamp", "attributes",
        )
        seq_path = _perspective_sequence_path(metadata, pid)
        (
            append_df.write
            .format("delta")
            .partitionBy("trace_id")
            .mode("append")
            .option("mergeSchema", "true")
            .save(seq_path)
        )
        l1_ms = (time.time() - t1) * 1000
        logger.debug(
            f"AdaptiveBuilders: '{pid}' sequence table updated "
            f"(L1 only) in {l1_ms:.1f}ms."
        )
        return l1_ms, 0.0

    l1_ms = (time.time() - t1) * 1000

    # ------------------------------------------------------------------
    # Phase 2 (L2 cost): extend intra-group positions from the
    # SequenceMetadata watermark and update the metadata table.
    # Only reached when has_pos is True.
    # ------------------------------------------------------------------
    t2 = time.time()

    meta_path = _perspective_seq_metadata_path(metadata, pid)
    try:
        meta_df = spark.read.format("delta").load(meta_path)
    except Exception:
        # First incremental batch after L2 promotion — metadata exists
        # but may be empty.  Treat all groups as starting from -1.
        meta_df = spark.createDataFrame([], schema=_seq_metadata_schema())

    # Join to get the last_pos for each affected group.
    # Groups not yet in metadata start at -1 so their first position is 0.
    new_events_with_offset = (
        new_events
        .join(
            meta_df.select(
                col("trace_id").alias("group_key"),
                col("last_pos"),
            ),
            col("trace_id") == col("group_key"),
            how="left",
        )
        .withColumn(
            "last_pos",
            coalesce(col("last_pos"), lit(-1)).cast("integer"),
        )
        .drop("group_key")
    )

    # Within each group, assign new positions starting from last_pos + 1.
    window = Window.partitionBy("trace_id").orderBy("start_timestamp")
    new_events_positioned = new_events_with_offset.withColumn(
        "position",
        (col("last_pos") + row_number().over(window)).cast("integer"),
    ).drop("last_pos")

    append_df = new_events_positioned.select(
        "trace_id", "activity", "start_timestamp", "attributes", "position",
    )

    seq_path = _perspective_sequence_path(metadata, pid)
    (
        append_df.write
        .format("delta")
        .partitionBy("trace_id")
        .mode("append")
        .option("mergeSchema", "true")
        .save(seq_path)
    )

    # Update SequenceMetadata with the new last positions per group.
    new_meta = (
        new_events_positioned
        .groupBy("trace_id")
        .agg(F.max("position").cast("integer").alias("last_pos"))
    )
    from delta.tables import DeltaTable
    try:
        dt = DeltaTable.forPath(spark, meta_path)
        (
            dt.alias("existing")
            .merge(
                new_meta.alias("incoming"),
                "existing.trace_id = incoming.trace_id",
            )
            .whenMatchedUpdate(set={"last_pos": col("incoming.last_pos")})
            .whenNotMatchedInsertAll()
            .execute()
        )
    except Exception:
        new_meta.write.format("delta").mode("append").save(meta_path)

    l2_ms = (time.time() - t2) * 1000
    logger.debug(
        f"AdaptiveBuilders: '{pid}' sequence table updated "
        f"(L1={l1_ms:.1f}ms, L2={l2_ms:.1f}ms)."
    )
    return l1_ms, l2_ms


# ===========================================================================
# L3: full historical pair build
# ===========================================================================

def build_pair_persistent(
    pid: str,
    act_a: str,
    act_b: str,
    lookback: str,
    lookback_mode: str,
    metadata: MetaData,
    storage: StorageManager,
    has_pos: bool,
) -> float:
    """
    Build the full historical PairsIndex for (act_a, act_b) under
    perspective pid.

    Reads the entire per-perspective grouped sequence table and extracts
    all STNM pairs for (act_a, act_b).  Writes the result to the per-pair
    PairsIndex Delta table and bootstraps the perspective's LastChecked
    table for this pair.

    Parameters
    ----------
    pid, act_a, act_b : perspective and pair identifiers
    lookback          : lookback window string (e.g. "7d", "255i")
    lookback_mode     : "time" | "position"
    metadata          : log metadata
    storage           : storage manager
    has_pos           : whether the perspective is at L2

    Returns
    -------
    float
        Elapsed wall-clock time in milliseconds.
    """
    logger.info(
        f"AdaptiveBuilders: build_pair_persistent ({act_a},{act_b}) "
        f"under '{pid}'."
    )
    t0 = time.time()
    spark = get_spark_session()

    seq_path = _perspective_sequence_path(metadata, pid)
    try:
        seq_df = spark.read.format("delta").load(seq_path)
    except Exception as exc:
        raise RuntimeError(
            f"build_pair_persistent: sequence table for '{pid}' not found "
            f"at {seq_path}."
        ) from exc

    pairs_df, last_checked_df = _extract_single_pair_from_df(
        seq_df=seq_df,
        act_a=act_a,
        act_b=act_b,
        lookback_str=lookback,
        previous_lc_df=None,   # full historical build: no prior watermark
        batch_min_ts=None,
        has_pos=has_pos,
    )

    if pairs_df.rdd.isEmpty():
        logger.info(
            f"AdaptiveBuilders: no pairs found for ({act_a},{act_b}) "
            f"under '{pid}' — writing empty tables."
        )

    # Write pair index.
    pairs_path = _perspective_pair_path(metadata, pid, act_a, act_b)
    (
        pairs_df.write
        .format("delta")
        .partitionBy("source")
        .mode("overwrite")
        .save(pairs_path)
    )

    # Bootstrap LastChecked.  Use append + MERGE so that other pairs'
    # LastChecked rows (if any) are not overwritten.
    lc_path = _perspective_last_checked_path(metadata, pid)
    _upsert_last_checked(spark, last_checked_df, lc_path)

    elapsed = (time.time() - t0) * 1000
    logger.info(
        f"AdaptiveBuilders: build_pair_persistent ({act_a},{act_b}) "
        f"under '{pid}' completed in {elapsed:.1f}ms."
    )
    return elapsed


# ===========================================================================
# L3-: transient pair build (no Delta write, for query planner)
# ===========================================================================

def build_pair_transient(
    pid: str,
    act_a: str,
    act_b: str,
    lookback: str,
    lookback_mode: str,
    candidate_group_ids: List[str],
    metadata: MetaData,
    storage: StorageManager,
    has_pos: bool,
) -> DataFrame:
    """
    Extract pairs for (act_a, act_b) under perspective pid on demand,
    restricted to the specified candidate groups.

    No Delta table is written.  The caller (query planner) is responsible
    for caching the returned DataFrame in its LRU cache.

    Parameters
    ----------
    candidate_group_ids : group values (v) to restrict the scan to.
                         An empty list means all groups (full scan).

    Returns
    -------
    DataFrame
        Pairs in EventPair schema, with "trace_id" holding the group
        value v.
    """
    spark = get_spark_session()
    seq_path = _perspective_sequence_path(metadata, pid)

    try:
        seq_df = spark.read.format("delta").load(seq_path)
    except Exception as exc:
        raise RuntimeError(
            f"build_pair_transient: sequence table for '{pid}' not found "
            f"at {seq_path}."
        ) from exc

    if candidate_group_ids:
        seq_df = seq_df.filter(col("trace_id").isin(candidate_group_ids))

    pairs_df, _ = _extract_single_pair_from_df(
        seq_df=seq_df,
        act_a=act_a,
        act_b=act_b,
        lookback_str=lookback,
        previous_lc_df=None,
        batch_min_ts=None,
        has_pos=has_pos,
    )
    return pairs_df


# ===========================================================================
# Per-batch pair index maintenance
# ===========================================================================

def incremental_update_persistent_pairs(
    pid: str,
    grouping_keys: List[str],
    batch_activity_df: DataFrame,
    batch_min_ts: int,
    persistent_pairs: List[Tuple[str, str]],
    lookback: str,
    lookback_mode: str,
    has_pos: bool,
    metadata: MetaData,
    storage: StorageManager,
) -> Dict[Tuple[str, str], float]:
    """
    Maintain the per-perspective grouped sequence table and all L3 pair
    indices for one ingest batch.

    This is the adaptive equivalent of build_last_checked_table +
    build_pairs_index from the eager indexer.  Key differences:
      - Operates on the per-perspective sequence table (grouped by v)
        rather than the shared SequenceTable (grouped by trace_id).
      - Only processes the pairs listed in persistent_pairs.
      - Returns per-pair timing so the catalog can track maintenance cost.

    The function always updates the per-perspective sequence table
    (Step 1) regardless of whether persistent_pairs is empty, because
    keeping the sequence table current is the L1/L2 maintenance work
    that must happen on every batch for every established perspective.

    Parameters
    ----------
    pid              : perspective ID
    grouping_keys    : attribute keys defining phi_G
    batch_activity_df: activity-index DataFrame for this batch
    batch_min_ts     : minimum start_timestamp in the batch (for LC pruning)
    persistent_pairs : list of (A, B) pairs at L3 status
    lookback         : lookback window string
    lookback_mode    : "time" | "position"
    has_pos          : whether the perspective is at L2
    metadata         : log metadata
    storage          : storage manager

    Returns
    -------
    dict
        {(A, B): elapsed_ms} for each processed pair.
    """
    spark = get_spark_session()

    # ------------------------------------------------------------------
    # Step 1: Update the per-perspective grouped sequence table.
    # This must happen before pair extraction so that the sequence table
    # reflects the events in this batch.
    # ------------------------------------------------------------------
    incremental_update_perspective(
        pid=pid,
        grouping_keys=grouping_keys,
        batch_activity_df=batch_activity_df,
        metadata=metadata,
        storage=storage,
        has_pos=has_pos,
    )

    if not persistent_pairs:
        return {}

    # ------------------------------------------------------------------
    # Step 2: Identify which groups received new events in this batch.
    # We only need to run pair extraction for groups that have new events
    # for either A or B in the pair.  Groups with no new events cannot
    # produce new pair instances.
    # ------------------------------------------------------------------
    batch_v_col = _grouping_col(grouping_keys)
    affected_groups_df = (
        batch_activity_df
        .withColumn("trace_id", batch_v_col)
        .filter(col("trace_id").isNotNull())
        .select("trace_id", "activity")
        .distinct()
    )

    # ------------------------------------------------------------------
    # Step 3: Read the per-perspective sequence table once, restricted
    # to affected groups.  Shared across all persistent pairs in this
    # batch to avoid repeated Delta reads.
    # ------------------------------------------------------------------
    seq_path = _perspective_sequence_path(metadata, pid)
    try:
        seq_df = spark.read.format("delta").load(seq_path)
    except Exception as exc:
        logger.error(
            f"AdaptiveBuilders: cannot read sequence table for '{pid}' "
            f"at {seq_path}: {exc}"
        )
        return {}

    # ------------------------------------------------------------------
    # Step 4: Read the entire LastChecked table for this perspective once.
    # We will filter to the relevant (source, target, trace_id) rows per
    # pair inside the loop.
    # ------------------------------------------------------------------
    lc_path = _perspective_last_checked_path(metadata, pid)
    try:
        all_lc_df = spark.read.format("delta").load(lc_path)
    except Exception:
        all_lc_df = spark.createDataFrame([], schema=Last_Checked_table_schema)

    real_lookback = _parse_lookback(lookback)
    batch_min_pos = (
        batch_activity_df.agg(F.min("position")).collect()[0][0]
        if has_pos else 0
    )

    # ------------------------------------------------------------------
    # Step 5: Per-pair extraction and write.
    # ------------------------------------------------------------------
    pair_elapsed: Dict[Tuple[str, str], float] = {}

    for (act_a, act_b) in persistent_pairs:
        t0 = time.time()

        # Find groups that have new events for act_a OR act_b.
        pair_affected = (
            affected_groups_df
            .filter(col("activity").isin([act_a, act_b]))
            .select("trace_id")
            .distinct()
        )

        if pair_affected.rdd.isEmpty():
            logger.debug(
                f"AdaptiveBuilders: no new events for ({act_a},{act_b}) "
                f"under '{pid}' this batch — skipping."
            )
            pair_elapsed[(act_a, act_b)] = 0.0
            continue

        # Restrict sequence table to affected groups.
        pair_seq_df = seq_df.join(pair_affected, on="trace_id", how="inner")

        # Get LastChecked watermarks for this pair in affected groups.
        pair_lc_df = (
            all_lc_df
            .filter(
                (col("source") == act_a) & (col("target") == act_b)
            )
            .join(pair_affected, on="trace_id", how="inner")
        )

        # Run STNM extraction.
        new_pairs_df, new_lc_df = _extract_single_pair_from_df(
            seq_df=pair_seq_df,
            act_a=act_a,
            act_b=act_b,
            lookback_str=lookback,
            previous_lc_df=pair_lc_df if pair_lc_df.rdd.count() > 0 else None,
            batch_min_ts=batch_min_ts,
            has_pos=has_pos,
        )

        if new_pairs_df.rdd.isEmpty():
            pair_elapsed[(act_a, act_b)] = (time.time() - t0) * 1000
            continue

        # Append new pair records.
        pairs_path = _perspective_pair_path(metadata, pid, act_a, act_b)
        (
            new_pairs_df.write
            .format("delta")
            .partitionBy("source")
            .mode("append")
            .option("mergeSchema", "true")
            .save(pairs_path)
        )

        # Merge updated LastChecked rows back, pruning stale entries.
        merged_lc_df = update_last_checked(
            previous_last_checked=pair_lc_df if not pair_lc_df.rdd.isEmpty() else None,
            current_last_checked=new_lc_df,
            batch_min_ts=batch_min_ts,
            batch_min_pos=batch_min_pos,
            real_lookback=real_lookback,
        )
        _upsert_last_checked(spark, merged_lc_df, lc_path)

        pair_elapsed[(act_a, act_b)] = (time.time() - t0) * 1000
        logger.debug(
            f"AdaptiveBuilders: updated ({act_a},{act_b}) under '{pid}' "
            f"in {pair_elapsed[(act_a, act_b)]:.1f}ms."
        )

    return pair_elapsed


# ===========================================================================
# Internal helpers
# ===========================================================================

def _extract_single_pair_from_df(
    seq_df: DataFrame,
    act_a: str,
    act_b: str,
    lookback_str: str,
    previous_lc_df: Optional[DataFrame],
    batch_min_ts: Optional[int],
    has_pos: bool,
) -> Tuple[DataFrame, DataFrame]:
    """
    Extract all STNM instances of (act_a, act_b) from seq_df, guided by
    the LastChecked watermarks in previous_lc_df.

    Reuses createTuples from index/computations.py directly; the only
    difference from the eager path is that we process one pair (A, B)
    at a time rather than all combinations.

    Returns
    -------
    (pairs_df, last_checked_df)
        pairs_df     : new pair records in EventPair schema
        last_checked_df : updated last-checked moments in
                         Last_Checked_table_schema
    """
    spark = get_spark_session()
    real_lookback = _parse_lookback(lookback_str)

    # Build RDD keyed by group value (trace_id).
    trace_rdd = seq_df.rdd.map(
        lambda row: (
            row.trace_id,
            (
                row.activity,
                row.start_timestamp,
                row.position if has_pos and hasattr(row, "position") else 0,
                row.attributes,
            ),
        )
    )

    # Build LastChecked map: group_id -> last_checked_moment for (A, B).
    if previous_lc_df is not None and not previous_lc_df.rdd.isEmpty():
        lc_map_rdd = previous_lc_df.rdd.map(
            lambda row: (row.trace_id, row.last_checked_moment)
        ).collectAsMap()
    else:
        lc_map_rdd = {}

    # Broadcast the LC map so executors don't serialise it per-partition.
    lc_broadcast = spark.sparkContext.broadcast(lc_map_rdd)

    def extract_for_group(kv):
        group_id, events = kv
        events = list(events)

        if not has_pos:
            # At L1 all positions are 0, which makes createTuples skip every
            # target (it requires source_pos < target_pos). Assign sequential
            # positions by timestamp so the ordering predicate is satisfied.
            events.sort(key=lambda e: e[1])  # e[1] = start_timestamp
            events = [
                (act, ts, idx, attrs)
                for idx, (act, ts, _, attrs) in enumerate(events)
            ]

        activity_map = defaultdict(list)
        for activity, ts, pos, attrs in events:
            activity_map[activity].append((ts, pos, attrs))

        for k in activity_map:
            activity_map[k].sort(key=lambda x: x[1])  # sort by position

        e_source = activity_map.get(act_a, [])
        e_target = activity_map.get(act_b, [])

        if not e_source or not e_target:
            return [], []

        last_ts = lc_broadcast.value.get(group_id, None)
        pairs = createTuples(
            act_a, act_b, e_source, e_target,
            real_lookback, last_ts, group_id,
        )
        # Last-checked entry: group_id, A, B, last_target_ts
        lc = [(group_id, act_a, act_b, pairs[-1][4])] if pairs else []
        return pairs, lc

    full_rdd = (
        trace_rdd
        .groupByKey()
        .map(extract_for_group)
    )

    pairs_flat  = full_rdd.flatMap(lambda x: x[0])
    lc_flat     = full_rdd.flatMap(lambda x: x[1])

    pairs_df = spark.createDataFrame(pairs_flat, schema=EventPair.get_schema())
    lc_df    = spark.createDataFrame(lc_flat,    schema=Last_Checked_table_schema)

    return pairs_df, lc_df


def _upsert_last_checked(
    spark,
    new_lc_df: DataFrame,
    lc_path: str,
) -> None:
    """
    Merge new LastChecked rows into the perspective's LastChecked table.

    Uses Delta MERGE so that:
      - Existing rows for the same (trace_id, source, target) are updated.
      - New rows are inserted.
      - Rows for other pairs are untouched.

    If the table does not exist yet, creates it from scratch.
    """
    from delta.tables import DeltaTable

    if new_lc_df.rdd.isEmpty():
        return

    try:
        dt = DeltaTable.forPath(spark, lc_path)
        (
            dt.alias("existing")
            .merge(
                new_lc_df.alias("incoming"),
                (
                    "existing.trace_id = incoming.trace_id "
                    "AND existing.source = incoming.source "
                    "AND existing.target = incoming.target"
                ),
            )
            .whenMatchedUpdate(
                set={"last_checked_moment": col("incoming.last_checked_moment")}
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    except Exception:
        # Table does not exist — write from scratch.
        (
            new_lc_df.write
            .format("delta")
            .partitionBy("source")
            .mode("overwrite")
            .save(lc_path)
        )


def _seq_metadata_schema():
    """Return the Spark schema for the SequenceMetadata table."""
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType
    return StructType([
        StructField("trace_id", StringType(),  False),  # group value v
        StructField("last_pos", IntegerType(), False),
    ])