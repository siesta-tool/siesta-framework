"""
Physical materialisation functions for the adaptive index lifecycle.

Each function corresponds to one lifecycle transition or maintenance
operation from the design notes:

    promote_to_l1
        L0 -> L1: no separate table is materialised.  The shared
        SequenceTable (built by the eager indexer) is used directly,
        with the grouping value v = phi_G(event) computed on the fly
        by every downstream operation.

    promote_to_l2
        L1 -> L2: compute intra-group positions (ordered by timestamp
        within each group) and write a compact positions overlay table
        that maps (original trace_id, original position) → (group_value,
        group_pos).  Bootstrap the SequenceMetadata table that tracks
        the last assigned position per group for incremental updates.

    build_pair_transient
        On-demand pair extraction for the query planner.  Reads the
        shared SequenceTable, computes phi_G on the fly, optionally
        joins with the positions overlay (L2), and runs STNM extraction
        for one (A, B) pair without writing to Delta.

    build_pair_persistent
        Full historical build for one (A, B) pair at L3.  Same read
        path as build_pair_transient, writes to the per-pair PairsIndex
        Delta table, and bootstraps the perspective's LastChecked table.

    incremental_update_perspective
        Per-batch maintenance.  At L1 this is a no-op (reads happen
        directly from the shared table).  At L2 new intra-group
        positions are computed for every new event and appended to the
        positions overlay; SequenceMetadata is updated via MERGE.

    incremental_update_persistent_pairs
        Per-batch maintenance for all L3 pair indices under one
        perspective.  Calls incremental_update_perspective for L2
        bookkeeping, then for each persistent (A, B) pair runs the
        LastChecked-guided STNM extraction and appends new records to
        the per-pair PairsIndex.  Returns a dict {(A, B): elapsed_ms}.

Storage layout (all relative to s3a://{namespace}/{log_name}/):

    sequence_table/                 shared; built by the eager indexer
                                    (trace_id = original case ID,
                                     position = intra-trace position)

    adaptive/{pid}/positions/       L2 only — compact positions overlay
                                    schema: trace_id, position,
                                            group_value, group_pos
                                    partitioned by group_value
    adaptive/{pid}/sequence_metadata/  group_value -> last_pos  (L2)
    adaptive/{pid}/pairs/{A}__{B}/  per-pair PairsIndex
    adaptive/{pid}/last_checked/    LastChecked for all L3 pairs

Design notes
------------
A single shared SequenceTable is used for all perspectives, avoiding
the O(N × events) storage replication of the earlier design where each
perspective owned a full copy of the event data.  The grouping value
v = phi_G(event) is computed on the fly whenever the shared table is
read; no materialised per-perspective sequence table exists.

At L2, intra-group positions are stored in a compact positions overlay
that contains only the identity columns (trace_id, original position)
plus (group_value, group_pos) — no event attributes are duplicated.
The helper _get_perspective_seq_df assembles the correct DataFrame for
STNM computation by joining the shared table with the overlay and
renaming columns so that downstream functions see the familiar
(trace_id = group_value, position = group_pos) schema.

Position semantics: at L1 (has_pos=False) _extract_single_pair_from_df
assigns sequential positions by timestamp, same as before.  At L2
(has_pos=True) group_pos values from the overlay are used.
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


def _perspective_positions_path(metadata: MetaData, pid: str) -> str:
    """Compact L2 positions overlay: (trace_id, position) → (group_value, group_pos)."""
    return f"{_adaptive_root(metadata, pid)}/positions"


def _perspective_seq_metadata_path(metadata: MetaData, pid: str) -> str:
    return f"{_adaptive_root(metadata, pid)}/sequence_metadata"


def _perspective_pair_path(
    metadata: MetaData, pid: str, act_a: str, act_b: str
) -> str:
    safe_a = act_a.replace("/", "_")
    safe_b = act_b.replace("/", "_")
    return f"{_adaptive_root(metadata, pid)}/pairs/{safe_a}__{safe_b}"


def _perspective_last_checked_path(metadata: MetaData, pid: str) -> str:
    return f"{_adaptive_root(metadata, pid)}/last_checked"


# ===========================================================================
# Grouping value computation
# ===========================================================================

def _grouping_col(grouping_keys: List[str]):
    """
    Return a Spark Column expression that computes the group value
    v = phi_G(event) as a deterministic string.

    Top-level columns (trace_id, activity, start_timestamp, position)
    are referenced directly; all other keys are looked up in the
    attributes MapType column.  Keys are sorted before concatenation so
    that the group value is independent of the order in grouping_keys.

    Returns NULL when any of the requested keys is missing on the event.
    Callers MUST filter null rows before partitioning.
    """
    parts = []
    for key in sorted(grouping_keys):
        if key in _TOP_LEVEL_COLS:
            parts.append(col(key).cast(StringType()))
        else:
            parts.append(col("attributes")[key].cast(StringType()))

    # Empty grouping_keys → case_id perspective: group by trace_id
    if len(parts) == 0:
        return col("trace_id").cast(StringType()).alias("group_value")

    if len(parts) == 1:
        return parts[0].alias("group_value")

    any_null = parts[0].isNull()
    for p in parts[1:]:
        any_null = any_null | p.isNull()
    return (
        when(any_null, lit(None).cast(StringType()))
        .otherwise(concat_ws("|||", *parts))
        .alias("group_value")
    )


# ===========================================================================
# Shared sequence table reader for a perspective
# ===========================================================================

def _get_perspective_seq_df(
    pid: str,
    grouping_keys: List[str],
    metadata: MetaData,
    storage: StorageManager,
    has_pos: bool,
    group_ids_filter: Optional[List[str]] = None,
) -> DataFrame:
    """
    Return a DataFrame suitable for STNM pair extraction under perspective pid.

    Reads the shared SequenceTable, computes phi_G on the fly, and —
    if the perspective is at L2 — joins with the compact positions
    overlay to attach intra-group positions.

    Output schema
    -------------
    trace_id (= group_value), activity, start_timestamp, attributes
    [, position (= group_pos)  — only when has_pos is True]

    Parameters
    ----------
    group_ids_filter : optional list of group_value strings to restrict
                       the read.  An empty list or None means all groups.
    """
    spark = get_spark_session()
    shared_df = storage.read_sequence_table(metadata)

    if has_pos:
        pos_path = _perspective_positions_path(metadata, pid)
        try:
            pos_df = spark.read.format("delta").load(pos_path)
        except Exception as exc:
            raise RuntimeError(
                f"_get_perspective_seq_df: positions table for '{pid}' "
                f"not found at {pos_path}.  "
                "Was promote_to_l2 called first?"
            ) from exc

        if group_ids_filter:
            pos_df = pos_df.filter(col("group_value").isin(group_ids_filter))

        # Join on the original event identity (trace_id, intra-trace position).
        joined = shared_df.join(
            pos_df.select("trace_id", "position", "group_value", "group_pos"),
            on=["trace_id", "position"],
            how="inner",
        )
        return joined.select(
            col("group_value").alias("trace_id"),
            col("activity"),
            col("start_timestamp"),
            col("attributes"),
            col("group_pos").alias("position"),
        )
    else:
        result = (
            shared_df
            .withColumn("group_value", _grouping_col(grouping_keys))
            .filter(col("group_value").isNotNull())
        )
        if group_ids_filter:
            result = result.filter(col("group_value").isin(group_ids_filter))
        return result.select(
            col("group_value").alias("trace_id"),
            col("activity"),
            col("start_timestamp"),
            col("attributes"),
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
    Register the perspective at L1.

    No per-perspective sequence table is written.  The shared
    SequenceTable is used directly by all downstream operations, with
    the grouping value computed on the fly via _grouping_col.

    Returns
    -------
    float
        Elapsed wall-clock time in milliseconds (nominally zero).
    """
    logger.info(
        f"AdaptiveBuilders: promote_to_l1 for perspective '{pid}' "
        f"(keys={grouping_keys}) — L1 uses shared SequenceTable; "
        "no materialisation needed."
    )
    return 0.0


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
    Build the compact positions overlay for the perspective.

    Reads the shared SequenceTable, assigns a 0-indexed intra-group
    position to each event within its group (ordered by start_timestamp),
    writes the compact positions overlay (trace_id, original position,
    group_value, group_pos) to Delta, and bootstraps SequenceMetadata
    with the last assigned group_pos per group.

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

    seq_df = storage.read_sequence_table(metadata)

    # Compute grouping value; filter events that don't carry the perspective's attributes.
    grouped_df = (
        seq_df
        .withColumn("group_value", _grouping_col(grouping_keys))
        .filter(col("group_value").isNotNull())
    )

    # Assign 0-indexed intra-group position ordered by timestamp.
    window = Window.partitionBy("group_value").orderBy("start_timestamp")
    positioned_df = grouped_df.withColumn(
        "group_pos", (row_number().over(window) - 1).cast("integer")
    )

    # Write compact positions overlay (no attributes — just identity + group info).
    positions_path = _perspective_positions_path(metadata, pid)
    (
        positioned_df
        .select(
            col("trace_id"),   # original case ID
            col("position"),   # original intra-trace position (join key)
            col("group_value"),
            col("group_pos"),
        )
        .write
        .format("delta")
        .partitionBy("group_value")
        .mode("overwrite")
        .save(positions_path)
    )

    # Bootstrap SequenceMetadata: group_value → last_pos.
    meta_df = (
        positioned_df
        .groupBy("group_value")
        .agg(F.max("group_pos").cast("integer").alias("last_pos"))
        .withColumnRenamed("group_value", "trace_id")
    )
    meta_path = _perspective_seq_metadata_path(metadata, pid)
    meta_df.write.format("delta").mode("overwrite").save(meta_path)

    elapsed = (time.time() - t0) * 1000
    logger.info(
        f"AdaptiveBuilders: L2 promotion for '{pid}' completed in "
        f"{elapsed:.1f}ms."
    )
    return elapsed


# ===========================================================================
# Per-batch perspective maintenance
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
    Update perspective bookkeeping for the current ingest batch.

    At L1 (has_pos=False) this is a no-op: the shared SequenceTable is
    kept current by the eager indexer, and reads always go to that table.

    At L2 (has_pos=True) new intra-group positions are computed for
    every qualifying event in the batch and appended to the compact
    positions overlay.  SequenceMetadata is updated via MERGE so that
    subsequent batches continue from the correct offset.

    Returns
    -------
    Tuple[float, float]
        (l1_ms, l2_ms) — same interface as before for retention accounting.
        Both are 0.0 at L1.
    """
    if not has_pos:
        return 0.0, 0.0

    spark = get_spark_session()
    t1 = time.time()

    # Compute group value for each new event, keeping original trace_id and position.
    new_events = (
        batch_activity_df
        .withColumn("group_value", _grouping_col(grouping_keys))
        .filter(col("group_value").isNotNull())
    )

    l1_ms = (time.time() - t1) * 1000

    # ------------------------------------------------------------------
    # Phase 2: extend intra-group positions from SequenceMetadata.
    # ------------------------------------------------------------------
    t2 = time.time()

    meta_path = _perspective_seq_metadata_path(metadata, pid)
    try:
        meta_df = spark.read.format("delta").load(meta_path)
    except Exception:
        meta_df = spark.createDataFrame([], schema=_seq_metadata_schema())

    # Join to get the last_pos for each affected group (default -1 → first pos = 0).
    new_events_with_offset = (
        new_events
        .join(
            meta_df.select(
                col("trace_id").alias("group_key"),
                col("last_pos"),
            ),
            col("group_value") == col("group_key"),
            how="left",
        )
        .withColumn(
            "last_pos",
            coalesce(col("last_pos"), lit(-1)).cast("integer"),
        )
        .drop("group_key")
    )

    window = Window.partitionBy("group_value").orderBy("start_timestamp")
    new_events_positioned = new_events_with_offset.withColumn(
        "group_pos",
        (col("last_pos") + row_number().over(window)).cast("integer"),
    ).drop("last_pos")

    # Append compact positions (no attributes) to the positions overlay.
    positions_path = _perspective_positions_path(metadata, pid)
    (
        new_events_positioned
        .select(
            col("trace_id"),   # original case ID
            col("position"),   # original intra-trace position
            col("group_value"),
            col("group_pos"),
        )
        .write
        .format("delta")
        .partitionBy("group_value")
        .mode("append")
        .option("mergeSchema", "true")
        .save(positions_path)
    )

    # Update SequenceMetadata with the new last positions per group.
    new_meta = (
        new_events_positioned
        .groupBy("group_value")
        .agg(F.max("group_pos").cast("integer").alias("last_pos"))
        .withColumnRenamed("group_value", "trace_id")
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
        f"AdaptiveBuilders: '{pid}' positions updated "
        f"(l1={l1_ms:.1f}ms, l2={l2_ms:.1f}ms)."
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
    grouping_keys: List[str],
    metadata: MetaData,
    storage: StorageManager,
    has_pos: bool,
) -> float:
    """
    Build the full historical PairsIndex for (act_a, act_b) under
    perspective pid.

    Reads the shared SequenceTable (joined with the positions overlay if
    has_pos), extracts all STNM pairs, writes to the per-pair PairsIndex
    Delta table, and bootstraps the perspective's LastChecked table.

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

    seq_df = _get_perspective_seq_df(
        pid=pid,
        grouping_keys=grouping_keys,
        metadata=metadata,
        storage=storage,
        has_pos=has_pos,
    )

    pairs_df, last_checked_df = _extract_single_pair_from_df(
        seq_df=seq_df,
        act_a=act_a,
        act_b=act_b,
        lookback_str=lookback,
        previous_lc_df=None,
        batch_min_ts=None,
        has_pos=has_pos,
    )

    if pairs_df.rdd.isEmpty():
        logger.info(
            f"AdaptiveBuilders: no pairs found for ({act_a},{act_b}) "
            f"under '{pid}' — writing empty tables."
        )

    pairs_path = _perspective_pair_path(metadata, pid, act_a, act_b)
    (
        pairs_df.write
        .format("delta")
        .mode("overwrite")
        .save(pairs_path)
    )

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
    grouping_keys: List[str],
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
    grouping_keys       : attribute keys defining phi_G (needed for the
                         on-the-fly group value computation at L1).

    Returns
    -------
    DataFrame
        Pairs in EventPair schema, with "trace_id" holding the group value v.
    """
    seq_df = _get_perspective_seq_df(
        pid=pid,
        grouping_keys=grouping_keys,
        metadata=metadata,
        storage=storage,
        has_pos=has_pos,
        group_ids_filter=candidate_group_ids if candidate_group_ids else None,
    )

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
    Maintain the positions overlay (L2 only) and all L3 pair indices
    for one ingest batch.

    At L1 the sequence-table bookkeeping is a no-op because the shared
    SequenceTable is kept current by the eager indexer.  At L2 the
    compact positions overlay is extended before pair extraction.

    Returns
    -------
    dict
        {(A, B): elapsed_ms} for each processed pair.
    """
    spark = get_spark_session()

    # ------------------------------------------------------------------
    # Step 1: Update positions overlay for L2 perspectives.
    # At L1 this is a no-op (0.0, 0.0).
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
    # Step 3: Read the shared SequenceTable (+ positions overlay for L2)
    # once for all pairs in this batch.
    # ------------------------------------------------------------------
    try:
        seq_df = _get_perspective_seq_df(
            pid=pid,
            grouping_keys=grouping_keys,
            metadata=metadata,
            storage=storage,
            has_pos=has_pos,
        )
    except Exception as exc:
        logger.error(
            f"AdaptiveBuilders: cannot build seq_df for '{pid}': {exc}"
        )
        return {}

    # ------------------------------------------------------------------
    # Step 4: Read the entire LastChecked table for this perspective once.
    # ------------------------------------------------------------------
    lc_path = _perspective_last_checked_path(metadata, pid)
    try:
        all_lc_df = spark.read.format("delta").load(lc_path)
    except Exception:
        all_lc_df = spark.createDataFrame([], schema=Last_Checked_table_schema)

    real_lookback = _parse_lookback(lookback)
    # batch_min_pos is used only for position-based lookback pruning.
    # At L2 the relevant unit is the intra-group position, which is not
    # available as a batch aggregate here; 0 is a safe conservative default.
    batch_min_pos = 0

    # ------------------------------------------------------------------
    # Step 5: Per-pair extraction and write.
    # ------------------------------------------------------------------
    pair_elapsed: Dict[Tuple[str, str], float] = {}

    for (act_a, act_b) in persistent_pairs:
        t0 = time.time()

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

        pair_seq_df = seq_df.join(pair_affected, on="trace_id", how="inner")

        pair_lc_df = (
            all_lc_df
            .filter(
                (col("source") == act_a) & (col("target") == act_b)
            )
            .join(pair_affected, on="trace_id", how="inner")
        )

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

        pairs_path = _perspective_pair_path(metadata, pid, act_a, act_b)
        (
            new_pairs_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(pairs_path)
        )

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

# ===========================================================================
# L3-: BATCHED transient pair build — one scan for many pairs
# ===========================================================================
 
def build_pairs_transient_batched(
    pid: str,
    pairs: List[Tuple[str, str]],
    lookback: str,
    lookback_mode: str,
    candidate_group_ids: List[str],
    grouping_keys: List[str],
    metadata: MetaData,
    storage: StorageManager,
    has_pos: bool,
) -> Dict[Tuple[str, str], list]:
    """
    Transient extraction for MANY pairs with ONE SequenceTable scan.
 
    The per-pair path (build_pair_transient) re-reads the perspective
    sequence DataFrame for every pair, so a cold length-n pattern pays
    C(n,2) full scans.  Here the seq_df — slimmed to the activities the
    requested pairs reference — is persisted once; the unchanged
    _extract_single_pair_from_df then runs per pair against the cached
    data, and the result rows are collected per pair for the caller's
    LRU.  Extraction logic is reused verbatim, so results are
    bit-identical to the per-pair path.
 
    Returns
    -------
    dict
        {(act_a, act_b): [Row, ...]} in EventPair schema, with
        "trace_id" holding the group value v (same as the per-pair
        path).  Pairs with no co-occurrences map to [].
    """
    from pyspark import StorageLevel
    from pyspark.sql.functions import col as _col
 
    if not pairs:
        return {}
 
    acts = sorted({a for a, _b in pairs} | {b for _a, b in pairs})
 
    seq_df = _get_perspective_seq_df(
        pid=pid,
        grouping_keys=grouping_keys,
        metadata=metadata,
        storage=storage,
        has_pos=has_pos,
        group_ids_filter=candidate_group_ids if candidate_group_ids else None,
    ).filter(_col("activity").isin(acts))
 
    seq_df = seq_df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        seq_df.count()  # materialise the shared scan exactly once
 
        out: Dict[Tuple[str, str], list] = {}
        for (act_a, act_b) in pairs:
            pairs_df, _lc = _extract_single_pair_from_df(
                seq_df=seq_df,
                act_a=act_a,
                act_b=act_b,
                lookback_str=lookback,
                previous_lc_df=None,
                batch_min_ts=None,
                has_pos=has_pos,
            )
            out[(act_a, act_b)] = pairs_df.collect()
        return out
    finally:
        seq_df.unpersist()
        
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

    Expects seq_df to have schema:
        trace_id (= group_value), activity, start_timestamp, attributes
        [, position (= group_pos)  — when has_pos is True]

    This function is unchanged from the previous implementation; the
    schema contract is now fulfilled by _get_perspective_seq_df.
    """
    spark = get_spark_session()
    real_lookback = _parse_lookback(lookback_str)

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

    if previous_lc_df is not None and not previous_lc_df.rdd.isEmpty():
        lc_map_rdd = previous_lc_df.rdd.map(
            lambda row: (row.trace_id, row.last_checked_moment)
        ).collectAsMap()
    else:
        lc_map_rdd = {}

    lc_broadcast = spark.sparkContext.broadcast(lc_map_rdd)

    def extract_for_group(kv):
        group_id, events = kv
        events = list(events)

        if not has_pos:
            events.sort(key=lambda e: (e[1], e[0]))
            events = [
                (act, ts, idx, attrs)
                for idx, (act, ts, _, attrs) in enumerate(events)
            ]

        activity_map = defaultdict(list)
        for activity, ts, pos, attrs in events:
            activity_map[activity].append((ts, pos, attrs))

        for k in activity_map:
            activity_map[k].sort(key=lambda x: x[1])

        e_source = activity_map.get(act_a, [])
        e_target = activity_map.get(act_b, [])

        if not e_source or not e_target:
            return [], []

        last_ts = lc_broadcast.value.get(group_id, None)
        pairs = createTuples(
            act_a, act_b, e_source, e_target,
            real_lookback, last_ts, group_id,
        )
        lc = [(group_id, act_a, act_b, pairs[-1][4])] if pairs else []
        return pairs, lc

    full_rdd = (
        trace_rdd
        .groupByKey()
        .map(extract_for_group)
    )

    pairs_flat = full_rdd.flatMap(lambda x: x[0])
    lc_flat    = full_rdd.flatMap(lambda x: x[1])

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
