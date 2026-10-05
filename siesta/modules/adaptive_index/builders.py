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

Position semantics: at L1 (has_pos=False) _get_perspective_seq_df ranks
each group's full event sequence by timestamp (ties: activity, trace_id,
position), before any activity filter, so every pair table of a perspective
uses the same positions.  At L2 (has_pos=True) group_pos values from the
overlay are used.
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
    group_ids_df: Optional[DataFrame] = None,
) -> DataFrame:
    """
    Return a DataFrame suitable for STNM pair extraction under perspective pid.

    Reads the shared SequenceTable, computes phi_G on the fly, and —
    if the perspective is at L2 — joins with the compact positions
    overlay to attach intra-group positions.

    Output schema
    -------------
    trace_id (= group_value), activity, start_timestamp, attributes, position

    ``position`` is the intra-group position: the positions overlay's
    ``group_pos`` at L2, and otherwise the event's rank within its group
    ordered by (start_timestamp, activity, trace_id, position).  The L1 rank
    is computed over the group's full sequence, before any activity filter,
    so pair tables built from filtered reads (e.g. batched transient builds)
    agree with those built from full reads.

    Parameters
    ----------
    group_ids_filter : optional list of group_value strings to restrict
                       the read.  An empty list or None means all groups.
    group_ids_df     : optional single-column DataFrame (``trace_id`` =
                       group value) of the groups to keep.  Applied as a
                       semi-join before positions are assigned, so it
                       scales to many groups where ``isin`` does not.
                       Whole groups are kept, so positions are unchanged.
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
        if group_ids_df is not None:
            pos_df = pos_df.join(
                group_ids_df.select(col("trace_id").alias("group_value")),
                on="group_value", how="left_semi",
            )

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
        if group_ids_df is not None:
            result = result.join(
                group_ids_df.select(col("trace_id").alias("group_value")),
                on="group_value", how="left_semi",
            )
        # Timestamp ties keep the trace order (trace_id, position), as in the
        # eager index; the L1/L2 position windows below use the same order.
        group_order = Window.partitionBy("group_value").orderBy(
            col("start_timestamp"), col("trace_id"), col("position")
        )
        return result.select(
            col("group_value").alias("trace_id"),
            col("activity"),
            col("start_timestamp"),
            col("attributes"),
            (F.row_number().over(group_order) - 1).alias("position"),
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
    window = Window.partitionBy("group_value").orderBy("start_timestamp", "trace_id", "position")
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

    window = Window.partitionBy("group_value").orderBy("start_timestamp", "trace_id", "position")
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
        _for_storage(pairs_df, metadata).coalesce(1).write
        .format("delta")
        .mode("overwrite")
        .save(pairs_path)
    )
    _pair_written(pairs_path)

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
    timings: Optional[Dict[str, float]] = None,
) -> Dict[Tuple[str, str], float]:
    """
    Maintain the positions overlay (L2 only) and all L3 pair indices
    for one ingest batch.

    At L1 the sequence-table bookkeeping is a no-op because the shared
    SequenceTable is kept current by the eager indexer.  At L2 the
    compact positions overlay is extended before pair extraction.

    Only groups that received new events of a persistent pair's
    activities are re-read (Algorithm 1: updated groups V).  Their events,
    restricted to those activities, are read once; one grouped pass
    extracts the new STNM instances of every persistent pair (each group
    only for the pairs whose activities it received), guided by the
    LastChecked watermarks.  Each pair's new rows are appended to its
    PairsIndex table (appends run concurrently) and the LastChecked rows
    of all pairs are merged in one MERGE.

    ``timings``, when given, receives ``l2_ms`` (positions overlay),
    ``extract_ms`` (read + extraction), ``write_ms`` (pair appends) and
    ``last_checked_ms`` (LastChecked merge).

    Returns
    -------
    dict
        {(A, B): elapsed_ms} for each persistent pair.  The shared
        extraction and LastChecked merge are attributed to the pairs in
        proportion to their new rows; the append is the pair's own.
    """
    from pyspark import StorageLevel

    spark = get_spark_session()
    timings = timings if timings is not None else {}
    for k in ("extract_ms", "write_ms", "last_checked_ms"):
        timings[k] = 0.0

    # ------------------------------------------------------------------
    # Step 1: Update positions overlay for L2 perspectives.
    # At L1 this is a no-op (0.0, 0.0).
    # ------------------------------------------------------------------
    _l1_ms, l2_ms = incremental_update_perspective(
        pid=pid,
        grouping_keys=grouping_keys,
        batch_activity_df=batch_activity_df,
        metadata=metadata,
        storage=storage,
        has_pos=has_pos,
    )
    timings["l2_ms"] = l2_ms

    if not persistent_pairs:
        return {}

    t_extract = time.time()
    pair_acts = sorted({a for a, _ in persistent_pairs} | {b for _, b in persistent_pairs})

    # ------------------------------------------------------------------
    # Step 2: (group, activity) of this batch's new events.
    # ------------------------------------------------------------------
    affected_df = (
        batch_activity_df
        .withColumn("trace_id", _grouping_col(grouping_keys))
        .filter(col("trace_id").isNotNull())
        .filter(col("activity").isin(pair_acts))
        .select("trace_id", "activity")
        .distinct()
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    cached: list[DataFrame] = [affected_df]
    try:
        if affected_df.count() == 0:
            timings["extract_ms"] = (time.time() - t_extract) * 1000
            return {pair: 0.0 for pair in persistent_pairs}

        # Per group, the activities that received events: a pair is
        # re-extracted in a group only if one of its activities did.
        new_acts_df = affected_df.groupBy("trace_id").agg(
            F.collect_set("activity").alias("new_acts")
        )

        # --------------------------------------------------------------
        # Step 3: The affected groups' events of the pairs' activities.
        # Positions are assigned over each whole group before the
        # activity filter, so they match a full read.
        # --------------------------------------------------------------
        seq_slice = (
            _get_perspective_seq_df(
                pid=pid,
                grouping_keys=grouping_keys,
                metadata=metadata,
                storage=storage,
                has_pos=has_pos,
                group_ids_df=new_acts_df.select("trace_id"),
            )
            .filter(col("activity").isin(pair_acts))
        )

        # --------------------------------------------------------------
        # Step 4: LastChecked rows of these pairs and groups.  Read as a
        # local checkpoint: it is MERGEd into below.
        # --------------------------------------------------------------
        lc_path = _perspective_last_checked_path(metadata, pid)
        try:
            all_lc_df = spark.read.format("delta").load(lc_path)
        except Exception:
            all_lc_df = spark.createDataFrame([], schema=Last_Checked_table_schema)
        pair_keys = spark.createDataFrame(
            [(a, b) for a, b in persistent_pairs], "source string, target string"
        )
        lc_slice = (
            all_lc_df
            .join(F.broadcast(pair_keys), on=["source", "target"], how="left_semi")
            .join(new_acts_df.select("trace_id"), on="trace_id", how="left_semi")
            .select("trace_id", "source", "target", "last_checked_moment")
            .localCheckpoint(eager=True)
        )

        # --------------------------------------------------------------
        # Step 5: One grouped extraction for all pairs.
        # --------------------------------------------------------------
        new_pairs_df, new_lc_df, grouped_rdd = _extract_pairs_multi(
            seq_df=seq_slice,
            pairs=persistent_pairs,
            lookback_str=lookback,
            previous_lc_df=lc_slice,
            has_pos=has_pos,
            new_acts_df=new_acts_df,
        )
        new_pairs_df = new_pairs_df.persist(StorageLevel.MEMORY_AND_DISK)
        new_lc_df = new_lc_df.persist(StorageLevel.MEMORY_AND_DISK)
        cached += [new_pairs_df, new_lc_df, grouped_rdd]
        rows_per_pair = {
            (r.source, r.target): r["count"]
            for r in new_pairs_df.groupBy("source", "target").count().collect()
        }
        timings["extract_ms"] = (time.time() - t_extract) * 1000

        # --------------------------------------------------------------
        # Step 6: Append each pair's new rows to its PairsIndex table.
        # --------------------------------------------------------------
        t_write = time.time()
        write_ms = _append_pairs_concurrently(
            new_pairs_df, list(rows_per_pair), metadata, pid, mode="append",
        )
        timings["write_ms"] = (time.time() - t_write) * 1000

        # --------------------------------------------------------------
        # Step 7: Merge the LastChecked rows of all pairs at once.
        # --------------------------------------------------------------
        t_lc = time.time()
        if rows_per_pair:
            merged_lc_df = _merge_last_checked_multi(
                previous_lc_df=lc_slice,
                current_lc_df=new_lc_df,
                batch_min_ts=batch_min_ts,
                real_lookback=_parse_lookback(lookback),
            )
            _upsert_last_checked(spark, merged_lc_df, lc_path)
        timings["last_checked_ms"] = (time.time() - t_lc) * 1000

        shared_ms = timings["extract_ms"] + timings["last_checked_ms"]
        total_rows = sum(rows_per_pair.values())
        pair_elapsed: Dict[Tuple[str, str], float] = {}
        for pair in persistent_pairs:
            n = rows_per_pair.get(pair, 0)
            share = shared_ms * n / total_rows if total_rows else shared_ms / len(persistent_pairs)
            pair_elapsed[pair] = share + write_ms.get(pair, 0.0)
        return pair_elapsed
    finally:
        for df in cached:
            df.unpersist()


def build_pairs_persistent_batched(
    pid: str,
    pairs: List[Tuple[str, str]],
    lookback: str,
    lookback_mode: str,
    grouping_keys: List[str],
    metadata: MetaData,
    storage: StorageManager,
    has_pos: bool,
) -> Dict[Tuple[str, str], float]:
    """
    Full historical PairsIndex build for many pairs with one read and one
    grouped extraction (the batched counterpart of build_pair_persistent).
    Each pair's table is overwritten; the LastChecked rows of all pairs
    are merged at once.  Pairs without instances get an empty table, as
    build_pair_persistent does.

    Returns {(A, B): elapsed_ms}, the shared cost split evenly plus the
    pair's own write.
    """
    from pyspark import StorageLevel

    if not pairs:
        return {}
    spark = get_spark_session()
    t0 = time.time()
    pair_acts = sorted({a for a, _ in pairs} | {b for _, b in pairs})
    seq_df = _get_perspective_seq_df(
        pid=pid,
        grouping_keys=grouping_keys,
        metadata=metadata,
        storage=storage,
        has_pos=has_pos,
    ).filter(col("activity").isin(pair_acts))

    pairs_df, lc_df, grouped_rdd = _extract_pairs_multi(
        seq_df=seq_df, pairs=pairs, lookback_str=lookback,
        previous_lc_df=None, has_pos=has_pos,
    )
    pairs_df = pairs_df.persist(StorageLevel.MEMORY_AND_DISK)
    lc_df = lc_df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        pairs_df.count()
        extract_ms = (time.time() - t0) * 1000
        write_ms = _append_pairs_concurrently(pairs_df, list(pairs), metadata, pid, mode="overwrite")
        t_lc = time.time()
        _upsert_last_checked(spark, lc_df, _perspective_last_checked_path(metadata, pid))
        shared = (extract_ms + (time.time() - t_lc) * 1000) / len(pairs)
        return {pair: shared + write_ms.get(pair, 0.0) for pair in pairs}
    finally:
        pairs_df.unpersist()
        lc_df.unpersist()
        grouped_rdd.unpersist()


def _pair_written(path: str) -> None:
    """A pair table changed: drop its cached file list (see state.pair_table_files)."""
    from siesta.modules.adaptive_query.state import invalidate_pair_files
    invalidate_pair_files(path)


def _for_storage(pairs_df: DataFrame, metadata: MetaData) -> DataFrame:
    """Pair rows as written to a PairsIndex table: attributes embedded unless disabled for the log."""
    from siesta.modules.adaptive_query.state import get_log_option
    if get_log_option(metadata, "embed_pair_attributes", True):
        return pairs_df
    from siesta.modules.index.pair_attributes import strip_pair_attributes
    return strip_pair_attributes(pairs_df)


def _append_pairs_concurrently(
    pairs_df: DataFrame,
    pairs: List[Tuple[str, str]],
    metadata: MetaData,
    pid: str,
    mode: str,
    max_workers: int = 8,
) -> Dict[Tuple[str, str], float]:
    """
    Write each pair's rows of ``pairs_df`` to its own PairsIndex table.
    The writes are independent Delta commits on different tables, so they
    run from a thread pool.  Returns {(A, B): elapsed_ms}.
    """
    from concurrent.futures import ThreadPoolExecutor

    pairs_df = _for_storage(pairs_df, metadata)

    def _write(pair):
        a, b = pair
        t0 = time.time()
        (
            pairs_df
            .where((col("source") == a) & (col("target") == b))
            .coalesce(1)  # one file per pair and batch; pair tables are small
            .write
            .format("delta")
            .mode(mode)
            .option("mergeSchema", "true")
            .save(_perspective_pair_path(metadata, pid, a, b))
        )
        _pair_written(_perspective_pair_path(metadata, pid, a, b))
        return pair, (time.time() - t0) * 1000

    if not pairs:
        return {}
    with ThreadPoolExecutor(max_workers=min(max_workers, len(pairs))) as ex:
        return dict(ex.map(_write, pairs))


def _merge_last_checked_multi(
    previous_lc_df: Optional[DataFrame],
    current_lc_df: DataFrame,
    batch_min_ts: int,
    real_lookback,
) -> DataFrame:
    """
    Multi-pair counterpart of update_last_checked: keep a previous row
    unless the current batch produced one for the same (group, pair),
    then prune rows that fell out of the lookback.
    """
    if previous_lc_df is not None:
        unchanged = previous_lc_df.join(
            current_lc_df.select("trace_id", "source", "target"),
            on=["trace_id", "source", "target"],
            how="left_anti",
        )
        merged = unchanged.unionByName(current_lc_df)
    else:
        merged = current_lc_df
    if real_lookback[1] == "time":
        return merged.filter(
            (col("last_checked_moment") >= batch_min_ts)
            | ((batch_min_ts - col("last_checked_moment")) < real_lookback[0] / 1000)
        )
    # Index-based lookback: batch_min_pos is not tracked (see
    # update_last_checked callers), so 0 is the conservative bound.
    return merged.filter(
        (col("last_checked_moment") >= 0)
        | ((0 - col("last_checked_moment")) < real_lookback[0])
    )


def _extract_pairs_multi(
    seq_df: DataFrame,
    pairs: List[Tuple[str, str]],
    lookback_str: str,
    previous_lc_df: Optional[DataFrame],
    has_pos: bool,
    new_acts_df: Optional[DataFrame] = None,
) -> Tuple[DataFrame, DataFrame]:
    """
    Extract the STNM instances of several pairs in one grouped pass.
    Returns (pairs_df, last_checked_df, grouped_rdd); the grouped RDD is
    persisted and must be unpersisted by the caller.

    Per group this is exactly _extract_single_pair_from_df applied to each
    pair: the same createTuples call with the pair's LastChecked watermark
    for the group.  The watermarks come in through a cogroup instead of a
    driver-side map, so the pass scales with the number of groups.

    ``new_acts_df`` (trace_id, new_acts) restricts each group to the pairs
    with an activity among the group's new events, mirroring the per-pair
    "affected groups" filter of incremental maintenance.
    """
    spark = get_spark_session()
    real_lookback = _parse_lookback(lookback_str)
    uses_pos = "position" in seq_df.columns
    pairs_b = spark.sparkContext.broadcast(list(pairs))

    events_rdd = seq_df.rdd.map(
        lambda row: (
            row.trace_id,
            ("e", (row.activity, row.start_timestamp,
                   row.position if uses_pos else 0, row.attributes)),
        )
    )
    tagged = [events_rdd]
    if previous_lc_df is not None:
        tagged.append(previous_lc_df.rdd.map(
            lambda r: (r.trace_id, ("w", (r.source, r.target, r.last_checked_moment)))
        ))
    if new_acts_df is not None:
        tagged.append(new_acts_df.rdd.map(
            lambda r: (r.trace_id, ("n", tuple(r.new_acts)))
        ))
    keyed = tagged[0] if len(tagged) == 1 else spark.sparkContext.union(tagged)

    def extract_for_group(kv):
        group_id, items = kv
        events, watermarks, new_acts = [], {}, None
        for tag, v in items:
            if tag == "e":
                events.append(v)
            elif tag == "w":
                watermarks[(v[0], v[1])] = v[2]
            else:
                new_acts = set(v)
        if not events:
            return [], []

        if not uses_pos:
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

        out_pairs, out_lc = [], []
        for act_a, act_b in pairs_b.value:
            if new_acts is not None and act_a not in new_acts and act_b not in new_acts:
                continue
            e_source = activity_map.get(act_a)
            e_target = activity_map.get(act_b)
            if not e_source or not e_target:
                continue
            found = createTuples(
                act_a, act_b, e_source, e_target,
                real_lookback, watermarks.get((act_a, act_b)), group_id,
            )
            if found:
                out_pairs.extend(found)
                out_lc.append((group_id, act_a, act_b, found[-1][4]))
        return out_pairs, out_lc

    from pyspark import StorageLevel

    # Both outputs derive from this RDD; persist it so the second is not a
    # second read + extraction.  The caller unpersists it.
    full_rdd = keyed.groupByKey().map(extract_for_group).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    pairs_df = spark.createDataFrame(full_rdd.flatMap(lambda x: x[0]), schema=EventPair.get_schema())
    lc_df = spark.createDataFrame(full_rdd.flatMap(lambda x: x[1]), schema=Last_Checked_table_schema)
    return pairs_df, lc_df, full_rdd


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
    Transient extraction for MANY pairs with ONE SequenceTable scan and
    ONE grouped pass.
 
    The seq_df is slimmed to the activities the requested pairs reference,
    and _extract_pairs_multi extracts every pair per group in a single
    groupBy (the same createTuples call per pair as the per-pair path, so
    results are identical); the rows are collected once and split per
    pair for the caller's LRU.  A Spark job per pair cost seconds of
    scheduling each, which dominated cold long patterns.
 
    Returns
    -------
    dict
        {(act_a, act_b): [Row, ...]} in EventPair schema, with
        "trace_id" holding the group value v (same as the per-pair
        path).  Pairs with no co-occurrences map to [].
    """
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
 
    pairs_df, _lc, grouped_rdd = _extract_pairs_multi(
        seq_df=seq_df,
        pairs=list(pairs),
        lookback_str=lookback,
        previous_lc_df=None,
        has_pos=has_pos,
    )
    try:
        rows = pairs_df.collect()
    finally:
        grouped_rdd.unpersist()
 
    out: Dict[Tuple[str, str], list] = {tuple(p): [] for p in pairs}
    for r in rows:
        out.setdefault((r.source, r.target), []).append(r)
    return out
        
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
        [, position (= intra-group position, see _get_perspective_seq_df)]
    When no position column is present, positions are assigned per group
    from the (start_timestamp, activity) order of the rows given.

    This function is unchanged from the previous implementation; the
    schema contract is now fulfilled by _get_perspective_seq_df.
    """
    spark = get_spark_session()
    real_lookback = _parse_lookback(lookback_str)
    uses_pos = "position" in seq_df.columns

    trace_rdd = seq_df.rdd.map(
        lambda row: (
            row.trace_id,
            (
                row.activity,
                row.start_timestamp,
                row.position if uses_pos else 0,
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

        if not uses_pos:
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
