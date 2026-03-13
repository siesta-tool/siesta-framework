from siesta.core.storageFactory import get_storage_manager
from siesta.model.StorageModel import MetaData
from siesta.model.MiningModel import Constraint
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, max as _max, lit, coalesce, countDistinct

import logging
logger = logging.getLogger("Mining")


def _update_all_activity_pairs(metadata: MetaData) -> DataFrame:
    """
    Incrementally maintain the all_activity_pairs Delta table.
    Detects new activities from the current activity index that don't yet
    appear in the stored pairs table, generates their cross-product pairs,
    and appends them.

    Returns the up-to-date all_pairs DataFrame (source < target).
    """
    storage = get_storage_manager()

    all_activities = storage.read_activity_index(metadata) \
        .select("activity").distinct()

    stored_pairs = storage.read_all_activity_pairs(metadata)

    # Current complete pair set (canonical: source < target)
    current_all_pairs = all_activities.alias("a").crossJoin(all_activities.alias("b")) \
        .filter(col("a.activity") < col("b.activity")) \
        .select(col("a.activity").alias("source"), col("b.activity").alias("target"))

    # New pairs not yet in the stored table
    new_pairs = current_all_pairs.join(stored_pairs, on=["source", "target"], how="left_anti")

    # Persist the updated table (full overwrite is cheap for this small table)
    storage.write_all_activity_pairs(metadata, current_all_pairs)

    return current_all_pairs, new_pairs


def discover_negations(evolved_df: DataFrame, metadata: MetaData,
                       include_trace_lists: bool = False) -> DataFrame:
    """
    Mine not-coexistence constraints: pairs (a,b) that do NOT both appear
    in a trace.  This is the exact complement of the coexistence template
    produced by the unordered miner.

    Two execution modes controlled by ``include_trace_lists``:

    **Support-only** (``include_trace_lists=False``, default):
        Computes support as ``(total_traces - coex_count) / total_traces``
        per pair with a single aggregation + join — no cross-join, no
        per-trace enumeration.  Returns one row per pair with a
        ``_support_count`` column (trace_id is NULL).

    **Trace-list** (``include_trace_lists=True``):
        Incrementally maintains per-(pair, trace) rows in storage.
        For evolved traces and newly discovered pairs, scoped cross-joins
        are used so we never explode the full pairs × traces space
        unnecessarily.

    :param evolved_df:  DataFrame[Event] with events after the last mining
                        timestamp.
    :param metadata:    MetaData with storage paths and log context.
    :param include_trace_lists:
        When True, produce one ConstraintEntry row per (pair, trace).
        When False, produce one row per pair with ``_support_count``.
    :return: DataFrame compatible with the mining output pipeline.
    """
    storage = get_storage_manager()

    # --- 1. Incrementally maintain all-activity-pairs ----------------------
    all_pairs, new_pairs = _update_all_activity_pairs(metadata)

    # --- 2. Read current coexistence constraints (already up-to-date) ------
    unordered = storage.read_unordered_constraints(metadata)
    coex = unordered.filter(col("template") == "coexistence") \
        .select("source", "target", "trace_id")

    # ------------------------------------------------------------------
    # SUPPORT-ONLY MODE
    # ------------------------------------------------------------------
    if not include_trace_lists:
        coex_counts = coex.groupBy("source", "target").agg(
            countDistinct("trace_id").alias("coex_count")
        )

        total_traces = metadata.trace_count

        not_coex = all_pairs.join(coex_counts, on=["source", "target"], how="left") \
            .withColumn("coex_count", coalesce(col("coex_count"), lit(0))) \
            .withColumn("_support_count", lit(total_traces) - col("coex_count")) \
            .withColumn("template", lit("not_coexistence")) \
            .withColumn("occurrences", lit(None).cast("int")) \
            .withColumn("trace_id", lit(None).cast("string")) \
            .select("template", "source", "trace_id", "target", "occurrences",
                    "_support_count")

        logger.info("Negation mining (support-only mode) complete.")
        return not_coex

    # ------------------------------------------------------------------
    # TRACE-LIST MODE  (incremental)
    # ------------------------------------------------------------------
    all_trace_ids = storage.read_activity_index(metadata) \
        .select("trace_id").distinct()

    # Read & materialize old negation constraints before overwriting
    old_negation = storage.read_negation_constraints(metadata).cache()
    old_negation.count()

    evolved_trace_ids = evolved_df.select("trace_id").distinct()

    # Keep constraints for traces that did NOT evolve AND pairs that are
    # not brand-new (new pairs need computation across all traces).
    unchanged = old_negation \
        .join(evolved_trace_ids, on="trace_id", how="left_anti") \
        .join(new_pairs, on=["source", "target"], how="left_anti")

    # -- Evolved traces × ALL pairs (scoped cross-join) --
    evolved_combos = all_pairs.crossJoin(evolved_trace_ids)
    evolved_coex = coex.join(evolved_trace_ids, on="trace_id", how="inner")
    new_for_evolved = evolved_combos.join(
        evolved_coex, on=["source", "target", "trace_id"], how="left_anti"
    ).withColumn("template", lit("not_coexistence")) \
     .withColumn("occurrences", lit(None).cast("int")) \
     .select("template", "source", "trace_id", "target", "occurrences")

    # -- New pairs × ALL traces (scoped cross-join) --
    # Exclude evolved traces (already handled above) to avoid duplicates
    non_evolved_traces = all_trace_ids.join(
        evolved_trace_ids, on="trace_id", how="left_anti"
    )
    new_pair_combos = new_pairs.crossJoin(non_evolved_traces)
    new_pair_coex = coex.join(new_pairs, on=["source", "target"], how="inner") \
        .join(non_evolved_traces, on="trace_id", how="inner")
    new_for_new_pairs = new_pair_combos.join(
        new_pair_coex, on=["source", "target", "trace_id"], how="left_anti"
    ).withColumn("template", lit("not_coexistence")) \
     .withColumn("occurrences", lit(None).cast("int")) \
     .select("template", "source", "trace_id", "target", "occurrences")

    # Union all three parts
    negation_constraints = unchanged \
        .unionByName(new_for_evolved, allowMissingColumns=True) \
        .unionByName(new_for_new_pairs, allowMissingColumns=True)

    negation_constraints = negation_constraints.cache()
    negation_constraints.count()
    old_negation.unpersist()

    storage.write_negation_constraints(metadata, negation_constraints)

    logger.info("Negation mining (trace-list mode) complete.")
    return negation_constraints