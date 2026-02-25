from typing import Any, Dict, List, Literal

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, functions as F, Window
from pyspark.sql.types import StringType, StructField, StructType

from siesta_framework.model.StorageModel import MetaData
from siesta_framework.core.storageFactory import get_storage_manager

# Output schema shared by both strategies
_CONSTRAINT_SCHEMA = StructType([
    StructField("trace_id", StringType(), False),
    StructField("template", StringType(), False),
    StructField("source", StringType(), False),
    StructField("target", StringType(), False),
])


# ---------------------------------------------------------------------------
# Pandas UDF: mine all ordered templates for a single trace
# ---------------------------------------------------------------------------
def _mine_trace_pandas(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Receives all events of ONE trace (columns: trace_id, activity, position).
    Returns a DataFrame with columns (trace_id, template, source, target).
    """
    trace_id = pdf["trace_id"].iloc[0]
    pdf = pdf.sort_values("position").reset_index(drop=True)
    activities = pdf["activity"].values
    n = len(activities)

    if n < 2:
        return pd.DataFrame(columns=["trace_id", "template", "source", "target"])

    # Pre-compute per-activity position lists for O(1) lookups
    act_positions: Dict[str, np.ndarray] = {}
    for act in np.unique(activities):
        act_positions[act] = np.where(activities == act)[0]

    next_activity = np.empty(n, dtype=object)
    next_activity[:-1] = activities[1:]
    next_activity[-1] = None

    prev_activity = np.empty(n, dtype=object)
    prev_activity[1:] = activities[:-1]
    prev_activity[0] = None

    rows: List = []

    # Iterate over all ordered (source, target) distinct pairs
    unique_acts = np.unique(activities)
    for src_act in unique_acts:
        src_indices = act_positions[src_act]
        for tgt_act in unique_acts:
            tgt_indices = act_positions[tgt_act]

            # Check if source ever appears before target
            if src_indices[0] >= tgt_indices[-1]:
                # No src position < any tgt position
                if not np.any(src_indices[:, None] < tgt_indices[None, :]):
                    continue

            # --- Response ---
            # Every src occurrence must have at least one later tgt
            is_response = True
            is_alt_response = True
            for i, s_idx in enumerate(src_indices):
                later = tgt_indices[tgt_indices > s_idx]
                if len(later) == 0:
                    is_response = False
                    is_alt_response = False
                    break
                # AlternateResponse: closest tgt before next src
                if is_alt_response and i + 1 < len(src_indices):
                    next_src_idx = src_indices[i + 1]
                    if later[0] >= next_src_idx:
                        is_alt_response = False

            # --- Precedence ---
            is_precedence = True
            is_alt_precedence = True
            for j, t_idx in enumerate(tgt_indices):
                earlier = src_indices[src_indices < t_idx]
                if len(earlier) == 0:
                    is_precedence = False
                    is_alt_precedence = False
                    break
                # AlternatePrecedence: closest src after previous tgt
                if is_alt_precedence and j > 0:
                    prev_tgt_idx = tgt_indices[j - 1]
                    if earlier[-1] <= prev_tgt_idx:
                        is_alt_precedence = False

            # --- ChainResponse ---
            is_chain_response = all(
                next_activity[s] == tgt_act for s in src_indices
            )

            # --- ChainPrecedence ---
            is_chain_precedence = all(
                prev_activity[t] == src_act for t in tgt_indices
            )

            # Skip if nothing matches
            if not any([
                is_response, is_precedence,
                is_alt_response, is_alt_precedence,
                is_chain_response, is_chain_precedence,
            ]):
                continue

            if is_response:
                rows.append((trace_id, "response", src_act, tgt_act))
            if is_precedence:
                rows.append((trace_id, "precedence", src_act, tgt_act))
            if is_response and is_precedence:
                rows.append((trace_id, "succession", src_act, tgt_act))
            if is_alt_response:
                rows.append((trace_id, "alternate_response", src_act, tgt_act))
            if is_alt_precedence:
                rows.append((trace_id, "alternate_precedence", src_act, tgt_act))
            if is_chain_response:
                rows.append((trace_id, "chain_response", src_act, tgt_act))
            if is_chain_precedence:
                rows.append((trace_id, "chain_precedence", src_act, tgt_act))
            if is_chain_response and is_chain_precedence:
                rows.append((trace_id, "chain_succession", src_act, tgt_act))

    return pd.DataFrame(rows, columns=["trace_id", "template", "source", "target"])


# ---------------------------------------------------------------------------
# Strategy: Pandas UDF (applyInPandas)
# ---------------------------------------------------------------------------
def _discover_ordered_pandas(evolved_df: DataFrame) -> DataFrame:
    """Mine ordered constraints using groupBy + applyInPandas (O(A²·N) per trace)."""
    events = evolved_df.select("trace_id", "activity", "position")
    return events.groupBy("trace_id").applyInPandas(_mine_trace_pandas, _CONSTRAINT_SCHEMA)


# ---------------------------------------------------------------------------
# Strategy: Spark joins (original)
# ---------------------------------------------------------------------------
def _discover_ordered_joins(evolved_df: DataFrame) -> DataFrame:
    """Mine ordered constraints using Spark-native joins and window functions."""
    src = evolved_df.select(
        F.col("trace_id"),
        F.col("activity").alias("source"),
        F.col("position").alias("src_pos"),
    ).cache()
    tgt = evolved_df.select(
        F.col("trace_id"),
        F.col("activity").alias("target"),
        F.col("position").alias("tgt_pos"),
    ).cache()

    distinct_pairs = (
        src.join(tgt, on="trace_id")
        .where(F.col("src_pos") < F.col("tgt_pos"))
        .select("trace_id", "source", "target")
        .distinct()
    ).cache()

    # -- Response & AlternateResponse -------------------------------------
    src_with_next = src.withColumn(
        "next_src_pos",
        F.lead("src_pos").over(
            Window.partitionBy("trace_id", "source").orderBy("src_pos")
        ),
    )

    src_tgt = (
        src_with_next.join(tgt, on="trace_id")
        .where(F.col("tgt_pos") > F.col("src_pos"))
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("trace_id", "source", "target", "src_pos")
                .orderBy("tgt_pos")
            ),
        )
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )

    src_tgt = src_tgt.withColumn(
        "alt_resp_ok",
        F.when(F.col("next_src_pos").isNull(), F.lit(True)).otherwise(
            F.col("tgt_pos") < F.col("next_src_pos")
        ),
    )

    total_src = src.groupBy("trace_id", "source").agg(
        F.count("*").alias("total_src_count")
    )

    resp_agg = src_tgt.groupBy("trace_id", "source", "target").agg(
        F.count("*").alias("resp_count"),
        F.sum(F.when(F.col("alt_resp_ok"), 1).otherwise(0)).alias("alt_resp_count"),
    )

    response_flags = (
        resp_agg.join(total_src, on=["trace_id", "source"])
        .withColumn("is_response", F.col("resp_count") == F.col("total_src_count"))
        .withColumn(
            "is_alternate_response",
            F.col("alt_resp_count") == F.col("total_src_count"),
        )
    )

    # -- Precedence & AlternatePrecedence ---------------------------------
    tgt_with_prev = tgt.withColumn(
        "prev_tgt_pos",
        F.lag("tgt_pos").over(
            Window.partitionBy("trace_id", "target").orderBy("tgt_pos")
        ),
    )

    tgt_src = (
        tgt_with_prev.join(src, on="trace_id")
        .where(F.col("src_pos") < F.col("tgt_pos"))
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("trace_id", "source", "target", "tgt_pos")
                .orderBy(F.desc("src_pos"))
            ),
        )
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )

    tgt_src = tgt_src.withColumn(
        "alt_prec_ok",
        F.when(F.col("prev_tgt_pos").isNull(), F.lit(True)).otherwise(
            F.col("src_pos") > F.col("prev_tgt_pos")
        ),
    )

    total_tgt = tgt.groupBy("trace_id", "target").agg(
        F.count("*").alias("total_tgt_count")
    )

    prec_agg = tgt_src.groupBy("trace_id", "source", "target").agg(
        F.count("*").alias("prec_count"),
        F.sum(F.when(F.col("alt_prec_ok"), 1).otherwise(0)).alias("alt_prec_count"),
    )

    precedence_flags = (
        prec_agg.join(total_tgt, on=["trace_id", "target"])
        .withColumn("is_precedence", F.col("prec_count") == F.col("total_tgt_count"))
        .withColumn(
            "is_alternate_precedence",
            F.col("alt_prec_count") == F.col("total_tgt_count"),
        )
    )

    # -- Chain flags -------------------------------------------------------
    trace_order = Window.partitionBy("trace_id").orderBy("position")
    act_with_neighbors = evolved_df.withColumn(
        "next_activity", F.lead("activity").over(trace_order)
    ).withColumn(
        "prev_activity", F.lag("activity").over(trace_order)
    )

    chain_resp_src = act_with_neighbors.select(
        "trace_id",
        F.col("activity").alias("source"),
        "next_activity",
    )
    cr = (
        distinct_pairs.join(chain_resp_src, on=["trace_id", "source"])
        .groupBy("trace_id", "source", "target")
        .agg(
            F.sum(
                F.when(F.col("next_activity") == F.col("target"), 1).otherwise(0)
            ).alias("chain_resp_hits"),
            F.count("*").alias("chain_resp_total"),
        )
        .withColumn(
            "is_chain_response",
            F.col("chain_resp_hits") == F.col("chain_resp_total"),
        )
    )

    chain_prec_tgt = act_with_neighbors.select(
        "trace_id",
        F.col("activity").alias("target"),
        "prev_activity",
    )
    cp = (
        distinct_pairs.join(chain_prec_tgt, on=["trace_id", "target"])
        .groupBy("trace_id", "source", "target")
        .agg(
            F.sum(
                F.when(F.col("prev_activity") == F.col("source"), 1).otherwise(0)
            ).alias("chain_prec_hits"),
            F.count("*").alias("chain_prec_total"),
        )
        .withColumn(
            "is_chain_precedence",
            F.col("chain_prec_hits") == F.col("chain_prec_total"),
        )
    )

    # -- Merge flags -------------------------------------------------------
    key_cols = ["trace_id", "source", "target"]

    flags = (
        distinct_pairs
        .join(
            response_flags.select(*key_cols, "is_response", "is_alternate_response"),
            on=key_cols, how="left",
        )
        .join(
            precedence_flags.select(*key_cols, "is_precedence", "is_alternate_precedence"),
            on=key_cols, how="left",
        )
        .join(cr.select(*key_cols, "is_chain_response"), on=key_cols, how="left")
        .join(cp.select(*key_cols, "is_chain_precedence"), on=key_cols, how="left")
        .fillna(False)
    )

    src.unpersist()
    tgt.unpersist()
    distinct_pairs.unpersist()

    # -- Explode flags into flat rows --------------------------------------
    return flags.select(
        "trace_id", "source", "target",
        F.explode(
            F.filter(
                F.array(
                    F.when(F.col("is_response"), F.lit("response")),
                    F.when(F.col("is_precedence"), F.lit("precedence")),
                    F.when(
                        F.col("is_response") & F.col("is_precedence"),
                        F.lit("succession"),
                    ),
                    F.when(F.col("is_alternate_response"), F.lit("alternate_response")),
                    F.when(F.col("is_alternate_precedence"), F.lit("alternate_precedence")),
                    F.when(F.col("is_chain_response"), F.lit("chain_response")),
                    F.when(F.col("is_chain_precedence"), F.lit("chain_precedence")),
                    F.when(
                        F.col("is_chain_response") & F.col("is_chain_precedence"),
                        F.lit("chain_succession"),
                    ),
                ),
                lambda x: x.isNotNull(),
            )
        ).alias("template"),
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
def discover_ordered(
    evolved_df: DataFrame,
    metadata: MetaData,
    strategy: Literal["joins", "pandas"] = "joins",
) -> DataFrame:
    """
    Ordered constraints (tight semantics: the relation must hold for ALL
    occurrences of the source/target pair within a given trace).

    Templates discovered (per activity-pair, per trace):
      1. Response(a,b)           – every 'a' is eventually followed by a 'b'.
      2. Precedence(a,b)         – every 'b' is preceded by an 'a'.
      3. Succession(a,b)         – Response ∧ Precedence.
      4. AlternateResponse(a,b)  – every 'a' is followed by 'b' before the next 'a'.
      5. AlternatePrecedence(a,b)– every 'b' is preceded by 'a' after the previous 'b'.
      6. ChainResponse(a,b)      – every 'a' is immediately followed by 'b'.
      7. ChainPrecedence(a,b)    – every 'b' is immediately preceded by 'a'.
      8. ChainSuccession(a,b)    – ChainResponse ∧ ChainPrecedence.

    Storage schema (flat ConstraintEntry): template | source | target | trace_id

    Incremental strategy
    --------------------
    * ``old_constraints_df`` holds constraints mined previously.
    * ``evolved_df`` contains new/continued events since last mining.
    * Old constraints whose trace_id appears in ``evolved_df`` are invalidated
      (the trace evolved, so old partial results are stale).
    * Ordered constraints are re-derived from scratch for every evolved trace.
    * Old constraints for untouched traces are kept as-is.
    * The final result is the union, written with overwrite mode.

    :param evolved_df: DataFrame[Event] with events after the last mining timestamp.
    :param metadata:   MetaData with storage paths and log context.
    :param strategy:   ``"joins"`` for Spark-native joins (best for short traces),
                       ``"pandas"`` for groupBy + applyInPandas (best for long traces).
    :return: DataFrame with columns (template, source, target, trace_id).
    """
    storage = get_storage_manager()
    old_constraints_df = storage.read_ordered_constraints(metadata)

    # -- 1. Invalidate old constraints for traces that have evolved -------
    evolved_trace_ids = evolved_df.select("trace_id").distinct()
    unchanged_constraints = old_constraints_df.join(
        evolved_trace_ids, on="trace_id", how="left_anti"
    )

    # -- 2. Mine constraints for evolved traces using chosen strategy -----
    if strategy == "pandas":
        new_constraints = _discover_ordered_pandas(evolved_df)
    else:
        new_constraints = _discover_ordered_joins(evolved_df)

    # -- 3. Combine with unchanged old constraints ------------------------
    ordered_constraints = unchanged_constraints.select(
        "template", "source", "target", "trace_id"
    ).unionByName(new_constraints)

    # Force materialization before overwriting the same S3 path
    ordered_constraints = ordered_constraints.cache()
    ordered_constraints.count()

    storage.write_ordered_constraints(metadata, ordered_constraints)

    return ordered_constraints
