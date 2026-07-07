"""
Query-side helpers for the dictionary-coding experiment.

The index stage stores ``activity`` (== ``source``/``target``) and ``trace_id`` as
integer codes. Queries arrive with the original activity-label strings, so we:

  1. encode the pattern's labels into integer codes before touching Spark, then
  2. decode result codes back into strings for display.

Activity dictionaries are small (one entry per distinct activity) and are safe to
collect to the driver. Trace dictionaries can be large, so trace_id codes are
decoded with a filtered lookup against only the codes that appear in the result.
"""
from typing import Dict, Iterable, Optional

from pyspark.sql import DataFrame, functions as F

from siesta.core.storageFactory import get_storage_manager
from siesta.model.StorageModel import MetaData


def load_activity_code_map(metadata: MetaData) -> Dict[str, int]:
    """Return {activity_label -> code} for the whole log (small; collected)."""
    rows = get_storage_manager().read_activity_dictionary(metadata).collect()
    return {r["name"]: r["code"] for r in rows}


def load_activity_label_map(metadata: MetaData) -> Dict[int, str]:
    """Return {code -> activity_label} for the whole log (small; collected)."""
    rows = get_storage_manager().read_activity_dictionary(metadata).collect()
    return {r["code"]: r["name"] for r in rows}


def decode_trace_ids(codes: Iterable[int], metadata: MetaData) -> Dict[int, str]:
    """Return {code -> original trace_id string} for the given result codes only."""
    wanted = [c for c in set(codes) if c is not None]
    if not wanted:
        return {}
    rows = (
        get_storage_manager()
        .read_trace_dictionary(metadata)
        .where(F.col("code").isin(wanted))
        .collect()
    )
    return {r["code"]: r["name"] for r in rows}


def decode_column_expr(label_map: Dict[int, str]) -> Optional["F.Column"]:
    """Build a Spark expression that maps an integer-code column to its label.

    Returns None when the dictionary is empty (caller should short-circuit).
    Usage::

        expr = decode_column_expr(label_map)
        df = df.withColumn("next_activity", expr[F.col("next_activity")])
    """
    if not label_map:
        return None
    kv = []
    for code, name in label_map.items():
        kv.append(F.lit(code))
        kv.append(F.lit(name))
    return F.create_map(*kv)
