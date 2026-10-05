"""
Event attributes in pair tables: embedded (default) or joined back.

By default every pair row carries ``source_attributes`` / ``target_attributes``
(MapType), so attribute-aware queries read predicates straight from the pair
rows.  With ``embed_pair_attributes = False`` the indexers write the pair rows
with null maps (smaller tables), and a query that needs attributes joins
them back from an event table on (trace_id, position):

* eager / case perspective: the Activity index (partitioned by activity, so
  only the pattern's activities are read);
* adaptive perspectives: the perspective's sequence view, whose
  ``position`` is the group position used by the pair tables.

Queries opt into the join with ``pair_attributes = "join"`` in the query
config; a log indexed without embedded attributes must be queried that way.
"""

from __future__ import annotations

import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType

def _null_map():
    return F.lit(None).cast(MapType(StringType(), StringType()))


def strip_pair_attributes(pairs_df: DataFrame) -> DataFrame:
    """Pair rows with null attribute maps (same schema)."""
    return (
        pairs_df
        .withColumn("source_attributes", _null_map())
        .withColumn("target_attributes", _null_map())
    )


def join_back_attributes(pairs_df: DataFrame, events_df: DataFrame) -> DataFrame:
    """
    Fill the attribute maps of ``pairs_df`` from ``events_df``
    (trace_id, position, attributes), matching each endpoint on
    (trace_id, position).  Column order of ``pairs_df`` is kept.
    """
    cols = pairs_df.columns
    ev = events_df.select("trace_id", "position", "attributes")
    src = ev.select(
        F.col("trace_id"),
        F.col("position").alias("source_position"),
        F.col("attributes").alias("source_attributes"),
    )
    tgt = ev.select(
        F.col("trace_id"),
        F.col("position").alias("target_position"),
        F.col("attributes").alias("target_attributes"),
    )
    joined = (
        pairs_df.drop("source_attributes", "target_attributes")
        .join(src, on=["trace_id", "source_position"], how="left")
        .join(tgt, on=["trace_id", "target_position"], how="left")
    )
    return joined.select(*cols)


_QUOTED = re.compile(r'"(?:[^"\\]|\\.)*"')


def has_attribute_constraints(pattern: str) -> bool:
    """True when the SeQL pattern has an attribute block ``[...]`` on some activity."""
    return "[" in _QUOTED.sub("", pattern)
