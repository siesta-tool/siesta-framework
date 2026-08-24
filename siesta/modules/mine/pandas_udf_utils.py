"""Shared helpers for the mining pandas grouped-map UDFs.

Returning a pandas DataFrame from ``applyInPandas`` under pandas 3.0 + Spark
4.0.1 can crash the Arrow output serializer with::

    TypeError: Expected Array, got <class 'pyarrow.lib.ChunkedArray'>

raised from ``pyarrow StructArray.from_arrays``. PySpark builds each output
column with ``pa.Array.from_pandas(series, type=<schema arrow type>)`` and then
assembles them with ``StructArray.from_arrays``, which requires every column to
be a contiguous ``Array`` — a ``ChunkedArray`` aborts the task. Two independent
pandas-3.0 behaviours produce a ``ChunkedArray`` for a *string* output column:

1. A column whose value is ``None`` for every row is inferred by pandas as
   ``float64`` (all-NaN). Converting that to the schema's ``StringType`` yields a
   ``ChunkedArray`` (and the NaNs would otherwise serialise as the text
   ``"nan"``). This bites unary branching, where ``target`` is always ``None``.
2. String columns use a PyArrow-backed extension dtype whose storage is a
   ``ChunkedArray``; when a value is carried through from an input group that
   spanned several Arrow batches the column becomes multi-chunk.

Neither is fixed by ``spark.sql.execution.arrow.useLargeVarTypes``. Coercing the
schema's string/array columns to a plain NumPy ``object`` dtype (with real
``None`` for missing values) rematerialises them as a single contiguous buffer
that serialises to one Arrow ``Array``. Numeric/boolean columns are left alone —
their all-NaN case already serialises correctly as a nullable Arrow array.
"""
from __future__ import annotations

from typing import List

import pandas as pd
from pyspark.sql.types import ArrayType, StringType, StructType


def sanitize_udf_output(df: pd.DataFrame, schema: StructType) -> pd.DataFrame:
    """Coerce a grouped-map UDF's string/array output columns to safe dtypes.

    For every ``StringType`` field, force object dtype and replace any missing
    value (``NaN``/``NaT``/``None``) with Python ``None`` so Arrow sees a proper
    null instead of a float ``NaN``. For every ``ArrayType`` field, force object
    dtype to collapse any multi-chunk Arrow backing. This prevents PySpark's
    Arrow output serializer from receiving a ``ChunkedArray``.
    """
    for field in schema.fields:
        if isinstance(field.dataType, StringType):
            series = df[field.name]
            if series.dtype != object:
                series = series.astype(object)
            df[field.name] = series.where(series.notna(), None)
        elif isinstance(field.dataType, ArrayType):
            df[field.name] = df[field.name].astype(object)
    return df


def find_chunked_output_columns(df: pd.DataFrame, schema) -> List[str]:
    """Replicate PySpark's per-column Arrow output conversion; list offenders.

    Mirrors ``ArrowStreamPandasSerializer._create_array``: run the pandas->Arrow
    converter, drop the mask for Arrow-backed series, and call
    ``pa.Array.from_pandas`` with the field's Arrow type (tried under both regular
    and large var-type widths). Returns a human-readable description for every
    column that comes back as a ``ChunkedArray`` (or raises) — i.e. every column
    that would trip ``StructArray.from_arrays``. Diagnostic only; never raises.
    """
    offenders: List[str] = []
    try:
        import pyarrow as pa
        from pyspark.sql.pandas.types import (
            to_arrow_type, from_arrow_type, _create_converter_from_pandas,
        )
    except Exception:  # noqa: BLE001
        return offenders

    for field in schema:
        series = df[field.name]
        for large in (False, True):
            try:
                atype = to_arrow_type(field.dataType, prefers_large_types=large)
                dt = from_arrow_type(atype, prefer_timestamp_ntz=True)
                conv = _create_converter_from_pandas(
                    dt, timezone=None, error_on_duplicated_field_names=False
                )
                s = conv(series)
                mask = None if hasattr(s.array, "__arrow_array__") else s.isnull()
                arr = pa.Array.from_pandas(s, mask=mask, type=atype, safe=False)
                kind = type(arr).__name__
            except Exception as exc:  # noqa: BLE001
                kind = f"RAISED {type(exc).__name__}: {exc}"
            if "Chunked" in kind or kind.startswith("RAISED"):
                offenders.append(f"{field.name}[large={large}] dtype={series.dtype} -> {kind}")
    return offenders
