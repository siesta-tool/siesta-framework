"""Human-readable duration formatting for analyser reports.

Analyser steps compute durations in raw seconds - activity times, transition
times, bottleneck gaps, loop time consumed. Those raw values stay in the
machine-readable outputs (CSV columns, JSON fields), but seconds alone are hard
to read once a span runs to hours, days or weeks. This module renders a seconds
value as a compact, human-friendly string, and for Spark result tables attaches
a companion ``*_human`` column next to each ``*_sec`` column.

``format_duration`` picks the largest unit that keeps the number small - seconds
(<1m), minutes (<1h), hours (<1d), days (<1w), then weeks - keeps one decimal
place, preserves the sign, and returns ``"n/a"`` for ``None``.
"""

from typing import Optional

_SEC_SUFFIX = "_sec"


def format_duration(seconds: Optional[float]) -> str:
    """Render a duration given in seconds as a compact string.

    Examples: ``90 -> "1.5m"``, ``9000 -> "2.5h"``, ``-30 -> "-30.0s"``,
    ``None -> "n/a"``.
    """
    if seconds is None:
        return "n/a"
    seconds = float(seconds)
    sign = "-" if seconds < 0 else ""
    seconds = abs(seconds)
    if seconds < 60:
        return f"{sign}{seconds:.1f}s"
    if seconds < 3600:
        return f"{sign}{seconds / 60:.1f}m"
    if seconds < 86400:
        return f"{sign}{seconds / 3600:.1f}h"
    if seconds < 604800:
        return f"{sign}{seconds / 86400:.1f}d"
    return f"{sign}{seconds / 604800:.1f}w"


def add_human_duration_columns(df, sec_columns: Optional[list] = None):
    """Return ``df`` with a ``<name>_human`` string column beside every
    ``<name>_sec`` column, formatted by :func:`format_duration`.

    Args:
        df: A Spark DataFrame carrying one or more second-valued columns.
        sec_columns: Explicit list of columns to humanize. When ``None``
            (the default), every column whose name ends in ``_sec`` is used.

    Each companion column is inserted immediately after its source column and
    named by replacing the trailing ``_sec`` with ``_human`` (e.g.
    ``avg_duration_sec -> avg_duration_human``). Row order is preserved, and the
    original ``*_sec`` columns are left untouched. If no matching column exists,
    ``df`` is returned unchanged. pyspark is imported lazily so callers that only
    need :func:`format_duration` incur no Spark dependency.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    if sec_columns is None:
        sec_columns = [c for c in df.columns if c.endswith(_SEC_SUFFIX)]
    if not sec_columns:
        return df

    to_human = F.udf(format_duration, StringType())
    select_exprs = []
    for col in df.columns:
        select_exprs.append(F.col(col))
        if col in sec_columns:
            human_col = col[: -len(_SEC_SUFFIX)] + "_human"
            select_exprs.append(to_human(F.col(col)).alias(human_col))
    return df.select(*select_exprs)
