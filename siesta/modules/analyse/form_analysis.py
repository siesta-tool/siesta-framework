import csv
import re
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import IO, Any, Dict

import pandas as pd

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
DIGIT_RE = re.compile(r"\d")
DATE_GROUPINGS = ("dow", "month", "quarter", "year", "hour", "date", "none")
SEPARATOR = "§"
BASE_TIME = datetime(2025, 1, 2, 9, 0, 0)
COUNTRY_LIST = Path(__file__).with_name("countries.txt")

"""
RULES:
100% - skip
>5% distinct - filled/unfilled
dates - Bucket + unfilled
numeric - Bucket + unfilled
<5% distinct - keep
"""


def _looks_like_dates(series: pd.Series) -> bool:
    values = [value for value in series.unique() if pd.notna(value)]
    if not values:
        return False
    return all(isinstance(value, str) and DATE_RE.fullmatch(value) for value in values)


def _bucket_dates(series: pd.Series, mode: str) -> pd.Series:
    match mode:
        case "dow":
            bucketed = series.dt.day_name()
        case "month":
            bucketed = series.dt.month_name()
        case "quarter":
            bucketed = "Q" + series.dt.quarter.astype("Int64").astype(str)
        case "year":
            bucketed = series.dt.year.astype("Int64").astype(str)
        case "hour":
            bucketed = series.dt.hour.astype("Int64").astype(str) + ":00"
        case "date":
            bucketed = series.dt.strftime("%Y-%m-%d")
        case _:
            raise ValueError(f"Unknown date_grouping: '{mode}'")
    return bucketed.where(series.notna())


def _clean(value: Any) -> str:
    return str(value).replace(",", "/").replace("\n", " ").strip()


@lru_cache(maxsize=1)
def _country_matcher() -> re.Pattern:
    """Regex finding a country name anywhere in a label.

    Names are matched longest-first so the most specific wins ("Guinea-Bissau"
    over "Guinea", "South Sudan" over "Sudan"), and only on letter boundaries so
    a name embedded in a longer word is ignored ("Oman" inside "Romania",
    "India" inside "British Indian Ocean Territory").
    """
    names = {line.strip() for line in COUNTRY_LIST.read_text(encoding="utf-8").splitlines() if line.strip()}
    letter = r"[^\W\d_]"
    alt = "|".join(re.escape(name) for name in sorted(names, key=len, reverse=True))
    return re.compile(r"(?<!%s)(?:%s)(?!%s)" % (letter, alt, letter))


def _is_country_noise(value: Any) -> bool:
    text = _clean(value)
    # return text.lower() != "yes" and not DIGIT_RE.search(text)
    return _clean(value).strip().lower() == "no"


def run_form_analysis(
    source: str | Path | IO[bytes],
    output_path: str,
    *,
    header_row: int = 1,
    sheet_name: str | int = 0,
    date_grouping: str = "dow",
    numeric_bins: int = 5,
    retain_threshold: float = 0.05,
    drop_constant: bool = True,
    drop_unique: bool = True,
) -> Dict[str, Any]:
    """Turn a form-style spreadsheet into a trace CSV the indexer can consume.

    Each row becomes a trace, each filled cell an event named `column§value`.
    Cells of country-scoped columns are only kept when they carry a `yes` or a digit,
    so the bulk of "not registered here" answers stays out of the traces.
    Returns a summary containing the output path, the trace / event counts and the
    per-column decisions taken while preprocessing.
    """
    if date_grouping not in DATE_GROUPINGS:
        raise ValueError(f"Unknown date_grouping '{date_grouping}'. Expected one of {list(DATE_GROUPINGS)}.")

    df = pd.read_excel(source, header=header_row, sheet_name=sheet_name)
    if df.empty:
        raise ValueError("The uploaded spreadsheet contains no data rows.")

    row_count = len(df)
    columns: list[Dict[str, Any]] = []
    country_rx = _country_matcher()
    country_columns = {col for col in df.columns if country_rx.search(str(col))}

    for col in df.columns:
        if _looks_like_dates(df[col]):
            df[col] = pd.to_datetime(df[col])

        distinct = df[col].nunique(dropna=False)

        if drop_constant and distinct == 1:
            df.drop(columns=col, inplace=True)
            columns.append({"column": str(col), "action": "dropped_constant"})
            continue

        if drop_unique and distinct == row_count and pd.api.types.is_string_dtype(df[col]):
            df.drop(columns=col, inplace=True)
            columns.append({"column": str(col), "action": "dropped_unique"})
            continue

        if pd.api.types.is_datetime64_any_dtype(df[col]):
            if date_grouping == "none":
                df.drop(columns=col, inplace=True)
                columns.append({"column": str(col), "action": "dropped_date"})
                continue
            df[col] = _bucket_dates(df[col], date_grouping)
            columns.append({"column": str(col), "action": "date_bucket", "detail": date_grouping})
            continue

        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = pd.cut(df[col], bins=numeric_bins)
            columns.append({"column": str(col), "action": "numeric_bucket", "detail": f"{numeric_bins} bins"})
            continue

        if distinct / row_count > retain_threshold:
            df[col] = df[col].apply(lambda x: "filled" if pd.notna(x) else x)
            columns.append({"column": str(col), "action": "filled_flag", "detail": f"{distinct} distinct values"})
            continue

        columns.append({"column": str(col), "action": "kept", "detail": f"{distinct} distinct values"})

    country_names = {str(col) for col in country_columns}
    for entry in columns:
        if entry["column"] in country_names:
            entry["country"] = True

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    trace_count = 0
    event_count = 0
    country_events_dropped = 0
    with open(output_path, "w", newline="", encoding="utf-8") as trace_file:
        writer = csv.writer(trace_file)
        writer.writerow(["trace_id", "position", "activity", "timestamp"])
        for row_number, row in df.iterrows():
            counter = 0
            for column, value in row.items():
                if pd.isna(value) or str(value) == "nan":
                    continue
                if column in country_columns and _is_country_noise(value):
                    country_events_dropped += 1
                    continue
                timestamp = (BASE_TIME + timedelta(minutes=counter)).isoformat()
                writer.writerow([row_number, counter, f"{_clean(column)}{SEPARATOR}{_clean(value)}", timestamp])
                counter += 1
            if counter:
                trace_count += 1
            event_count += counter

    return {
        "output_path": output_path,
        "trace_count": trace_count,
        "event_count": event_count,
        "country_events_dropped": country_events_dropped,
        "columns": columns,
    }
