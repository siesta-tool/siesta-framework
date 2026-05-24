"""Time bottleneck detection for process traces."""

import json
import logging
from datetime import datetime, timedelta, time as dtime, timezone
from typing import Optional

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


def _working_seconds(ts1: int, ts2: int, work_start: dtime, work_end: dtime) -> float:
    """Return elapsed working seconds between two Unix timestamps.

    Counts only time within [work_start, work_end] on Monday–Friday.
    Weekend days and off-hours intervals are excluded entirely.
    """
    dt1 = datetime.fromtimestamp(ts1, tz=timezone.utc)
    dt2 = datetime.fromtimestamp(ts2, tz=timezone.utc)
    if dt2 <= dt1:
        return 0.0
    total = 0.0
    date = dt1.date()
    while date <= dt2.date():
        if date.weekday() < 5:  # Monday=0 … Friday=4
            day_start = datetime.combine(date, work_start, tzinfo=timezone.utc)
            day_end = datetime.combine(date, work_end, tzinfo=timezone.utc)
            seg_start = max(dt1, day_start)
            seg_end = min(dt2, day_end)
            if seg_end > seg_start:
                total += (seg_end - seg_start).total_seconds()
        date += timedelta(days=1)
    return total


def discover_time_bottlenecks(
    events_df: DataFrame,
    trace_labels: DataFrame,
    threshold: float = 3.0,
    work_start: Optional[dtime] = None,
    work_end: Optional[dtime] = None,
) -> dict:
    """Detect activity-transition bottlenecks per trace group.

    A transition A→B is a bottleneck for a trace when its working-hours
    duration exceeds ``global_mean + threshold * global_std``.  Global mean
    and std are computed across **all** transitions in **all** groups; results
    are then reported separately for each label group.

    Parameters
    ----------
    events_df : DataFrame
        Must contain columns: ``trace_id``, ``activity``,
        ``start_timestamp`` (Unix epoch seconds, long).
    trace_labels : DataFrame
        Must contain columns: ``trace_id``, ``label`` (int).
    threshold : float
        Multiplier on the global std to set the bottleneck cutoff.
    work_start, work_end : datetime.time or None
        Inclusive working-hour bounds.  When *None*, derived automatically
        from the globally earliest and latest time-of-day observed in
        ``events_df``.

    Returns
    -------
    dict
        ``global_stats`` – mean/std/cutoff and working-hour bounds used.
        ``groups`` – mapping ``"label_<N>"`` → list of bottleneck records,
        each with keys ``from_activity``, ``to_activity``, ``count``,
        ``mean_duration_seconds``, ``max_duration_seconds``, ``trace_ids``.
    """
    win = Window.partitionBy("trace_id").orderBy("start_timestamp")

    # Build consecutive event pairs (activity bigrams with timestamps)
    paired = (
        events_df
        .withColumn("next_activity", F.lead("activity").over(win))
        .withColumn("next_ts", F.lead("start_timestamp").over(win).cast("long"))
        .filter(F.col("next_activity").isNotNull())
        .select(
            "trace_id", "activity",
            F.col("start_timestamp").cast("long").alias("start_timestamp"),
            "next_activity", "next_ts",
        )
        .join(trace_labels.select("trace_id", "label"), on="trace_id", how="left")
        .fillna(0, subset=["label"])
    )

    pairs_pd = paired.toPandas()

    if pairs_pd.empty:
        return {
            "global_stats": {"error": "No consecutive event pairs found."},
            "groups": {},
        }

    # Derive working hours from the full events DataFrame when not supplied
    if work_start is None or work_end is None:
        all_ts = events_df.select("start_timestamp").toPandas()["start_timestamp"].dropna()
        all_times = [datetime.fromtimestamp(int(t), tz=timezone.utc).time() for t in all_ts]
        if work_start is None:
            work_start = min(all_times)
        if work_end is None:
            work_end = max(all_times)

    logger.info("Time bottleneck: working hours %s – %s, threshold=%.2f", work_start, work_end, threshold)

    # Compute effective working-hours duration for every transition
    pairs_pd["duration"] = pairs_pd.apply(
        lambda r: _working_seconds(
            int(r["start_timestamp"]), int(r["next_ts"]), work_start, work_end
        ),
        axis=1,
    )

    # Per-transition-pair global stats (computed across ALL groups / traces)
    pair_stats = (
        pairs_pd
        .groupby(["activity", "next_activity"])["duration"]
        .agg(
            global_mean="mean",
            global_std=lambda x: float(x.std(ddof=1)) if len(x) > 1 else 0.0,
        )
        .reset_index()
    )
    pairs_pd = pairs_pd.merge(pair_stats, on=["activity", "next_activity"], how="left")
    pairs_pd["cutoff"] = pairs_pd["global_mean"] + threshold * pairs_pd["global_std"]
    pairs_pd["is_bottleneck"] = pairs_pd["duration"] > pairs_pd["cutoff"]

    global_stats = {
        "threshold_multiplier": threshold,
        "work_start": str(work_start),
        "work_end": str(work_end),
        "total_transitions": int(len(pairs_pd)),
        "unique_transition_pairs": int(len(pair_stats)),
    }

    # Per-group bottleneck analysis
    groups: dict = {}
    for label_val in sorted(pairs_pd["label"].unique()):
        grp = pairs_pd[pairs_pd["label"] == label_val]
        bottlenecks = grp[grp["is_bottleneck"]].copy()

        if bottlenecks.empty:
            groups[f"label_{int(label_val)}"] = []
            continue

        agg = (
            bottlenecks
            .groupby(["activity", "next_activity"], as_index=False)
            .agg(
                count=pd.NamedAgg(column="duration", aggfunc="count"),
                observed_mean_seconds=pd.NamedAgg(column="duration", aggfunc="mean"),
                max_duration_seconds=pd.NamedAgg(column="duration", aggfunc="max"),
                global_mean_seconds=pd.NamedAgg(column="global_mean", aggfunc="first"),
                global_std_seconds=pd.NamedAgg(column="global_std", aggfunc="first"),
                cutoff_seconds=pd.NamedAgg(column="cutoff", aggfunc="first"),
                trace_ids=pd.NamedAgg(
                    column="trace_id",
                    aggfunc=lambda x: sorted(set(str(v) for v in x)),
                ),
            )
            .rename(columns={"activity": "from_activity", "next_activity": "to_activity"})
            .sort_values("observed_mean_seconds", ascending=False)
        )

        groups[f"label_{int(label_val)}"] = agg.to_dict(orient="records")

    return {"global_stats": global_stats, "groups": groups}


def save_time_bottleneck_results(result: dict, output_path: str) -> None:
    """Write bottleneck detection results to a JSON file."""
    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(result, fh, indent=2, default=str)
    logger.info("Time bottleneck results written to %s.", output_path)
