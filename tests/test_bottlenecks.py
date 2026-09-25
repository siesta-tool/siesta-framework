"""
Tests for the new bottleneck-detection capability (siesta.modules.analyser.bottlenecks).

Uses a small hand-crafted in-memory DataFrame (via the Spark session from the
siesta_app fixture) rather than a preprocessed/S3 log, so exact durations and
frequencies are fully under test control.
"""
import pytest

from siesta.model.DataModel import Event
from siesta.modules.analyser.bottlenecks import compute_bottlenecks

pytestmark = pytest.mark.usefixtures("siesta_app")


def _event(trace_id, activity, position, start_timestamp, attributes=None):
    # Plain dict, not pyspark.sql.Row: Row(**kwargs) + an explicit schema matches
    # by keyword-insertion POSITION, not by name, and silently scrambles columns
    # whose order differs from the schema. Dicts match by key name reliably.
    return {
        "trace_id": trace_id, "activity": activity, "position": position,
        "start_timestamp": start_timestamp, "attributes": attributes or {},
    }


def _df(spark, rows):
    return spark.createDataFrame(rows, schema=Event.get_schema())


def _make_events(spark):
    """
    3 traces sharing the same two-step pattern A -> B -> C, plus one rare pair:

      trace_1: A(t=0) -> B(t=10s)  -> C(t=1810s)   [B->C: 1800s, slow]
      trace_2: A(t=0) -> B(t=10s)  -> C(t=1810s)   [B->C: 1800s, slow]
      trace_3: A(t=0) -> B(t=10s)  -> C(t=1810s)   [B->C: 1800s, slow]
      trace_4: A(t=0) -> D(t=3600s)                [A->D: 3600s, slower but rare]

    B->C is slow AND frequent (occurs 3x) -> should have the highest impact_score.
    A->D is slower but occurs only once -> lower impact_score despite the longer
    single duration, since impact_score = avg_duration_sec * occurrence_count.
    """
    rows = []
    for i in range(1, 4):
        trace = f"trace_{i}"
        rows += [
            _event(trace, "A", 0, 0),
            _event(trace, "B", 1, 10),
            _event(trace, "C", 2, 1810),
        ]
    rows += [
        _event("trace_4", "A", 0, 0),
        _event("trace_4", "D", 1, 3600),
    ]
    return _df(spark, rows)


def _get_spark(siesta_app):
    from siesta.core.sparkManager import get_spark_session
    return get_spark_session()


class TestComputeBottlenecks:

    def test_impact_score_ranks_frequent_slow_pair_first(self, siesta_app):
        spark = _get_spark(siesta_app)
        events_df = _make_events(spark)

        result = {
            (r["source"], r["target"]): r
            for r in compute_bottlenecks(events_df, trace_count=4).collect()
        }

        assert set(result.keys()) == {("A", "B"), ("B", "C"), ("A", "D")}
        assert result[("B", "C")]["avg_duration_sec"] == pytest.approx(1800.0)
        assert result[("B", "C")]["occurrence_count"] == 3
        assert result[("A", "D")]["avg_duration_sec"] == pytest.approx(3600.0)
        assert result[("A", "D")]["occurrence_count"] == 1

        # impact_score = avg_duration_sec * occurrence_count:
        #   B->C: 1800 * 3 = 5400   A->D: 3600 * 1 = 3600
        # B->C should rank above A->D despite A->D's single instance being slower.
        assert result[("B", "C")]["impact_score"] == pytest.approx(5400.0)
        assert result[("A", "D")]["impact_score"] == pytest.approx(3600.0)
        assert result[("B", "C")]["impact_score"] > result[("A", "D")]["impact_score"]

    def test_ordering_is_impact_score_desc(self, siesta_app):
        spark = _get_spark(siesta_app)
        events_df = _make_events(spark)

        rows = compute_bottlenecks(events_df, trace_count=4).collect()
        scores = [r["impact_score"] for r in rows]
        assert scores == sorted(scores, reverse=True)

    def test_top_k_limits_output(self, siesta_app):
        spark = _get_spark(siesta_app)
        events_df = _make_events(spark)

        rows = compute_bottlenecks(events_df, trace_count=4, top_k=1).collect()
        assert len(rows) == 1
        assert (rows[0]["source"], rows[0]["target"]) == ("B", "C")

    def test_zscore_flags_extreme_outlier(self, siesta_app):
        """A population of 6 pair-level average durations - 5 clustered close
        together (100-120s) plus 1 clear outlier (10000s) - should flag only the
        outlier at the default zscore_threshold, and its z-score should dwarf
        every other pair's.
        """
        spark = _get_spark(siesta_app)
        normal_durations = [100, 105, 110, 115, 120]
        rows = []
        for i, dur in enumerate(normal_durations):
            trace = f"norm_{i}"
            rows += [_event(trace, f"P{i}", 0, 0), _event(trace, f"Q{i}", 1, dur)]
        # Clear outlier: 10000s, ~90x the largest normal duration.
        rows += [_event("outlier", "X", 0, 0), _event("outlier", "Y", 1, 10000)]
        events_df = _df(spark, rows)

        result = compute_bottlenecks(events_df, trace_count=6).collect()  # default zscore_threshold=3.5
        by_pair = {(r["source"], r["target"]): r for r in result}

        assert by_pair[("X", "Y")]["flagged"] is True
        assert all(
            by_pair[("X", "Y")]["zscore"] > by_pair[(f"P{i}", f"Q{i}")]["zscore"]
            for i in range(len(normal_durations))
        )
        assert all(by_pair[(f"P{i}", f"Q{i}")]["flagged"] is False for i in range(len(normal_durations)))
