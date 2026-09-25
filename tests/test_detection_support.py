"""
process_detection_query on a local Spark session with in-memory storage:
pattern-level support, the support threshold, and the single-event path.
"""
from types import SimpleNamespace

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession, functions as F

import siesta.core.sparkManager as spark_manager
from siesta.model.DataModel import Event, EventPair
from siesta.modules.index.computations import _calculate_pairs_stnm, _parse_lookback
from siesta.modules.query.processors import detection_query as dq

TRACES = {
    "t1": ["A", "B", "C"],
    "t2": ["A", "C"],
    "t3": ["B"],
    "t4": ["C", "A"],
}


@pytest.fixture(scope="module")
def storage():
    spark = (SparkSession.builder.master("local[1]").appName("detection-support-test")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
             .getOrCreate())
    previous = spark_manager.spark_session
    spark_manager.spark_session = spark
    lookback = _parse_lookback("30d")
    rows, events = [], []
    for tid, labels in TRACES.items():
        trace = [(label, pos * 60, pos, {}) for pos, label in enumerate(labels)]
        rows += _calculate_pairs_stnm((tid, trace), None, lookback)[0]
        events += [(label, tid, pos, ts, attrs) for label, ts, pos, attrs in trace]
    pairs = spark.createDataFrame(rows, schema=EventPair.get_schema())
    activity = spark.createDataFrame(events, schema=Event.get_schema())
    fake = SimpleNamespace(
        read_pairs_index=lambda _m: pairs,
        read_activity_events=lambda _m, acts: activity.where(F.col("activity").isin(list(acts))),
    )
    original = dq.get_storage_manager
    dq.get_storage_manager = lambda: fake
    yield fake
    dq.get_storage_manager = original
    spark_manager.spark_session = previous


def run(pattern, threshold=0.0):
    config = {"query": {"pattern": pattern}, "support_threshold": threshold}
    return dq.process_detection_query(config, SimpleNamespace(trace_count=len(TRACES)))


def test_support_is_fraction_of_matching_traces(storage):
    out = run("A C")
    assert out["support"] == pytest.approx(0.5)
    assert sorted(d["trace_id"] for d in out["detected"]) == ["t1", "t2"]
    assert all(d["support"] == pytest.approx(0.5) for d in out["detected"])
    assert out["total"] == 2


def test_threshold_applies_to_the_pattern(storage):
    assert run("A C", threshold=0.5)["total"] == 2
    below = run("A C", threshold=0.6)
    assert below["total"] == 0 and below["detected"] == []
    assert below["support"] == pytest.approx(0.5)


def test_single_event_pattern_uses_activity_index(storage):
    out = run("B")
    assert sorted(d["trace_id"] for d in out["detected"]) == ["t1", "t3"]
    assert {d["trace_id"]: d["positions"] for d in out["detected"]} == {"t1": [1], "t3": [0]}
    assert out["support"] == pytest.approx(0.5)
