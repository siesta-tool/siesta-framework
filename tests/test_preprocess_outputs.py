"""
Integration tests for the Preprocessor module outputs stored in S3.

Verifies that the Preprocessor correctly creates and populates all
required tables in S3 (MinIO) storage:

  - Sequence Table     : raw events with positions
  - Activity Index     : events partitioned by activity type
  - Trace Metadata     : max position per trace
  - Pairs Index        : event-type pairs per trace (STNM policy)
  - Count Table        : aggregated pair statistics
  - Last Checked Table : last pair timestamp per trace

Test dataset  (datasets/test_preprocess.csv)
-----------------------------------------------
  t1 : A -> B -> C           (3 events)
  t2 : A -> D               (2 events)
  t3 : B -> C -> D -> E     (4 events)

All activities are unique within each trace, so pair generation is
deterministic regardless of internal position-assignment order:
for *n* unique activities in a trace the pair count is C(n, 2).
"""

import pytest
from collections import defaultdict

# --------------- Dataset constants ---------------

EXPECTED_TRACES = {"t1": 3, "t2": 2, "t3": 4}
EXPECTED_TOTAL_EVENTS = 9
EXPECTED_ACTIVITIES = {"A", "B", "C", "D", "E"}
# t1: C(3,2)=3,  t2: C(2,2)=1,  t3: C(4,2)=6  => total 10
EXPECTED_TOTAL_PAIRS = 10


# --------------- Helpers ---------------

def _read_pairs_df(preprocessed):
    """Read the pairs index directly (S3Manager.read_pairs_index returns None)."""
    from siesta.core.sparkManager import get_spark_session

    spark = get_spark_session()
    return spark.read.format("delta").load(
        preprocessed["metadata"].pairs_index_path
    )


# =====================================================
#  Sequence Table
# =====================================================

class TestSequenceTable:
    """Verify the Sequence Table stored in S3."""

    def test_schema(self, preprocessed):
        from siesta.model.DataModel import Event

        seq_df = preprocessed["storage"].read_sequence_table(preprocessed["metadata"])
        expected = {f.name for f in Event.get_schema().fields}
        actual = {f.name for f in seq_df.schema.fields}
        assert actual == expected

    def test_total_event_count(self, preprocessed):
        seq_df = preprocessed["storage"].read_sequence_table(preprocessed["metadata"])
        assert seq_df.count() == EXPECTED_TOTAL_EVENTS

    def test_trace_ids(self, preprocessed):
        seq_df = preprocessed["storage"].read_sequence_table(preprocessed["metadata"])
        ids = {r.trace_id for r in seq_df.select("trace_id").distinct().collect()}
        assert ids == set(EXPECTED_TRACES.keys())

    def test_events_per_trace(self, preprocessed):
        from pyspark.sql.functions import count

        seq_df = preprocessed["storage"].read_sequence_table(preprocessed["metadata"])
        counts = {
            r.trace_id: r["count"]
            for r in seq_df.groupBy("trace_id").agg(count("*").alias("count")).collect()
        }
        assert counts == EXPECTED_TRACES

    def test_positions_unique_per_trace(self, preprocessed):
        seq_df = preprocessed["storage"].read_sequence_table(preprocessed["metadata"])
        for tid in EXPECTED_TRACES:
            positions = [
                r.position
                for r in seq_df.filter(seq_df.trace_id == tid).select("position").collect()
            ]
            assert len(positions) == len(set(positions)), \
                f"Duplicate positions in {tid}"

    def test_positions_consecutive(self, preprocessed):
        """Positions must form a contiguous ascending range within each trace."""
        seq_df = preprocessed["storage"].read_sequence_table(preprocessed["metadata"])
        for tid, n in EXPECTED_TRACES.items():
            positions = sorted(
                r.position
                for r in seq_df.filter(seq_df.trace_id == tid).select("position").collect()
            )
            expected = list(range(positions[0], positions[0] + n))
            assert positions == expected, \
                f"Non-consecutive positions in {tid}: {positions}"

    def test_activities_present(self, preprocessed):
        seq_df = preprocessed["storage"].read_sequence_table(preprocessed["metadata"])
        activities = {r.activity for r in seq_df.select("activity").distinct().collect()}
        assert activities == EXPECTED_ACTIVITIES

    def test_timestamps_not_null(self, preprocessed):
        seq_df = preprocessed["storage"].read_sequence_table(preprocessed["metadata"])
        assert seq_df.filter(seq_df.start_timestamp.isNull()).count() == 0

    def test_attributes_keys(self, preprocessed):
        """Attributes map must contain 'resource' and 'cost' keys for every event."""
        seq_df = preprocessed["storage"].read_sequence_table(preprocessed["metadata"])
        for row in seq_df.select("attributes").collect():
            attrs = row.attributes
            assert attrs is not None, "Attributes map is None"
            assert "resource" in attrs, f"Missing 'resource' in {attrs}"
            assert "cost" in attrs, f"Missing 'cost' in {attrs}"


# =====================================================
#  Activity Index
# =====================================================

class TestActivityIndex:

    def test_total_event_count(self, preprocessed):
        activity_df = preprocessed["storage"].read_activity_index(preprocessed["metadata"])
        assert activity_df.count() == EXPECTED_TOTAL_EVENTS

    def test_activities_present(self, preprocessed):
        activity_df = preprocessed["storage"].read_activity_index(preprocessed["metadata"])
        activities = {r.activity for r in activity_df.select("activity").distinct().collect()}
        assert activities == EXPECTED_ACTIVITIES


# =====================================================
#  Trace Metadata
# =====================================================

class TestTraceMetadata:

    def test_all_traces_present(self, preprocessed):
        tm_df = preprocessed["storage"].read_trace_metadata_table(preprocessed["metadata"])
        ids = {r.trace_id for r in tm_df.select("trace_id").distinct().collect()}
        assert ids == set(EXPECTED_TRACES.keys())

    def test_max_position_per_trace(self, preprocessed):
        """max_pos must equal n_events - 1 (0-based indexing)."""
        tm_df = preprocessed["storage"].read_trace_metadata_table(preprocessed["metadata"])
        for row in tm_df.collect():
            n_events = EXPECTED_TRACES[row.trace_id]
            assert row.max_pos == n_events - 1, \
                f"max_pos for {row.trace_id} ({row.max_pos}) != {n_events - 1}"


# =====================================================
#  Pairs Index
# =====================================================

class TestPairsIndex:

    def test_schema(self, preprocessed):
        from siesta.model.DataModel import EventPair

        pairs_df = _read_pairs_df(preprocessed)
        expected = {f.name for f in EventPair.get_schema().fields}
        actual = {f.name for f in pairs_df.schema.fields}
        assert actual == expected

    def test_total_pair_count(self, preprocessed):
        pairs_df = _read_pairs_df(preprocessed)
        assert pairs_df.count() == EXPECTED_TOTAL_PAIRS

    def test_source_position_before_target(self, preprocessed):
        """source_position must be strictly less than target_position in every pair."""
        pairs_df = _read_pairs_df(preprocessed)
        violations = pairs_df.filter(
            pairs_df.source_position.cast("int") >= pairs_df.target_position.cast("int")
        ).count()
        assert violations == 0, "Found pairs where source_position >= target_position"

    def test_pair_trace_consistency(self, preprocessed):
        """Source and target activities must both exist in the pair's trace."""
        pairs_df = _read_pairs_df(preprocessed)
        seq_df = preprocessed["storage"].read_sequence_table(preprocessed["metadata"])
        trace_activities = {
            (r.trace_id, r.activity)
            for r in seq_df.select("trace_id", "activity").collect()
        }
        for row in pairs_df.collect():
            assert (row.trace_id, row.source) in trace_activities, \
                f"Source '{row.source}' not in trace '{row.trace_id}'"
            assert (row.trace_id, row.target) in trace_activities, \
                f"Target '{row.target}' not in trace '{row.trace_id}'"

    def test_pairs_match_expected_from_positions(self, preprocessed):
        """
        Derive expected pairs from actual event positions in the Sequence Table.

        For traces with all-unique activities, each ordered pair (A, B) where
        pos(A) < pos(B) produces exactly one pair.
        """
        pairs_df = _read_pairs_df(preprocessed)
        seq_df = preprocessed["storage"].read_sequence_table(preprocessed["metadata"])

        traces = defaultdict(list)
        for r in seq_df.collect():
            traces[r.trace_id].append((r.activity, r.position))

        expected_pairs = set()
        for tid, events in traces.items():
            for act_a, pos_a in events:
                for act_b, pos_b in events:
                    if pos_a < pos_b:
                        expected_pairs.add((act_a, act_b, tid))

        actual_pairs = {
            (r.source, r.target, r.trace_id) for r in pairs_df.collect()
        }
        assert actual_pairs == expected_pairs

    def test_no_self_pairs(self, preprocessed):
        """Dataset has unique activities per trace, so self-pairs must not exist."""
        pairs_df = _read_pairs_df(preprocessed)
        assert pairs_df.filter(pairs_df.source == pairs_df.target).count() == 0

    def test_timestamps_valid(self, preprocessed):
        """Pair timestamps must be valid positive unix-second values."""
        pairs_df = _read_pairs_df(preprocessed)
        for row in pairs_df.collect():
            src_ts = int(row.source_timestamp)
            tgt_ts = int(row.target_timestamp)
            assert src_ts > 0, f"Invalid source_timestamp: {row.source_timestamp}"
            assert tgt_ts > 0, f"Invalid target_timestamp: {row.target_timestamp}"
            assert src_ts != tgt_ts, "source and target have identical timestamps"


# =====================================================
#  Count Table
# =====================================================

class TestCountTable:

    def test_schema(self, preprocessed):
        count_df = preprocessed["storage"].read_count_table(preprocessed["metadata"])
        expected_cols = {
            "source", "target", "total_duration",
            "total_completions", "min_duration", "max_duration",
            "sum_squared_duration",
        }
        assert expected_cols <= set(count_df.columns)

    def test_completions_sum(self, preprocessed):
        """Sum of total_completions must equal total pair count."""
        from pyspark.sql.functions import sum as spark_sum

        count_df = preprocessed["storage"].read_count_table(preprocessed["metadata"])
        total = count_df.agg(spark_sum("total_completions")).collect()[0][0]
        assert total == EXPECTED_TOTAL_PAIRS

    def test_completions_match_pairs_index(self, preprocessed):
        """Per-pair-type completions must match counts in the Pairs Index."""
        from pyspark.sql.functions import count

        pairs_df = _read_pairs_df(preprocessed)
        count_df = preprocessed["storage"].read_count_table(preprocessed["metadata"])

        pair_type_counts = {
            (r.source, r.target): r["cnt"]
            for r in pairs_df.groupBy("source", "target")
            .agg(count("*").alias("cnt"))
            .collect()
        }

        for row in count_df.collect():
            key = (row.source, row.target)
            assert key in pair_type_counts, \
                f"Count table pair {key} not in pairs_index"
            assert row.total_completions == pair_type_counts[key], \
                f"Completions mismatch for {key}: {row.total_completions} != {pair_type_counts[key]}"

    def test_duration_bounds(self, preprocessed):
        """min_duration <= max_duration and both >= 0 for every pair type."""
        count_df = preprocessed["storage"].read_count_table(preprocessed["metadata"])
        for row in count_df.collect():
            pair = f"({row.source},{row.target})"
            assert row.min_duration >= 0, f"Negative min_duration for {pair}"
            assert row.max_duration >= 0, f"Negative max_duration for {pair}"
            assert row.min_duration <= row.max_duration, \
                f"min > max for {pair}: {row.min_duration} > {row.max_duration}"

    def test_no_duplicate_pair_types(self, preprocessed):
        from pyspark.sql.functions import count

        count_df = preprocessed["storage"].read_count_table(preprocessed["metadata"])
        duplicates = (
            count_df.groupBy("source", "target")
            .agg(count("*").alias("cnt"))
            .filter("cnt > 1")
            .count()
        )
        assert duplicates == 0, "Duplicate pair types in count table"


# =====================================================
#  Last Checked Table
# =====================================================

class TestLastCheckedTable:

    def test_not_empty(self, preprocessed):
        lc_df = preprocessed["storage"].read_last_checked_table(preprocessed["metadata"])
        assert lc_df.count() > 0

    def test_schema(self, preprocessed):
        from siesta.model.DataModel import Last_Checked_table_schema

        lc_df = preprocessed["storage"].read_last_checked_table(preprocessed["metadata"])
        expected = {f.name for f in Last_Checked_table_schema.fields}
        actual = {f.name for f in lc_df.schema.fields}
        assert actual == expected

    def test_entries_correspond_to_pairs(self, preprocessed):
        """Every last-checked entry must map to a pair in the Pairs Index."""
        pairs_df = _read_pairs_df(preprocessed)
        lc_df = preprocessed["storage"].read_last_checked_table(preprocessed["metadata"])

        pair_keys = {
            (r.trace_id, r.source, r.target)
            for r in pairs_df.select("trace_id", "source", "target").collect()
        }
        for row in lc_df.collect():
            assert (row.trace_id, row.source, row.target) in pair_keys, \
                f"Last-checked entry ({row.trace_id}, {row.source}, {row.target}) " \
                f"not found in pairs_index"

    def test_one_entry_per_pair_type_per_trace(self, preprocessed):
        from pyspark.sql.functions import count

        lc_df = preprocessed["storage"].read_last_checked_table(preprocessed["metadata"])
        duplicates = (
            lc_df.groupBy("trace_id", "source", "target")
            .agg(count("*").alias("cnt"))
            .filter("cnt > 1")
            .count()
        )
        assert duplicates == 0, "Duplicate last-checked entries"

    def test_timestamp_positive(self, preprocessed):
        lc_df = preprocessed["storage"].read_last_checked_table(preprocessed["metadata"])
        for row in lc_df.collect():
            assert row.last_checked_moment > 0, \
                f"Invalid timestamp for ({row.trace_id}, {row.source}, {row.target})"
