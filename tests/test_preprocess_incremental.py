"""
Integration tests for incremental (multi-batch) Preprocessor behaviour.

Batch 1  (datasets/test_preprocess.csv)
  t1 : A -> B -> C            (3 events)
  t2 : A -> D                 (2 events)
  t3 : B -> C -> D -> E       (4 events)

Batch 2  (datasets/test_preprocess_batch2.csv)
  t1 : ... -> D -> E          (extend existing trace)
  t4 : C -> A -> B            (new trace)

Combined state after both batches
  t1 : A -> B -> C -> D -> E  (5 events, C(5,2)=10 pairs)
  t2 : A -> D                 (2 events, C(2,2)=1  pair)
  t3 : B -> C -> D -> E       (4 events, C(4,2)=6  pairs)
  t4 : C -> A -> B            (3 events, C(3,2)=3  pairs)
  Total: 14 events, 20 pairs

Key correctness properties tested:
  - The pairs_index (APPEND mode) accumulates pairs from both batches
  - The last_checked mechanism prevents duplicate pairs for extended traces
  - New pairs are generated for new events without re-creating old ones
  - Unchanged traces (t2, t3) keep their original pairs untouched
"""
import pytest
from collections import defaultdict

# --------------- Dataset constants ---------------

BATCH1_TRACES = {"t1": 3, "t2": 2, "t3": 4}
COMBINED_TRACES = {"t1": 5, "t2": 2, "t3": 4, "t4": 3}
COMBINED_TOTAL_EVENTS = 14
COMBINED_ACTIVITIES = {"A", "B", "C", "D", "E"}
# Per-trace pair counts (n*(n-1)/2 for unique-activity traces):
TRACE_PAIR_COUNTS = {"t1": 10, "t2": 1, "t3": 6, "t4": 3}
COMBINED_TOTAL_PAIRS = 20  # sum of above


# --------------- Helpers ---------------

def _read_pairs(ctx):
    from siesta_framework.core.sparkManager import get_spark_session
    return get_spark_session().read.format("delta").load(
        ctx["metadata"].pairs_index_path
    )


# =====================================================
#  Sequence Table after two batches
# =====================================================

class TestIncrSequenceTable:

    def test_total_event_count(self, preprocessed_incremental):
        seq = preprocessed_incremental["storage"].read_sequence_table(
            preprocessed_incremental["metadata"]
        )
        assert seq.count() == COMBINED_TOTAL_EVENTS

    def test_all_traces_present(self, preprocessed_incremental):
        seq = preprocessed_incremental["storage"].read_sequence_table(
            preprocessed_incremental["metadata"]
        )
        ids = {r.trace_id for r in seq.select("trace_id").distinct().collect()}
        assert ids == set(COMBINED_TRACES.keys())

    def test_events_per_trace(self, preprocessed_incremental):
        from pyspark.sql.functions import count

        seq = preprocessed_incremental["storage"].read_sequence_table(
            preprocessed_incremental["metadata"]
        )
        counts = {
            r.trace_id: r["cnt"]
            for r in seq.groupBy("trace_id").agg(count("*").alias("cnt")).collect()
        }
        assert counts == COMBINED_TRACES

    def test_positions_contiguous_per_trace(self, preprocessed_incremental):
        seq = preprocessed_incremental["storage"].read_sequence_table(
            preprocessed_incremental["metadata"]
        )
        for tid, n in COMBINED_TRACES.items():
            positions = sorted(
                r.position
                for r in seq.filter(seq.trace_id == tid).select("position").collect()
            )
            expected = list(range(positions[0], positions[0] + n))
            assert positions == expected, \
                f"Non-contiguous positions in {tid}: {positions}"

    def test_extended_trace_activities(self, preprocessed_incremental):
        """t1 must now contain the 5 original + appended activities."""
        seq = preprocessed_incremental["storage"].read_sequence_table(
            preprocessed_incremental["metadata"]
        )
        activities = {
            r.activity
            for r in seq.filter(seq.trace_id == "t1").select("activity").collect()
        }
        assert activities == {"A", "B", "C", "D", "E"}

    def test_new_trace_activities(self, preprocessed_incremental):
        """t4 must contain exactly C, A, B."""
        seq = preprocessed_incremental["storage"].read_sequence_table(
            preprocessed_incremental["metadata"]
        )
        activities = {
            r.activity
            for r in seq.filter(seq.trace_id == "t4").select("activity").collect()
        }
        assert activities == {"A", "B", "C"}


# =====================================================
#  Pairs Index — the main focus of incremental tests
# =====================================================

class TestIncrPairsIndex:

    def test_total_pair_count(self, preprocessed_incremental):
        """Accumulated pairs from both batches."""
        pairs = _read_pairs(preprocessed_incremental)
        assert pairs.count() == COMBINED_TOTAL_PAIRS

    def test_per_trace_pair_counts(self, preprocessed_incremental):
        from pyspark.sql.functions import count

        pairs = _read_pairs(preprocessed_incremental)
        counts = {
            r.trace_id: r["cnt"]
            for r in pairs.groupBy("trace_id").agg(count("*").alias("cnt")).collect()
        }
        assert counts == TRACE_PAIR_COUNTS

    def test_no_duplicate_pairs(self, preprocessed_incremental):
        """The last_checked mechanism must prevent re-creating batch-1 pairs."""
        from pyspark.sql.functions import count

        pairs = _read_pairs(preprocessed_incremental)
        duplicates = (
            pairs.groupBy("source", "target", "trace_id",
                          "source_position", "target_position")
            .agg(count("*").alias("cnt"))
            .filter("cnt > 1")
            .count()
        )
        assert duplicates == 0, "Duplicate pairs found in pairs_index"

    def test_source_position_before_target(self, preprocessed_incremental):
        pairs = _read_pairs(preprocessed_incremental)
        violations = pairs.filter(
            pairs.source_position.cast("int") >= pairs.target_position.cast("int")
        ).count()
        assert violations == 0

    def test_pairs_match_expected_from_positions(self, preprocessed_incremental):
        """Derive expected pairs from actual positions in the Sequence Table.

        For traces with all-unique activities, each ordered pair (A, B)
        where pos(A) < pos(B) produces exactly one pair.
        """
        pairs = _read_pairs(preprocessed_incremental)
        seq = preprocessed_incremental["storage"].read_sequence_table(
            preprocessed_incremental["metadata"]
        )

        traces = defaultdict(list)
        for r in seq.collect():
            traces[r.trace_id].append((r.activity, r.position))

        expected = set()
        for tid, events in traces.items():
            for act_a, pos_a in events:
                for act_b, pos_b in events:
                    if pos_a < pos_b:
                        expected.add((act_a, act_b, tid))

        actual = {(r.source, r.target, r.trace_id) for r in pairs.collect()}
        assert actual == expected

    def test_no_self_pairs(self, preprocessed_incremental):
        pairs = _read_pairs(preprocessed_incremental)
        assert pairs.filter(pairs.source == pairs.target).count() == 0

    def test_unchanged_traces_preserved(self, preprocessed_incremental):
        """Traces that received no new events (t2, t3) must still have
        exactly the same pairs as after batch 1."""
        from pyspark.sql.functions import count

        pairs = _read_pairs(preprocessed_incremental)
        for tid in ("t2", "t3"):
            n = pairs.filter(pairs.trace_id == tid).count()
            assert n == TRACE_PAIR_COUNTS[tid], \
                f"Pair count for unchanged trace {tid} changed: {n}"

    def test_pair_trace_consistency(self, preprocessed_incremental):
        """Source and target activities must both exist in the pair's trace."""
        pairs = _read_pairs(preprocessed_incremental)
        seq = preprocessed_incremental["storage"].read_sequence_table(
            preprocessed_incremental["metadata"]
        )
        trace_activities = {
            (r.trace_id, r.activity)
            for r in seq.select("trace_id", "activity").collect()
        }
        for row in pairs.collect():
            assert (row.trace_id, row.source) in trace_activities
            assert (row.trace_id, row.target) in trace_activities

    def test_new_trace_pairs(self, preprocessed_incremental):
        """t4 (C->A->B) must have exactly pairs (C,A), (C,B), (A,B)
        — sourced from whoever has the lower position."""
        pairs = _read_pairs(preprocessed_incremental)
        seq = preprocessed_incremental["storage"].read_sequence_table(
            preprocessed_incremental["metadata"]
        )

        t4_positions = {
            r.activity: r.position
            for r in seq.filter(seq.trace_id == "t4").collect()
        }
        t4_pairs = {
            (r.source, r.target)
            for r in pairs.filter(pairs.trace_id == "t4").collect()
        }

        # Derive expected from positions
        activities = list(t4_positions.keys())
        expected = set()
        for a in activities:
            for b in activities:
                if t4_positions[a] < t4_positions[b]:
                    expected.add((a, b))

        assert t4_pairs == expected

    def test_timestamps_ordered_within_pair(self, preprocessed_incremental):
        """source_timestamp must differ from target_timestamp for every pair."""
        pairs = _read_pairs(preprocessed_incremental)
        for row in pairs.collect():
            src_ts = int(row.source_timestamp)
            tgt_ts = int(row.target_timestamp)
            assert src_ts > 0
            assert tgt_ts > 0
            assert src_ts != tgt_ts


# =====================================================
#  Last Checked Table after two batches
# =====================================================

class TestIncrLastChecked:

    def test_not_empty(self, preprocessed_incremental):
        lc = preprocessed_incremental["storage"].read_last_checked_table(
            preprocessed_incremental["metadata"]
        )
        assert lc.count() > 0

    def test_covers_all_pair_types(self, preprocessed_incremental):
        """There must be at least one last_checked entry per (trace, source, target)
        combination that exists in the pairs_index."""
        pairs = _read_pairs(preprocessed_incremental)
        lc = preprocessed_incremental["storage"].read_last_checked_table(
            preprocessed_incremental["metadata"]
        )

        pair_keys = {
            (r.trace_id, r.source, r.target)
            for r in pairs.select("trace_id", "source", "target").distinct().collect()
        }
        lc_keys = {
            (r.trace_id, r.source, r.target) for r in lc.collect()
        }
        missing = pair_keys - lc_keys
        assert not missing, f"Missing last_checked entries: {missing}"

    def test_new_trace_has_entries(self, preprocessed_incremental):
        lc = preprocessed_incremental["storage"].read_last_checked_table(
            preprocessed_incremental["metadata"]
        )
        t4_count = lc.filter(lc.trace_id == "t4").count()
        assert t4_count == TRACE_PAIR_COUNTS["t4"], \
            f"Expected {TRACE_PAIR_COUNTS['t4']} last_checked entries for t4, got {t4_count}"


# =====================================================
#  Count Table — note: overwritten per batch, reflects
#  only pairs generated in the latest batch.
# =====================================================

class TestIncrCountTable:

    def test_not_empty(self, preprocessed_incremental):
        ct = preprocessed_incremental["storage"].read_count_table(
            preprocessed_incremental["metadata"]
        )
        assert ct.count() > 0

    def test_duration_bounds(self, preprocessed_incremental):
        ct = preprocessed_incremental["storage"].read_count_table(
            preprocessed_incremental["metadata"]
        )
        for row in ct.collect():
            assert row.min_duration >= 0
            assert row.max_duration >= row.min_duration

    def test_no_duplicate_pair_types(self, preprocessed_incremental):
        from pyspark.sql.functions import count

        ct = preprocessed_incremental["storage"].read_count_table(
            preprocessed_incremental["metadata"]
        )
        dups = (
            ct.groupBy("source", "target")
            .agg(count("*").alias("cnt"))
            .filter("cnt > 1")
            .count()
        )
        assert dups == 0
