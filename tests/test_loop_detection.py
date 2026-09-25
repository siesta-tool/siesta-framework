"""Tests for the loop-detection cost/repeated-pattern rework
(siesta.modules.analyser.loop_detection._find_loops).

_find_loops is a plain Python function (pre-UDF-wrapping), so these run without
a Spark session or MinIO - unlike test_bottlenecks.py's compute_bottlenecks
tests, which need a live DataFrame.
"""
from siesta.modules.analyser.loop_detection import _find_loops


def _event(activity, position, start_timestamp):
    return {"activity": activity, "position": position, "start_timestamp": start_timestamp}


def _by_type(results, loop_type):
    return [r for r in results if r["loop_type"] == loop_type]


class TestRepeatedPatterns:

    def test_recurring_block_reported_with_occurrence_and_cost(self):
        # A -> B -> C repeats verbatim, non-overlapping, twice.
        seq = [_event(a, i, i * 10) for i, a in enumerate(["A", "B", "C", "A", "B", "C"])]
        results = _find_loops(seq, 8)
        repeated = _by_type(results, "repeated_pattern")

        assert len(repeated) == 1
        entry = repeated[0]
        assert entry["pattern"] == "A -> B -> C"
        assert entry["occurrences"] == 2
        # Two disjoint 3-event occurrences covering all 6 positions.
        assert entry["events_consumed"] == 6
        # (t=20-t=0) + (t=50-t=30) = 20 + 20 = 40s; the two occurrences don't
        # touch/overlap in position (end=2, next start=3), so their spans sum
        # without merging into one interval.
        assert entry["time_consumed_sec"] == 40.0
        assert entry["pct_trace_events"] == 1.0

    def test_maximal_only_shorter_subsumed_pattern_is_dropped(self):
        # Same trace as above: "A -> B" also recurs (twice, at the same
        # offset), but is fully explained by the longer "A -> B -> C" - only
        # the longer pattern should be reported.
        seq = [_event(a, i, i * 10) for i, a in enumerate(["A", "B", "C", "A", "B", "C"])]
        results = _find_loops(seq, 8)
        repeated = _by_type(results, "repeated_pattern")

        patterns = {r["pattern"] for r in repeated}
        assert patterns == {"A -> B -> C"}
        assert "A -> B" not in patterns
        assert "B -> C" not in patterns

    def test_pure_single_activity_window_excluded(self):
        # "A A A A": a length-2 window "A A" would technically recur, but
        # that's self_loop's domain, not repeated_pattern's.
        seq = [_event("A", i, i * 5) for i in range(4)]
        results = _find_loops(seq, 8)
        assert _by_type(results, "repeated_pattern") == []

    def test_no_repeated_pattern_search_when_disabled(self):
        seq = [_event(a, i, i * 10) for i, a in enumerate(["A", "B", "C", "A", "B", "C"])]
        results = _find_loops(seq, 0)
        assert _by_type(results, "repeated_pattern") == []


class TestSelfLoops:

    def test_three_in_a_row_merges_without_double_counting(self):
        # "A A A": two overlapping self-loop occurrences (0,1) and (1,2) that
        # share position 1 must merge into one interval, so events_consumed
        # is 3 (the union), not 4 (the naive sum of two length-2 intervals).
        seq = [_event("A", i, i * 5) for i in range(3)]
        results = _find_loops(seq, 8)
        self_loops = _by_type(results, "self_loop")

        assert len(self_loops) == 1
        entry = self_loops[0]
        assert entry["pattern"] == "A"
        assert entry["occurrences"] == 2
        assert entry["events_consumed"] == 3
        assert entry["time_consumed_sec"] == 10.0  # ts[2] - ts[0]

    def test_no_self_loop_when_no_immediate_repeat(self):
        seq = [_event(a, i, i * 5) for i, a in enumerate(["A", "B", "C"])]
        assert _by_type(_find_loops(seq, 8), "self_loop") == []


class TestNonSelfLoops:

    def test_minimal_cycle_detected(self):
        seq = [_event(a, i, i * 10) for i, a in enumerate(["A", "B", "A"])]
        non_self = _by_type(_find_loops(seq, 8), "non_self_loop")

        assert len(non_self) == 1
        assert non_self[0]["pattern"] == "A -> B -> A"
        assert non_self[0]["occurrences"] == 1
        assert non_self[0]["events_consumed"] == 3
        assert non_self[0]["time_consumed_sec"] == 20.0

    def test_bounding_activity_inside_body_is_not_minimal(self):
        # A -> A -> B -> A: the (0,3) span has "A" at position 1 inside the
        # body, so it fails minimality; only the self-loop at (0,1) and the
        # minimal cycle starting at position 1 should be found.
        seq = [_event(a, i, i * 10) for i, a in enumerate(["A", "A", "B", "A"])]
        results = _find_loops(seq, 8)
        non_self_patterns = {r["pattern"] for r in _by_type(results, "non_self_loop")}
        assert "A -> A -> B -> A" not in non_self_patterns


class TestEmptyAndShortSequences:

    def test_empty_sequence(self):
        assert _find_loops([], 8) == []

    def test_single_event(self):
        assert _find_loops([_event("A", 0, 0)], 8) == []

    def test_no_loops_in_acyclic_sequence(self):
        seq = [_event(a, i, i * 10) for i, a in enumerate(["A", "B", "C", "D"])]
        assert _find_loops(seq, 8) == []
