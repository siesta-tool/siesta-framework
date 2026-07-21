"""
Smoke/regression tests for the functions relocated (unchanged logic) from the
former `analyse` and `compare` modules into `siesta.modules.analyser`, run
against the small deterministic `query_preprocessed` fixture log
(tests/conftest.py, datasets/test.csv):

    trace_1: A(r1,clerk,5.0)   09:00 -> B(r2,analyst,7.5) 09:05 -> C(r3,system,2.0) 09:10
    trace_2: A(r1,clerk,5.0)   10:00 -> C(r3,system,2.0)  10:07
    trace_3: B(r2,analyst,7.5) 11:00 -> C(r3,system,2.0)  11:03 -> D(r4,manager,12.0) 11:15
    trace_4: A(r1,clerk,5.0)   12:00 -> D(r4,manager,12.0) 12:20

This guards against import-path/wiring regressions introduced by the module
move; the underlying algorithms themselves are untouched.
"""
import pytest
from pyspark.sql import functions as F

from siesta.modules.analyser.directly_follows import compute_directly_follows
from siesta.modules.analyser.loop_detection import compute_loop_detection
from siesta.modules.analyser.durations import compute_activity_durations, compute_group_durations
from siesta.modules.analyser.attribute_deviations import compute_attribute_deviations, ALL_STEPS
from siesta.modules.analyser.ngrams import discover_ngrams
from siesta.modules.analyser.dm import discover_rare_rules, discover_targeted_rules
from siesta.modules.analyser.trace_labels import resolve_trace_labels
from siesta.modules.mine.ordered import discover_ordered


def _events(query_preprocessed):
    """Returns (events_df, trace_count).

    trace_count is computed directly from the sequence table's distinct trace_id
    values rather than trusted from metadata.trace_count: on this shared,
    persistent test MinIO instance the metadata table can accumulate a stale
    count across repeated fixture runs (a pre-existing conftest/index-module
    issue, unrelated to the analyser code under test here), while the
    sequence table itself - the actual input to every function tested below -
    reliably reflects just the intended 4-trace/10-event dataset.
    """
    storage = query_preprocessed["storage"]
    metadata = query_preprocessed["metadata"]
    metadata = storage.read_metadata_table(metadata)
    events_df = storage.read_sequence_table(metadata)
    trace_count = events_df.select("trace_id").distinct().count()
    return events_df, trace_count


def _by_pair(rows, source, target):
    return next(r for r in rows if r["source"] == source and r["target"] == target)


class TestComputeDirectlyFollows:

    def test_pairs_and_support(self, query_preprocessed):
        events_df, trace_count = _events(query_preprocessed)
        result = compute_directly_follows(events_df, trace_count).collect()

        pairs = {(r["source"], r["target"]) for r in result}
        assert pairs == {("A", "B"), ("B", "C"), ("A", "C"), ("C", "D"), ("A", "D")}

        # A->B only in trace_1; B->C in trace_1 and trace_3.
        assert _by_pair(result, "A", "B")["support"] == pytest.approx(0.25)
        assert _by_pair(result, "B", "C")["support"] == pytest.approx(0.5)

    def test_transition_durations_seconds(self, query_preprocessed):
        events_df, trace_count = _events(query_preprocessed)
        result = compute_directly_follows(events_df, trace_count).collect()

        # trace_1: A 09:00 -> B 09:05 = 300s
        assert _by_pair(result, "A", "B")["avg_duration_sec"] == pytest.approx(300.0)
        # trace_3: C 11:03 -> D 11:15 = 720s
        assert _by_pair(result, "C", "D")["avg_duration_sec"] == pytest.approx(720.0)

    def test_support_threshold_filtering(self, query_preprocessed):
        events_df, trace_count = _events(query_preprocessed)
        result = compute_directly_follows(
            events_df, trace_count, support_threshold=0.4
        ).collect()
        # Only B->C has support >= 0.4 (0.5); everything else is 0.25.
        assert {(r["source"], r["target"]) for r in result} == {("B", "C")}


class TestComputeLoopDetection:

    def test_no_loops_in_acyclic_traces(self, query_preprocessed):
        events_df, _ = _events(query_preprocessed)
        result = compute_loop_detection(events_df)
        # None of the 4 traces revisit an activity.
        assert result["self_loops"] == []
        assert result["non_self_loops"] == []
        assert result["total_groups"] == 4


class TestComputeDurations:

    def test_activity_durations_occurrence_counts(self, query_preprocessed):
        events_df, _ = _events(query_preprocessed)
        result = {
            r["activity"]: r
            for r in compute_activity_durations(events_df).collect()
        }
        # Default (transition-time) mode only counts occurrences that have a next
        # event in the same trace. A is never trace-terminal (3 occurrences, all
        # followed by something); D is always trace-terminal (trace_3, trace_4),
        # so it has zero valid transition-time occurrences and is absent entirely.
        assert result["A"]["occurrence_count"] == 3
        assert "D" not in result

    def test_group_durations_span_per_trace(self, query_preprocessed):
        events_df, _ = _events(query_preprocessed)
        result = {r["trace_id"]: r["duration_sec"] for r in compute_group_durations(events_df).collect()}
        # trace_1: 09:00 -> 09:10 = 600s
        assert result["trace_1"] == pytest.approx(600.0)
        # trace_4: 12:00 -> 12:20 = 1200s
        assert result["trace_4"] == pytest.approx(1200.0)


class TestComputeAttributeDeviations:

    def test_runs_and_returns_expected_shape(self, query_preprocessed):
        events_df, trace_count = _events(query_preprocessed)
        records, active_steps = compute_attribute_deviations(
            events_df=events_df,
            total_traces=trace_count,
            steps=list(ALL_STEPS),
            excluded_keys=None,
            surprise_threshold=4.0,
            zscore_threshold=3.5,
            n_buckets=5,
            ngram_n=2,
            min_group_size=2,
            support_threshold=None,
            filter_out=False,
        )
        assert isinstance(records, list)
        assert set(active_steps) <= {
            "value_freq_inter", "value_freq_intra", "activity_attribute",
            "position_conditioned", "ngram_context", "value_transition",
        }
        for rec in records:
            assert {"trace_id", "position", "activity", "attribute", "value", "flagged_by", "scores"} <= rec.keys()


class TestDiscoverNgrams:

    def test_balance_direction_matches_hand_computed_labels(self, query_preprocessed):
        events_df, _ = _events(query_preprocessed)
        events_df = events_df.dropDuplicates(["trace_id", "activity", "start_timestamp"])

        # label 1 = traces containing D (trace_3, trace_4); label 0 = the rest.
        trace_labels = resolve_trace_labels(events_df, "activity", [["D"]])
        result = {
            r["ngram"]: r
            for r in discover_ngrams(events_df, trace_labels, n=2).collect()
        }

        # A -> B only occurs in trace_1 (label 0).
        assert result["A -> B"]["balance"] == pytest.approx(-0.5)
        assert result["A -> B"]["direction"] == "label_0"
        # C -> D only occurs in trace_3 (label 1).
        assert result["C -> D"]["balance"] == pytest.approx(0.5)
        assert result["C -> D"]["direction"] == "label_1"
        # B -> C occurs once in each group -> balanced.
        assert result["B -> C"]["balance"] == pytest.approx(0.0)
        assert result["B -> C"]["direction"] == "neutral"


class TestDiscoverRulesRelocatedWiring:
    """Loose smoke tests: discover_ordered + discover_rare_rules/discover_targeted_rules
    is complex, pre-existing logic - here we only guard against import/wiring
    regressions from the module move, not re-derive its full semantics.
    """

    def test_rare_and_targeted_rules_do_not_crash(self, query_preprocessed):
        events_df, trace_count = _events(query_preprocessed)
        events_df = events_df.dropDuplicates(["trace_id", "activity", "start_timestamp"])
        trace_labels = resolve_trace_labels(events_df, "activity", [["D"]])

        storage = query_preprocessed["storage"]
        metadata = storage.read_metadata_table(query_preprocessed["metadata"])
        ordered_constraints_df = discover_ordered(events_df, metadata)

        rare = discover_rare_rules(
            ordered_constraints_df, trace_labels, trace_count, support_pct=0.5
        )
        assert isinstance(rare, list)
        for rule in rare:
            assert {"source", "target", "template", "trace_ids"} <= rule.keys()

        targeted = discover_targeted_rules(
            ordered_constraints_df, trace_labels, target_label=1,
            support_threshold=0.5, filtering_support=0.5,
        )
        assert isinstance(targeted, list)
        for rule in targeted:
            assert {"source", "target", "template", "trace_ids"} <= rule.keys()
