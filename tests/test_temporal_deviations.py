"""
Tests for the new temporal-deviation-analysis capability
(siesta.modules.analyser.temporal_deviations) - duration-based discriminating
rules between groups of traces, reproducing the shape of the bank-scenario
example from the spec: "manual review and credit reassessment within five
minutes of each other correlates with rejection".
"""
import pytest

from siesta.model.DataModel import Event
from siesta.modules.analyser.temporal_deviations import (
    _compute_gaps, _resolve_candidate_pairs, compute_temporal_deviations,
)
from siesta.modules.analyser.trace_labels import resolve_trace_labels

pytestmark = pytest.mark.usefixtures("siesta_app")


def _event(trace_id, activity, position, start_timestamp, attributes=None):
    return {
        "trace_id": trace_id, "activity": activity, "position": position,
        "start_timestamp": start_timestamp, "attributes": attributes or {},
    }


def _df(spark, rows):
    return spark.createDataFrame(rows, schema=Event.get_schema())


def _get_spark(siesta_app):
    from siesta.core.sparkManager import get_spark_session
    return get_spark_session()


REJECTED_GAPS = [100, 150, 250, 270]   # Manual Review -> Credit Reassessment, seconds
APPROVED_GAPS = [900, 1000, 1100, 1200]


def _bank_scenario_events(spark):
    """4 rejected traces where the gap between the two activities is small
    (<=270s), 4 approved traces where it's large (>=900s) - a clean separation
    with a gap in the distribution between 270s and 900s.
    """
    rows = []
    for i, gap in enumerate(REJECTED_GAPS):
        trace = f"rej_{i}"
        rows += [
            _event(trace, "Start", 0, 0),
            _event(trace, "ManualReview", 1, 0),
            _event(trace, "CreditReassessment", 2, gap),
            _event(trace, "End", 3, gap + 50, {"decision": "rejected"}),
        ]
    for i, gap in enumerate(APPROVED_GAPS):
        trace = f"app_{i}"
        rows += [
            _event(trace, "Start", 0, 0),
            _event(trace, "ManualReview", 1, 0),
            _event(trace, "CreditReassessment", 2, gap),
            _event(trace, "End", 3, gap + 50, {"decision": "approved"}),
        ]
    return _df(spark, rows)


class TestResolveCandidatePairs:

    def test_explicit_pairs_used_as_is(self, siesta_app):
        spark = _get_spark(siesta_app)
        events_df = _bank_scenario_events(spark)
        pairs = _resolve_candidate_pairs(
            events_df, trace_count=8,
            activity_pairs=[["ManualReview", "CreditReassessment"]],
            max_auto_pairs=50,
        )
        assert pairs == [("ManualReview", "CreditReassessment")]

    def test_auto_derived_includes_frequent_pair(self, siesta_app):
        spark = _get_spark(siesta_app)
        events_df = _bank_scenario_events(spark)
        pairs = _resolve_candidate_pairs(
            events_df, trace_count=8, activity_pairs=None, max_auto_pairs=50,
        )
        # ManualReview -> CreditReassessment is directly-following in all 8 traces
        # (support 1.0) - must appear among the auto-derived top pairs.
        assert ("ManualReview", "CreditReassessment") in pairs


class TestComputeGaps:

    def test_min_gap_with_repeated_occurrences(self, siesta_app):
        """S occurs twice, T occurs twice; valid (source-before-target) combos are
        (S@0,T@1)=50s, (S@0,T@3)=200s, (S@2,T@3)=100s - minimum must be 50s."""
        spark = _get_spark(siesta_app)
        events_df = _df(spark, [
            _event("t1", "S", 0, 0),
            _event("t1", "T", 1, 50),
            _event("t1", "S", 2, 100),
            _event("t1", "T", 3, 200),
        ])
        result = _compute_gaps(events_df, [("S", "T")]).collect()
        assert len(result) == 1
        assert result[0]["gap_sec"] == pytest.approx(50.0)

    def test_no_gap_when_target_never_follows_source(self, siesta_app):
        spark = _get_spark(siesta_app)
        events_df = _df(spark, [
            _event("t1", "T", 0, 0),
            _event("t1", "S", 1, 50),   # T occurs before S, not after
        ])
        result = _compute_gaps(events_df, [("S", "T")]).collect()
        assert result == []


class TestComputeTemporalDeviations:

    def test_discovers_the_discriminating_gap_rule(self, siesta_app):
        spark = _get_spark(siesta_app)
        events_df = _bank_scenario_events(spark)
        trace_labels = resolve_trace_labels(events_df, "decision", [["rejected"]])

        result = compute_temporal_deviations(
            events_df=events_df,
            trace_labels=trace_labels,
            trace_count=8,
            activity_pairs=[["ManualReview", "CreditReassessment"]],
            min_group_size=2,
        ).collect()

        assert len(result) > 0
        best = max(result, key=lambda r: abs(r["balance"]))

        assert best["source"] == "ManualReview"
        assert best["target"] == "CreditReassessment"
        # Any threshold in [270, 900) perfectly separates the two groups.
        assert 270 <= best["threshold_sec"] < 900
        assert best["balance"] == pytest.approx(1.0)
        assert best["confidence_1"] == pytest.approx(1.0)
        assert best["support"] == pytest.approx(0.5)
        assert best["direction"] == "label_1"
        assert best["rule"] == f"gap(ManualReview -> CreditReassessment) <= {int(best['threshold_sec'])}s"

    def test_min_group_size_filters_low_count_results(self, siesta_app):
        spark = _get_spark(siesta_app)
        events_df = _bank_scenario_events(spark)
        trace_labels = resolve_trace_labels(events_df, "decision", [["rejected"]])

        result = compute_temporal_deviations(
            events_df=events_df,
            trace_labels=trace_labels,
            trace_count=8,
            activity_pairs=[["ManualReview", "CreditReassessment"]],
            min_group_size=100,  # impossible to satisfy with 8 traces
        ).collect()
        assert result == []

    def test_top_k_per_pair_caps_output(self, siesta_app):
        spark = _get_spark(siesta_app)
        events_df = _bank_scenario_events(spark)
        trace_labels = resolve_trace_labels(events_df, "decision", [["rejected"]])

        result = compute_temporal_deviations(
            events_df=events_df,
            trace_labels=trace_labels,
            trace_count=8,
            activity_pairs=[["ManualReview", "CreditReassessment"]],
            min_group_size=1,
            top_k_per_pair=2,
        ).collect()
        assert len(result) <= 2
