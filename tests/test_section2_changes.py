"""Unit tests for the Section 2 evaluation changes: cost_scale, attribute-block
detection and the benchmark's ground-truth matcher."""

import pandas as pd
import pytest

from siesta.model.PerspectiveModel import PairStats
from siesta.modules.adaptive_index.retention import RetentionPolicy


def _pair(build=1000.0, savings=500.0, queries=4.0):
    ps = PairStats()
    ps.query_count = queries
    ps.total_savings_ms = savings * queries
    ps.build_cost_ms = build
    ps.last_decay_ts = 0.0
    return ps


@pytest.fixture(autouse=True)
def no_decay(monkeypatch):
    monkeypatch.setattr(RetentionPolicy, "_decay_pair", lambda self, ps: None)


def test_cost_scale_one_is_default():
    assert RetentionPolicy().cost_scale == 1.0


def test_cost_scale_flips_persist_decision():
    # utility 2000 vs build 1000: persisted at scale 1, not when costs look 4x higher
    assert RetentionPolicy(cost_scale=1.0).should_persist_pair(_pair())
    assert not RetentionPolicy(cost_scale=4.0).should_persist_pair(_pair())


def test_cost_scale_low_promotes_marginal_pair():
    marginal = _pair(build=2000.0, savings=500.0, queries=4.0)  # utility 2000 < 2000 * 1.15
    assert not RetentionPolicy().should_persist_pair(marginal)
    assert RetentionPolicy(cost_scale=0.5).should_persist_pair(marginal)


def test_has_attribute_constraints():
    pytest.importorskip("pyspark")
    from siesta.modules.index.pair_attributes import has_attribute_constraints

    assert not has_attribute_constraints('A B "C [x]"')
    assert has_attribute_constraints('A B[org:resource="u1"] C')
    assert has_attribute_constraints('"W x" "W y"[r!=$1]')


def _truth(rows, perspective="case"):
    from tests.vldb_eval.pattern_common import TruthIndex, group_order

    df = pd.DataFrame(rows, columns=["trace_id", "position", "activity", "ts", "r"])
    return TruthIndex(group_order(df, perspective))


def test_truth_bindings_need_backtracking():
    # A(r=1) A(r=2) B(r=2): "A B[r=$1]" matches only with the second A
    t = _truth([("t", 0, "A", 0, "1"), ("t", 1, "A", 1, "2"), ("t", 2, "B", 2, "2")])
    q = [{"activity": "A", "preds": []}, {"activity": "B", "preds": [{"attr": "r", "op": "=", "ref": 1}]}]
    assert t.match_count(q) == 1
    q_neq = [{"activity": "A", "preds": []}, {"activity": "B", "preds": [{"attr": "r", "op": "!=", "ref": 1}]}]
    assert t.match_count(q_neq) == 1


def test_truth_order_repeats_and_nulls():
    t = _truth([("t1", 0, "B", 0, None), ("t1", 1, "A", 1, None),
                ("t2", 0, "A", 0, "x"), ("t2", 1, "A", 1, None), ("t2", 2, "B", 2, None)])
    ab = [{"activity": "A", "preds": []}, {"activity": "B", "preds": []}]
    assert t.match_count(ab) == 1                       # t1 has B before A
    aab = [{"activity": "A", "preds": []}] * 2 + [{"activity": "B", "preds": []}]
    assert t.match_count(aab) == 1                      # repeats are distinct events
    bind_null = [{"activity": "A", "preds": []},
                 {"activity": "A", "preds": [{"attr": "r", "op": "!=", "ref": 1}]}]
    assert t.match_count(bind_null) == 0                # a null never satisfies a binding
    lit = [{"activity": "A", "preds": [{"attr": "r", "op": "=", "value": "x"}]}, {"activity": "B", "preds": []}]
    assert t.match_count(lit) == 1


def test_truth_perspective_order_breaks_ties_by_trace_then_position():
    # perspective r: one group "g"; same ts -> order (trace_id, position): t1.B before t2.A
    t = _truth([("t2", 0, "A", 5, "g"), ("t1", 0, "B", 5, "g")], perspective="r")
    ab = [{"activity": "A", "preds": []}, {"activity": "B", "preds": []}]
    ba = [{"activity": "B", "preds": []}, {"activity": "A", "preds": []}]
    assert t.match_count(ab) == 0 and t.match_count(ba) == 1
