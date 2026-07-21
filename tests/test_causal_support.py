"""
Unit tests for the ad-hoc causal-support algorithm
(:mod:`siesta.modules.mine.causal`).

These tests exercise the pure-pandas/numpy core only -- no Spark or MinIO
required -- so they run fast and in isolation from the storage-backed fixtures
in ``conftest.py``.
"""
import numpy as np
import pandas as pd
import pytest

from siesta.modules.mine import causal


# ---------------------------------------------------------------------------
# Primitives
# ---------------------------------------------------------------------------
def test_z_critical_matches_known_values():
    assert causal._z_critical(0.05) == pytest.approx(1.959964, abs=1e-3)
    assert causal._z_critical(0.01) == pytest.approx(2.575829, abs=1e-3)


def test_fisher_z_significance_monotonic():
    # Strong correlation with plenty of samples is significant ...
    assert causal._fisher_z_significant(0.9, 100, 0.01)
    # ... a near-zero one is not ...
    assert not causal._fisher_z_significant(0.02, 100, 0.01)
    # ... and too few samples can never be significant.
    assert not causal._fisher_z_significant(0.99, 3, 0.01)


def test_numeric_frame_encodes_categoricals_and_parses_numbers():
    raw = pd.DataFrame(
        {"age": ["30", "40", "50"], "gender": ["M", "F", "M"]},
        index=["t1", "t2", "t3"],
    )
    out = causal._numeric_frame(raw)
    # numeric column parsed straight through
    assert list(out["age"]) == [30.0, 40.0, 50.0]
    # categorical label-encoded to two distinct float codes
    assert out["gender"].nunique() == 2
    assert out["gender"].dtype == float


def test_partial_correlation_removes_confounder():
    # z is a common cause of x and y; given z they are independent.
    rng = np.random.default_rng(0)
    z = pd.Series(rng.normal(size=500))
    x = 2 * z + pd.Series(rng.normal(size=500))
    y = 3 * z + pd.Series(rng.normal(size=500))
    marginal = causal._pearson(x, y)
    partial, _ = causal._partial_correlation(x, y, [z])
    assert abs(marginal) > 0.7          # strongly correlated marginally
    assert abs(partial) < 0.15          # ~independent given the confounder


# ---------------------------------------------------------------------------
# Strategies (Plain / Max / Diff)
# ---------------------------------------------------------------------------
def test_strategy_plain_keeps_both_directions():
    s = {("a", "b"): 0.8, ("b", "a"): 0.5}
    assert causal._apply_strategy(s, "plain") == s


def test_strategy_max_keeps_stronger_direction():
    s = {("a", "b"): 0.8, ("b", "a"): 0.5}
    out = causal._apply_strategy(s, "max")
    assert out == {("a", "b"): 0.8}


def test_strategy_diff_subtracts_weaker_direction():
    s = {("a", "b"): 0.8, ("b", "a"): 0.5}
    out = causal._apply_strategy(s, "diff")
    assert out == pytest.approx({("a", "b"): 0.3})


def test_strategy_diff_zeros_symmetric_edges():
    # Equal strengths in both directions -> no causal support retained.
    s = {("a", "b"): 0.5, ("b", "a"): 0.5}
    assert causal._apply_strategy(s, "diff") == {}


# ---------------------------------------------------------------------------
# Widest path (max-min)
# ---------------------------------------------------------------------------
def test_widest_path_max_min():
    weights = {("a", "b"): 0.9, ("b", "c"): 0.4, ("a", "c"): 0.2}
    sup = causal._widest_path(weights, ["a", "b", "c"])
    # direct a->c is 0.2, but a->b->c has bottleneck min(0.9, 0.4) = 0.4
    assert sup[("a", "c")] == pytest.approx(0.4)
    assert sup[("a", "b")] == pytest.approx(0.9)
    # no path c -> a
    assert sup[("c", "a")] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# End-to-end on the synthetic e-commerce log (paper's example)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def ecommerce_support():
    log = causal.generate_ecommerce_example(n_traces=1500, seed=11)
    support = causal.compute_causal_support(log)
    return {(r.source, r.target): r for r in support.itertuples()}


def _sup(table, a, b, strategy="diff"):
    row = table.get((a, b))
    return getattr(row, strategy) if row is not None else 0.0


def test_synthetic_causal_edges_have_high_support(ecommerce_support):
    # Direct causal relationships from the generative mechanism.
    for a, b in [("access", "login"), ("access", "purchase"),
                 ("purchase", "shipping"), ("login", "feedback"),
                 ("purchase", "feedback")]:
        assert _sup(ecommerce_support, a, b, "diff") > 0.8, f"{a}->{b} too weak"


def test_synthetic_non_causal_pair_collapses(ecommerce_support):
    # login and purchase are interleaved but NOT causally related: Diff -> ~0.
    assert _sup(ecommerce_support, "login", "purchase", "diff") < 0.1
    assert _sup(ecommerce_support, "purchase", "login", "diff") < 0.1


def test_synthetic_indirect_support_via_widest_path(ecommerce_support):
    # access -> shipping has no direct edge but is reachable through purchase.
    assert _sup(ecommerce_support, "access", "shipping", "plain") > 0.8
