"""
Unit tests for the retention policy's decayed counters (no Spark needed).

Counters used to be rounded on every decay step, which made them sticky:
with frequent evaluations round(n * factor) == n, so demand never decayed.
They are now floats, compared against integer thresholds with a
half-query tolerance.
"""

import time

import pytest

from siesta.model.PerspectiveModel import PairStats, PairStatus, PerspectiveStats
from siesta.modules.adaptive_index.retention import RetentionPolicy


def _aged(ps: PairStats, seconds: float) -> PairStats:
    """Pretend the last decay happened `seconds` ago."""
    ps.last_decay_ts = time.time() - seconds
    return ps


def test_frequent_evaluations_still_decay():
    policy = RetentionPolicy(half_life_seconds=60.0)
    ps = PairStats(query_count=3.0)
    # 120 evaluations one second apart: two half-lives in total.
    ps.last_decay_ts = time.time()
    for _ in range(120):
        ps.last_decay_ts -= 1.0
        policy.decay_pair(ps)
    assert ps.query_count == pytest.approx(0.75, rel=0.05)


def test_one_half_life_halves_the_counter():
    policy = RetentionPolicy(half_life_seconds=100.0)
    ps = _aged(PairStats(query_count=4.0, total_savings_ms=1000.0), 100.0)
    policy.decay_pair(ps)
    assert ps.query_count == pytest.approx(2.0, rel=1e-3)
    assert ps.total_savings_ms == pytest.approx(500.0, rel=1e-3)


def test_quick_repeated_queries_pass_the_demand_gate():
    policy = RetentionPolicy(half_life_seconds=3600.0, min_query_count=3, hysteresis=0.15)
    ps = PairStats(build_cost_ms=1000.0)
    ps.last_decay_ts = time.time() - 30.0
    # Three touches 30 s apart, each decayed before it is added (as the
    # catalog does): the decayed sum is slightly below 3.
    for _ in range(3):
        policy.decay_pair(ps)
        ps.query_count += 1
        ps.last_decay_ts -= 30.0
    ps.total_savings_ms = 2000.0  # two warm hits saved the build cost twice
    assert ps.query_count < 3.0
    assert policy.should_persist_pair(ps)


def test_demand_gate_blocks_two_queries():
    policy = RetentionPolicy(min_query_count=3)
    ps = PairStats(query_count=2.0, build_cost_ms=1000.0, total_savings_ms=10_000.0)
    assert not policy.should_persist_pair(ps)


def test_cost_gate_needs_savings_above_build_cost():
    policy = RetentionPolicy(min_query_count=3, hysteresis=0.15)
    ps = PairStats(query_count=3.0, build_cost_ms=1000.0, total_savings_ms=1100.0)
    assert not policy.should_persist_pair(ps)  # 1100 < 1.15 * 1000
    ps.total_savings_ms = 1200.0
    assert policy.should_persist_pair(ps)


def test_idle_pair_decays_to_zero_and_demotes():
    policy = RetentionPolicy(half_life_seconds=60.0)
    ps = PairStats(
        query_count=3.0, build_cost_ms=1000.0, total_savings_ms=3000.0,
        status=PairStatus.PERSISTENT, last_accessed_ts=time.time() - 600.0,
    )
    _aged(ps, 600.0)  # ten half-lives
    assert policy.should_demote_pair(ps)
    assert ps.query_count < 0.5


def test_active_pair_is_not_demoted():
    policy = RetentionPolicy(half_life_seconds=3600.0, hysteresis=0.15)
    ps = PairStats(
        query_count=5.0, build_cost_ms=1000.0, total_savings_ms=4000.0,
        total_maintenance_ms=500.0, maintenance_batch_count=5,
        status=PairStatus.PERSISTENT, last_accessed_ts=time.time(),
    )
    _aged(ps, 1.0)
    assert not policy.should_demote_pair(ps)


def test_perspective_counters_decay_as_floats():
    policy = RetentionPolicy(half_life_seconds=10.0)
    stats = PerspectiveStats(l1_query_count=5.0, l2_pos_query_count=2.0)
    stats.last_decay_ts = time.time() - 10.0
    policy.decay_perspective(stats)
    assert stats.l1_query_count == pytest.approx(2.5, rel=1e-3)
    assert stats.l2_pos_query_count == pytest.approx(1.0, rel=1e-3)
    assert isinstance(stats.l1_query_count, float)


def test_catalog_row_round_trip_keeps_float_counters_and_decay_state():
    from pyspark.sql import Row

    from siesta.modules.adaptive_index.catalog import _row_to_stats, _stats_to_row

    stats = PerspectiveStats(l1_query_count=2.4, l2_pos_query_count=0.7, last_decay_ts=123.0)
    stats.pairs[("A", "B")] = PairStats(
        query_count=2.6, build_cost_ms=10.0, status=PairStatus.PERSISTENT,
        last_decay_ts=456.0, maintenance_batch_count=3, total_maintenance_ms=9.0,
    )
    back = _row_to_stats(Row(**_stats_to_row("x", stats)))
    assert back.l1_query_count == pytest.approx(2.4)
    assert back.l2_pos_query_count == pytest.approx(0.7)
    assert back.last_decay_ts == 123.0
    ps = back.pairs[("A", "B")]
    assert ps.query_count == pytest.approx(2.6)
    assert ps.last_decay_ts == 456.0
    assert ps.maintenance_batch_count == 3
    assert ps.status == PairStatus.PERSISTENT


def test_catalog_touch_decays_before_counting():
    from siesta.modules.adaptive_index.catalog import PerspectiveCatalog

    cat = PerspectiveCatalog.__new__(PerspectiveCatalog)  # no Delta load
    import threading
    cat._lock = threading.RLock()
    cat._dirty = set()
    stats = PerspectiveStats()
    ps = PairStats(query_count=2.0)
    ps.last_decay_ts = time.time() - 60.0
    stats.pairs[("A", "B")] = ps
    cat._cache = {"p": stats}

    cat.record_query_touch(
        pid="p", pairs_touched=[("A", "B")], references_pos=False,
        total_query_ms=100.0, pair_savings_ms={},
        decay=RetentionPolicy(half_life_seconds=60.0),
    )
    # 2 decayed by one half-life, plus this query at full weight.
    assert ps.query_count == pytest.approx(2.0, rel=1e-3)


def test_pinned_pair_is_never_demoted():
    policy = RetentionPolicy(half_life_seconds=1.0)
    ps = _aged(PairStats(status=PairStatus.PERSISTENT, pinned=True), 1000.0)
    assert not policy.should_demote_pair(ps)
