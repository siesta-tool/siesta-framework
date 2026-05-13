"""
Stateless retention-predicate evaluator for the adaptive index lifecycle.

Three independent predicates govern three independent granularities.
All three share the same structure: a breakeven comparison between
the cumulative utility of maintaining an artefact and its total cost.

    Eq. 1  Perspective L1 retention
    --------------------------------------------------------------
    Retain G at L1 iff:

        f_est_decayed * mean(s_est)  >  c_est1  +  mean(m_est) * f_est_decayed

    Left-hand side:  cumulative time saved by having L1 materialised.
    Right-hand side: one-time build cost + total maintenance cost over
                     the observed (decayed) query history.

    Eq. 2  Perspective L2 retention (promotion from L1)
    ---------------------------------------------------------------
    Promote to L2 iff:

        f_pos_decayed * mean(s_pos)  >  c_est2  +  mean(m_pos) * f_pos_decayed

    Gated by f_pos: if no query ever references pos, the LHS is zero
    and L2 is never promoted regardless of L1 popularity.

    Eq. 3  Pair L3 retention
    ----------------------------------------------------------------
    Persist pair (A, B) iff:

        f_pair_decayed * mean(s_pair)  >  c_pair  +  mean(m_pair) * batches_per_query

    Each pair's decision is independent.

Time-scale
──────────
All query counters are subject to exponential decay with a configurable
half-life (default 3600 seconds).  Decay is applied lazily, immediately
before each predicate evaluation, so that demand that fades over time
is reflected in the predicate without requiring a background decay job.
The half-life is the sole time-scale parameter — there is no separate
horizon T.  A short half-life makes the policy reactive to workload
shifts; a long half-life amortises one-time build costs over more
queries.

Hysteresis
──────────
To prevent thrashing near the threshold, promotion requires
LHS > (1 + ε) * RHS and demotion requires LHS < (1 - ε) * RHS,
with ε configurable (default 0.15).

Cold start
──────────
When an artefact has never been built (build_cost_ms == 0) or has
never been queried (query_count == 0), the predicates return False for
promotion and False for demotion, keeping the artefact in its current
state.  The query planner populates build_cost_ms on the first
transient build, which serves as the cold-start baseline.

Units
─────
All costs and savings are in milliseconds.  The half_life_seconds
parameter is in seconds.  Query counts are dimensionless decayed
values; their absolute magnitude is meaningful only relative to the
hysteresis thresholds and the cost terms.
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass

from siesta.model.PerspectiveModel import PairStats, PerspectiveStats

logger = logging.getLogger(__name__)


@dataclass
class RetentionPolicy:
    """
    Stateless evaluator for the three retention predicates.

    Parameters
    ----------
    half_life_seconds : float
        Half-life of the exponential decay applied to query counters.
        After one half-life without queries, a counter is halved.
        Shorter values make the policy more reactive to workload shifts;
        longer values amortise one-time build costs over more queries.
        Default: 3600 (1 hour).

    hysteresis : float
        ε in [0, 1).  Controls the width of the hysteresis band.
        Promotion requires LHS > (1 + ε) * RHS; demotion requires
        LHS < (1 - ε) * RHS.  Default: 0.15.

    min_query_count : int
        Minimum number of queries touching an artefact before any
        promotion decision is made.  Protects against premature
        promotion based on a single expensive first query.  Default: 3.
    """

    half_life_seconds: float = 3600.0
    hysteresis: float = 0.15
    min_query_count: int = 3

    # ------------------------------------------------------------------
    # Decay helpers
    # ------------------------------------------------------------------

    def _decay_perspective(self, stats: PerspectiveStats) -> None:
        """
        Apply exponential decay to perspective counters in place.

        Called lazily at the start of every predicate evaluation so
        that counters reflect demand recency without a background job.
        Maintenance cost fields (l1_maintenance_ms_per_batch,
        l2_maintenance_ms_per_batch) are NOT decayed — they are already
        EMAs that self-update on each ingestion batch and should reflect
        current steady-state cost, not historical query activity.
        """
        now = time.time()
        if stats.last_decay_ts == 0.0:
            stats.last_decay_ts = now
            return
        elapsed = now - stats.last_decay_ts
        if elapsed <= 0:
            return
        factor = math.exp(-elapsed * math.log(2) / self.half_life_seconds)
        stats.l1_query_count = int(stats.l1_query_count * factor)
        stats.l1_total_savings_ms *= factor
        stats.l2_pos_query_count = int(stats.l2_pos_query_count * factor)
        stats.l2_total_savings_ms *= factor
        stats.last_decay_ts = now

    def _decay_pair(self, ps: PairStats) -> None:
        """
        Apply exponential decay to pair counters in place.

        Same rationale as _decay_perspective.  total_maintenance_ms
        and maintenance_batch_count are NOT decayed — they accumulate
        physical cost that should not be forgotten by decay.
        """
        now = time.time()
        if ps.last_decay_ts == 0.0:
            ps.last_decay_ts = now
            return
        elapsed = now - ps.last_decay_ts
        if elapsed <= 0:
            return
        factor = math.exp(-elapsed * math.log(2) / self.half_life_seconds)
        ps.query_count = int(ps.query_count * factor)
        ps.total_savings_ms *= factor
        ps.last_decay_ts = now

    # ------------------------------------------------------------------
    # Eq. 1  Perspective L1
    # ------------------------------------------------------------------

    def should_promote_l1(self, stats: PerspectiveStats) -> bool:
        """
        Should perspective G be promoted from L0 to L1?

        Returns True when cumulative decayed savings exceeds build cost
        plus estimated total maintenance cost by at least (1 + ε).
        Returns False when there is insufficient data (< min_query_count
        queries after decay).
        """
        self._decay_perspective(stats)

        if stats.l1_query_count < self.min_query_count:
            return False
        if stats.l1_build_cost_ms <= 0.0:
            return False

        mean_savings = stats.l1_total_savings_ms / stats.l1_query_count
        utility = stats.l1_query_count * mean_savings
        cost = (
            stats.l1_build_cost_ms
            + stats.l1_maintenance_ms_per_batch * stats.l1_query_count
        )
        return utility > cost * (1 + self.hysteresis)

    def should_demote_l1(self, stats: PerspectiveStats) -> bool:
        """
        Should perspective G be demoted from L1 back to L0?

        A perspective whose decayed query count has reached zero has
        seen no recent demand and is unconditionally eligible for
        demotion.  Otherwise demotion fires when utility drops below
        (1 - ε) * cost.
        """
        self._decay_perspective(stats)

        if stats.l1_query_count == 0:
            return True

        mean_savings = stats.l1_total_savings_ms / stats.l1_query_count
        utility = stats.l1_query_count * mean_savings
        cost = (
            stats.l1_build_cost_ms
            + stats.l1_maintenance_ms_per_batch * stats.l1_query_count
        )
        return utility < cost * (1 - self.hysteresis)

    # ------------------------------------------------------------------
    # Eq. 2  Perspective L2 (promotion from L1)
    # ------------------------------------------------------------------

    def should_promote_l2(self, stats: PerspectiveStats) -> bool:
        """
        Should perspective G be promoted from L1 to L2?

        Hard-gated by l2_pos_query_count: if no query has ever
        referenced pos after decay, the LHS is zero and L2 is never
        justified regardless of L1 popularity.
        """
        self._decay_perspective(stats)

        if stats.l2_pos_query_count < self.min_query_count:
            return False
        if stats.l2_build_cost_ms <= 0.0:
            return False

        mean_savings = stats.l2_total_savings_ms / stats.l2_pos_query_count
        utility = stats.l2_pos_query_count * mean_savings
        cost = (
            stats.l2_build_cost_ms
            + stats.l2_maintenance_ms_per_batch * stats.l2_pos_query_count
        )
        return utility > cost * (1 + self.hysteresis)

    def should_demote_l2(self, stats: PerspectiveStats) -> bool:
        """
        Should perspective G be demoted from L2 back to L1?

        When the decayed pos query count reaches zero, L2 maintenance
        is unjustified — no query has referenced pos recently enough
        to matter.  Demotion stops positional annotation from being
        maintained incrementally; the query planner falls back to
        timestamp-based sorting for existing pair indices.
        """
        self._decay_perspective(stats)

        if stats.l2_pos_query_count == 0:
            return True

        mean_savings = stats.l2_total_savings_ms / stats.l2_pos_query_count
        utility = stats.l2_pos_query_count * mean_savings
        cost = (
            stats.l2_build_cost_ms
            + stats.l2_maintenance_ms_per_batch * stats.l2_pos_query_count
        )
        return utility < cost * (1 - self.hysteresis)

    # ------------------------------------------------------------------
    # Eq. 3  Pair L3
    # ------------------------------------------------------------------

    def should_persist_pair(self, pair_stats: PairStats) -> bool:
        """
        Should pair (A, B) be promoted from ABSENT/TRANSIENT to PERSISTENT?

        Two gates:
          1. Demand gate — `query_count` must reach `min_query_count` after
             decay.  This protects against premature promotion based on a
             single expensive first query.
          2. Cost-function gate — accumulated savings must dominate the
             build cost plus expected maintenance, by a hysteresis margin.

        Eager-promotion bypass
        ----------------------
        When `min_query_count <= 1` the operator has explicitly signalled
        that promotion should fire as soon as a pair is queried — for
        instance during the warm-up benchmark (Experiment 6.3.1) where
        the goal is to observe the L3 transition rather than to optimise
        long-run cost.  In this regime the cost-function gate is bypassed
        because a single observed query cannot yet produce enough savings
        to amortise the build cost; the predicate becomes a pure count
        check.

        For pairs that have never been maintained the maintenance term
        is zero, biasing the cost-function gate slightly toward
        promotion — intentional, because a pair with observed demand
        and unknown maintenance cost deserves a chance.  Demotion will
        correct the decision at the next evaluation cycle if maintenance
        proves expensive.
        """
        self._decay_pair(pair_stats)

        # Gate 1 — demand
        if pair_stats.query_count < self.min_query_count:
            return False
        if pair_stats.build_cost_ms <= 0.0:
            return False

        # Eager-promotion bypass: low min_query_count signals operator
        # intent to promote on demand evidence alone.
        if self.min_query_count <= 1:
            return True

        # Gate 2 — cost-function
        mean_savings = pair_stats.total_savings_ms / pair_stats.query_count
        mean_maintenance = (
            pair_stats.total_maintenance_ms / pair_stats.maintenance_batch_count
            if pair_stats.maintenance_batch_count > 0
            else 0.0
        )
        # batches_per_query: expected number of ingest batches between
        # consecutive queries — scales maintenance cost to the same
        # query-count basis as utility.
        batches_per_query = (
            pair_stats.maintenance_batch_count / pair_stats.query_count
            if pair_stats.query_count > 0
            else 0.0
        )

        utility = pair_stats.query_count * mean_savings
        cost = (
            pair_stats.build_cost_ms
            + mean_maintenance * batches_per_query * pair_stats.query_count
        )
        return utility > cost * (1 + self.hysteresis)

    def should_demote_pair(self, pair_stats: PairStats) -> bool:
        """
        Should a PERSISTENT pair be demoted to TRANSIENT?

        The pair's PairsIndex Delta table is not deleted — it is
        excluded from incremental maintenance and the catalog marks it
        TRANSIENT.  The LRU cache handles the final TRANSIENT -> ABSENT
        transition based on memory pressure.

        A pair whose decayed query count has reached zero and has not
        been accessed within one half-life is unconditionally eligible
        for demotion.
        """
        self._decay_pair(pair_stats)

        if pair_stats.query_count == 0:
            if pair_stats.last_accessed_ts > 0:
                idle_seconds = time.time() - pair_stats.last_accessed_ts
                return idle_seconds > self.half_life_seconds
            return True

        if pair_stats.build_cost_ms <= 0.0:
            return False

        mean_savings = pair_stats.total_savings_ms / pair_stats.query_count
        mean_maintenance = (
            pair_stats.total_maintenance_ms / pair_stats.maintenance_batch_count
            if pair_stats.maintenance_batch_count > 0
            else 0.0
        )
        batches_per_query = (
            pair_stats.maintenance_batch_count / pair_stats.query_count
            if pair_stats.query_count > 0
            else 0.0
        )

        utility = pair_stats.query_count * mean_savings
        cost = (
            pair_stats.build_cost_ms
            + mean_maintenance * batches_per_query * pair_stats.query_count
        )
        return utility < cost * (1 - self.hysteresis)

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def explain_l1(self, stats: PerspectiveStats) -> dict:
        """
        Return the raw values feeding the L1 predicate for debugging.
        Applies decay before computing — returned values reflect the
        same state the predicate would see.
        """
        self._decay_perspective(stats)

        if stats.l1_query_count == 0:
            return {"query_count": 0, "verdict": "insufficient_data"}

        mean_savings = stats.l1_total_savings_ms / stats.l1_query_count
        utility = stats.l1_query_count * mean_savings
        cost = (
            stats.l1_build_cost_ms
            + stats.l1_maintenance_ms_per_batch * stats.l1_query_count
        )
        return {
            "query_count_decayed":      stats.l1_query_count,
            "mean_savings_ms":          round(mean_savings, 2),
            "build_cost_ms":            round(stats.l1_build_cost_ms, 2),
            "maintenance_ms_per_batch": round(stats.l1_maintenance_ms_per_batch, 2),
            "utility":                  round(utility, 4),
            "cost":                     round(cost, 4),
            "ratio":  round(utility / cost, 4) if cost > 0 else float("inf"),
            "promote_threshold":        round(1 + self.hysteresis, 2),
            "demote_threshold":         round(1 - self.hysteresis, 2),
        }

    def explain_l2(self, stats: PerspectiveStats) -> dict:
        """Same as explain_l1 but for the L2 predicate."""
        self._decay_perspective(stats)

        if stats.l2_pos_query_count == 0:
            return {
                "pos_query_count": 0,
                "verdict": "insufficient_data (no pos queries)",
            }

        mean_savings = stats.l2_total_savings_ms / stats.l2_pos_query_count
        utility = stats.l2_pos_query_count * mean_savings
        cost = (
            stats.l2_build_cost_ms
            + stats.l2_maintenance_ms_per_batch * stats.l2_pos_query_count
        )
        return {
            "pos_query_count_decayed":  stats.l2_pos_query_count,
            "mean_savings_ms":          round(mean_savings, 2),
            "build_cost_ms":            round(stats.l2_build_cost_ms, 2),
            "maintenance_ms_per_batch": round(stats.l2_maintenance_ms_per_batch, 2),
            "utility":                  round(utility, 4),
            "cost":                     round(cost, 4),
            "ratio":  round(utility / cost, 4) if cost > 0 else float("inf"),
            "promote_threshold":        round(1 + self.hysteresis, 2),
            "demote_threshold":         round(1 - self.hysteresis, 2),
        }

    def explain_pair(self, pair_stats: PairStats) -> dict:
        """Same as explain_l1 but for the pair L3 predicate."""
        self._decay_pair(pair_stats)

        if pair_stats.query_count == 0:
            return {"query_count": 0, "verdict": "insufficient_data"}

        mean_savings = pair_stats.total_savings_ms / pair_stats.query_count
        mean_maintenance = (
            pair_stats.total_maintenance_ms / pair_stats.maintenance_batch_count
            if pair_stats.maintenance_batch_count > 0
            else 0.0
        )
        batches_per_query = (
            pair_stats.maintenance_batch_count / pair_stats.query_count
            if pair_stats.query_count > 0
            else 0.0
        )
        utility = pair_stats.query_count * mean_savings
        cost = (
            pair_stats.build_cost_ms
            + mean_maintenance * batches_per_query * pair_stats.query_count
        )
        return {
            "query_count_decayed":  pair_stats.query_count,
            "mean_savings_ms":      round(mean_savings, 2),
            "build_cost_ms":        round(pair_stats.build_cost_ms, 2),
            "mean_maintenance_ms":  round(mean_maintenance, 2),
            "batches_per_query":    round(batches_per_query, 2),
            "utility":              round(utility, 4),
            "cost":                 round(cost, 4),
            "ratio":  round(utility / cost, 4) if cost > 0 else float("inf"),
            "promote_threshold":    round(1 + self.hysteresis, 2),
            "demote_threshold":     round(1 - self.hysteresis, 2),
            "status":               pair_stats.status.name,
        }