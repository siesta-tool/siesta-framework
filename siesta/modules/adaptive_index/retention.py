"""
Stateless retention-predicate evaluator for the adaptive index lifecycle.

Three independent predicates govern three independent granularities

    Eq. 1  Perspective L1 retention
    --------------------------------------------------------------
    Retain G at L1 iff:

        f_est  *  mean(s_est)   >   c_est1 / T  +  mean(m_est)

    Left-hand side: how much query time L1 saves per unit of horizon.
    Right-hand side: amortised build cost + ongoing maintenance cost.

    Eq. 2  Perspective L2 retention (promotion from L1)
    ---------------------------------------------------------------
    Promote to L2 iff:

        f_pos  *  mean(s_pos)   >   c_est2 / T  +  mean(m_pos)

    Gated by f_pos: if no query ever references pos, the LHS is zero
    and L2 is never promoted regardless of L1 popularity.

    Eq. 3  Pair L3 retention
    ----------------------------------------------------------------
    Persist PairsIndex[A,B] iff:

        f_pair  *  mean(s_pair)   >   c_pair / T  +  mean(m_pair)

    Each pair's decision is independent.

Hysteresis
──────────
To prevent thrashing near the threshold, promotion requires
LHS > (1 + ε) * RHS and demotion requires LHS < (1 - ε) * RHS,
with ε configurable (default 0.15).  The gap between the two
thresholds is the hysteresis band.

Cold start
──────────
When a pair has never been built (build_cost_ms == 0) or has never
been queried (query_count == 0), the predicates return False for
promotion and False for demotion, keeping the artefact in its current
state.  The query planner is responsible for populating build_cost_ms
on the first transient build, which is the cold-start baseline.

Units
─────
All costs and savings are in milliseconds.  The horizon T is in
seconds.  Internally, query_count values are interpreted as counts
within the horizon (i.e. they are the raw counters maintained by the
catalog — per-unit-time conversion uses T directly).

The cost function can be summarised as:

    utility_per_second  =  (query_count / T)  *  mean_savings_ms
    cost_per_second     =  build_cost_ms / T  +  maintenance_ms_per_batch

    promote   when   utility_per_second  >  cost_per_second * (1 + ε)
    demote    when   utility_per_second  <  cost_per_second * (1 - ε)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
import time

from siesta.model.PerspectiveModel import PairStats, PerspectiveStats

logger = logging.getLogger(__name__)


@dataclass
class RetentionPolicy:
    """
    Stateless evaluator for the three retention predicates.

    Parameters
    ----------
    horizon_seconds : float
        Sliding window T over which query frequencies and costs are
        amortised.  Shorter values make the policy more reactive to
        workload shifts; longer values amortise one-time build costs
        over more queries.  Default: 3600 (1 hour).

    hysteresis : float
        ε in [0, 1).  Controls the width of the hysteresis band.
        Promotion requires LHS > (1 + ε) * RHS; demotion requires
        LHS < (1 - ε) * RHS.  Default: 0.15.

    min_query_count : int
        Minimum number of queries touching an artefact before any
        promotion decision is made.  Protects against premature
        promotion based on a single expensive first query.  Default: 3.
    """

    T: float          = 3600.0
    hysteresis: float = 0.15
    min_query_count: int = 3

    # ------------------------------------------------------------------
    # Eq. 1  Perspective L1
    # ------------------------------------------------------------------

    def should_promote_l1(self, stats: PerspectiveStats) -> bool:
        """
        Should perspective G be retained at L1?

        Returns True when the observed per-second utility of L1 exceeds
        the amortised build + maintenance cost by at least (1 + ε).

        Returns False when there is insufficient data (< min_query_count
        queries) — the perspective stays at L0 and each query pays the
        lazy-scan cost until enough evidence accumulates.
        """
        if stats.l1_query_count < self.min_query_count:
            return False
        if stats.l1_build_cost_ms <= 0.0:
            return False

        mean_savings = stats.l1_total_savings_ms / stats.l1_query_count
        utility = (stats.l1_query_count / self.T) * mean_savings
        cost = (
            stats.l1_build_cost_ms / self.T
            + stats.l1_maintenance_ms_per_batch
        )
        return utility > cost * (1 + self.hysteresis)

    def should_demote_l1(self, stats: PerspectiveStats) -> bool:
        """
        Should perspective G be demoted from L1 back to L0?

        A perspective that has never been queried (l1_query_count == 0)
        is eligible for demotion — it was promoted eagerly or by
        backstop and has seen no use.

        Otherwise, demotion fires when utility drops below
        (1 - ε) * cost.
        """
        if stats.l1_query_count == 0:
            return True

        mean_savings = stats.l1_total_savings_ms / stats.l1_query_count
        utility = (stats.l1_query_count / self.T) * mean_savings
        cost = (
            stats.l1_build_cost_ms / self.T
            + stats.l1_maintenance_ms_per_batch
        )
        return utility < cost * (1 - self.hysteresis)

    # ------------------------------------------------------------------
    # Eq. 2  Perspective L2 (promotion from L1)
    # ------------------------------------------------------------------

    def should_promote_l2(self, stats: PerspectiveStats) -> bool:
        """
        Should perspective G be promoted from L1 to L2?

        Hard-gated by l2_pos_query_count: if no query has ever
        referenced pos, the left-hand side is zero and L2 is never
        justified, regardless of how popular G is at L1.

        The min_query_count threshold applies to pos-referencing
        queries specifically — general query volume is irrelevant.
        """
        if stats.l2_pos_query_count < self.min_query_count:
            return False
        if stats.l2_build_cost_ms <= 0.0:
            # Build cost unknown: cannot evaluate the predicate.
            # The query planner should record c_est2 on first L2 touch
            # (even if it doesn't persist, the measurement should be
            # stored for future evaluation).  Until then, stay at L1.
            return False

        mean_savings = stats.l2_total_savings_ms / stats.l2_pos_query_count
        utility = (stats.l2_pos_query_count / self.T) * mean_savings
        cost = (
            stats.l2_build_cost_ms / self.T
            + stats.l2_maintenance_ms_per_batch
        )
        return utility > cost * (1 + self.hysteresis)

    def should_demote_l2(self, stats: PerspectiveStats) -> bool:
        """
        Should perspective G be demoted from L2 back to L1?

        Demotion means the pos column is no longer maintained
        incrementally.  Existing pair indices built at L2 keep their
        pos data but become stale if new batches arrive — the query
        planner falls back to ts-sorting for those pairs.
        """
        if stats.l2_pos_query_count == 0:
            return True

        mean_savings = stats.l2_total_savings_ms / stats.l2_pos_query_count
        utility = (stats.l2_pos_query_count / self.T) * mean_savings
        cost = (
            stats.l2_build_cost_ms / self.T
            + stats.l2_maintenance_ms_per_batch
        )
        return utility < cost * (1 - self.hysteresis)

    # ------------------------------------------------------------------
    # Eq. 3  Pair L3
    # ------------------------------------------------------------------

    def should_persist_pair(self, pair_stats: PairStats) -> bool:
        """
        Should pair (A, B) be promoted from ABSENT/TRANSIENT to PERSISTENT?

        Like L1, requires both a minimum query count and a known build
        cost.  The build cost is recorded by the query planner the first
        time it builds the pair transiently (cold-start baseline).

        The maintenance cost per batch is estimated as the mean across
        all batches since the pair's stats were initialised.  For pairs
        that have never been persistent, this is zero, which biases the
        predicate toward promotion — this is intentional: a pair with
        observed query demand and unknown maintenance cost should be
        given a chance.  If maintenance turns out to be expensive, the
        next evaluation cycle will demote it.
        """
        if pair_stats.query_count < self.min_query_count:
            return False
        if pair_stats.build_cost_ms <= 0.0:
            return False

        mean_savings = pair_stats.total_savings_ms / pair_stats.query_count

        # Maintenance per batch: use cumulative total / query_count as a
        # rough proxy.  This slightly conflates "batches seen" with
        # "queries served" but the two are correlated in steady state.
        # A more precise estimate would track batch_count separately;
        # this is acceptable for the initial implementation.
        mean_maintenance = (
            pair_stats.total_maintenance_ms / pair_stats.maintenance_batch_count
            if pair_stats.maintenance_batch_count > 0
            else 0.0
        )

        utility = (pair_stats.query_count / self.T) * mean_savings
        cost = (
            pair_stats.build_cost_ms / self.T
            + mean_maintenance
        )
        return utility > cost * (1 + self.hysteresis)

    def should_demote_pair(self, pair_stats: PairStats) -> bool:
        """
        Should a PERSISTENT pair be demoted to ABSENT?

        The pair's PairsIndex Delta table is not deleted — it is simply
        excluded from incremental maintenance, and the catalog marks it
        ABSENT.  If demand returns, the pair is rebuilt (or the stale
        table is read as-is for a best-effort answer, depending on the
        query planner's policy).
        """
        if pair_stats.query_count == 0:
            # Only demote if the pair hasn't been touched for at least one
            # full horizon.  This prevents immediate demotion of recently
            # promoted pairs that haven't had a chance to be queried yet.
            if pair_stats.last_accessed_ts > 0:
                idle_seconds = time.time() - pair_stats.last_accessed_ts
                return idle_seconds > self.T
            return True  # never been accessed at all — safe to evict
        if pair_stats.build_cost_ms <= 0.0:
            # Cannot evaluate — keep the pair to avoid data loss.
            return False

        mean_savings = pair_stats.total_savings_ms / pair_stats.query_count
        mean_maintenance = (
            pair_stats.total_maintenance_ms / pair_stats.maintenance_batch_count
            if pair_stats.maintenance_batch_count > 0
            else 0.0
        )

        utility = (pair_stats.query_count / self.T) * mean_savings
        cost = (
            pair_stats.build_cost_ms / self.T
            + mean_maintenance
        )
        return utility < cost * (1 - self.hysteresis)

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def explain_l1(self, stats: PerspectiveStats) -> dict:
        """
        Return the raw values feeding the L1 predicate for debugging
        and logging.  Does not make a decision.
        """
        if stats.l1_query_count == 0:
            return {
                "query_count": 0,
                "verdict": "insufficient_data",
            }
        mean_savings = stats.l1_total_savings_ms / stats.l1_query_count
        utility = (stats.l1_query_count / self.T) * mean_savings
        cost = (
            stats.l1_build_cost_ms / self.T
            + stats.l1_maintenance_ms_per_batch
        )
        return {
            "query_count":              stats.l1_query_count,
            "mean_savings_ms":          round(mean_savings, 2),
            "build_cost_ms":            round(stats.l1_build_cost_ms, 2),
            "maintenance_ms_per_batch": round(stats.l1_maintenance_ms_per_batch, 2),
            "utility_per_sec":          round(utility, 4),
            "cost_per_sec":             round(cost, 4),
            "ratio":                    round(utility / cost, 4) if cost > 0 else float("inf"),
            "promote_threshold":        round(1 + self.hysteresis, 2),
            "demote_threshold":         round(1 - self.hysteresis, 2),
        }

    def explain_l2(self, stats: PerspectiveStats) -> dict:
        """Same as explain_l1 but for the L2 predicate."""
        if stats.l2_pos_query_count == 0:
            return {
                "pos_query_count": 0,
                "verdict": "insufficient_data (no pos queries)",
            }
        mean_savings = stats.l2_total_savings_ms / stats.l2_pos_query_count
        utility = (stats.l2_pos_query_count / self.T) * mean_savings
        cost = (
            stats.l2_build_cost_ms / self.T
            + stats.l2_maintenance_ms_per_batch
        )
        return {
            "pos_query_count":          stats.l2_pos_query_count,
            "mean_savings_ms":          round(mean_savings, 2),
            "build_cost_ms":            round(stats.l2_build_cost_ms, 2),
            "maintenance_ms_per_batch": round(stats.l2_maintenance_ms_per_batch, 2),
            "utility_per_sec":          round(utility, 4),
            "cost_per_sec":             round(cost, 4),
            "ratio":                    round(utility / cost, 4) if cost > 0 else float("inf"),
            "promote_threshold":        round(1 + self.hysteresis, 2),
            "demote_threshold":         round(1 - self.hysteresis, 2),
        }

    def explain_pair(self, pair_stats: PairStats) -> dict:
        """Same as explain_l1 but for the pair L3 predicate."""
        if pair_stats.query_count == 0:
            return {
                "query_count": 0,
                "verdict": "insufficient_data",
            }
        mean_savings = pair_stats.total_savings_ms / pair_stats.query_count
        mean_maintenance = (
            pair_stats.total_maintenance_ms / pair_stats.maintenance_batch_count
            if pair_stats.maintenance_batch_count > 0
            else 0.0
        )
        utility = (pair_stats.query_count / self.T) * mean_savings
        cost = (
            pair_stats.build_cost_ms / self.T
            + mean_maintenance
        )
        return {
            "query_count":          pair_stats.query_count,
            "mean_savings_ms":      round(mean_savings, 2),
            "build_cost_ms":        round(pair_stats.build_cost_ms, 2),
            "mean_maintenance_ms":  round(mean_maintenance, 2),
            "utility_per_sec":      round(utility, 4),
            "cost_per_sec":         round(cost, 4),
            "ratio":                round(utility / cost, 4) if cost > 0 else float("inf"),
            "promote_threshold":    round(1 + self.hysteresis, 2),
            "demote_threshold":     round(1 - self.hysteresis, 2),
            "status":               pair_stats.status.name,
        }