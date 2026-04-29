class RetentionPolicy:
    """
    Evaluates retention predicates (eqs. 1-3 from the design notes)
    and emits promotion/demotion decisions.
    """
    HYSTERESIS = 0.15   # epsilon: promote at 1.15x threshold, demote at 0.85x

    def __init__(self, horizon_seconds: float = 3600.0):
        self.T = horizon_seconds

    def should_promote_l1(self, stats: PerspectiveStats) -> bool:
        """
        Retain G at L1 iff:
          f_est * mean(s_est) > c_est1 / T + mean(m_est)
        """
        if stats.l1_query_count == 0:
            return False
        mean_savings = stats.l1_total_savings_ms / stats.l1_query_count
        amortised_build = stats.l1_build_cost_ms / self.T
        threshold = amortised_build + stats.l1_maintenance_ms_per_batch
        utility = stats.l1_query_count * mean_savings / self.T
        return utility > threshold * (1 + self.HYSTERESIS)

    def should_promote_l2(self, stats: PerspectiveStats) -> bool:
        """
        Promote to L2 iff:
          f_pos * mean(s_pos) > c_est2 / T + mean(m_pos)
        Gated: if f_pos == 0, never promote regardless of L1 popularity.
        """
        if stats.l2_pos_query_count == 0:
            return False   # hard gate: pos never referenced
        mean_savings = stats.l2_total_savings_ms / stats.l2_pos_query_count
        amortised_build = stats.l2_build_cost_ms / self.T
        threshold = amortised_build + stats.l2_maintenance_ms_per_batch
        utility = stats.l2_pos_query_count * mean_savings / self.T
        return utility > threshold * (1 + self.HYSTERESIS)

    def should_demote_l1(self, stats: PerspectiveStats) -> bool:
        if stats.l1_query_count == 0:
            return True
        mean_savings = stats.l1_total_savings_ms / stats.l1_query_count
        amortised_build = stats.l1_build_cost_ms / self.T
        threshold = amortised_build + stats.l1_maintenance_ms_per_batch
        utility = stats.l1_query_count * mean_savings / self.T
        return utility < threshold * (1 - self.HYSTERESIS)

    def should_persist_pair(self, pair_stats: PairStats) -> bool:
        """
        Persist (A,B) iff:
          f_pair * mean(s_pair) > c_pair / T + mean(m_pair)
        """
        if pair_stats.query_count == 0:
            return False
        mean_savings = pair_stats.total_savings_ms / pair_stats.query_count
        amortised_build = pair_stats.build_cost_ms / self.T
        mean_maintenance = (pair_stats.total_maintenance_ms / pair_stats.query_count
                            if pair_stats.query_count else 0.0)
        threshold = amortised_build + mean_maintenance
        utility = pair_stats.query_count * mean_savings / self.T
        return utility > threshold * (1 + self.HYSTERESIS)

    def should_evict_pair(self, pair_stats: PairStats) -> bool:
        if pair_stats.query_count == 0:
            return True
        mean_savings = pair_stats.total_savings_ms / pair_stats.query_count
        amortised_build = pair_stats.build_cost_ms / self.T
        mean_maintenance = pair_stats.total_maintenance_ms / max(pair_stats.query_count, 1)
        threshold = amortised_build + mean_maintenance
        utility = pair_stats.query_count * mean_savings / self.T
        return utility < threshold * (1 - self.HYSTERESIS)