class QueryPlanner:
    """
    Given a parsed query (P, Psi, G), determines what needs to be
    materialised before the lookup phase can run, triggers any needed
    promotions via the catalog, and returns a resolved ExecutionPlan.
    """

    def __init__(self, catalog: PerspectiveCatalog,
                 retention: RetentionPolicy,
                 lru_cache: PairLRUCache):
        self._catalog = catalog
        self._retention = retention
        self._lru = lru_cache

    def plan(self, grouping_keys: list[str], required_pairs: list[tuple[str,str]],
             references_pos: bool, lookback: str, lookback_mode: str,
             metadata: MetaData) -> "ExecutionPlan":
        """
        Returns an ExecutionPlan: for each required pair, specifies
        whether to use L3 (Delta read), L3- (LRU hit), or lazy scan.
        Also specifies whether a pos-sort or ts-sort should be used
        in pseudo-sequence reconstruction.
        """
        pid, stats = self._catalog.get_or_declare(
            grouping_keys, lookback, lookback_mode
        )

        # --- Perspective level resolution ---
        if stats.level == PerspectiveLevel.L0_DECLARED:
            # First touch: promote to L1 immediately (cheap)
            t0 = time.time()
            promote_to_l1(pid, grouping_keys, metadata, get_storage_manager())
            elapsed = (time.time() - t0) * 1000
            stats.l1_build_cost_ms = elapsed
            self._catalog.promote(pid, PerspectiveLevel.L1_POS_FREE)

        if references_pos and stats.level < PerspectiveLevel.L2_POS_ESTABLISHED:
            t0 = time.time()
            promote_to_l2(pid, grouping_keys, metadata, get_storage_manager())
            elapsed = (time.time() - t0) * 1000
            stats.l2_build_cost_ms = elapsed
            self._catalog.promote(pid, PerspectiveLevel.L2_POS_ESTABLISHED)

        # --- Pair resolution ---
        pair_sources: dict[tuple[str,str], PairSource] = {}
        for (a, b) in required_pairs:
            status = self._catalog.get_pair_status(pid, a, b)
            if status == PairStatus.PERSISTENT:
                pair_sources[(a,b)] = PairSource.L3_DELTA
            elif status == PairStatus.TRANSIENT and self._lru.contains(pid, a, b):
                pair_sources[(a,b)] = PairSource.L3_MINUS_LRU
            else:
                pair_sources[(a,b)] = PairSource.LAZY_SCAN

        sort_key = "pos" if references_pos else "ts"

        return ExecutionPlan(pid=pid, pair_sources=pair_sources,
                             sort_key=sort_key, stats=stats)

    def apply_retention_decisions(self, plan: "ExecutionPlan",
                                   query_elapsed_ms: float,
                                   lazy_scan_costs: dict[tuple[str,str], float]):
        """
        Called after a query completes. Records savings, then evaluates
        retention predicates and triggers promotions if warranted.
        """
        pid = plan.pid
        stats = self._catalog._cache[pid]

        # Record savings per pair
        for (a,b), source in plan.pair_sources.items():
            if source == PairSource.LAZY_SCAN:
                cost = lazy_scan_costs.get((a,b), 0.0)
                ps = stats.pairs.setdefault((a,b), PairStats())
                ps.build_cost_ms = ps.build_cost_ms or cost  # cold-start: first touch = baseline
                ps.query_count += 1
                ps.total_savings_ms += 0.0   # no savings this time (we scanned)
                ps.last_accessed_ts = time.time()
                # Should we persist this pair now?
                if self._retention.should_persist_pair(ps):
                    build_pair_persistent(pid, a, b, ...)
                    self._catalog.promote_pair(pid, a, b, PairStatus.PERSISTENT)
            elif source in (PairSource.L3_DELTA, PairSource.L3_MINUS_LRU):
                ps = stats.pairs[(a,b)]
                ps.query_count += 1
                ps.total_savings_ms += lazy_scan_costs.get((a,b), ps.build_cost_ms)
                ps.last_accessed_ts = time.time()

        self._catalog.record_query_touch(pid, list(plan.pair_sources.keys()),
                                          plan.sort_key == "pos",
                                          query_elapsed_ms, lazy_scan_costs)