def detect_adaptive(pattern: str, grouping_keys: list[str],
                    constraints: dict, config: dict, metadata: MetaData,
                    planner: QueryPlanner):

    # 1. Parse pattern -> required pairs (reuse existing parse_seql logic)
    required_pairs = extract_required_pairs(pattern)
    references_pos = constraints_reference_pos(constraints)

    # 2. Ask planner what to use for each pair
    plan = planner.plan(grouping_keys, required_pairs, references_pos,
                        config.get("lookback","7d"), "time", metadata)

    # 3. Fetch data according to plan (L3 Delta read vs LRU vs lazy scan)
    lazy_costs = {}
    index_df = fetch_pairs_per_plan(plan, metadata, lazy_costs)  # new function

    # 4. Candidate group pruning - same logic as existing detect(),
    #    but keyed on group value v instead of trace_id
    pruned_groups = prune_candidates(index_df, required_pairs, plan.pid)

    # 5. Validation - same mapPartitions+OpenCEP logic as existing,
    #    sort key comes from plan.sort_key ("pos" or "ts")
    results = validate_groups(index_df, pruned_groups, pattern,
                               constraints, sort_key=plan.sort_key)

    # 6. Record workload and apply retention decisions
    planner.apply_retention_decisions(plan, elapsed_ms, lazy_costs)

    return results