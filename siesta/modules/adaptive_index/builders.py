def promote_to_l1(pid: str, grouping_keys: list[str],
                  metadata: MetaData, storage: StorageManager) -> float:
    """
    Materialises the grouping column v = phi_G(.) on SequenceTable.
    Returns elapsed ms (stored as l1_build_cost_ms).
    No pos assignment, no SequenceMetadata bootstrap.
    Uses Delta schema evolution to add the column non-destructively.
    """
    ...

def promote_to_l2(pid: str, grouping_keys: list[str],
                  metadata: MetaData, storage: StorageManager) -> float:
    """
    Assigns intra-group positions and bootstraps SequenceMetadata[G].
    Reads the already-partitioned view from L1, sorts per group by ts,
    assigns pos via window function, writes back.
    Returns elapsed ms.
    """
    ...

def build_pair_transient(pid: str, act_a: str, act_b: str,
                         candidate_groups: list[str],
                         metadata: MetaData, storage: StorageManager,
                         has_pos: bool) -> DataFrame:
    """
    Builds PairsIndex[A,B] transiently for a specific set of candidate groups.
    Does NOT write to Delta, does NOT update LastChecked.
    Returns the pairs DataFrame directly (caller caches in LRU).
    Reuses the STNM two-pointer extraction from computations.py.
    """
    ...

def build_pair_persistent(pid: str, act_a: str, act_b: str,
                          metadata: MetaData, storage: StorageManager,
                          has_pos: bool) -> float:
    """
    Full L3 materialisation: writes PairsIndex[A,B] to Delta,
    bootstraps LastChecked for (A,B) under this perspective,
    registers incremental maintenance.
    Returns elapsed ms (stored as build_cost_ms).
    """
    ...

def incremental_update_persistent_pairs(pid: str, batch_df: DataFrame,
                                         persistent_pairs: list[tuple[str,str]],
                                         metadata: MetaData,
                                         storage: StorageManager):
    """
    Called on each ingestion batch. For each L3 pair under pid,
    runs the LastChecked-guided extraction and appends to PairsIndex.
    Mirrors existing build_last_checked_table logic but scoped to
    the specific (pid, A, B) combinations that are persistent.
    """
    ...