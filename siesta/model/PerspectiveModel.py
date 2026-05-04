from enum import IntEnum
from dataclasses import dataclass, field
from pyspark.sql.types import *

class PerspectiveLevel(IntEnum):
    L0_DECLARED         = 0     # only grouping key G in catalog, no physical change
    L1_POS_FREE         = 1     # grouping column v materialised in SequenceTable
    L2_POS_ESTABLISHED  = 2     # pos column materialised, SequenceMetadata maintained
    L3_PAIR_PERSISTED   = 3     # full PairsIndex[A,B] + LastChecked + incremental maint.

class PairStatus(IntEnum):
    ABSENT     = 0   # never built
    TRANSIENT  = 1   # L3-: built for last query, in LRU cache, no LastChecked
    PERSISTENT = 2   # L3:  full incremental maintenance

@dataclass
class PairStats:
    """Per-(G, A, B) workload statistics used by RetentionPolicy."""
    build_cost_ms: float = 0.0       # c_pair, measured on first build
    query_count: int = 0             # f_pair, number of queries touching this pair
    total_savings_ms: float = 0.0    # cumulative savings vs lazy scan
    total_maintenance_ms: float = 0.0
    maintenance_batch_count: int = 0
    status: PairStatus = PairStatus.ABSENT
    last_accessed_ts: float = 0.0
    last_decay_ts: float = 0.0  


@dataclass
class PerspectiveStats:
    """Per-G workload statistics used by RetentionPolicy."""
    level: PerspectiveLevel = PerspectiveLevel.L0_DECLARED
    grouping_keys: list[str] = field(default_factory=list)
    # Lookback config
    lookback: str = "7d"
    lookback_mode: str = "time"      # "time" or "position"
    # L1 stats
    l1_build_cost_ms: float = 0.0
    l1_query_count: int = 0
    l1_total_savings_ms: float = 0.0
    l1_maintenance_ms_per_batch: float = 0.0
    # L2 stats
    l2_build_cost_ms: float = 0.0
    l2_pos_query_count: int = 0      # f_pos: queries referencing pos
    l2_total_savings_ms: float = 0.0
    l2_maintenance_ms_per_batch: float = 0.0
    # Per-pair stats: keyed by (A, B)
    pairs: dict[tuple[str,str], PairStats] = field(default_factory=dict)
    last_decay_ts: float = 0.0   #  wall-clock when counters were last decayed

    @staticmethod
    def get_catalog_schema() -> StructType:
        # Serialised as a Delta table row per perspective
        return StructType([
            StructField("perspective_id", StringType(), False),
            StructField("grouping_keys", ArrayType(StringType()), False),
            StructField("level", IntegerType(), False),
            StructField("lookback", StringType(), False),
            StructField("lookback_mode", StringType(), False),
            StructField("stats_json", StringType(), True),  # full stats as JSON blob
        ])