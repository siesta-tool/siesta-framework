from typing import Dict, Any
from siesta.core.sparkManager import get_spark_session
from siesta.core.storageFactory import get_storage_manager
from siesta.modules.query.parse_seql import extract_responded_pairs
from siesta.model.StorageModel import MetaData



def process_stats_query(config: Dict[str, Any], metadata: MetaData) -> list[any]|None|str:
    """
    Splits the query events in pairs and retrieves the statistics for each pair from the count table.
    """
    spark = get_spark_session()
    count_table = get_storage_manager().read_count_table(metadata)

    pair_branches = set([(x.source.label, x.target.label) for x in extract_responded_pairs(config.get("query", {}).get("pattern", ""))])
    
    
    pairs_df = spark.createDataFrame(list(pair_branches), ["source", "target"])
    df = count_table.join(pairs_df, on=["source", "target"], how="inner")

    return {
        f"{row['source']},{row['target']}": {k: v for k, v in row.asDict().items() if k not in ("source", "target")}
        for row in df.collect()
    }