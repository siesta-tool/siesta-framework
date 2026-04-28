import time
from typing import Dict, Any
from siesta.core.sparkManager import get_spark_session
from siesta.core.storageFactory import get_storage_manager
from siesta.modules.query.parse_seql import extract_responded_pairs
from siesta.model.StorageModel import MetaData

import logging
logger = logging.getLogger(__name__)


def process_stats_query(config: Dict[str, Any], metadata: MetaData) -> list[any]|None|str:
    """
    Splits the query events in pairs and retrieves the statistics for each pair from the count table.
    """
    spark = get_spark_session()
    count_table = get_storage_manager().read_count_table(metadata)

    start_time = time.time()
    pair_branches = set([(x.source.label, x.target.label) for x in extract_responded_pairs(config.get("query", {}).get("pattern", ""))])
    
    
    pairs_df = spark.createDataFrame(list(pair_branches), ["source", "target"])
    df = count_table.join(pairs_df, on=["source", "target"], how="inner")

    end_time = time.time()
    logger.info(f"Stats Query completed in {end_time - start_time:.2f} seconds")

    return {"code" : 200 , "time": end_time - start_time} | {
        f"{row['source']},{row['target']}": {k: v for k, v in row.asDict().items() if k not in ("source", "target")}
        for row in df.collect()
    }