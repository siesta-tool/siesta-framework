import time
from typing import Dict, Any
from siesta.core.sparkManager import get_spark_session
from siesta.core.storageFactory import get_storage_manager
from siesta.modules.query.parse_seql import extract_responded_pairs
from siesta.modules.query.dictionary import load_activity_code_map
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

    # Dictionary-code the pattern: encode labels to integer codes for the join,
    # keep the reverse map to decode source/target back to labels for display.
    code_map = load_activity_code_map(metadata)
    label_map = {code: label for label, code in code_map.items()}

    label_pairs = set(
        (x.source.label, x.target.label)
        for x in extract_responded_pairs(config.get("query", {}).get("pattern", ""))
    )
    # Unknown labels (absent from the log) can never match -> drop them.
    pair_branches = {
        (code_map[s], code_map[t])
        for s, t in label_pairs
        if s in code_map and t in code_map
    }

    pairs_df = spark.createDataFrame(list(pair_branches), schema="source int, target int")
    df = count_table.join(pairs_df, on=["source", "target"], how="inner")

    end_time = time.time()
    logger.info(f"Stats Query completed in {end_time - start_time:.2f} seconds")

    return {"code" : 200 , "time": end_time - start_time} | {
        f"{label_map.get(row['source'], row['source'])},{label_map.get(row['target'], row['target'])}":
            {k: v for k, v in row.asDict().items() if k not in ("source", "target")}
        for row in df.collect()
    }