from siesta_framework.core.sparkManager import get_spark_session
from siesta_framework.core.storageFactory import get_storage_manager
from siesta_framework.model.StorageModel import MetaData
from siesta_framework.model.SystemModel import Query_Config
from pyspark.sql import functions as F
import logging
logger = logging.getLogger("Query Processors")

def stats_query_processor(config: Query_Config, metadata: MetaData) -> list[str]|None:
    """
    Splits the query events in pairs and retrieves the statistics for each pair from the count table.
    """
    spark = get_spark_session()
    count_table = get_storage_manager().read_count_table(metadata)
    
    activities = sorted( config.get("query", {}).get("pattern", []),\
                                key=lambda x: x.get("position", 0))
    pairs = list(zip(map(lambda x: x.get("activity"), activities), map(lambda x: x.get("activity"), activities[1:])))
    
    logger.info(activities)
    pairs_df = spark.createDataFrame(pairs, ["source", "target"])
    df = count_table.join(pairs_df, on=["source", "target"], how="inner")
    df.show()



