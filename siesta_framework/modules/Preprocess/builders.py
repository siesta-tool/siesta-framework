from typing import Any, Dict
from siesta_framework.core.sparkManager import get_spark_session
from siesta_framework.core.storageFactory import get_metadata, get_storage_manager
from siesta_framework.core.config import get_system_config
from siesta_framework.model.DataModel import Event, EventConfig
from pyspark.sql import SparkSession, DataFrame
from parsers import process_events_batch, parse_log_file


def build_sequence_table(preprocess_config: Dict):
    """
    Build the Sequence Table from the log file, supporting both batch and streaming modes.
    """
    print("Preprocessor: Building Sequence Table...")
    if preprocess_config.get("enable_streaming", False):

        schema = EventConfig.from_preprocess_config(preprocess_config, "json").get_source_schema()
    
        storage = get_storage_manager()

        event_stream_agg = (get_spark_session().readStream
        .format("json")     
        .schema(schema)
        .option("schemaInference", "true")
        .option("columnNameOfCorruptRecord", "_corrupt_record") 
        .load(storage.get_steaming_collector_path(preprocess_config))) 

        def process_microbatch(batch_df, batch_id):
            process_events_batch(preprocess_config, batch_df, batch_id)

        query = (event_stream_agg.writeStream
            .foreachBatch(process_microbatch)
            .outputMode("append")
            .option("checkpointLocation", storage.get_checkpoint_location(preprocess_config, "sequence_table"))
            .start())
    
    else:
        
        events_df = parse_log_file(preprocess_config, local=True)
        process_events_batch(preprocess_config, events_df)
        