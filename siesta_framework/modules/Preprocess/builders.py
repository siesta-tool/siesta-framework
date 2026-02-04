from typing import Any, Dict
import threading

from pandas import DataFrame
from siesta_framework.core.sparkManager import get_spark_session
from siesta_framework.core.storageFactory import get_storage_manager
from siesta_framework.model.DataModel import Event, EventConfig
from siesta_framework.model.StorageModel import MetaData
from siesta_framework.modules.Preprocess.parsers import process_events_batch, process_event_log
from pyspark.sql.streaming import StreamingQuery
import logging
logger = logging.getLogger(__name__)



def build_sequence_table(preprocess_config: Dict, metadata: MetaData) -> DataFrame | StreamingQuery | None:
    """
    Build the Sequence Table from the log file, supporting both batch and streaming modes.
    
    Returns:
        DataFrame in batch mode, or tuple of (StreamingQuery, FirstBatchListener) in streaming mode
    """
    logger.info("Preprocess.builders: Building Sequence Table...")

    if preprocess_config.get("enable_streaming", False):

        schema = EventConfig.from_preprocess_config(preprocess_config, "json").get_source_schema()
    
        storage = get_storage_manager()
        spark = get_spark_session()

        event_stream_agg = (spark.readStream
        .format("json")     
        .schema(schema)
        .option("schemaInference", "true")
        .option("columnNameOfCorruptRecord", "_corrupt_record") 
        .load(storage.get_steaming_collector_path(preprocess_config))) 

        def process_microbatch(batch_df, batch_id):
            process_events_batch(preprocess_config, batch_df, batch_id, metadata)

        write_seq_job = (event_stream_agg.writeStream
            .queryName("build_sequence_table")
            .foreachBatch(process_microbatch)
            .outputMode("append")
            .option("checkpointLocation", storage.get_checkpoint_location(metadata, "sequence_table"))
            .start())
        return write_seq_job
    
    else:
        return process_event_log(preprocess_config, metadata)


def build_single_table(metadata: MetaData, events_df: DataFrame | StreamingQuery) -> DataFrame | None:
    """
    Build the Single Table from the Sequence Table, supporting both batch and streaming modes.
    
    Args:
        metadata: Metadata configuration
        events_df: DataFrame (batch) or StreamingQuery (streaming)
    """
    logger.info("Preprocess.builders: Building Single Table...")
    
    storage = get_storage_manager()
    
    if isinstance(events_df, StreamingQuery):        

        sequence_table_df = (get_spark_session().readStream
        .format("delta")
        .load(metadata.sequence_table_path)) 

        def process_microbatch(batch_df, batch_id):
            storage.write_single_table(batch_df, metadata)
            storage.write_metadata_table(metadata) #temporary for dev

    
        write_single_job = (sequence_table_df.writeStream
            .queryName("build_single_table")
            .foreachBatch(process_microbatch)
            .outputMode("append")
            .option("checkpointLocation", storage.get_checkpoint_location(metadata, "single_table"))
            .start())
        # return write_single_job
    
    else:
    
        storage.write_single_table(events_df=events_df, metadata=metadata)
        storage.write_metadata_table(metadata) #temporary for dev

        return events_df


def build_index_table(preprocess_config: Dict):
    """
    Build the Index Table from the Sequence Table.
    """
    logger.info("Preprocess.builders: Building Index Table...")
    # Implementation for building index table goes here
    pass