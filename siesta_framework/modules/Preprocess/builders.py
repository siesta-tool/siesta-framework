from typing import Any, Dict, Tuple
import threading

from pyspark.sql import DataFrame
from siesta_framework.core.sparkManager import get_spark_session
from siesta_framework.core.storageFactory import get_storage_manager
from siesta_framework.model.DataModel import Event, EventConfig
from siesta_framework.model.StorageModel import MetaData
from siesta_framework.modules.Preprocess.parsers import process_events_batch, process_event_log
from pyspark.sql.functions import col, from_json
from pyspark.sql.streaming.query import StreamingQuery
from siesta_framework.modules.Preprocess.computations import extract_pairs_and_last_checked, extract_counts
import logging
logger = logging.getLogger(__name__)



def build_sequence_table(preprocess_config: Dict, metadata: MetaData) -> DataFrame | StreamingQuery:
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


def build_single_table(metadata: MetaData, events_df: DataFrame | StreamingQuery) -> DataFrame | StreamingQuery:
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
        return write_single_job
    
    else:
    
        storage.write_single_table(events_df=events_df, metadata=metadata)
        storage.write_metadata_table(metadata) #temporary for dev

        return events_df


def build_pairs_index_table(preprocess_config: Dict, metadata: MetaData, batch_pairs_index_df: DataFrame | StreamingQuery) -> DataFrame|StreamingQuery:
    """
    Build the Index Table from the Last_checked pairs.
    """
    logger.info("Preprocess.builders: Building Index Table...")
    # Implementation for building index table goes here
    storage = get_storage_manager()

    if isinstance(batch_pairs_index_df, StreamingQuery):
        #TODO: Do properly
        def process_batch(pairs_streaming_query, id):
            storage.write_pairs_index_table(new_pairs=pairs_streaming_query, metadata=metadata)
        
        pairs_streaming_df = (get_spark_session().readStream.format("delta").load(metadata.index_table_path))
        job = (pairs_streaming_df.writeStream
               .queryName("build_index_df")
               .foreachBatch(process_batch)
               .outputMode("append")
               .option("checkpointLocation", storage.get_checkpoint_location(metadata, "index_table"))
               .start())
        return job
    else:
        storage.write_pairs_index_table(new_pairs=batch_pairs_index_df, metadata=metadata)
        return batch_pairs_index_df

def build_count_table(preprocess_config: Dict, metadata: MetaData, batch_pairs_index_df: DataFrame):
    """
    Build the Count Table from the Last_checked pairs.
    """
    logger.info("Preprocess.builders: Building Count Table...")
    
    storage = get_storage_manager()

    if isinstance(batch_pairs_index_df, StreamingQuery):
        #TODO: Do properly
        def process_batch(pairs_streaming_query, id):
            count_table = extract_counts(pairs_streaming_query)
            storage.write_count_table(count_df=count_table, metadata=metadata)
        
        pairs_streaming_df = (get_spark_session().readStream.format("delta").load(metadata.count_table_path))
        job = (pairs_streaming_df.writeStream
               .queryName("build_count_table")
               .foreachBatch(process_batch)
               .outputMode("append")
               .option("checkpointLocation", storage.get_checkpoint_location(metadata, "count_table"))
               .start())
        return job
    else:
        count_table = extract_counts(batch_pairs_index_df)
        storage.write_count_table(count_df=count_table, metadata=metadata)
        return count_table


def buildPairs(preprocess_config: Dict, metadata: MetaData, batch_single_df: DataFrame | StreamingQuery) -> DataFrame | StreamingQuery:
    storage = get_storage_manager()
    pairs_df = "Test"
    pass



def build_last_checked_table(preprocess_config: Dict, metadata: MetaData, batch_single_df: DataFrame | StreamingQuery) -> Tuple[DataFrame, DataFrame] | Tuple[StreamingQuery, None]:
    """
    Build the Last Checked Table.
    """
    logger.info("Preprocess.builders: Building Last Checked Table...")

    storage = get_storage_manager()
    lookback = preprocess_config.get("lookback", 7)

    

    # Implementation for building last checked table goes here
    if isinstance(batch_single_df, StreamingQuery):
        batch_single_df = (get_spark_session().readStream
        .format("delta")
        .load(metadata.sequence_table_path))

        def process_batch(batch_single_df, batch_id, streaming=True):
            updated_trace_ids = batch_single_df.select("trace_id").distinct()

            # Filtering for the updated traces to update our pairs
            last_checked = storage.read_last_checked_table(metadata).join(other=updated_trace_ids, on="trace_id", how="inner")
            sequence_df = storage.read_sequence_table(metadata).join(other=updated_trace_ids, on="trace_id", how="inner")

            pairs_df, last_checked_df = extract_pairs_and_last_checked(updated_sequence_table_DF=sequence_df, last_checked=last_checked, lookback=lookback)

            storage.write_last_checked_table(last_checked_df, metadata)
            return (pairs_df, last_checked_df) if not streaming else None

        write_last_checked_job = (batch_single_df.writeStream
            .queryName("build_last_checked_table")
            .foreachBatch(process_batch) # type: ignore
            .option("checkpointLocation", storage.get_checkpoint_location(metadata, "last_checked"))
            .start())
        return write_last_checked_job, None

    else:

        updated_trace_ids = batch_single_df.select("trace_id").distinct()


        # Filtering for the updated traces to update our pairs
        last_checked = storage.read_last_checked_table(metadata).join(other=updated_trace_ids, on="trace_id", how="inner")
        sequence_df = storage.read_sequence_table(metadata).join(other=updated_trace_ids, on="trace_id", how="inner")

        pairs_df, last_checked_df = extract_pairs_and_last_checked(updated_sequence_table_DF=sequence_df, last_checked=last_checked, lookback=lookback)
        # logger.info(pairs_df.show())

        storage.write_last_checked_table(last_checked_df, metadata)

        return pairs_df, last_checked_df