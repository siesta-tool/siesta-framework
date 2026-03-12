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
from siesta_framework.modules.Preprocess.computations import extract_last_checked_and_all_pairs, extract_counts
from pyspark.sql.functions import min
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


def build_activity_index(metadata: MetaData, events_df: DataFrame | StreamingQuery) -> DataFrame | StreamingQuery:
    """
    Build the Activity index Table from the Sequence Table, supporting both batch and streaming modes.
    
    Args:
        metadata: Metadata configuration
        events_df: DataFrame (batch) or StreamingQuery (streaming)
    """
    logger.info("Preprocess.builders: Building Activity Index Table...")
    
    storage = get_storage_manager()
    
    if isinstance(events_df, StreamingQuery):        

        sequence_table_df = (get_spark_session().readStream
        .format("delta")
        .load(metadata.sequence_table_path)) 

        def process_microbatch(batch_df, batch_id):
            storage.write_activity_index(batch_df, metadata)
            storage.write_metadata_table(metadata) #temporary for dev

    
        write_activity_index_job = (sequence_table_df.writeStream
            .queryName("build_activity_index")
            .foreachBatch(process_microbatch)
            .outputMode("append")
            .option("checkpointLocation", storage.get_checkpoint_location(metadata, "activity_index"))
            .start())
        return write_activity_index_job
    
    else:
    
        storage.write_activity_index(events_df=events_df, metadata=metadata)
        storage.write_metadata_table(metadata) #temporary for dev

        return events_df


def build_pairs_index(preprocess_config: Dict, metadata: MetaData, batch_pairs_index_df: DataFrame | StreamingQuery):
    """
    Build the Index Table from the Active pairs table.
    """
    logger.info("Preprocess.builders: Building Index Table...")
    # Implementation for building index table goes here
    storage = get_storage_manager()

    if isinstance(batch_pairs_index_df, StreamingQuery):
        # Already handled inside build_last_checked_table's foreachBatch
        logger.info("Preprocess.builders: Pairs index handled by streaming job, skipping.")
        return None
    else:
        storage.write_pairs_index(new_pairs=batch_pairs_index_df, metadata=metadata)
        return batch_pairs_index_df


def build_count_table(preprocess_config: Dict, metadata: MetaData, batch_pairs_index_df: DataFrame):
    """
    Build the Count Table from the last checked.
    """
    logger.info("Preprocess.builders: Building Count Table...")
    
    storage = get_storage_manager()

    if isinstance(batch_pairs_index_df, StreamingQuery):
        # Already handled inside build_last_checked_table's foreachBatch
        logger.info("Preprocess.builders: Count table handled by streaming job, skipping.")
        return None
    else:
        count_table = extract_counts(batch_pairs_index_df)
        
        storage.write_count_table(count_df=count_table, metadata=metadata)
        return count_table



def build_last_checked_table(preprocess_config: Dict, metadata: MetaData, batch_activity_index_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Build Last Checked.
    """
    logger.info("Preprocess.builders: Building Last Checked Table...")

    storage = get_storage_manager()
    lookback = preprocess_config.get("lookback", "7d")

    updated_trace_ids = batch_activity_index_df.select("trace_id").distinct()
    batch_min_ts = batch_activity_index_df.agg(min("start_timestamp")).collect()[0][0]
    batch_min_pos = batch_activity_index_df.agg(min("position")).collect()[0][0]

    previous_last_checked = (
        storage.read_last_checked_table(metadata)
        .join(updated_trace_ids, on="trace_id", how="inner")
    )
    sequence_df = (
        storage.read_sequence_table(metadata)
        .join(updated_trace_ids, on="trace_id", how="inner")
    )

    pairs_df, last_checked_df = extract_last_checked_and_all_pairs(
        updated_sequence_table_DF=sequence_df,
        previous_last_checked=previous_last_checked,
        lookback=lookback,
        batch_min_ts=batch_min_ts,
        batch_min_pos=batch_min_pos
    )

    # Cache pairs_df so the expensive cogroup isn't recomputed for
    # pairs_index and count_table writes that follow.
    pairs_df.cache()

    storage.write_last_checked_table(last_checked_df, metadata)
    return pairs_df, last_checked_df


def build_last_checked_index_and_count_streamed(preprocess_config: Dict, metadata: MetaData, batch_activity_index_df: StreamingQuery) -> StreamingQuery:
    """
    Building the Last Checked, Pairs Index, and Count Tables concurrently.
    In streaming mode, all three are handled here since pairs_df 
    only exists transiently inside foreachBatch.
    """
    logger.info("Preprocess.builders: Building Last Checked Table...")

    storage = get_storage_manager()
    lookback = preprocess_config.get("lookback", "7d")

    sequence_stream_df = (
            get_spark_session().readStream
            .format("delta")
            .load(metadata.sequence_table_path)
        )

    def process_batch(micro_batch_df: DataFrame, batch_id: int):
        if micro_batch_df.isEmpty():
            return


        batch_min_ts = micro_batch_df.agg(min("start_timestamp")).collect()[0][0]
        batch_min_pos = micro_batch_df.agg(min("position")).collect()[0][0]
        updated_trace_ids = micro_batch_df.select("trace_id").distinct()

        previous_last_checked = (
            storage.read_last_checked_table(metadata)
            .join(updated_trace_ids, on="trace_id", how="inner")
        )
        sequence_df = (
            storage.read_sequence_table(metadata)
            .join(updated_trace_ids, on="trace_id", how="inner")
        )

        pairs_df, last_checked_df = extract_last_checked_and_all_pairs(
            updated_sequence_table_DF=sequence_df,
            previous_last_checked=previous_last_checked,
            lookback=lookback,
            batch_min_ts=batch_min_ts,
            batch_min_pos=batch_min_pos
        )

        # Cache pairs_df to avoid recomputing the cogroup for each write.
        pairs_df.cache()

        # Write all three tables that depend on pairs here,
        # since pairs_df only lives inside this foreachBatch scope.
        storage.write_last_checked_table(last_checked_df, metadata)
        storage.write_pairs_index(new_pairs=pairs_df, metadata=metadata)

        count_df = extract_counts(pairs_df)
        storage.write_count_table(count_df=count_df, metadata=metadata)

        pairs_df.unpersist()


    job = (
        sequence_stream_df.writeStream
        .queryName("build_pairs_tables")   # renamed: reflects wider responsibility
        .foreachBatch(process_batch)
        .option("checkpointLocation", storage.get_checkpoint_location(metadata, "last_checked"))
        .start()
    )
    return job