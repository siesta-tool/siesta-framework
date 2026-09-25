from typing import Any, Dict, Tuple
import threading
import os
import time

from pyspark.sql import DataFrame
from siesta.core.sparkManager import get_spark_session
from siesta.core.storageFactory import get_storage_manager
from siesta.model.DataModel import Event, EventConfig
from siesta.model.StorageModel import MetaData
from siesta.modules.index.parsers import process_events_batch, process_event_log
from pyspark.sql.functions import col, from_json
from pyspark.sql.streaming.query import StreamingQuery
from siesta.modules.index.computations import extract_last_checked_and_all_pairs, extract_counts
from pyspark.sql.functions import min
import logging
logger = logging.getLogger(__name__)


def _log_ttq_batch(index_config: Dict, micro_batch_df: DataFrame, batch_id: int, commit_time: float) -> None:
    """Time-to-Queryable instrumentation (experiment-only, off by default).

    Emits one per-micro-batch record measuring the delay between when an event
    was handed to the Kafka producer (its ``_produced_at`` wall-clock marker,
    carried through as an attribute) and ``commit_time`` -- the moment the
    CountTable write for this batch completed, i.e. when the batch became
    visible to detection/stats queries.

    Enabled only when ``index_config['ttq_logging']`` is truthy, so the normal
    ingestion path is untouched. ``commit_time`` must be captured by the caller
    immediately after the CountTable write (no other step in between); the
    aggregation below runs afterwards and does not affect that measurement.
    """
    if not index_config.get("ttq_logging", False):
        return
    try:
        from pyspark.sql.functions import (
            count as _count, min as _min, max as _max, avg as _avg,
        )
        produced_at = col("attributes")["_produced_at"].cast("double")
        lam_col = col("attributes")["_lambda"].cast("double")
        stats = micro_batch_df.select(
            _count("*").alias("n"),
            _min(produced_at).alias("min_pa"),
            _max(produced_at).alias("max_pa"),
            _avg(produced_at).alias("avg_pa"),
            _max(lam_col).alias("lam"),
        ).collect()[0]

        n_events = stats["n"] or 0
        avg_pa = stats["avg_pa"]
        min_pa = stats["min_pa"]
        if not n_events or avg_pa is None or min_pa is None:
            # No instrumented events in this batch (e.g. warm-up); nothing to log.
            return

        # lambda: prefer the marker carried in the payload, else the config hint.
        lam = stats["lam"]
        if lam is None:
            lam = index_config.get("ttq_lambda", -1)

        batch_ttq_avg = commit_time - float(avg_pa)
        batch_ttq_tail = commit_time - float(min_pa)  # earliest event = worst case

        logger.info(
            f"ttq_batch lambda={lam:g} batch_id={batch_id} n_events={n_events} "
            f"avg_ttq={batch_ttq_avg:.6f} tail_ttq={batch_ttq_tail:.6f} "
            f"commit_time={commit_time:.6f}"
        )

        ttq_log_path = index_config.get("ttq_log_path")
        if ttq_log_path:
            new_file = not os.path.exists(ttq_log_path)
            os.makedirs(os.path.dirname(os.path.abspath(ttq_log_path)) or ".", exist_ok=True)
            with open(ttq_log_path, "a") as fh:
                if new_file:
                    fh.write("lambda,batch_id,n_events,avg_ttq,tail_ttq,commit_time\n")
                fh.write(
                    f"{lam:g},{batch_id},{n_events},{batch_ttq_avg:.6f},"
                    f"{batch_ttq_tail:.6f},{commit_time:.6f}\n"
                )
    except Exception as e:  # never let instrumentation break ingestion
        logger.warning(f"ttq_batch logging failed for batch {batch_id}: {e}")



def build_sequence_table(index_config: Dict, metadata: MetaData) -> DataFrame | StreamingQuery:
    """
    Build the Sequence Table from the log file, supporting both batch and streaming modes.
    
    Returns:
        DataFrame in batch mode, or tuple of (StreamingQuery, FirstBatchListener) in streaming mode
    """
    logger.info("Building Sequence Table...")

    if index_config.get("enable_streaming", False):

        schema = EventConfig.from_preprocess_config(index_config, "json").get_source_schema()
    
        storage = get_storage_manager()
        spark = get_spark_session()

        event_stream_agg = (spark.readStream
        .format("json")     
        .schema(schema)
        .option("schemaInference", "true")
        .option("columnNameOfCorruptRecord", "_corrupt_record") 
        .load(storage.get_steaming_collector_path(index_config))) 

        def process_microbatch(batch_df, batch_id):
            process_events_batch(index_config, batch_df, batch_id, metadata)

        write_seq_job = (event_stream_agg.writeStream
            .queryName("build_sequence_table")
            .foreachBatch(process_microbatch)
            .outputMode("append")
            .option("checkpointLocation", storage.get_checkpoint_location(metadata, "sequence_table"))
            .start())
        return write_seq_job
    
    else:
        return process_event_log(index_config, metadata)


def build_activity_index(metadata: MetaData, events_df: DataFrame | StreamingQuery) -> DataFrame | StreamingQuery:
    """
    Build the Activity index Table from the Sequence Table, supporting both batch and streaming modes.
    
    Args:
        metadata: Metadata configuration
        events_df: DataFrame (batch) or StreamingQuery (streaming)
    """
    logger.info("Building Activity Index Table...")
    
    storage = get_storage_manager()
    
    if isinstance(events_df, StreamingQuery):        

        sequence_table_df = (get_spark_session().readStream
        .format("delta")
        .load(metadata.sequence_table_path)) 

        def process_microbatch(batch_df, batch_id):
            storage.write_activity_index(batch_df, metadata)


        write_activity_index_job = (sequence_table_df.writeStream
            .queryName("build_activity_index")
            .foreachBatch(process_microbatch)
            .outputMode("append")
            .option("checkpointLocation", storage.get_checkpoint_location(metadata, "activity_index"))
            .start())
        return write_activity_index_job

    else:

        storage.write_activity_index(events_df=events_df, metadata=metadata)

        return events_df


def build_pairs_index(index_config: Dict, metadata: MetaData, batch_pairs_index_df: DataFrame | StreamingQuery):
    """
    Build the Index Table from the Active pairs table.
    """
    logger.info("Building Index Table...")
    # Implementation for building index table goes here
    storage = get_storage_manager()

    if isinstance(batch_pairs_index_df, StreamingQuery):
        # Already handled inside build_last_checked_table's foreachBatch
        logger.info("Pairs index handled by streaming job, skipping.")
        return None
    else:
        storage.write_pairs_index(new_pairs=batch_pairs_index_df, metadata=metadata)
        return batch_pairs_index_df


def build_count_table(index_config: Dict, metadata: MetaData, batch_pairs_index_df: DataFrame):
    """
    Build the Count Table from the last checked.
    """
    logger.info("Building Count Table...")
    
    storage = get_storage_manager()

    if isinstance(batch_pairs_index_df, StreamingQuery):
        # Already handled inside build_last_checked_table's foreachBatch
        logger.info("Count table handled by streaming job, skipping.")
        return None
    else:
        count_table = extract_counts(batch_pairs_index_df)
        
        storage.write_count_table(count_df=count_table, metadata=metadata)
        return count_table



def build_last_checked_table(index_config: Dict, metadata: MetaData, batch_activity_index_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Build Last Checked.
    """
    logger.info("Building Last Checked Table...")

    storage = get_storage_manager()
    lookback = index_config.get("lookback", "7d")

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


def build_last_checked_index_and_count_streamed(index_config: Dict, metadata: MetaData, batch_activity_index_df: StreamingQuery) -> StreamingQuery:
    """
    Building the Last Checked, Pairs Index, and Count Tables concurrently.
    In streaming mode, all three are handled here since pairs_df 
    only exists transiently inside foreachBatch.
    """
    logger.info("Building Last Checked Table...")

    storage = get_storage_manager()
    lookback = index_config.get("lookback", "7d")

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

        # Queryability boundary: the CountTable write above is the last of the
        # five structures to be persisted and is what detection/stats queries
        # read. Capture the commit instant here, before any further work.
        commit_time = time.time()

        pairs_df.unpersist()

        storage.write_metadata_table(metadata)

        # Time-to-Queryable instrumentation (no-op unless ttq_logging is set).
        _log_ttq_batch(index_config, micro_batch_df, batch_id, commit_time)


    job = (
        sequence_stream_df.writeStream
        .queryName("build_pairs_tables")   # renamed: reflects wider responsibility
        .foreachBatch(process_batch)
        .option("checkpointLocation", storage.get_checkpoint_location(metadata, "last_checked"))
        .start()
    )
    return job