from typing import Any, Dict
from siesta_framework.core.sparkManager import get_spark_session
from siesta_framework.core.storageFactory import get_storage_manager
from siesta_framework.model.DataModel import EventConfig
from siesta_framework.model.StorageModel import MetaData
from siesta_framework.modules.Preprocess.parsers import process_events_batch, process_event_log
import logging
logger = logging.getLogger(__name__)


def build_sequence_table(preprocess_config: Dict, metadata: MetaData):
    """
    Build the Sequence Table from the log file, supporting both batch and streaming modes.
    """
    logger.info("Preprocess.builders: Building Sequence Table...")
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

        write_seq_job = (event_stream_agg.writeStream
            .foreachBatch(process_microbatch)
            .outputMode("append")
            .option("checkpointLocation", storage.get_checkpoint_location(preprocess_config, "sequence_table"))
            .start())
    else:
        process_event_log(preprocess_config, metadata)


def build_index_table(preprocess_config: Dict):
    """
    Build the Index Table from the Sequence Table.
    """
    logger.info("Preprocess.builders: Building Index Table...")
    # Implementation for building index table goes here
    pass