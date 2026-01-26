from typing import Any
from siesta_framework.modules.Example.main import Example
from siesta_framework.core.interfaces import SiestaModule
from siesta_framework.core.config import get_config
from siesta_framework.core.storageFactory import get_storage_manager, get_metadata
from siesta_framework.model.DataModel import EventConfig, Event
from siesta_framework.core.sparkManager import get_spark_session
from .parse_log import parse_log_file, _parse_rows


class Preprocessor(SiestaModule):
        
    name = "preprocess"
    version = "1.2.0"

    def __init__(self):
        super().__init__()
        self.config = None
        self.spark = None
        self.storage = None

    def startup(self):
        self.config = get_config()
        self.spark = get_spark_session()
        if self.config.get("enable_streaming", False):
            self.storage = get_storage_manager()
            if self.storage:
                self.storage.initialize_streaming_collector(self.config)
                self.build_sequence_table()
            else:
                raise RuntimeError("Storage manager could not be initialized for streaming")

    
    def _process_events_batch(self, batch_df, batch_id=None):
        """
        Core processing logic for event batches; parses and stores events in Sequence Table.
        Args:
            batch_df: DataFrame containing the batch of events
            batch_id: Optional batch identifier for logging
        """
        if batch_df.isEmpty():
            return
        
        try:
            event_config = EventConfig.from_system_config(self.config, "json")
            events_rdd = _parse_rows(event_config, batch_df)
            
            event_dicts_rdd = events_rdd.map(lambda event: event.to_dict())
            
            events_df = self.spark.createDataFrame(event_dicts_rdd, schema=Event.get_schema())
            
            self.storage.write_sequence_table(events_df, self.config)
        except Exception as e:
            batch_info = f"batch {batch_id}" if batch_id is not None else "batch"
            print(f"Error processing {batch_info}: {e}")
    
    def build_sequence_table(self):
        """
        Build the Sequence Table from the log file, supporting both batch and streaming modes.
        """
        print("Preprocessor: Building Sequence Table...")
        if self.config.get("enable_streaming", False):

            schema = EventConfig.from_system_config(self.config, "json").get_source_schema()
        
            event_stream_agg = (self.spark.readStream
            .format("json")     
            .schema(schema)
            .option("schemaInference", "true")
            .option("columnNameOfCorruptRecord", "_corrupt_record") 
            .load(self.storage.get_steaming_collector_path(self.config))) 

            config = self.config
            spark = self.spark
            storage = self.storage

            def process_microbatch(batch_df, batch_id):
                self._process_events_batch(batch_df, batch_id)

            query = (event_stream_agg.writeStream
                .foreachBatch(process_microbatch)
                .outputMode("append")
                .option("checkpointLocation", storage.get_checkpoint_location(config, "sequence_table"))
                .start())
        
        else:
            
            events_rdd = parse_log_file()
            event_dicts_rdd = events_rdd.map(lambda event: event.to_dict())
            events_df = self.spark.createDataFrame(event_dicts_rdd, schema=Event.get_schema())
            
            self._process_events_batch(events_df)

    def run(self, *args: Any, **kwargs: Any) -> Any:
        print(f"{self.name} is running with args: {args} and kwargs: {kwargs}")
        
        module = args[0] if args else None
        match module:
            case ["testparse"]:
                print("Running testparse...")
                pass    
            