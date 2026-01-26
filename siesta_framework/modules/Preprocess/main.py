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

    def startup(self):
        self.config = get_config()
        self.spark = get_spark_session()
        if self.config.get("enable_streaming", False):
            storage = get_storage_manager()
            if storage:
                storage.initialize_streaming_collector(self.config)
            else:
                raise RuntimeError("Storage manager could not be initialized for streaming")

    def run(self, *args: Any, **kwargs: Any) -> Any:
        print(f"{self.name} is running with args: {args} and kwargs: {kwargs}")
        
        module = args[0] if args else None
        match module:
            case ["testparse"]:
                print("Running testparse...")
                
                if not hasattr(self, 'spark') or self.spark is None:
                    self.spark = get_spark_session()
                
                path = f"s3a://{self.config.get('storage_namespace', 'siesta')}/{self.config.get('log_name', 'default_log')}/{self.config.get('raw_events_dir', 'raw_events')}/*/*"
                
                try:
                    # Generic parsing function that determines the format and path from config.
                    unified_stream = (self.spark.readStream
                        .format("cloudFiles")
                        .option("cloudFiles.format", "json")     
                        .load(path)) 
                except Exception:
                    # Use EventConfig to get the expected source schema
                    source_schema = EventConfig.from_system_config(self.config, "json").get_source_schema()
                                    
                    # Set up a unified listener for streamed (rt/) and batched (batched/) data processing
                    unified_stream = (self.spark.readStream
                        .format("json")
                        .schema(source_schema)
                        .load(path)) 

                def process_batch(batch_df, batch_id):
                    if batch_df.isEmpty():
                        return
                    
                    try:
                        event_config = EventConfig.from_system_config(self.config, "json")
                        events_rdd = _parse_rows(event_config, batch_df)
                        
                        event_dicts_rdd = events_rdd.map(lambda event: event.to_dict())
                        
                        events_df = self.spark.createDataFrame(event_dicts_rdd, schema=Event.get_schema())
                        
                        events_df.write \
                            .format("delta") \
                            .mode("append") \
                            .option("path", "s3a://siesta/data/indexed_logs/") \
                            .option("mergeSchema", "true") \
                            .save()
                    except Exception as e:
                        print(f"Error processing batch {batch_id}: {e}")

                query = (unified_stream.writeStream
                    .foreachBatch(process_batch)
                    .outputMode("append")
                    .option("checkpointLocation", "s3a://siesta/checkpoints/index/")
                    .start())
                
                query.awaitTermination()
