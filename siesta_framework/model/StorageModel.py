from datetime import datetime
from . import DataModel

class MetaData:
    log_name: str
    trace_count: int
    event_count: int    
    pair_count: int
    
    is_continued: bool
    is_streaming: bool
    
    start_timestamp: datetime
    end_timestamp: datetime    
    last_mined_timestamp: datetime

    # Generic storage namespace (bucket/container/prefix/domain)
    storage_namespace: str
    
    index_table_path: str
    sequence_table_path: str
    metadata_table_path: str
    single_table_path: str
    count_table_path: str
    last_checked_table_path: str
    declare_table_path: str

    def __init__(self, storage_namespace: str = "siesta", log_name: str = None):
        self.storage_namespace = storage_namespace
        self.log_name = log_name if log_name else "default"
        
        # Initialize table paths using the storage namespace
        self.index_table_path = f"s3a://{self.storage_namespace}/{self.log_name}/index.parquet"
        self.sequence_table_path = f"s3a://{self.storage_namespace}/{self.log_name}/sequence.parquet"
        self.metadata_table_path = f"s3a://{self.storage_namespace}/{self.log_name}/metadata.parquet"
        self.single_table_path = f"s3a://{self.storage_namespace}/{self.log_name}/single.parquet"
        self.count_table_path = f"s3a://{self.storage_namespace}/{self.log_name}/count.parquet"
        self.last_checked_table_path = f"s3a://{self.storage_namespace}/{self.log_name}/last_checked.parquet"
        self.declare_table_path = f"s3a://{self.storage_namespace}/{self.log_name}/declare.parquet"
        
        self.is_continued = False
        self.is_streaming = False


class IndexTableEntry(DataModel.EventPair):
    pass
    # @property
    # def trace_id(self) -> str:
    #     if self.source.trace_id != self.target.trace_id:
    #         raise ValueError("Source and target events do not belong to the same trace.")
    #     return self.source.trace_id

class SequenceTableEntry(DataModel.Trace):
    pass