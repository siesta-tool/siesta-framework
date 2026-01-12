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

    storage_namespace: str

    @property
    def s3_index_table(self) -> str:
        return f"s3://{self.storage_namespace}/{self.log_name}/index_table.parquet"
    @property
    def s3_sequence_table(self) -> str:
        return f"s3://{self.storage_namespace}/{self.log_name}/sequence_table.parquet"
    @property
    def s3_metadata_table(self) -> str:
        return f"s3://{self.storage_namespace}/{self.log_name}/metadata_table.parquet"
    @property
    def s3_single_table(self) -> str:
        return f"s3://{self.storage_namespace}/{self.log_name}/single_table.parquet"
    @property
    def s3_count_table(self) -> str:
        return f"s3://{self.storage_namespace}/{self.log_name}/count_table.parquet"
    @property
    def s3_last_checked_table(self) -> str:
        return f"s3://{self.storage_namespace}/{self.log_name}/last_checked_table.parquet"
    @property
    def s3_mining(self) -> str:
        return f"s3://{self.storage_namespace}/{self.log_name}/declare_table"

    def __init__(self, storage_namespace: str = "siesta", log_name: str = None):
        self.storage_namespace = storage_namespace
        self.log_name = log_name if log_name else "default"        
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