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
        return f"s3a://{self.storage_namespace}/{self.log_name}/index_table.parquet"
    @property
    def s3_sequence_table(self) -> str:
        return f"s3a://{self.storage_namespace}/{self.log_name}/sequence_table.parquet"
    @property
    def s3_metadata_table(self) -> str:
        return f"s3a://{self.storage_namespace}/{self.log_name}/metadata_table.parquet"
    @property
    def s3_single_table(self) -> str:
        return f"s3a://{self.storage_namespace}/{self.log_name}/single_table.parquet"
    @property
    def s3_count_table(self) -> str:
        return f"s3a://{self.storage_namespace}/{self.log_name}/count_table.parquet"
    @property
    def s3_last_checked_table(self) -> str:
        return f"s3a://{self.storage_namespace}/{self.log_name}/last_checked_table.parquet"
    @property
    def s3_mining(self) -> str:
        return f"s3a://{self.storage_namespace}/{self.log_name}/declare_table"
    
    # Aliases for S3Manager compatibility
    @property
    def index_table_path(self) -> str:
        return self.s3_index_table
    @property
    def sequence_table_path(self) -> str:
        return self.s3_sequence_table
    @property
    def metadata_table_path(self) -> str:
        return self.s3_metadata_table
    @property
    def single_table_path(self) -> str:
        return self.s3_single_table
    @property
    def count_table_path(self) -> str:
        return self.s3_count_table
    @property
    def last_checked_table_path(self) -> str:
        return self.s3_last_checked_table

    def __init__(self, storage_namespace: str = "siesta", log_name: str = None):
        self.storage_namespace = storage_namespace
        self.log_name = log_name if log_name else "default"        
        self.is_continued = False
        self.is_streaming = False

    def to_dict(self) -> dict:
        return {
            "log_name": self.log_name,
            "trace_count": getattr(self, 'trace_count', None),
            "event_count": getattr(self, 'event_count', None),
            "pair_count": getattr(self, 'pair_count', None),
            "is_continued": self.is_continued,
            "is_streaming": self.is_streaming,
            "start_timestamp": self.start_timestamp.isoformat() if getattr(self, 'start_timestamp', None) else None,
            "end_timestamp": self.end_timestamp.isoformat() if getattr(self, 'end_timestamp', None) else None,
            "last_mined_timestamp": self.last_mined_timestamp.isoformat() if getattr(self, 'last_mined_timestamp', None) else None,
            "storage_namespace": self.storage_namespace,
            "s3_index_table": self.s3_index_table,
            "s3_sequence_table": self.s3_sequence_table,
            "s3_metadata_table": self.s3_metadata_table,
            "s3_single_table": self.s3_single_table,
            "s3_count_table": self.s3_count_table,
            "s3_last_checked_table": self.s3_last_checked_table,
            "s3_mining": self.s3_mining
        }

class IndexTableEntry(DataModel.EventPair):
    def to_dict(self) -> dict:
        return super().to_dict()
    # @property
    # def trace_id(self) -> str:
    #     if self.source.trace_id != self.target.trace_id:
    #         raise ValueError("Source and target events do not belong to the same trace.")
    #     return self.source.trace_id

class SequenceTableEntry(DataModel.Trace):
    def to_dict(self) -> dict:
        return super().to_dict()