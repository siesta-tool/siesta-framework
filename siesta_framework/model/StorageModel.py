from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from . import DataModel
import xxhash

class MetaData:
    log_name: str
    storage_namespace: str
    storage_type: str
    
    trace_count: int
    approx_unique_traces: set[int]

    event_count: int    
    approx_unique_activities: set[int]

    pair_count: int

    first_timestamp: datetime
    last_timestamp: datetime    
    last_mined_timestamp: datetime

    # S3 Table paths
    @property
    def s3_index_table(self) -> str:
        return f"s3a://{self.storage_namespace}/{self.log_name}/index_table"
    @property
    def s3_sequence_table(self) -> str:
        return f"s3a://{self.storage_namespace}/{self.log_name}/sequence_table"
    @property
    def s3_metadata_table(self) -> str:
        return f"s3a://{self.storage_namespace}/{self.log_name}/metadata_table"
    @property
    def s3_single_table(self) -> str:
        return f"s3a://{self.storage_namespace}/{self.log_name}/single_table"
    @property
    def s3_count_table(self) -> str:
        return f"s3a://{self.storage_namespace}/{self.log_name}/count_table.parquet"
    @property
    def s3_last_checked_table(self) -> str:
        return f"s3a://{self.storage_namespace}/{self.log_name}/last_checked_table"
    @property
    def s3_mining(self) -> str:
        return f"s3a://{self.storage_namespace}/{self.log_name}/declare_table"
    
    # Table paths for Storage Managers (extend for more compatibility)
    @property
    def index_table_path(self) -> str:
        return self.s3_index_table if self.storage_type == "s3" else ""
    @property
    def sequence_table_path(self) -> str:
        return self.s3_sequence_table if self.storage_type == "s3" else ""
    @property
    def metadata_table_path(self) -> str:
        return self.s3_metadata_table if self.storage_type == "s3" else ""
    @property
    def single_table_path(self) -> str:
        return self.s3_single_table if self.storage_type == "s3" else ""
    @property
    def count_table_path(self) -> str:
        return self.s3_count_table if self.storage_type == "s3" else ""
    @property
    def last_checked_table_path(self) -> str:
        return self.s3_last_checked_table if self.storage_type == "s3" else ""

    def __init__(self, storage_namespace: str = "siesta", storage_type: str = "s3", log_name: str = "default_log"):
        self.storage_namespace = storage_namespace
        self.log_name = log_name
        self.storage_type = storage_type
        self.approx_unique_traces = set()
        self.approx_unique_activities = set()

    @staticmethod
    def get_schema() -> StructType:
        """Return the Spark schema for MetaData serialization."""
        return StructType([
            StructField("log_name", StringType(), False),
            StructField("storage_namespace", StringType(), False),
            StructField("trace_count", IntegerType(), False),
            StructField("event_count", IntegerType(), False),
            StructField("pair_count", IntegerType(), False),
            StructField("first_timestamp", StringType(), True),
            StructField("last_timestamp", StringType(), True),
            StructField("last_mined_timestamp", StringType(), True),
            StructField("approx_unique_traces", ArrayType(IntegerType()), True),
            StructField("approx_unique_activities", ArrayType(IntegerType()), True),
        ])

    def to_dict(self) -> dict:
        return {
            "log_name": self.log_name,
            "storage_namespace": self.storage_namespace,
            "trace_count": getattr(self, 'trace_count', 0),
            "event_count": getattr(self, 'event_count', 0),
            "pair_count": getattr(self, 'pair_count', 0),
            "first_timestamp": self.first_timestamp.isoformat() if getattr(self, 'first_timestamp', None) else None,
            "last_timestamp": self.last_timestamp.isoformat() if getattr(self, 'last_timestamp', None) else None,
            "last_mined_timestamp": self.last_mined_timestamp.isoformat() if getattr(self, 'last_mined_timestamp', None) else None,
            "approx_unique_traces": list(self.approx_unique_traces) if getattr(self, 'approx_unique_traces', None) else [],
            "approx_unique_activities": list(self.approx_unique_activities) if getattr(self, 'approx_unique_activities', None) else [],
        }
    
    def __str__(self) -> str:
        return self.to_dict().__str__()


class IndexTableEntry(DataModel.EventPair):
    def to_dict(self) -> dict:
        return super().to_dict()
    # @property
    # def trace_id(self) -> str:
    #     if self.source.trace_id != self.target.trace_id:
    #         raise ValueError("Source and target events do not belong to the same trace.")
    #     return self.source.trace_id

class SequenceTableEntry(DataModel.Event):
    @staticmethod
    def get_schema() -> StructType:
        """Return the Spark schema for SequenceTableEntry serialization."""
        return DataModel.Event.get_schema()
    
    def to_dict(self) -> dict:
        return super().to_dict()
    

def hash_str(string: str) -> int:
    """
    Generate a consistent, deterministic 128-bit integer hash for a given string.
    """
    hash_bytes = xxhash.xxh128(string.encode('utf-8')).digest()
    return int.from_bytes(hash_bytes, byteorder='big')
