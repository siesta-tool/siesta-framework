from datetime import datetime
from typing import Optional
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType



class Event:
    activity: str
    
    trace_id: str
    position: int
    
    start_timestamp: Optional[datetime]
    end_timestamp: Optional[datetime]

    attributes: Optional[dict[str, str | datetime | int | float | bool]]

    def __init__(self, activity: str = None, trace_id: str = None, position: int = None,
                 start_timestamp: Optional[datetime] = None, end_timestamp: Optional[datetime] = None,
                 attributes: Optional[dict] = None):
        self.activity = activity
        self.trace_id = trace_id
        self.position = position
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.attributes = attributes if attributes else {}

    @staticmethod
    def get_schema() -> StructType:
        """Return the Spark schema for Event serialization."""
        return StructType([
            StructField("activity", StringType(), True),
            StructField("trace_id", StringType(), True),
            StructField("position", IntegerType(), True),
            StructField("start_timestamp", StringType(), True),
            StructField("end_timestamp", StringType(), True),
            StructField("attributes", MapType(StringType(), StringType()), True)
        ])

    def to_dict(self) -> dict:
        r = {
            "activity": self.activity,
            "trace_id": self.trace_id,
            "position": self.position,
            "start_timestamp": self.start_timestamp.isoformat() if self.start_timestamp else None,
            "end_timestamp": self.end_timestamp.isoformat() if self.end_timestamp else None,
            "attributes": self.attributes if self.attributes else {}
        }
        return r


class EventPair:
    source: Event
    target: Event
    @property
    def trace_id(self) -> str:
        return self.source.trace_id
    @property
    def start_position(self) -> int:
        return self.source.position
    @property
    def end_position(self) -> int:
        return self.target.position
    @property
    def start_timestamp(self) -> Optional[datetime]:
        return self.source.start_timestamp
    @property
    def end_timestamp(self) -> Optional[datetime]:
        return self.target.end_timestamp
    @property
    def position_diff(self) -> int:
        return self.target.position - self.source.position
    @property
    def start_timestamp_diff(self) -> Optional[float]:
        if self.source.start_timestamp and self.target.start_timestamp:
            return (self.target.start_timestamp - self.source.start_timestamp).total_seconds()
        return None

    def to_dict(self) -> dict:
        return {
            "source": self.source.to_dict() if self.source else None,
            "target": self.target.to_dict() if self.target else None,
            "position_diff": self.position_diff,
            "start_timestamp_diff": self.start_timestamp_diff
        }


class Trace:
    events: list[Event]
    @property
    def trace_id(self) -> str:
        return self.events[0].trace_id
    @property
    def start_position(self) -> int:
        return 0
    @property
    def end_position(self) -> int:
        return len(self.events) - 1
    @property
    def start_timestamp(self) -> Optional[datetime]:
        if self.events[0].start_timestamp:
            return self.events[0].start_timestamp
        return None
    @property
    def end_timestamp(self) -> Optional[datetime]:
        return self.events[-1].end_timestamp

    def to_dict(self) -> dict:
        return {
            "trace_id": self.trace_id,
            "events": [event.to_dict() for event in self.events] if self.events else [],
            "start_position": self.start_position,
            "end_position": self.end_position,
            "start_timestamp": self.start_timestamp.isoformat() if self.start_timestamp else None,
            "end_timestamp": self.end_timestamp.isoformat() if self.end_timestamp else None
        }