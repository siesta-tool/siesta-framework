from datetime import datetime
from typing import Optional
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType


class EventConfig:
    """Configuration for mapping source data attributes to Event fields.
    
    Loads field mappings from system config. To customize mappings, edit config.json
    or pass a custom config to parse functions.
    """
    
    def __init__(self, 
                 field_mappings: dict[str, Optional[str]],
                 trace_level_fields: set[str],
                 timestamp_fields: set[str],
                 attributes_mapping: Optional[list[str]] = None):
        """
        Initialize EventConfig with field mappings.
        
        Args:
            field_mappings: Dict mapping Event field names to source attribute keys.
                          Use None as value for computed fields.
            trace_level_fields: Set of fields extracted from trace level
            timestamp_fields: Set of fields containing timestamps
            attributes_mapping: List of source keys to include in attributes. 
                              None/Empty means store nothing. ["*"] means store all unmapped.
        """
        self.field_mappings = field_mappings
        self.trace_level_fields = trace_level_fields
        self.timestamp_fields = timestamp_fields
        self.attributes_mapping = attributes_mapping
    
    @staticmethod
    def from_preprocess_config(config: dict, log_format: str = 'xes') -> 'EventConfig':
        """Create EventConfig from preprocess configuration.
        
        Args:
            config: Preprocess configuration dictionary
            log_format: Log format to use ('xes', 'csv', 'json', or custom defined in config)
            
        Returns:
            EventConfig initialized from preprocess config
        """
        field_mappings = config.get('field_mappings', {}).get(log_format, {}).copy()

        attributes_mapping = field_mappings.pop('attributes', None)

        if not field_mappings:
            # Fallback to default XES for default Event class
            field_mappings = {
                'activity': 'concept:name',
                'trace_id': 'concept:name',
                'position': None,
                'start_timestamp': 'time:timestamp'
            }
        
        expected_attrs = set(dir(Event()))
        field_mappings_keys = set(field_mappings.keys())
        if not field_mappings_keys <= expected_attrs:
            print(f"Given field mappings: {field_mappings_keys}")
            print(f"Expected Event fields: {expected_attrs}")
            raise Exception("Incompatible field mappings given in Config")
        
        return EventConfig(
            field_mappings=field_mappings,
            trace_level_fields=set(config.get('trace_level_fields', ['trace_id'])),
            timestamp_fields=set(config.get('timestamp_fields', ['start_timestamp'])),
            attributes_mapping=attributes_mapping
        )
    
    def get_event_fields(self) -> dict[str, Optional[str]]:
        """Get mappings for event-level fields only."""
        return {k: v for k, v in self.field_mappings.items() if k not in self.trace_level_fields}
    
    def get_trace_fields(self) -> dict[str, Optional[str]]:
        """Get mappings for trace-level fields only."""
        return {k: v for k, v in self.field_mappings.items() if k in self.trace_level_fields}
    
    def is_timestamp_field(self, field_name: str) -> bool:
        """Check if a field should be parsed as timestamp."""
        return field_name in self.timestamp_fields
    
    def is_computed_field(self, field_name: str) -> bool:
        """Check if a field is computed (not extracted from source)."""
        return self.field_mappings.get(field_name) is None

    def get_event_schema(self) -> StructType:
        """Return the Spark schema for Event based on field mappings."""
        return StructType([StructField(str(x), StringType(), True) for x in self.field_mappings.keys()])
    
    def get_source_schema(self) -> StructType:
        """Return the Spark schema using source field names for parsing raw data."""
        fields = []
        for event_field, source_field in self.field_mappings.items():
            if source_field is not None:  # Skip computed fields
                fields.append(StructField(str(source_field), StringType(), True))
        return StructType(fields)
    
    def __reduce__(self):
        return (self.__class__, (self.field_mappings, self.trace_level_fields, 
                             self.timestamp_fields, self.attributes_mapping))


class Event:
    activity: str
    
    trace_id: str
    position: int
    
    start_timestamp: Optional[datetime]

    attributes: Optional[dict[str, str | datetime | int | float | bool]]

    def __init__(self, activity: str = None, trace_id: str = None, position: int = None,
                 start_timestamp: Optional[datetime] = None, end_timestamp: Optional[datetime] = None,
                 attributes: Optional[dict] = None, **kwargs):
        self.activity = activity
        self.trace_id = trace_id
        self.position = position
        self.start_timestamp = start_timestamp
        self.attributes = attributes if attributes else {}
        
        # Support dynamic field assignment for extensibility
        for key, value in kwargs.items():
            setattr(self, key, value)
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Event':
        """Create Event from dictionary with dynamic field support."""
        return cls(**data)

    @staticmethod
    def get_schema() -> StructType:
        """Return the Spark schema for Event serialization."""
        return StructType([
            StructField("activity", StringType(), False),
            StructField("trace_id", StringType(), True),
            StructField("position", IntegerType(), False),
            StructField("start_timestamp", StringType(), True),
            StructField("attributes", MapType(StringType(), StringType()), True)
        ])

    def to_dict(self) -> dict:
        r = {
            "activity": self.activity,
            "trace_id": self.trace_id if self.trace_id else None,
            "position": self.position,
            "start_timestamp": self.start_timestamp.isoformat() if self.start_timestamp and hasattr(self.start_timestamp, "isoformat") else self.start_timestamp,
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
        if self.events[-1].end_timestamp:
            return self.events[-1].end_timestamp
        if "end_timestamp" in self.events[-1].attributes:
            return self.events[-1].attributes["end_timestamp"]
        return None
    
    def to_dict(self) -> dict:
        return {
            "trace_id": self.trace_id,
            "events": [event.to_dict() for event in self.events] if self.events else [],
            "start_position": self.start_position,
            "end_position": self.end_position,
            "start_timestamp": self.start_timestamp.isoformat() if self.start_timestamp else None,
            "end_timestamp": self.end_timestamp.isoformat() if self.end_timestamp else None
        }