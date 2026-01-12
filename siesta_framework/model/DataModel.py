from datetime import datetime
from typing import Optional

class Activity:
    name: str
    attributes: Optional[dict]


class ActivityPair:
    source: Activity
    target: Activity


class Event:
    activity: Activity
    
    trace_id: str
    position: int
    
    start_timestamp: Optional[datetime]
    end_timestamp: Optional[datetime]


class EventPair:
    source: Event
    target: Event
    @property
    def trace_id(self) -> str:
        return self.event_pair.source.trace_id
    @property
    def start_position(self) -> int:
        return self.event_pair.source.position
    @property
    def end_position(self) -> int:
        return self.event_pair.target.position
    @property
    def start_timestamp(self) -> Optional[datetime]:
        return self.event_pair.source.start_timestamp
    @property
    def end_timestamp(self) -> Optional[datetime]:
        return self.event_pair.target.end_timestamp
    @property
    def position_diff(self) -> int:
        return self.target.position - self.source.position
    @property
    def start_timestamp_diff(self) -> Optional[float]:
        if self.source.start_timestamp and self.target.start_timestamp:
            return (self.target.start_timestamp - self.source.start_timestamp).total_seconds()
        return None
    

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