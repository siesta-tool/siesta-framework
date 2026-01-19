from datetime import datetime
from typing import Optional

class Activity:
    name: str
    attributes: Optional[dict[str, str | int | float | bool]]

    def to_dict(self) -> dict:
        r = {"name": self.name}
        if self.attributes:
            for k, v in self.attributes.items():
                r[k] = v
        return r

class ActivityPair:
    source: Activity
    target: Activity

    def to_dict(self) -> dict:
        return {
            "source": self.source.to_dict() if self.source else None,
            "target": self.target.to_dict() if self.target else None
        }


class Event:
    activity: Activity
    
    trace_id: str
    position: int
    
    start_timestamp: Optional[datetime]
    end_timestamp: Optional[datetime]

    def to_dict(self) -> dict:
        r = {
            "activity": self.activity.name,
            "trace_id": self.trace_id,
            "position": self.position,
            "start_timestamp": self.start_timestamp.isoformat() if self.start_timestamp else None,
            "end_timestamp": self.end_timestamp.isoformat() if self.end_timestamp else None
        }
        if self.activity.attributes:
            for k, v in self.activity.attributes.items():
                r[k] = v
        return r


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