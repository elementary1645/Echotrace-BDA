"""Adapt without mutating the original WebSocket message."""
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from echotrace.schemas import EventMsg
from bda.schema import AnalyticsEvent, record_identity

SCHEMA_VERSION = "1.0"


def adapt_event(message: dict, *, source_id: str, session_id: str,
                source_timezone: str | None = None) -> dict:
    event = EventMsg.model_validate(message).data.model_dump()
    wall = datetime.fromisoformat(event.pop("wall").replace("Z", "+00:00"))
    if wall.utcoffset() is None:
        if not source_timezone:
            raise ValueError("naive wall timestamp requires explicit BDA_SOURCE_TIMEZONE")
        zone = ZoneInfo(source_timezone)
        choices = []
        for fold in (0, 1):
            candidate = wall.replace(tzinfo=zone, fold=fold)
            if candidate.astimezone(timezone.utc).astimezone(zone).replace(tzinfo=None) == wall:
                choices.append(candidate)
        if not choices or len({c.utcoffset() for c in choices}) != 1:
            raise ValueError("ambiguous or nonexistent local wall timestamp")
        wall = choices[0]
    event_id = event.pop("id")
    return AnalyticsEvent(source_id=source_id, session_id=session_id, event_id=event_id,
                          record_id=record_identity(source_id, session_id, event_id),
                          event_time=wall, **event).model_dump(mode="json")
