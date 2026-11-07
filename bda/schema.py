"""Strict v1 analytics wire contract, independent of the WebSocket contract."""
import hashlib
import json
from datetime import datetime, timezone
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Identifier = Annotated[str, Field(min_length=1, pattern=r"\S")]


def record_identity(source_id: str, session_id: str, event_id: str) -> str:
    parts = json.dumps([source_id, session_id, event_id], ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(parts.encode("utf-8")).hexdigest()


class AnalyticsEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False, frozen=True)
    schema_version: Literal["1.0"] = "1.0"
    record_type: Literal["event"] = "event"
    record_id: Identifier
    source_id: Identifier
    session_id: Identifier
    event_id: Identifier
    source: Literal["LIVE", "RECORDED", "SIMULATED"]
    event_time: datetime
    t: float = Field(ge=0)
    t_start: float = Field(ge=0)
    t_end: float = Field(ge=0)
    category: Identifier
    label: Identifier
    confidence: float = Field(ge=0, le=1)
    zone: Literal["LEFT", "CENTRE", "RIGHT", "UNKNOWN"]
    zone_confidence: Literal["high", "low"] | None = None
    angle_deg: float | None = None

    @field_validator("event_time", mode="before")
    @classmethod
    def timestamp(cls, value):
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if not isinstance(value, datetime) or value.utcoffset() is None:
            raise ValueError("event_time must have an explicit timezone")
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def consistent(self):
        if self.t_end < self.t_start or self.t + 0.001 < self.t_end:
            raise ValueError("require t_start <= t_end <= t (allow 1 ms rounding)")
        if self.record_id != record_identity(self.source_id, self.session_id, self.event_id):
            raise ValueError("record_id does not match source/session/event identity")
        return self

    def serialize(self) -> bytes:
        return json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False, allow_nan=False).encode("utf-8")
