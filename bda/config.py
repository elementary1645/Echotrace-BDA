"""Environment configuration; Kafka is opt-in."""
import os
from uuid import uuid4
from pydantic import BaseModel, Field
from bda.schema import Identifier


class KafkaSettings(BaseModel):
    enabled: bool = False
    brokers: Identifier = "localhost:9092"
    topic: Identifier = "echotrace.events.v1"
    source_id: Identifier = "echotrace-local"
    session_id: Identifier = Field(default_factory=lambda: str(uuid4()))
    source_timezone: str | None = None
    queue_size: int = Field(default=1024, gt=0)
    delivery_timeout_ms: int = Field(default=10000, ge=1000)
    flush_timeout: float = Field(default=5, gt=0, allow_inf_nan=False)

    @classmethod
    def from_env(cls):
        names = {"enabled": "KAFKA_ENABLED", "brokers": "KAFKA_BOOTSTRAP_SERVERS",
                 "topic": "KAFKA_TOPIC", "source_id": "BDA_SOURCE_ID", "session_id": "BDA_SESSION_ID",
                 "source_timezone": "BDA_SOURCE_TIMEZONE", "queue_size": "KAFKA_QUEUE_SIZE",
                 "delivery_timeout_ms": "KAFKA_DELIVERY_TIMEOUT_MS", "flush_timeout": "KAFKA_FLUSH_TIMEOUT"}
        return cls(**{key: os.environ[name] for key, name in names.items() if name in os.environ})
