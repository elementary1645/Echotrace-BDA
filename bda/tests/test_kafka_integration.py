"""Opt-in live broker test; never implicitly depends on Docker."""
import os
import time
from uuid import uuid4

import pytest

from bda.config import KafkaSettings
from bda.producer import EventProducer
from bda.schema import AnalyticsEvent

pytestmark = [pytest.mark.integration, pytest.mark.skipif(
    os.getenv("RUN_KAFKA_INTEGRATION") != "1", reason="set RUN_KAFKA_INTEGRATION=1 for live Kafka")]


def test_real_kafka_delivery():
    from confluent_kafka import Consumer
    from echotrace.pipeline import Pipeline
    from echotrace.audio.sources import SimSource
    session = str(uuid4())
    settings = KafkaSettings.from_env().model_copy(update={"enabled": True, "session_id": session,
                                                          "source_timezone": "UTC"})
    consumer = Consumer({"bootstrap.servers": settings.brokers, "group.id": f"bda-test-{session}",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    worker = EventProducer(settings)
    received = set()
    try:
        consumer.subscribe([settings.topic])
        worker.start()
        pipeline = Pipeline(SimSource(realtime=False, loops=1, scenario="breakin"))
        messages = pipeline.run_to_end()
        events = [msg for msg in messages if msg["type"] == "event"]
        assert events
        for msg in events:
            assert worker.submit(msg, 1)
        deadline = time.monotonic() + 30
        while len(received) < len(events) and time.monotonic() < deadline:
            msg = consumer.poll(.5)
            if msg is None:
                continue
            assert not msg.error(), msg.error()
            # Existing smoke records may predate the versioned schema.
            import json
            payload = json.loads(msg.value())
            if payload.get("session_id") != session + "/g1":
                continue
            event = AnalyticsEvent.model_validate(payload)
            assert event.source == "SIMULATED"
            assert msg.key().decode() == event.record_id
            received.add(event.event_id)
        assert received == {msg["data"]["id"] for msg in events}
        worker.close()
        assert worker.snapshot().get("delivered") == len(events)
        print(f"Verified {len(received)} pipeline events, session={session}, metrics={worker.snapshot()}")
    finally:
        worker.close()
        consumer.close()
