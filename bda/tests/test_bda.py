import asyncio
import copy
import json
import time

import pytest

from bda.config import KafkaSettings
from bda.event_adapter import adapt_event
from bda.producer import EventProducer
from bda.schema import AnalyticsEvent, record_identity


@pytest.fixture
def message():
    return {"type": "event", "data": {"source": "SIMULATED", "t": 3.,
            "wall": "2026-01-01T12:00:03+05:30", "id": "e1", "t_start": 1., "t_end": 2.,
            "category": "IMPACT", "label": "Crash", "confidence": .8, "zone": "LEFT",
            "zone_confidence": "high", "angle_deg": -30.}}


def adapt(message, **kwargs):
    return adapt_event(message, source_id="node", session_id=kwargs.pop("session_id", "session"), **kwargs)


def test_conversion_identity_and_preservation(message):
    original = copy.deepcopy(message)
    record = adapt(message)
    assert record["event_time"] == "2026-01-01T06:30:03Z"
    assert record["source"] == "SIMULATED"
    assert adapt(message) == record
    assert adapt(message, session_id="other")["record_id"] != record["record_id"]
    assert record_identity("a:b", "c", "d") != record_identity("a", "b:c", "d")
    assert message == original
    event = AnalyticsEvent.model_validate(record)
    assert event.serialize() == AnalyticsEvent.model_validate_json(event.serialize()).serialize()


@pytest.mark.parametrize("key,value", [("confidence", -1), ("confidence", 1.1),
    ("confidence", float("nan")), ("t", float("inf")), ("t_start", -1), ("t_end", 0),
    ("angle_deg", float("inf")), ("wall", "broken"), ("id", " "), ("t", 0)])
def test_invalid_event(message, key, value):
    message["data"][key] = value
    with pytest.raises(ValueError):
        adapt(message)


@pytest.mark.parametrize("source", ["LIVE", "RECORDED", "SIMULATED"])
def test_provenance(message, source):
    message["data"]["source"] = source
    assert adapt(message)["source"] == source


@pytest.mark.parametrize("identifier", ["", "  "])
def test_invalid_identifiers(message, identifier):
    for field in ("source_id", "session_id"):
        args = dict(source_id="node", session_id="session")
        args[field] = identifier
        with pytest.raises(ValueError):
            adapt_event(message, **args)


def test_naive_timezone_and_dst(message):
    message["data"]["wall"] = "2026-01-01T12:00:03"
    with pytest.raises(ValueError):
        adapt(message)
    assert adapt(message, source_timezone="Asia/Kolkata")["event_time"].endswith("06:30:03Z")
    for wall in ("2026-11-01T01:30:00", "2026-03-08T02:30:00"):
        message["data"]["wall"] = wall
        with pytest.raises(ValueError):
            adapt(message, source_timezone="America/New_York")


class FakeProducer:
    def __init__(self, config):
        self.config = config
        self.messages = []
        self.flushed = False

    def produce(self, topic, **kwargs):
        self.messages.append((topic, kwargs))
        kwargs["on_delivery"](None, None)

    def poll(self, timeout):
        pass

    def flush(self, timeout):
        self.flushed = True
        return 0


def wait_for(predicate):
    deadline = time.monotonic() + 3
    while not predicate() and time.monotonic() < deadline:
        time.sleep(.01)
    assert predicate()


def test_delivery_lifecycle(message):
    fake = FakeProducer({})
    worker = EventProducer(KafkaSettings(enabled=True), lambda _: fake)
    worker.start()
    worker.start()
    assert worker.submit(message, 1)
    wait_for(lambda: worker.snapshot().get("delivered") == 1)
    worker.close()
    assert fake.flushed and not worker.thread.is_alive()
    assert not worker.submit(message, 1)
    data = AnalyticsEvent.model_validate_json(fake.messages[0][1]["value"])
    assert data.session_id.endswith("/g1")
    assert fake.messages[0][1]["key"].decode() == data.record_id


def test_saturation_and_disabled(message):
    worker = EventProducer(KafkaSettings(enabled=True, queue_size=1))
    worker.accepting = True
    assert worker.submit(message, 1)
    assert not worker.submit(message, 1)
    assert worker.snapshot()["dropped_queue_full"] == 1
    disabled = EventProducer(KafkaSettings())
    disabled.start()
    assert disabled.thread is None and not disabled.submit(message, 1)


def test_failures(message):
    class Broken(FakeProducer):
        def produce(self, *args, **kwargs):
            raise BufferError("broker unavailable")

    worker = EventProducer(KafkaSettings(enabled=True), Broken)
    worker.start()
    worker.submit(message, 1)
    wait_for(lambda: worker.snapshot().get("send_failed") == 1)
    worker._delivered("delivery timeout", None)
    worker.close()
    assert worker.snapshot()["delivery_failed"] == 1


def test_startup_failure(message):
    def broken(config):
        raise RuntimeError("cannot initialize")
    worker = EventProducer(KafkaSettings(enabled=True), broken)
    worker.start()
    wait_for(lambda: worker.snapshot().get("worker_failed") == 1)
    worker.close()
    assert not worker.accepting


def test_consumer_validation(message, capsys):
    from bda.consumer import display_record
    display_record(json.dumps(adapt(message)))
    assert '"event_id": "e1"' in capsys.readouterr().out
    with pytest.raises(ValueError):
        display_record(b'{"invalid":true}')


def test_broadcaster_stale_and_latency(message):
    from echotrace.api.server import Engine
    async def run():
        engine = Engine(tagger_factory=lambda: None)
        engine.generation = 2
        engine.queue = asyncio.Queue()
        fake = FakeProducer({})
        engine.analytics = EventProducer(KafkaSettings(enabled=True), lambda _: fake)
        engine.analytics.start()
        await engine.queue.put((1, message, None))
        await engine.queue.put((2, message, None))
        task = asyncio.create_task(engine.broadcaster())
        await asyncio.sleep(.1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        engine.analytics.close()
        assert len(fake.messages) == 1
        record = AnalyticsEvent.model_validate_json(fake.messages[0][1]["value"])
        assert record.session_id.endswith("/g2")
    asyncio.run(run())


def test_queued_stale_dropped(message):
    fake = FakeProducer({})
    worker = EventProducer(KafkaSettings(enabled=True), lambda _: fake, is_current=lambda g: g == 2)
    worker.accepting = True
    worker.submit(message, 1)
    worker.submit(message, 2)
    worker.start()
    wait_for(lambda: worker.snapshot().get("delivered") == 1)
    worker.close()
    assert worker.snapshot()["dropped_stale"] == 1


def test_app_lifecycle(monkeypatch):
    from fastapi.testclient import TestClient
    from echotrace.api.server import Engine, create_app
    monkeypatch.setenv("KAFKA_ENABLED", "false")
    engine = Engine(tagger_factory=lambda: None, realtime=True)
    with TestClient(create_app(engine)) as client:
        assert client.get("/api/status").status_code == 200
        assert engine.analytics.thread is None
    assert engine.pipeline is None


def test_configuration(monkeypatch):
    monkeypatch.setenv("KAFKA_ENABLED", "true")
    monkeypatch.setenv("KAFKA_QUEUE_SIZE", "3")
    assert KafkaSettings.from_env().queue_size == 3
    assert KafkaSettings.from_env().enabled
    monkeypatch.setenv("BDA_SOURCE_ID", " ")
    with pytest.raises(ValueError):
        KafkaSettings.from_env()


def test_invalid_submission_and_flush(message):
    class Unflushed(FakeProducer):
        def flush(self, timeout):
            return 1
    worker = EventProducer(KafkaSettings(enabled=True), Unflushed)
    worker.start()
    message["data"]["confidence"] = float("nan")
    assert not worker.submit(message, 1)
    worker.close()
    assert worker.snapshot()["invalid"] == 1
    assert worker.snapshot()["flush_incomplete"] == 1


def test_app_invalid_config_continues(monkeypatch):
    from fastapi.testclient import TestClient
    from echotrace.api.server import Engine, create_app
    monkeypatch.setenv("KAFKA_QUEUE_SIZE", "0")
    engine = Engine(tagger_factory=lambda: None)
    with TestClient(create_app(engine)) as client:
        assert client.get("/api/status").status_code == 200
        assert engine.analytics is None
