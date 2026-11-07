"""Bounded, best-effort analytics side channel. All Kafka I/O runs on one thread."""
import json
import logging
import queue
import threading
import time
from datetime import datetime, timezone
from collections import deque
from collections import Counter

from bda.event_adapter import adapt_event
from bda.schema import AnalyticsEvent

log = logging.getLogger(__name__)


class EventProducer:
    def __init__(self, settings, factory=None, is_current=None):
        self.settings = settings
        self.factory = factory
        self.is_current = is_current or (lambda generation: True)
        self.queue = queue.Queue(settings.queue_size)
        self.stop_event = threading.Event()
        self.thread = None
        self.lock = threading.Lock()
        self.metrics = Counter()
        self.accepting = False
        self.last_ack_at = None
        self.last_error_at = None
        self.last_ack_monotonic = None
        self.last_error_monotonic = None
        self.ack_latencies_ms = deque(maxlen=4096)

    def report(self, name, error=None):
        with self.lock:
            self.metrics[name] += 1
            if error is not None:
                self.last_error_at = datetime.now(timezone.utc).isoformat()
                self.last_error_monotonic = time.monotonic()
        if error is not None:
            log.warning(json.dumps({"component": "bda.kafka", "metric": name, "error": str(error)}))

    def snapshot(self):
        with self.lock:
            return {**self.metrics, "queue_depth": self.queue.qsize(), "accepting": self.accepting,
                    "worker_alive": bool(self.thread and self.thread.is_alive()),
                    "last_ack_at": self.last_ack_at,"last_error_at": self.last_error_at,
                    "connectivity": self._connectivity()}

    def _connectivity(self):
        if not self.accepting:
            return 'stopped'
        if self.last_error_monotonic is not None and (self.last_ack_monotonic is None or self.last_error_monotonic>self.last_ack_monotonic):
            return 'errors observed; connectivity unverified'
        if self.last_ack_monotonic is not None:
            return 'recent acknowledgement' if time.monotonic()-self.last_ack_monotonic<=30 else 'acknowledgement stale'
        return 'accepting; connectivity unverified'

    def start(self):
        if self.thread is not None or not self.settings.enabled:
            return
        self.accepting = True
        self.thread = threading.Thread(target=self._run, name="bda-kafka", daemon=True)
        self.thread.start()

    def submit(self, message, generation):
        if not self.accepting or message.get("type") != "event":
            return False
        try:
            record = adapt_event(message, source_id=self.settings.source_id,
                                 session_id=f"{self.settings.session_id}/g{generation}",
                                 source_timezone=self.settings.source_timezone)
            payload = AnalyticsEvent.model_validate(record).serialize()
        except Exception as exc:
            self.report("invalid", exc)
            return False
        try:
            self.queue.put_nowait((generation, record["record_id"].encode(), payload, time.monotonic()))
            self.report("enqueued")
            return True
        except queue.Full:
            self.report("dropped_queue_full", "drop newest")
            return False

    def _run(self):
        producer = None
        try:
            factory = self.factory
            if factory is None:
                from confluent_kafka import Producer
                factory = Producer
            producer = factory({"bootstrap.servers": self.settings.brokers, "enable.idempotence": True,
                                "acks": "all", "message.timeout.ms": self.settings.delivery_timeout_ms,
                                "queue.buffering.max.messages": self.settings.queue_size,
                                "queue.buffering.max.kbytes": 1024,
                                "error_cb": lambda e: self.report("broker_errors", e)})
            while not self.stop_event.is_set() or not self.queue.empty():
                try:
                    generation, key, payload, created = self.queue.get(timeout=0.05)
                except queue.Empty:
                    producer.poll(0)
                    continue
                try:
                    if not self.is_current(generation):
                        self.report("dropped_stale")
                        continue
                    producer.produce(self.settings.topic, key=key, value=payload, on_delivery=lambda error,message,start=created: self._delivered(error,message,start))
                    self.report("submitted")
                except Exception as exc:
                    self.report("send_failed", exc)
                finally:
                    self.queue.task_done()
                producer.poll(0)
        except Exception as exc:
            self.report("worker_failed", exc)
        finally:
            self.accepting = False
            if producer is not None:
                try:
                    remaining = producer.flush(self.settings.flush_timeout)
                    if remaining:
                        self.report("flush_incomplete", f"{remaining} records unresolved")
                except Exception as exc:
                    self.report("flush_failed", exc)
            log.info(json.dumps({"component": "bda.kafka", "metrics": self.snapshot()}))

    def _delivered(self, error, message, created=None):
        if not error:
            with self.lock:
                self.last_ack_at = datetime.now(timezone.utc).isoformat()
                self.last_ack_monotonic = time.monotonic()
                if created is not None:
                    self.ack_latencies_ms.append((self.last_ack_monotonic-created)*1000)
        self.report("delivery_failed" if error else "delivered", error)

    def close(self):
        self.accepting = False
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(self.settings.flush_timeout + 2)
            if self.thread.is_alive():
                self.report("shutdown_timeout", "producer worker still stopping")
