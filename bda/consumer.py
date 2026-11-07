"""Standalone development consumer: python -m bda.consumer."""
import argparse
import json
import signal
import sys

from bda.config import KafkaSettings
from bda.schema import AnalyticsEvent


def display_record(payload):
    record = AnalyticsEvent.model_validate_json(payload)
    print(json.dumps(record.model_dump(mode="json"), indent=2, sort_keys=True), flush=True)
    return record


def main():
    from confluent_kafka import Consumer
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--group", default="echotrace-bda-dev-v1")
    args = parser.parse_args()
    settings = KafkaSettings.from_env()
    consumer = Consumer({"bootstrap.servers": settings.brokers, "group.id": args.group,
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    try:
        consumer.subscribe([settings.topic])
        while running:
            msg = consumer.poll(1)
            if msg is None:
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}", file=sys.stderr)
                continue
            try:
                display_record(msg.value())
            except Exception as exc:
                print(f"INVALID {msg.topic()}:{msg.partition()}:{msg.offset()}: {exc}; "
                      f"payload={msg.value()!r}", file=sys.stderr, flush=True)
            # Development policy: report invalid records, then advance past them.
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
