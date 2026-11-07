"""Reuse the wire contract instead of maintaining a divergent SQL validator."""
import json
from bda.schema import AnalyticsEvent


def validate_record(raw, key):
    try:
        payload = json.loads(raw)
        if payload.get('schema_version') != '1.0' or payload.get('record_type') != 'event':
            raise ValueError('unsupported or missing schema_version/record_type')
        event = AnalyticsEvent.model_validate(payload)
        if key != event.record_id:
            raise ValueError('Kafka key does not match record_id')
        return (event.serialize().decode(), None)
    except (ValueError, TypeError, AttributeError) as exc:
        return (None, str(exc)[:4096])
