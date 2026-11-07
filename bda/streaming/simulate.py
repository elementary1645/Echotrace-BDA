"""Finite actual EchoTrace simulation -> Kafka, with an independent expected manifest."""
import argparse
import json
from collections import defaultdict
from datetime import datetime, timedelta
from uuid import uuid4
from confluent_kafka import Producer
from echotrace.pipeline import Pipeline
from echotrace.audio.sources import SimSource
from bda.event_adapter import adapt_event
from bda.schema import AnalyticsEvent, record_identity


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--topic', required=True)
    p.add_argument('--brokers', default='localhost:9092')
    p.add_argument('--manifest', default='/tmp/echotrace-simulated-expected.json')
    args = p.parse_args()
    if args.topic == 'echotrace.events.v1':
        p.error('finite demo with watermark control records requires a separate topic')
    session = 'simulation-' + uuid4().hex
    messages = Pipeline(SimSource(realtime=False,loops=1,scenario='breakin')).run_to_end()
    records = [adapt_event(m,source_id='phase2-simulation',session_id=session,source_timezone='UTC')
               for m in messages if m['type'] == 'event']
    assert records
    producer = Producer({'bootstrap.servers':args.brokers,'enable.idempotence':True,'acks':'all'})
    errors = []
    def delivered(error, _):
        if error:
            errors.append(str(error))
    expected = defaultdict(lambda: {'count':0,'confidence_sum':0.})
    for record in records:
        event = AnalyticsEvent.model_validate(record)
        producer.produce(args.topic,key=event.record_id,value=event.serialize(),on_delivery=delivered)
        minute = event.event_time.replace(second=0,microsecond=0).isoformat()
        key = json.dumps([minute,event.source,event.source_id,event.category,event.zone])
        expected[key]['count'] += 1
        expected[key]['confidence_sum'] += event.confidence
    control = dict(records[-1])
    control.update(source_id='synthetic-watermark-control',event_id='advance',category='CONTROL',
        event_time=(max(datetime.fromisoformat(r['event_time'].replace('Z','+00:00')) for r in records)+timedelta(minutes=10)).isoformat())
    control['record_id'] = record_identity(control['source_id'],session,control['event_id'])
    event = AnalyticsEvent.model_validate(control)
    producer.produce(args.topic,key=event.record_id,value=event.serialize(),on_delivery=delivered)
    assert producer.flush(30) == 0
    assert not errors, errors
    manifest = dict(session=session,topic=args.topic,records=records,expected=dict(expected))
    with open(args.manifest,'w') as out:
        json.dump(manifest,out,indent=2)
    print(json.dumps({'session':session,'events':len(records),'manifest':args.manifest}))


if __name__ == '__main__':
    main()
