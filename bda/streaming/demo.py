"""Publish isolated deterministic SIMULATED fixtures to a dedicated test topic."""
import argparse
import json
from datetime import datetime, timedelta, timezone
from confluent_kafka import Producer
from bda.schema import AnalyticsEvent, record_identity


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--brokers',default='localhost:9092')
    p.add_argument('--topic',required=True)
    p.add_argument('--session',default='phase2-synthetic-v1')
    p.add_argument('--late-only', action='store_true')
    args = p.parse_args()
    if args.topic == 'echotrace.events.v1':
        p.error('synthetic workload requires a separate topic')
    producer = Producer({'bootstrap.servers':args.brokers,'enable.idempotence':True,'acks':'all'})
    failures = []
    def delivered(err, msg):
        if err:
            failures.append(str(err))
    base = datetime(2026,1,1,tzinfo=timezone.utc)
    if args.late_only:
        event = AnalyticsEvent(source_id='synthetic-benchmark',session_id=args.session,
            event_id='late-after-watermark',record_id=record_identity('synthetic-benchmark',args.session,'late-after-watermark'),
            source='SIMULATED',event_time=base+timedelta(seconds=1),t=1,t_start=0,t_end=1,
            category='IMPACT',label='late synthetic impact',confidence=.8,zone='LEFT')
        producer.produce(args.topic,key=event.record_id,value=event.serialize(),on_delivery=delivered)
        assert producer.flush(30) == 0
        assert not failures, failures
        print('Published one deliberately late Kafka event')
        return
    expected = []
    for minute, count in enumerate([2,2,2,10,1]):
        for i in range(count):
            event_id = f'{minute}-{i}'
            event = AnalyticsEvent(source_id='synthetic-benchmark',session_id=args.session,
                event_id=event_id,record_id=record_identity('synthetic-benchmark',args.session,event_id),
                source='SIMULATED',event_time=base+timedelta(minutes=minute,seconds=i),
                t=1,t_start=0,t_end=1,category='IMPACT',label='synthetic impact',confidence=.8,zone='LEFT')
            producer.produce(args.topic,key=event.record_id,value=event.serialize(),on_delivery=delivered)
            if minute == 0 and i == 0:
                producer.produce(args.topic,key=event.record_id,value=event.serialize(),on_delivery=delivered)
        expected.append(count)
    producer.produce(args.topic,key='malformed',value=b'{',on_delivery=delivered)
    # A later window advances the watermark so the analytical windows can finalize.
    event = event.model_copy(update={'event_id':'advance','record_id':record_identity('synthetic-benchmark',args.session,'advance'),
                                   'event_time':base+timedelta(minutes=10)})
    producer.produce(args.topic,key=event.record_id,value=event.serialize(),on_delivery=delivered)
    assert producer.flush(30) == 0
    assert not failures, failures
    print(json.dumps({'topic':args.topic,'session':args.session,'expected_first_five_window_counts':expected,
        'unique_events':18,'duplicate_records':1,'malformed_records':1,'anomaly_minute':3}))


if __name__ == '__main__':
    main()
