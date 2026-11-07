"""Paced, bounded SIMULATED workloads and actual acoustic source publication."""
from datetime import datetime, timezone
import json
import math
import time
from pathlib import Path


def percentiles(values):
    if not values: return None
    a=sorted(values)
    def percentile(p):
        x=(len(a)-1)*p;lo=math.floor(x);hi=math.ceil(x)
        return a[lo]+(a[hi]-a[lo])*(x-lo)
    return {'population':len(a),'p50_ms':percentile(.5),'p95_ms':percentile(.95),'p99_ms':percentile(.99)}


def wait_until(predicate,seconds=30):
    deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        if predicate(): return
        time.sleep(.05)
    raise TimeoutError('condition not observed before deadline')


def message(identifier,second=1,category='IMPACT'):
    from datetime import timedelta
    wall=datetime(2026,1,1,tzinfo=timezone.utc)+timedelta(seconds=second)
    return {'type':'event','data':dict(source='SIMULATED',t=second,t_start=max(0,second-.5),t_end=second,
        wall=wall.isoformat(),id=identifier,category=category,label='isolated Phase 5 synthetic workload',confidence=.8,
        zone='LEFT',zone_confidence='high',angle_deg=-30.)}


def producer(brokers,topic,source,session,timeout=10000,queue_size=1024):
    from bda.producer import EventProducer
    from bda.config import KafkaSettings
    if not topic.startswith('echotrace-p5-'): raise ValueError('dedicated Phase 5 topic required')
    return EventProducer(KafkaSettings(enabled=True,brokers=brokers,topic=topic,source_id=source,
        session_id=session,source_timezone='UTC',queue_size=queue_size,delivery_timeout_ms=timeout,flush_timeout=10))


def acoustic(brokers,topic,session,output):
    from echotrace.audio.sources import SimSource
    from echotrace.pipeline import Pipeline
    from bda.event_adapter import adapt_event
    worker=producer(brokers,topic,'p5-acoustic',session)
    messages=Pipeline(SimSource(realtime=False,loops=1,scenario='breakin'),
        start_wall=datetime(2026,1,1)).run_to_end()
    events=[m for m in messages if m['type']=='event']
    assert len(events)==8
    worker.start()
    for msg in events: assert worker.submit(msg,1)
    # Same identity replay; analytics must deduplicate it within the watermark horizon.
    assert worker.submit(events[0],1)
    worker.close()
    assert worker.snapshot().get('delivered')==9,worker.snapshot()
    rows=[adapt_event(m,source_id='p5-acoustic',session_id=session+'/g1',source_timezone='UTC') for m in events]
    result={'source':'SIMULATED','acoustic_events':8,'acknowledged_with_duplicate':9,'metrics':worker.snapshot(),
            'enqueue_to_ack':percentiles(worker.ack_latencies_ms),'records':rows}
    Path(output).write_text(json.dumps(result,indent=2)+'\n')
    return result


def paced(brokers,topic,session,rate=100,warmup=15,duration=60):
    if not 1<=rate<=1000 or not 5<=warmup<=30 or not 60<=duration<=120:
        raise ValueError('safe campaign bounds: rate 1..1000, warmup 5..30 s, measured 60..120 s')
    worker=producer(brokers,topic,'p5-throughput',session,queue_size=4096)
    worker.start();measure_samples=[];acks_by_phase={'warmup':0,'measured':0};expected={'warmup':0,'measured':0}
    started=time.monotonic()
    try:
        for phase,seconds in [('warmup',warmup),('measured',duration)]:
            before=time.monotonic();phase_count=rate*seconds
            ack_before=worker.snapshot().get('delivered',0)
            # Separate delivery identity at enqueue time; drain each phase so no cross-phase pooling.
            for i in range(phase_count):
                deadline=before+i/rate
                delay=deadline-time.monotonic()
                if delay>0: time.sleep(delay)
                if time.monotonic()-deadline>5: raise RuntimeError('producer cannot maintain pacing; stop safely')
                if i%rate==0: resource_guard(Path.cwd(),reserve_bytes=1024**3,min_available=700*1024**2)
                assert worker.submit(message(f'{phase}-{i}',second=1+i/max(1,rate*seconds)*50),1)
                expected[phase]+=1
            # Finish the explicit scheduled interval, rather than dividing by last-event time.
            remaining=before+seconds-time.monotonic()
            if remaining>0: time.sleep(remaining)
            enqueue_seconds=time.monotonic()-before
            wait_until(lambda:worker.snapshot().get('delivered',0)>=ack_before+phase_count,30)
            ack_seconds=time.monotonic()-before
            acks_by_phase[phase]=worker.snapshot().get('delivered',0)-ack_before
            if phase=='warmup': worker.ack_latencies_ms.clear()
            else:
                measure_samples=list(worker.ack_latencies_ms)
                result=dict(requested_rate=rate,measured_seconds=seconds,enqueue_seconds=enqueue_seconds,
                    acknowledgement_drain_seconds=ack_seconds,produced=phase_count,acknowledged=acks_by_phase[phase],
                    actual_produced_rate=phase_count/enqueue_seconds,acknowledged_rate=acks_by_phase[phase]/ack_seconds)
    finally:
        worker.close()
    result.update(warmup_seconds=warmup,warmup_count=expected['warmup'],total_records=sum(expected.values()),
        enqueue_to_ack_ms=percentiles(measure_samples),latency_sample_policy='last at most 4096 measured-phase acknowledgements; bounded ring buffer',
        producer_metrics=worker.snapshot(),elapsed_seconds=time.monotonic()-started)
    return result


def resource_guard(path,reserve_bytes=2*1024**3,min_available=3*1024**3):
    import shutil
    free=shutil.disk_usage(path).free
    available=None
    for line in Path('/proc/meminfo').read_text().splitlines():
        if line.startswith('MemAvailable:'): available=int(line.split()[1])*1024
    if free<reserve_bytes or available is None or available<min_available:
        raise RuntimeError('resource guard refused workload: preserve disk/RAM reserve')
    return {'disk_free_bytes':free,'available_memory_bytes':available}
