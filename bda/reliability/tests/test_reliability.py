from datetime import datetime,timedelta,timezone
import json
import sqlite3
import threading
import time

import pytest
from bda.streaming.telemetry import Telemetry,atomic_json,offset_backlog
from bda.analytics.observability import read_telemetry
from bda.reliability.workload import percentiles,resource_guard


def test_offset_backlog_not_group_lag():
    assert offset_backlog([{'latestOffset':'{"topic":{"0":10,"1":20}}','endOffset':'{"topic":{"0":7,"1":18}}'}])==5
    assert offset_backlog([{'latestOffset':None}]) is None
    assert offset_backlog([]) is None


def test_telemetry_lifecycle(tmp_path):
    t=Telemetry(tmp_path,interval=.05)
    try:
        t.started('classified','query')
        t.progress({'name':'classified','batchId':0,'numInputRows':7,'inputRowsPerSecond':2.,
                    'processedRowsPerSecond':3.,'durationMs':{'triggerExecution':5},'sources':[],
                    'eventTime':{'watermark':'2026-01-01T00:00:00Z'}})
        rows=read_telemetry(tmp_path)['queries']
        assert rows[0]['state']=='running' and rows[0]['progress']['num_input_rows']==7
        assert rows[0]['progress']['discovered_offset_backlog'] is None
        t.terminated('query',None)
        assert read_telemetry(tmp_path)['state']=='stopped'
    finally:t.close()
    assert not t.thread.is_alive()


def test_stale_heartbeat_never_healthy(tmp_path):
    now=datetime.now(timezone.utc)
    old=(now-timedelta(minutes=2)).isoformat()
    atomic_json(tmp_path/'telemetry/events.json',dict(schema_version=1,name='events',state='running',
                observed_at=old,last_progress_at=old,progress={}))
    r=read_telemetry(tmp_path,now)['queries'][0]
    assert r['state']=='unknown; heartbeat stale' and r['progress_stale']
    assert read_telemetry(tmp_path,now)['consumer_group_lag'] is None


def test_no_progress_is_different_from_dead_writer(tmp_path):
    atomic_json(tmp_path/'telemetry/windows.json',dict(schema_version=1,name='windows',state='running',
                observed_at=datetime.now(timezone.utc).isoformat(),last_progress_at=None,progress={}))
    row=read_telemetry(tmp_path)['queries'][0]
    assert row['state']=='running' and row['progress_stale'] and not row['heartbeat_stale']


def test_invalid_telemetry_and_path_redaction(tmp_path):
    p=tmp_path/'telemetry/classified.json';p.parent.mkdir();p.write_text('{')
    assert 'invalid' in read_telemetry(tmp_path)['queries'][0]['state']
    p.write_text(json.dumps(dict(schema_version=1,name='classified',state='running',observed_at=datetime.now(timezone.utc).isoformat(),
        progress={'secret':'credential','arbitrary_path':'/private/file'})))
    assert 'credential' not in json.dumps(read_telemetry(tmp_path))
    assert '/private' not in json.dumps(read_telemetry(tmp_path))
    p.write_text('x'*65537)
    assert 'invalid' in read_telemetry(tmp_path)['queries'][0]['state']
    assert read_telemetry()['state']=='unavailable'


def test_percentiles_valid_population():
    assert percentiles([]) is None
    r=percentiles([1,2,3,4,5])
    assert r['population']==5 and r['p50_ms']==3 and r['p95_ms']==pytest.approx(4.8)


def test_resource_guard_refuses(tmp_path):
    with pytest.raises(RuntimeError,match='resource guard'):
        resource_guard(tmp_path,reserve_bytes=10**30)


def test_producer_queue_bound_and_recovery_observations():
    from bda.producer import EventProducer
    from bda.config import KafkaSettings
    from bda.reliability.workload import message
    from bda.tests.test_bda import FakeProducer,wait_for
    gate=threading.Event()
    class Blocked(FakeProducer):
        def produce(self,*a,**kw):
            gate.wait(3)
            return super().produce(*a,**kw)
    worker=EventProducer(KafkaSettings(enabled=True,queue_size=1),Blocked)
    worker.start()
    try:
        assert worker.submit(message('one'),1)
        wait_for(lambda:worker.queue.qsize()==0)
        assert worker.submit(message('two'),1)
        assert not worker.submit(message('dropped'),1)
        assert worker.snapshot()['queue_depth']==1
        worker.report('delivery_failed',RuntimeError('timeout'))
        assert 'errors observed' in worker.snapshot()['connectivity']
        gate.set();wait_for(lambda:worker.snapshot().get('delivered')==2)
        assert worker.snapshot()['connectivity']=='recent acknowledgement'
        assert worker.snapshot()['delivery_failed']==1 and worker.snapshot()['dropped_queue_full']==1
        assert percentiles(worker.ack_latencies_ms)['population']==2
    finally:
        gate.set();worker.close()
    assert not worker.thread.is_alive()


def test_atomic_reader_during_replacement(tmp_path):
    path=tmp_path/'atomic.json'
    atomic_json(path,{'i':0})
    errors=[]
    def writer():
        for i in range(30):atomic_json(path,{'i':i})
    thread=threading.Thread(target=writer);thread.start()
    while thread.is_alive():
        try:json.loads(path.read_text())
        except ValueError as exc:errors.append(str(exc))
    thread.join();assert not errors


def test_verified_output_summary_is_bounded_and_redacted(tmp_path):
    atomic_json(tmp_path/'output-summary.json',dict(classified=10,quarantined=1,validated=8,
        finalized_windows=3,window_events=7,detected_anomalies=1,observed_at=datetime.now(timezone.utc).isoformat(),secret='hidden'))
    summary=read_telemetry(tmp_path)['verified_output']
    assert summary['quarantined']==1 and 'secret' not in summary


def test_last_successful_progress_survives_same_checkpoint_query_restart(tmp_path):
    t=Telemetry(tmp_path,interval=1)
    t.started('windows','checkpoint-query')
    t.progress({'name':'windows','batchId':3,'numInputRows':7})
    first=read_telemetry(tmp_path)['queries'][0]['last_progress_at']
    t.terminated('checkpoint-query',None);t.close()
    second=Telemetry(tmp_path,interval=1)
    try:
        second.started('windows','checkpoint-query')
        row=read_telemetry(tmp_path)['queries'][0]
        assert row['last_progress_at']==first and row['progress']['num_input_rows']==7
        second.started('windows','different-query')
        assert read_telemetry(tmp_path)['queries'][0]['last_progress_at'] is None
    finally:second.close()
