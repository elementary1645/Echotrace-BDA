import json
import pytest
from bda.schema import AnalyticsEvent, record_identity
from bda.streaming.validation import validate_record


def payload(**changes):
    data = dict(schema_version='1.0',record_type='event',source_id='test',session_id='synthetic',
        event_id='e1',source='SIMULATED',event_time='2026-01-01T05:30:03+05:30',t=3.,
        t_start=1.,t_end=2.,category='IMPACT',label='Crash',confidence=.8,zone='LEFT',
        zone_confidence='high',angle_deg=-30.)
    data.update(changes)
    data['record_id'] = record_identity(data['source_id'],data['session_id'],data['event_id'])
    return data


def test_valid():
    data = payload()
    canonical, error = validate_record(json.dumps(data), data['record_id'])
    assert error is None
    assert json.loads(canonical)['event_time'] == '2026-01-01T00:00:03Z'


@pytest.mark.parametrize('changes', [dict(schema_version='2'),dict(confidence=2),
    dict(event_time='bad'),dict(event_time='2026-01-01'),dict(source='FAKE'),
    dict(event_id=' '),dict(t_end=10),dict(extra=True)])
def test_reject(changes):
    data = payload(**changes)
    assert validate_record(json.dumps(data),data['record_id'])[1]


def test_malformed_identity():
    assert validate_record('{', 'x')[1]
    assert validate_record('null', 'x')[1]
    assert validate_record(json.dumps(payload()), 'wrong')[1]


@pytest.mark.parametrize('source',['LIVE','RECORDED','SIMULATED'])
def test_source(source):
    data = payload(source=source)
    canonical, error = validate_record(json.dumps(data),data['record_id'])
    assert error is None
    assert json.loads(canonical)['source'] == source


def test_retention_is_opt_in(tmp_path):
    from bda.streaming.retention import candidates
    retired = tmp_path/'retired'
    retired.mkdir()
    (retired/'.retired').touch()
    active = tmp_path/'active'
    active.mkdir()
    now = (retired/'.retired').stat().st_mtime + 31*86400
    assert candidates(tmp_path,30,now) == [retired]
    assert candidates(tmp_path,32,now) == []


def test_storage_guard(tmp_path):
    from bda.streaming.storage import claim
    locks = claim(tmp_path,'ingest',{'topic':'test'})
    try:
        with pytest.raises(BlockingIOError):
            claim(tmp_path,'ingest',{'topic':'test'})
        with pytest.raises(ValueError,match='configuration differs'):
            claim(tmp_path,'events',{'topic':'other'})
    finally:
        for lock in locks:
            lock.close()
    for lock in claim(tmp_path,'all',{'topic':'test'}):
        lock.close()
