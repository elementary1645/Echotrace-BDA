"""Read-only integration contracts against genuine Phase 3 aggregate outputs."""
import json
import sqlite3

import httpx
import pytest
from echotrace.api import analytics
from echotrace.api.server import create_app, Engine


@pytest.fixture
def client():
    # ASGI transport avoids neural model/audio startup; production routing is exercised.
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(Engine())),base_url='http://test')


def request(client, path):
    import asyncio
    return asyncio.run(client.get('/api/v1/analytics/'+path))


def test_overview_provenance(client):
    r=request(client,'overview'); assert r.status_code==200
    body=r.json(); row=body['rows'][0]
    assert row['event_count']==100000 and row['sources']==16
    assert sum(r['value'] for r in row['categories'])==100000
    assert body['provenance']['source']=='SIMULATED'
    assert body['provenance']['dataset_id'].startswith('22a52e')


def test_dates_and_pagination(client):
    path='historical?start=2026-01-01T00:00:00Z&end=2026-01-03T00:00:00Z'
    body=request(client,path).json()
    assert sum(r['value'] for r in body['rows'])==28426 and body['total']==48
    first=request(client,path+'&limit=3').json()
    second=request(client,path+'&limit=3&offset=3').json()
    assert first['rows']==body['rows'][:3] and second['rows']==body['rows'][3:6]
    assert first['provenance']==second['provenance']


@pytest.mark.parametrize('query',[
    'limit=501','offset=-1','dimension=sql','category=ALARM%27',
    'start=2026-01-01T00:30:00Z','start=2026-01-01T00:00:00',
    'start=2026-01-03T00:00:00Z&end=2026-01-01T00:00:00Z',
    'start=2024-01-01T00:00:00Z&end=2026-01-01T00:00:00Z'])
def test_invalid_filters(client,query):
    assert request(client,'historical?'+query).status_code==422


def test_filter_consistency(client):
    params='?category=ALARM&source_id=synthetic-node-00000&start=2026-01-04T00:00:00Z&end=2026-01-05T00:00:00Z'
    totals=[]
    for dimension in ['hour','day','zone','confidence_bin','source_id','category']:
        r=request(client,'historical'+params+'&dimension='+dimension).json()
        totals.append(sum(row['value'] for row in r['rows']))
    assert len(set(totals))==1 and totals[0]>0


def test_empty(client):
    r=request(client,'historical?source_id=nonexistent').json()
    assert r['total']==0 and r['rows']==[]


def test_missing_and_malformed(client,monkeypatch,tmp_path):
    monkeypatch.setattr(analytics,'DATABASE',tmp_path/'bad.sqlite')
    assert request(client,'overview').status_code==503
    analytics.DATABASE.write_text('corrupt')
    assert request(client,'overview').status_code==503
    analytics.DATABASE.unlink()
    db=sqlite3.connect(analytics.DATABASE);db.execute('CREATE TABLE metadata (key,value)');db.close()
    assert request(client,'historical').status_code==503


def test_anomaly_contract(client):
    body=request(client,'anomalies?limit=2').json()
    assert len(body['rows'])==2
    for row in body['rows']:
        assert row['kind']=='detected' and row['explanation']
        assert row['threshold_exceeded'] or row['baseline_exceeded']
        assert 'severity' not in row


def test_benchmarks(client):
    body=request(client,'benchmarks').json()
    rows=body['rows']; assert len(rows)==72
    largest=[r for r in rows if r['record_count']==5000000 and not r['warmup']]
    assert len(largest)==6
    assert all(r['duration_seconds']>0 for r in largest)
    assert '/artifacts' not in json.dumps(body)


def test_bad_benchmarks(client,monkeypatch,tmp_path):
    analytics.benchmark_report.cache_clear()
    monkeypatch.setattr(analytics,'REPORTS',tmp_path)
    (tmp_path/'trials.json').write_text('[1]')
    assert request(client,'benchmarks').status_code==503
    analytics.benchmark_report.cache_clear()


def test_health_and_streaming(client):
    body=request(client,'health').json()
    assert body['spark']['state']=='unavailable' and body['kafka']['state']=='unavailable'
    assert body['acoustic']=='stopped'
    assert request(client,'streaming').status_code==503


def test_ground_truth_is_separate(client):
    body=request(client,'ground-truth').json()
    assert body['source']=='SIMULATED'
    assert body['rows']==[{'hour':'2026-01-04T12:00:00Z','source_id':'synthetic-node-00000','category':'ALARM','injected_events':1000}]
    assert 'not detected' in body['origin']


def test_provenance_schema_rejects_contradiction(client,monkeypatch,tmp_path):
    source=sqlite3.connect(analytics.DATABASE)
    target=tmp_path/'wrong.sqlite';dest=sqlite3.connect(target);source.backup(dest);source.close()
    dest.execute("UPDATE metadata SET value='\"LIVE\"' WHERE key='source'");dest.commit();dest.close()
    monkeypatch.setattr(analytics,'DATABASE',target)
    assert request(client,'overview').status_code==503


def test_offset_hour_is_not_silently_rounded(client):
    assert request(client,'historical?start=2026-01-01T00:00:00%2B05:30').status_code==422


def test_empty_export_and_restore(client,monkeypatch,tmp_path):
    original=analytics.DATABASE
    target=tmp_path/'empty.sqlite'
    source=sqlite3.connect(original);dest=sqlite3.connect(target);source.backup(dest);source.close()
    dest.execute('DELETE FROM cells');dest.execute('DELETE FROM anomalies');dest.execute("UPDATE metadata SET value='0' WHERE key='event_count'");dest.commit();dest.close()
    monkeypatch.setattr(analytics,'DATABASE',target)
    row=request(client,'overview').json()['rows'][0]
    assert row['event_count']==0 and row['avg_confidence'] is None and row['categories']==[]
    moved=tmp_path/'temporarily-unavailable.sqlite';target.rename(moved)
    assert request(client,'overview').status_code==503
    moved.rename(target)
    assert request(client,'overview').status_code==200


def test_health_does_not_expose_configured_paths(client,monkeypatch,tmp_path):
    monkeypatch.setenv('BDA_TELEMETRY_ROOT',str(tmp_path/'private-location'))
    body=request(client,'health').json()
    assert str(tmp_path) not in json.dumps(body)
    assert body['spark']['state']=='unavailable'
