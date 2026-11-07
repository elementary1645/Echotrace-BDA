"""Opt-in API contract over the outputs of the full isolated Kafka/Spark run."""
import asyncio
import json
import os
from pathlib import Path
import pytest

pytestmark=pytest.mark.skipif(not os.getenv('PHASE5_RUN_DIR'),reason='requires a completed isolated Phase 5 run')


def test_generated_api_and_anomaly_windows(monkeypatch):
    import httpx
    from echotrace.api.server import Engine,create_app
    root=Path(os.environ['PHASE5_RUN_DIR']).resolve()
    assert json.loads((root/'receipt.json').read_text())['status']=='passed'
    monkeypatch.setenv('BDA_STREAMING_DATABASE',str(root/'streaming.sqlite'))
    monkeypatch.setenv('BDA_HISTORY_DATABASE',str(root/'streaming.sqlite'))
    monkeypatch.setenv('BDA_TELEMETRY_ROOT',str(root/'acoustic'))
    async def check():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(Engine())),base_url='http://test') as c:
            stream=(await c.get('/api/v1/analytics/streaming')).json()
            assert stream['provenance']['event_count']==25
            assert sum(r['event_count'] for r in stream['rows'] if r['source_id']=='p5-acoustic')==8
            overview=(await c.get('/api/v1/analytics/overview')).json()
            assert overview['rows'][0]['event_count']==25
            anomalies=(await c.get('/api/v1/analytics/anomalies')).json()['rows']
            assert len(anomalies)==2
            assert any(r['hour']=='2026-01-01T00:03:00Z' and r['source_id']=='p5-fixture' and r['event_count']==10 for r in anomalies)
            health=(await c.get('/api/v1/analytics/health')).json()
            assert health['spark']['state']=='stopped' and health['spark']['verified_output']['quarantined']==1
    asyncio.run(check())


def test_generated_ground_truth_does_not_borrow_another_population(monkeypatch):
    import httpx
    from echotrace.api.server import create_app,Engine
    root=Path(os.environ['PHASE5_RUN_DIR']).resolve()
    monkeypatch.setenv('BDA_HISTORY_DATABASE',str(root/'streaming.sqlite'))
    async def check():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(Engine())),base_url='http://test') as c:
            truth=(await c.get('/api/v1/analytics/ground-truth')).json()
            assert truth['rows']==[] and truth['source']=='unavailable'
            rows=(await c.get('/api/v1/analytics/anomalies')).json()['rows']
            assert all('None' not in r['explanation'] for r in rows)
    asyncio.run(check())
