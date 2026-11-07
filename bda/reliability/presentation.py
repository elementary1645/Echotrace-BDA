"""Complete generated-output API -> browser verification using an owned local server."""
import argparse
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from bda.reliability.workload import wait_until,percentiles
from bda.streaming.telemetry import atomic_json


def main():
    p=argparse.ArgumentParser();p.add_argument('--run',required=True);p.add_argument('--port',type=int,default=8005)
    args=p.parse_args();root=Path(args.run).resolve();repo=Path(__file__).resolve().parents[2]
    if (root/'presentation.json').exists():raise ValueError('presentation evidence already exists; retain it and use a new pipeline run for another full observation')
    receipt=json.loads((root/'receipt.json').read_text())
    if receipt.get('status')!='passed':raise ValueError('pipeline verification must pass first')
    if not (root/'streaming.sqlite').is_file():raise ValueError('generated export missing')
    with socket.socket() as s:
        s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1);s.bind(('127.0.0.1',args.port))
    env={**os.environ,'BDA_STREAMING_DATABASE':str(root/'streaming.sqlite'),
         'BDA_TELEMETRY_ROOT':str(root/'acoustic'),'KAFKA_ENABLED':'false'}
    before=time.monotonic();url=f'http://127.0.0.1:{args.port}'
    result={'schema_version':1,'run':root.name,'source':'SIMULATED','api_samples':[]}
    with (root/'api-server.log').open('w') as log:
        server=subprocess.Popen([sys.executable,'-m','bda.analytics.demo','--port',str(args.port)],cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT)
        try:
            import httpx
            with httpx.Client(base_url=url,timeout=10,trust_env=False) as client:
                def ready():
                    if server.poll() is not None:raise RuntimeError('demo server exited')
                    try:return client.get('/api/v1/analytics/streaming').status_code==200
                    except httpx.TransportError:return False
                wait_until(ready,30)
                result['server_start_to_first_verified_api_ms']=(time.monotonic()-before)*1000
                for _ in range(21):
                    start=time.monotonic();r=client.get('/api/v1/analytics/streaming');elapsed=(time.monotonic()-start)*1000
                    r.raise_for_status();body=r.json()
                    assert body['provenance']['event_count']==25 and body['provenance']['origin']=='streaming-finalized'
                    acoustic=[row for row in body['rows'] if row['source_id']=='p5-acoustic']
                    assert sum(row['event_count'] for row in acoustic)==8
                    assert all(row['source']=='SIMULATED' for row in body['rows'])
                    result['api_samples'].append(elapsed)
                health=client.get('/api/v1/analytics/health').json()
                assert health['spark']['state']=='stopped'
                assert health['spark']['verified_output']['quarantined']==1
                assert str(root) not in json.dumps(health)
                result['observed_health']=health
            browser_env={**env,'P5_BROWSER_URL':url}
            with (root/'browser.log').open('w') as out:
                browser=subprocess.run(['npm','run','test:e2e','--','e2e/phase5.spec.ts'],cwd=repo/'frontend',env=browser_env,
                                       stdout=out,stderr=subprocess.STDOUT,timeout=90)
            assert browser.returncode==0,'browser verification failed; see browser.log'
            result['status']='passed'
            result['api_http_latency_ms']=percentiles(result['api_samples'][1:])
            result['api_warmups_excluded']=1
            result['method']='same-host real HTTP streaming snapshot reads, sequential, 20 measured requests; not event transport latency'
        except BaseException as e:
            result['status']='failed';result['failure_type']=type(e).__name__;raise
        finally:
            server.terminate()
            try:server.wait(timeout=20)
            except subprocess.TimeoutExpired:server.kill();server.wait();result['forced_server_shutdown']=True
            result['server_exit_code']=server.returncode
            result['graceful_shutdown_observed']='Application shutdown complete.' in (root/'api-server.log').read_text()
            result['finished_at']=datetime.now(timezone.utc).isoformat()
            atomic_json(root/'presentation.json',result)


if __name__=='__main__':main()
