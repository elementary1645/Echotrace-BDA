"""Repeatable in-process API timing (excludes HTTP network and Spark setup)."""
import asyncio
import json
from pathlib import Path
import statistics
import sys
import time


async def main():
    sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'backend'))
    import httpx
    from echotrace.api.server import create_app,Engine
    results={}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(Engine())),base_url='http://test') as client:
        for route in ('overview','historical?dimension=hour','anomalies?limit=20','benchmarks'):
            samples=[]
            for _ in range(11):
                start=time.perf_counter()
                r=await client.get('/api/v1/analytics/'+route)
                r.raise_for_status()
                samples.append((time.perf_counter()-start)*1000)
            results[route]={'median_ms':statistics.median(samples[1:]),'max_ms':max(samples[1:]),
                            'payload_bytes':len(r.content),'samples':10,'warmup_excluded':1}
    print(json.dumps({'method':'ASGI transport; sequential local requests; first excluded; OS cache uncontrolled; no network timing',
                      'results':results},indent=2))


if __name__=='__main__':
    asyncio.run(main())
