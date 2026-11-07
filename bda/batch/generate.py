"""Deterministic bounded-memory v1 event generator; outputs never overwrite a dataset."""
import argparse
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from functools import cached_property
import gzip
import hashlib
import json
import random
import shutil
from pathlib import Path
from bda.schema import record_identity

VERSION = '1.0.0'
CATEGORIES = ['SPEECH','IMPACT','ALARM','MUSIC','AMBIENT']
ZONES = ['LEFT','CENTRE','RIGHT','UNKNOWN']


@dataclass(frozen=True)
class Config:
    count: int = 100000
    seed: int = 42
    sources: int = 16
    days: int = 7
    start: str = '2026-01-01T00:00:00+00:00'
    peak_hour: int = 18
    anomaly_every: int = 100
    chunk: int = 100000
    format: str = 'jsonl'

    def validate(self):
        if min(self.count, self.sources, self.days, self.chunk) < 1 or self.sources > 10000 or self.days > 366:
            raise ValueError('positive counts required; at most 10000 sources and 366 days')
        if not 0 <= self.peak_hour <= 23 or self.anomaly_every < 0:
            raise ValueError('invalid peak hour or anomaly interval')
        if self.format not in ('jsonl','jsonl.gz'):
            raise ValueError('format must be jsonl or jsonl.gz')
        start = datetime.fromisoformat(self.start.replace('Z','+00:00'))
        if (start.hour,start.minute,start.second,start.microsecond) != (0,0,0,0):
            raise ValueError('start must be UTC midnight so peak_hour describes a clock hour')
        if start.utcoffset() != timedelta(0):
            raise ValueError('start must be explicit UTC')

    @cached_property
    def identity(self):
        return hashlib.sha256(json.dumps({'version':VERSION, **asdict(self)},sort_keys=True).encode()).hexdigest()


def records(config):
    config.validate()
    rng = random.Random(config.seed)
    start = datetime.fromisoformat(config.start.replace('Z','+00:00'))
    weights = [1 if h < 6 or h >= 23 else 4 for h in range(24)]
    weights[config.peak_hour] = 12
    for i in range(config.count):
        source = rng.randrange(config.sources)
        day = rng.randrange(config.days)
        hour = rng.choices(range(24),weights=weights)[0]
        category = rng.choices(CATEGORIES,weights=[35,20,5,15,25])[0]
        injected = config.anomaly_every > 0 and i % config.anomaly_every == 0
        if injected:
            source, day, hour, category = 0, config.days//2, 12, 'ALARM'
        event_time = start + timedelta(days=day,hours=hour,seconds=rng.randrange(3600),microseconds=rng.randrange(1000)*1000)
        source_id = f'synthetic-node-{source:05d}'
        session = f'generated-{config.identity[:16]}/{source:05d}'
        event_id = f'e{i:012d}'
        t = round((event_time-start).total_seconds()+2,3)
        zone = rng.choices(ZONES,weights=[30,35,30,5])[0]
        yield dict(schema_version='1.0',record_type='event',record_id=record_identity(source_id,session,event_id),
            source_id=source_id,session_id=session,event_id=event_id,source='SIMULATED',
            event_time=event_time.isoformat().replace('+00:00','Z'),t=t,t_start=round(t-1,3),t_end=t,
            category=category,label=f'Synthetic {category.lower()}',confidence=round(rng.betavariate(8,2),6),
            zone=zone,zone_confidence=None if zone=='UNKNOWN' else 'high',
            angle_deg=None if zone=='UNKNOWN' else round(rng.uniform(-90,90),3)), injected


def checksum(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024*1024), b''):
            digest.update(block)
    return digest.hexdigest()


def resource_check(path, count):
    path = Path(path)
    ancestor = path
    while not ancestor.exists():
        ancestor = ancestor.parent
    # Reserve raw JSON + Parquet/intermediate/headroom, independent of gzip's likely savings.
    needed = int(count * 1000 * 1.5) + 2 * 1024**3
    free = shutil.disk_usage(ancestor).free
    if free < needed:
        raise RuntimeError(f'insufficient disk: need {needed} bytes including reserve, have {free}')
    return {'free_disk_bytes':free,'estimated_required_bytes':needed}


def generate(path, config):
    config.validate()
    resource_check(path,config.count)
    path = Path(path)
    path.mkdir(parents=True,exist_ok=False)
    categories, hours, sources, zones, days, trends = (Counter() for _ in range(6))
    sums = defaultdict(float)
    injections = Counter()
    files = []
    stream = None
    raw = None
    try:
        for i,(record,injected) in enumerate(records(config)):
            if i % config.chunk == 0:
                if stream:
                    stream.close()
                    if raw:
                        raw.close()
                name = f'part-{i//config.chunk:05d}.{config.format}'
                if config.format == 'jsonl.gz':
                    # Filename and mtime are omitted from gzip headers for byte reproducibility.
                    raw = (path/name).open('wb')
                    stream = gzip.GzipFile(filename='',mode='wb',fileobj=raw,mtime=0,compresslevel=1)
                else:
                    stream = (path/name).open('wb')
            stream.write((json.dumps(record,separators=(',',':'),ensure_ascii=False,allow_nan=False)+'\n').encode())
            c = record['category']
            categories[c] += 1
            sums[c] += record['confidence']
            hour = record['event_time'][:13]+':00:00Z'
            hours[hour] += 1
            sources[record['source_id']] += 1
            zones[record['zone']] += 1
            days[record['event_time'][:10]] += 1
            trends[record['event_time'][:10]+'|'+c] += 1
            if injected:
                injections[hour+'|'+record['source_id']+'|'+c] += 1
        if stream:
            stream.close()
        if raw:
            raw.close()
        for file in sorted(path.glob('part-*')):
            digest = checksum(file)
            files.append(dict(path=file.name,bytes=file.stat().st_size,sha256=digest))
        manifest = dict(generator_version=VERSION,schema_version='1.0',dataset_id=config.identity,
            provenance='synthetic',source_classification='SIMULATED',record_count=config.count,
            config=asdict(config),files=files,file_count=len(files),total_bytes=sum(f['bytes'] for f in files),
            expected=dict(categories=dict(categories),confidence_sums=dict(sums),hourly=dict(hours),
                          sources=dict(sources),zones=dict(zones),daily=dict(days),daily_categories=dict(trends)),
            injected_anomalies=dict(injections),injected_count=sum(injections.values()))
        (path/'manifest.json').write_text(json.dumps(manifest,sort_keys=True,indent=2)+'\n')
        return manifest
    finally:
        if stream:
            stream.close()
        if raw:
            raw.close()


def main():
    p = argparse.ArgumentParser()
    for name,default in [('count',100000),('seed',42),('sources',16),('days',7),('peak-hour',18),('anomaly-every',100),('chunk',100000)]:
        p.add_argument('--'+name,type=int,default=default)
    p.add_argument('--start',default=Config.start)
    p.add_argument('--format',choices=['jsonl','jsonl.gz'],default='jsonl')
    p.add_argument('--output',required=True)
    args = vars(p.parse_args())
    output = args.pop('output')
    result = generate(output,Config(**args))
    print(json.dumps({k:result[k] for k in ('dataset_id','record_count','file_count','total_bytes')},sort_keys=True))


if __name__ == '__main__':
    main()
