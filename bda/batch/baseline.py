"""Independent single-process, bounded-memory analytical reference."""
from collections import Counter, defaultdict
from datetime import datetime, timezone
import gzip
import json
from pathlib import Path
from bda.batch.generate import checksum


def manifest(path, verify=True):
    root = Path(path)
    data = json.loads((root/'manifest.json').read_text())
    if data.get('schema_version') != '1.0' or data.get('provenance') != 'synthetic':
        raise ValueError('unsupported benchmark manifest')
    for entry in data['files']:
        file = root/entry['path']
        if file.parent.resolve() != root.resolve():
            raise ValueError('manifest file must be directly inside dataset root')
        if file.stat().st_size != entry['bytes'] or (verify and checksum(file) != entry['sha256']):
            raise ValueError('dataset checksum/size mismatch: '+str(file))
    return data


def iter_records(path, metadata=None):
    root = Path(path)
    for entry in (metadata or manifest(root))['files']:
        file = root/entry['path']
        opener = gzip.open if file.suffix == '.gz' else open
        with opener(file,'rt',encoding='utf-8') as stream:
            for line in stream:
                yield json.loads(line)


def summary(path, metadata):
    counts = {key:Counter() for key in ('category','hour','source')}
    sums = defaultdict(float)
    for record in iter_records(path,metadata):
        # Parse offsets independently of Spark, instead of relying on string slicing.
        time = datetime.fromisoformat(record['event_time'].replace('Z','+00:00')).astimezone(timezone.utc)
        hour = time.strftime('%Y-%m-%dT%H:00:00Z')
        counts['category'][record['category']] += 1
        counts['hour'][hour] += 1
        counts['source'][record['source_id']] += 1
        sums[record['category']] += record['confidence']
    return sorted([dict(dimension=d,key=k,event_count=v,
                       avg_confidence=sums[k]/v if d=='category' else None)
                   for d,groups in counts.items() for k,v in groups.items()],key=lambda r:(r['dimension'],r['key']))


def expected(metadata):
    stats = metadata['expected']
    return sorted([dict(dimension=d,key=k,event_count=v,
                       avg_confidence=stats['confidence_sums'][k]/v if d=='category' else None)
                   for d,groups in [('category',stats['categories']),('hour',stats['hourly']),('source',stats['sources'])]
                   for k,v in groups.items()],key=lambda r:(r['dimension'],r['key']))


def assert_equivalent(actual, reference):
    if len(actual) != len(reference):
        raise AssertionError('aggregate cardinality mismatch')
    for a,b in zip(actual,reference):
        for key in ('dimension','key','event_count'):
            if a[key] != b[key]:
                raise AssertionError(f'aggregate mismatch: {a} != {b}')
        if a['avg_confidence'] is not None and abs(a['avg_confidence']-b['avg_confidence']) > 1e-8:
            raise AssertionError('confidence mean mismatch')


if __name__ == '__main__':
    import argparse
    import time
    p = argparse.ArgumentParser()
    p.add_argument('--input',required=True)
    p.add_argument('--output',required=True)
    args = p.parse_args()
    metadata = manifest(args.input)
    start = time.perf_counter()
    result = summary(args.input,metadata)
    seconds = time.perf_counter()-start
    assert_equivalent(result,expected(metadata))
    with Path(args.output).open('x') as stream:
        json.dump({'duration_seconds':seconds,'record_count':metadata['record_count'],'correct':True,'results':result},stream,sort_keys=True)
    print(json.dumps({'duration_seconds':seconds,'records_per_second':metadata['record_count']/seconds}))
