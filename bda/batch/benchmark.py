"""Journaled, independently checked Python/Spark comparisons on the same raw files."""
import argparse
import json
import os
from pathlib import Path
import platform
import shutil
import threading
import time
from uuid import uuid4
from bda.batch.baseline import manifest, summary, expected, assert_equivalent
from bda.batch.generate import checksum


def read_number(path):
    try:
        return int(Path(path).read_text().strip())
    except (OSError,ValueError):
        return None


def cpu_seconds():
    try:
        values = dict(line.split() for line in Path('/sys/fs/cgroup/cpu.stat').read_text().splitlines())
        return int(values['usage_usec'])/1e6
    except (OSError,KeyError,ValueError):
        return None


class Meter:
    def __enter__(self):
        self.peak = 0
        self.done = threading.Event()
        self.cpu = cpu_seconds()
        def sample():
            while not self.done.is_set():
                self.peak = max(self.peak,read_number('/sys/fs/cgroup/memory.current') or 0)
                self.done.wait(.05)
        self.thread = threading.Thread(target=sample,daemon=True)
        self.thread.start()
        return self

    def __exit__(self,*_):
        self.done.set()
        self.thread.join()
        after = cpu_seconds()
        self.cpu_used = after-self.cpu if after is not None and self.cpu is not None else None


def append(path, record):
    with Path(path).open('a') as stream:
        stream.write(json.dumps(record,sort_keys=True)+'\n')
        stream.flush()
        os.fsync(stream.fileno())


def records(path):
    """Keep failed/truncated/incomplete trials visible instead of silently discarding them."""
    started, outcomes, errors = {},{},[]
    for number,line in enumerate(Path(path).read_text().splitlines(),1):
        try:
            row = json.loads(line)
        except ValueError:
            errors.append({'status':'invalid_journal_line','line':number})
            continue
        run = row.get('run_id')
        if row.get('record_type') == 'started':
            started[run] = row
        elif row.get('record_type') == 'result':
            outcomes[run] = row
        else:
            errors.append({'status':'unknown_journal_record','line':number})
    return [outcomes.get(run,dict(row,status='incomplete')) for run,row in started.items()] + errors


def event_metrics(directory):
    stages, groups, jobs = {},{},{}
    for file in sorted(Path(directory).rglob('*')):
        if not file.is_file() or not file.name.startswith('events'):
            continue
        with file.open() as stream:
            for line in stream:
                event = json.loads(line)
                if event['Event'] == 'SparkListenerJobStart':
                    group = (event.get('Properties') or {}).get('spark.jobGroup.id','setup')
                    jobs[event['Job ID']] = group
                    for stage in event['Stage IDs']:
                        stages[stage] = group
                elif event['Event'] == 'SparkListenerTaskEnd':
                    group = stages.get(event['Stage ID'],'setup')
                    result = groups.setdefault(group,dict(tasks=0,executor_run_ms=0,executor_cpu_ns=0,input_bytes=0,
                        shuffle_read_bytes=0,shuffle_write_bytes=0,disk_spilled_bytes=0,memory_spilled_bytes=0))
                    metrics = event.get('Task Metrics',{})
                    result['tasks'] += 1
                    result['executor_run_ms'] += metrics.get('Executor Run Time',0)
                    result['executor_cpu_ns'] += metrics.get('Executor CPU Time',0)
                    result['input_bytes'] += metrics.get('Input Metrics',{}).get('Bytes Read',0)
                    read = metrics.get('Shuffle Read Metrics',{})
                    result['shuffle_read_bytes'] += read.get('Remote Bytes Read',0)+read.get('Local Bytes Read',0)
                    result['shuffle_write_bytes'] += metrics.get('Shuffle Write Metrics',{}).get('Shuffle Bytes Written',0)
                    result['disk_spilled_bytes'] += metrics.get('Disk Bytes Spilled',0)
                    result['memory_spilled_bytes'] += metrics.get('Memory Bytes Spilled',0)
    return groups


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--input',required=True)
    p.add_argument('--output',required=True)
    p.add_argument('--shuffle',type=int,default=2)
    p.add_argument('--input-partitions',type=int,default=2)
    p.add_argument('--runs',type=int,default=3)
    p.add_argument('--warmups',type=int,default=1)
    p.add_argument('--storage-manifest')
    args = p.parse_args()
    if min(args.runs,args.shuffle,args.input_partitions) < 1 or args.warmups < 0:
        p.error('runs/partitions must be positive; warmups nonnegative')
    target = Path(args.output)
    target.mkdir(parents=True,exist_ok=False)
    journal = target/'trials.jsonl'
    setup = time.perf_counter()
    if shutil.disk_usage(target).free < 2*1024**3:
        raise RuntimeError('benchmark needs at least 2 GiB free disk for output/spill headroom')
    limit = read_number('/sys/fs/cgroup/memory.max')
    if limit is not None and limit < 2*1024**3:
        raise RuntimeError('benchmark container requires at least 2 GiB memory')
    metadata = manifest(args.input)
    reference = expected(metadata)
    from pyspark.sql import SparkSession
    from bda.streaming.transforms import EVENT
    from bda.batch.queries import benchmark_summary
    logs = target/'event-log'
    logs.mkdir()
    spark = (SparkSession.builder.appName('EchoTrace-benchmark').config('spark.sql.session.timeZone','UTC')
        .config('spark.sql.shuffle.partitions',args.shuffle).config('spark.eventLog.enabled','true')
        .config('spark.eventLog.dir',str(logs)).config('spark.eventLog.compress','false').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    environment = dict(spark_version=spark.version,java_version=spark._jvm.java.lang.System.getProperty('java.version'),
        python_version=platform.python_version(),cpu_count=os.cpu_count(),master=spark.sparkContext.master,
        memory_limit_bytes=read_number('/sys/fs/cgroup/memory.max'),
        cpu_limit=Path('/sys/fs/cgroup/cpu.max').read_text().strip(),
        container='echotrace-spark:4.0.1; cpus=2; memory=3g; heap=1g',
        spark_configuration=dict(spark.sparkContext.getConf().getAll()),setup_seconds=time.perf_counter()-setup)
    (target/'environment.json').write_text(json.dumps(environment,sort_keys=True,indent=2)+'\n')
    paths = [str(Path(args.input)/f['path']) for f in metadata['files']]
    # Deterministic split-size control changes input partitions without a timed repartition shuffle.
    spark.conf.set('spark.sql.files.maxPartitionBytes',max(1,metadata['total_bytes']//args.input_partitions))
    spark.conf.set('spark.sql.files.openCostInBytes',0)
    storage = json.loads(Path(args.storage_manifest).read_text()) if args.storage_manifest else {}
    common = dict(dataset_id=metadata['dataset_id'],manifest_sha256=checksum(Path(args.input)/'manifest.json'),record_count=metadata['record_count'],
        input_bytes=metadata['total_bytes'],validated_bytes=storage.get('validated_bytes'),
        master=spark.sparkContext.master,shuffle_partitions=args.shuffle,requested_input_partitions=args.input_partitions,
        cache_policy='no persisted DataFrame; uncontrolled OS cache; warmups separate',format='JSONL',
        workload='category count/mean + UTC hourly count + source count in one grouping-sets scan')
    failure = False
    try:
        for engine in ('python','spark'):
            for index in range(args.warmups+args.runs):
                run = uuid4().hex
                info = dict(common,run_id=run,engine=engine,iteration=index,
                            warmup=index<args.warmups,record_type='started')
                append(journal,info)
                started = time.perf_counter()
                try:
                    with Meter() as meter:
                        if engine == 'python':
                            actual = summary(args.input,metadata)
                            partitions = 1
                        else:
                            spark.sparkContext.setJobGroup(run,'measured analytical bundle')
                            df = spark.read.schema(EVENT).json(paths)
                            partitions = df.rdd.getNumPartitions()
                            rows = benchmark_summary(df).limit(100001).collect()
                            if len(rows)>100000:
                                raise RuntimeError('aggregate output exceeds bounded driver result limit')
                            actual = sorted([row.asDict() for row in rows],key=lambda r:(r['dimension'],r['key']))
                    seconds = time.perf_counter()-started
                    # Checks and result serialization are outside the analytical timer for BOTH engines.
                    assert_equivalent(actual,reference)
                    data = json.dumps(actual,sort_keys=True).encode()
                    (target/f'{run}.json').write_bytes(data)
                    append(journal,dict(info,record_type='result',status='success',correct=True,
                        duration_seconds=seconds,records_per_second=metadata['record_count']/seconds,
                        input_mib_per_second=metadata['total_bytes']/seconds/1024**2,
                        cpu_seconds=meter.cpu_used,sampled_peak_container_bytes=meter.peak,
                        output_bytes=len(data),input_partitions=partitions))
                except Exception as exc:
                    failure = True
                    append(journal,dict(info,record_type='result',status='failed',error=repr(exc),
                                        duration_seconds=time.perf_counter()-started))
    finally:
        spark.stop()
        (target/'task-metrics.json').write_text(json.dumps(event_metrics(logs),sort_keys=True,indent=2)+'\n')
        (target/'summary.json').write_text(json.dumps(records(journal),sort_keys=True,indent=2)+'\n')
    if failure:
        raise SystemExit('one or more trials failed; retained in journal')


if __name__ == '__main__':
    main()
