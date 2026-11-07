"""Immutable historical snapshots; raw input is referenced, never moved or modified."""
import argparse
import json
import time
import shutil
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from pyspark import StorageLevel
from bda.streaming.transforms import EVENT, EVENT_OUTPUT, RESULT, aggregate, anomalies
from bda.streaming.validation import validate_record
from bda.batch.baseline import manifest
from bda.batch.generate import resource_check, checksum
from bda.batch.queries import query, QUERIES


def size(path):
    return sum(p.stat().st_size for p in Path(path).rglob('*') if p.is_file())


def build(spark, source, output, phase2=False, partitions=2, threshold=100, fixture=False):
    if phase2 and fixture:
        raise ValueError('phase2 and fixture are mutually exclusive')
    started = time.perf_counter()
    target = Path(output)
    if phase2:
        ancestor = target.parent
        while not ancestor.exists():
            ancestor = ancestor.parent
        if shutil.disk_usage(ancestor).free < size(source)*2+2*1024**3:
            raise RuntimeError('insufficient disk for Phase 2 snapshot and reserve')
    if not phase2:
        if fixture:
            description = json.loads(Path(source,'fixture.json').read_text())
            if description.get('fixture_only') is not True:
                raise ValueError('not an explicitly labeled quality fixture')
            metadata = dict(record_count=description['records'],dataset_id=checksum(Path(source,'quality.jsonl')),
                files=[{'path':'quality.jsonl'}],expected_quality=description)
        else:
            metadata = manifest(source)
        resource_check(target,metadata['record_count'])
    target.mkdir(parents=True,exist_ok=False)
    if phase2:
        # Spark reads the original dataset root, respecting the streaming sink metadata log.
        df = spark.read.schema(EVENT_OUTPUT).parquet(str(source))
        if df.filter("schema_version IS NULL OR schema_version <> '1.0'").limit(1).count():
            raise ValueError('Phase 2 import requires schema version 1.0')
        provenance = {'origin':'application','raw_reference':str(source),'schema_version':'1.0'}
        checked = None
    else:
        paths = [str(Path(source)/f['path']) for f in metadata['files']]
        raw = spark.read.text(paths)
        checked = raw.withColumn('validation',F.udf(validate_record,RESULT)(
            'value',F.get_json_object('value','$.record_id'))).persist(StorageLevel.DISK_ONLY)
        df = checked.filter('validation.error IS NULL').select(F.from_json('validation.canonical',EVENT).alias('event')).select('event.*')
        provenance = {'origin':'synthetic-quality' if fixture else 'synthetic','raw_reference':str(source),'dataset_id':metadata['dataset_id'],'manifest':metadata}
    df = df.withColumn('event_time',F.col('event_time').cast('timestamp')).withColumn('event_date',F.to_date('event_time'))
    df = df.dropDuplicates(['record_id']).withColumn('dataset_origin',F.lit(provenance['origin']))
    if not phase2:
        df = df.withColumn('dataset_id',F.lit(metadata['dataset_id']))
    write_started = time.perf_counter()
    (df.repartition(partitions,'event_date').write.option('maxRecordsPerFile',250000)
        .partitionBy('event_date').parquet(str(target/'validated')))
    validation_write_seconds = time.perf_counter()-write_started
    if checked is not None:
        checked.filter('validation.error IS NOT NULL').select('value','validation.error').write.parquet(str(target/'quarantine'))
        checked.unpersist()
    stored = spark.read.parquet(str(target/'validated'))
    windows = aggregate(stored,'1 hour')
    anomalies(windows,threshold=threshold,history=24,minimum=3).write.parquet(str(target/'anomalies'))
    for name in ('daily','hourly','trends'):
        query(stored,name).write.parquet(str(target/'aggregated'/name))
    provenance.update(validated_count=stored.count(),validated_bytes=size(target/'validated'),
        output_bytes=size(target),partitions=partitions,threshold=threshold,
        build_seconds=time.perf_counter()-started,validation_write_seconds=validation_write_seconds,
        validated_output_bytes_per_second=size(target/'validated')/validation_write_seconds,
        schema=json.loads(stored.schema.json()))
    (target/'manifest.json').write_text(json.dumps(provenance,sort_keys=True,indent=2)+'\n')
    (target/'_COMPLETE').touch()
    return provenance


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--input',required=True)
    p.add_argument('--output',required=True)
    p.add_argument('--phase2',action='store_true')
    p.add_argument('--fixture',action='store_true')
    p.add_argument('--partitions',type=int,default=2)
    p.add_argument('--threshold',type=int,default=100)
    p.add_argument('--query',choices=list(QUERIES))
    p.add_argument('--start')
    p.add_argument('--end')
    args = p.parse_args()
    spark = SparkSession.builder.appName('EchoTrace-historical').config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions',args.partitions).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    try:
        if args.query:
            if not (Path(args.input)/'_COMPLETE').exists():
                raise ValueError('historical snapshot is incomplete')
            df = spark.read.parquet(str(Path(args.input)/'validated'))
            anomaly_rows = spark.read.parquet(str(Path(args.input)/'anomalies')) if args.query=='anomalies' else None
            query_started = time.perf_counter()
            result = query(df,args.query,args.start,args.end,anomaly_rows)
            result.write.json(args.output)
            execution = dict(query=args.query,start=args.start,end=args.end,
                query_and_write_seconds=time.perf_counter()-query_started,output_bytes=size(args.output),
                input_snapshot=str(args.input))
            Path(args.output,'_execution.json').write_text(json.dumps(execution,sort_keys=True)+'\n')
            Path(args.output,'_schema.json').write_text(result.schema.json()+'\n')
        else:
            result = build(spark,args.input,args.output,args.phase2,args.partitions,args.threshold,args.fixture)
            print(json.dumps({k:result[k] for k in ('validated_count','validated_bytes','output_bytes')}))
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
