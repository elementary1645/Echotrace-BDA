"""Sequential available-now stages or continuous micro-batches; no driver row collection."""
import argparse
import json
import signal
import threading
from pathlib import Path
from pyspark.sql import SparkSession
from bda.streaming.transforms import classify, events, aggregate, anomalies, CLASSIFIED, EVENT_OUTPUT, WINDOW_OUTPUT


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--root', default='/data/analytics')
    p.add_argument('--brokers', default='kafka:29092')
    p.add_argument('--topic', default='echotrace.events.v1')
    p.add_argument('--stage', choices=['ingest','events','windows','anomalies','all'], default='all')
    p.add_argument('--continuous', action='store_true')
    p.add_argument('--stop-file',help='Operator stop marker within this root; graceful continuous-query shutdown')
    p.add_argument('--watermark', default='2 minutes')
    p.add_argument('--window', default='1 minute')
    p.add_argument('--parallelism', type=int, default=2)
    p.add_argument('--max-offsets', type=int, default=10000)
    p.add_argument('--threshold', type=int, default=20)
    p.add_argument('--history', type=int, default=10)
    p.add_argument('--minimum-history', type=int, default=3)
    p.add_argument('--z-threshold', type=float, default=3.0)
    p.add_argument('--snapshot-retention', type=int, default=3)
    args = p.parse_args()
    if args.continuous and args.stage == 'all':
        p.error('continuous mode requires a single stage; run separate stage services')
    if min(args.parallelism, args.max_offsets, args.history, args.minimum_history, args.snapshot_retention) < 1:
        p.error('parallelism, offsets, history and retention must be positive')
    root = Path(args.root)
    root.mkdir(parents=True, exist_ok=True)
    if args.stop_file and (not args.continuous or Path(args.stop_file).resolve().parent!=root.resolve()):
        p.error('stop-file requires continuous mode and a marker directly inside the query root')
    from bda.streaming.storage import claim
    locks = claim(root, args.stage, {k:getattr(args,k) for k in
        ('brokers','topic','watermark','window','parallelism')})
    spark = (SparkSession.builder.appName('EchoTrace-Phase2')
        .config('spark.sql.session.timeZone','UTC')
        .config('spark.sql.shuffle.partitions',args.parallelism).getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    from bda.streaming.metrics import ProgressLog
    listener = ProgressLog(root)
    spark.streams.addListener(listener)
    def stop(*_):
        for query in spark.streams.active:
            query.stop()
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    watcher_stop=threading.Event()
    def watch():
        while not watcher_stop.wait(.5):
            if args.stop_file and Path(args.stop_file).exists():
                stop();return
    watcher=threading.Thread(target=watch,daemon=True)
    if args.stop_file: watcher.start()

    def run(df, name, partitions=()):
        writer = (df.writeStream.format('parquet').outputMode('append')
            .option('path', str(root / name))
            .option('checkpointLocation', str(root / 'checkpoints' / name))
            .option('maxRecordsPerFile', 100000).queryName(name))
        if partitions:
            writer = writer.partitionBy(*partitions)
        query = (writer.trigger(processingTime='5 seconds') if args.continuous else
                 writer.trigger(availableNow=True)).start()
        try:
            query.awaitTermination()
        finally:
            # Bounded Spark progress history includes rates, offsets, watermark and state metrics.
            with (root / f'{name}-progress.jsonl').open('a') as out:
                for progress in query.recentProgress:
                    out.write(json.dumps(progress, default=str) + '\n')
            query.stop()

    def read(name):
        (root / name).mkdir(exist_ok=True)
        schema = {'classified':CLASSIFIED,'events':EVENT_OUTPUT}[name]
        return spark.readStream.schema(schema).parquet(str(root / name))
    try:
        if args.stage in ('all','ingest'):
            raw = (spark.readStream.format('kafka').option('kafka.bootstrap.servers',args.brokers)
                .option('subscribe',args.topic).option('startingOffsets','earliest')
                .option('failOnDataLoss','true').option('maxOffsetsPerTrigger',args.max_offsets).load())
            run(classify(raw), 'classified', ('valid','ingest_date'))
        if args.stage in ('all','events'):
            valid = events(read('classified')).withWatermark('event_time',args.watermark)
            run(valid.dropDuplicatesWithinWatermark(['record_id']), 'events', ('event_date',))
        if args.stage in ('all','windows'):
            run(aggregate(read('events'),args.window,args.watermark), 'windows')
        if args.stage in ('all','anomalies'):
            # Immutable snapshot: recomputation is atomic on the local shared filesystem.
            import os
            import uuid
            target = root / 'anomaly-snapshots' / str(uuid.uuid4())
            anomalies(spark.read.schema(WINDOW_OUTPUT).parquet(str(root / 'windows')), args.threshold, args.history, args.minimum_history, args.z_threshold).write.parquet(str(target))
            temporary = root / 'anomalies-current.tmp'
            temporary.write_text(str(target) + '\n')
            os.replace(temporary, root / 'anomalies-current')
            import shutil
            snapshots = sorted(target.parent.iterdir(), key=lambda path: path.stat().st_mtime, reverse=True)
            for expired in snapshots[args.snapshot_retention:]:
                shutil.rmtree(expired)
    finally:
        watcher_stop.set()
        if args.stop_file: watcher.join(timeout=2)
        stop()
        spark.streams.removeListener(listener)
        listener.telemetry.close()
        spark.stop()
        for handle in locks:
            handle.close()


if __name__ == '__main__':
    main()
