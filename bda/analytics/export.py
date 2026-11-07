"""Offline Spark -> SQLite serving export. Never invoked by an HTTP request."""
import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def export(spark, root, output, streaming=False):
    from pyspark.sql import functions as F
    root, output = Path(root), Path(output)
    if not streaming and not (root / '_COMPLETE').is_file():
        raise ValueError('complete historical snapshot required')
    if output.exists():
        raise ValueError('export already exists; use a new output')
    manifest = json.loads((root / ('configuration.json' if streaming else 'manifest.json')).read_text())
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix('.partial')
    if temporary.exists():
        raise ValueError('partial export exists')
    db = sqlite3.connect(temporary)
    try:
        db.executescript('''CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE cells (hour TEXT, source TEXT, source_id TEXT, category TEXT, zone TEXT,
          confidence_bin INTEGER, event_count INTEGER, confidence_sum REAL);
        CREATE TABLE anomalies (hour TEXT, source TEXT, source_id TEXT, category TEXT,
          event_count INTEGER, history_count INTEGER, baseline_mean REAL, baseline_stddev REAL,
          z_score REAL, threshold_exceeded INTEGER, baseline_exceeded INTEGER);
        CREATE INDEX cells_filter ON cells(hour,source_id,category);
        CREATE INDEX anomalies_filter ON anomalies(hour,source_id,category);''')
        if streaming:
            df = spark.read.parquet(str(root / 'windows'))
            cells = df.select(F.date_format('window.start', "yyyy-MM-dd'T'HH:mm:ss'Z'").alias('hour'),
                'source','source_id','category','zone', F.lit(None).cast('integer').alias('confidence_bin'),
                'event_count',(F.col('avg_confidence')*F.col('event_count')).alias('confidence_sum'))
        else:
            df = spark.read.parquet(str(root / 'validated'))
            cells = (df.withColumn('hour', F.date_format('event_time', "yyyy-MM-dd'T'HH':00:00Z'"))
                .withColumn('confidence_bin', F.least(F.lit(9), F.floor(F.col('confidence') * 10)))
                .groupBy('hour','source','source_id','category','zone','confidence_bin')
                .agg(F.count('*').alias('event_count'), F.sum('confidence').alias('confidence_sum')))
        # Iterator bounds driver memory to a partition, never collects the event population.
        for row in cells.toLocalIterator():
            db.execute('INSERT INTO cells VALUES (?,?,?,?,?,?,?,?)', tuple(row))
        anomaly_path = root / 'anomalies'
        if streaming:
            anomaly_path = Path((root / 'anomalies-current').read_text().strip()).resolve()
            if root.resolve() not in anomaly_path.parents:
                raise ValueError('anomaly snapshot must belong to its configured root')
        flagged = (spark.read.parquet(str(anomaly_path)).filter('is_anomaly')
            .select(F.date_format('window.start', "yyyy-MM-dd'T'HH:mm:ss'Z'").alias('hour'),
                    'source','source_id','category','event_count','history_count','baseline_mean',
                    'baseline_stddev','z_score','threshold_exceeded','baseline_exceeded'))
        for row in flagged.toLocalIterator():
            db.execute('INSERT INTO anomalies VALUES (?,?,?,?,?,?,?,?,?,?,?)', tuple(row))
        total = db.execute('SELECT sum(event_count) FROM cells').fetchone()[0] or 0
        if not streaming and total != manifest['validated_count']:
            raise ValueError('export count differs from completed manifest')
        meta = dict(dataset_id=manifest.get('dataset_id'), origin='streaming-finalized' if streaming else manifest['origin'],
                    event_count=total, exported_at=datetime.now(timezone.utc).isoformat(),
                    threshold=None if streaming else manifest['threshold'], granularity='finalized windows' if streaming else 'hour', schema_version=1,
                    engine='Apache Spark 4.0.1', source='MIXED; inspect row source' if streaming else ('SIMULATED' if manifest['origin']=='synthetic' else 'application'))
        db.executemany('INSERT INTO metadata VALUES (?,?)', [(k,json.dumps(v)) for k,v in meta.items()])
        # Cover the dashboard's measured category and distinct-source scans.
        db.execute('CREATE INDEX cells_category_cover ON cells(category,event_count)')
        db.execute('CREATE INDEX cells_source_cover ON cells(source_id)')
        db.commit()
        db.close()
        temporary.rename(output)
    finally:
        db.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--input', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--streaming', action='store_true')
    args = p.parse_args()
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.appName('EchoTrace-dashboard-export')
             .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions',2).getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        export(spark,args.input,args.output,args.streaming)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
