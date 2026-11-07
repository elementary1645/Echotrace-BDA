"""Pinned Spark verifier and export, invoked only by isolated test orchestration."""
import argparse
import json
from pathlib import Path
from datetime import datetime,timezone
from pyspark.sql import SparkSession,functions as F
from bda.analytics.export import export
from bda.streaming.transforms import CLASSIFIED,EVENT_OUTPUT,WINDOW_OUTPUT
from bda.streaming.telemetry import atomic_json


def main():
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--expected',required=True)
    p.add_argument('--output',required=True);p.add_argument('--export');args=p.parse_args()
    spark=SparkSession.builder.appName('Phase5-verify').config('spark.sql.session.timeZone','UTC').getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    try:
        root=Path(args.root);expected=json.loads(Path(args.expected).read_text())
        classified=spark.read.schema(CLASSIFIED).parquet(str(root/'classified'))
        events=spark.read.schema(EVENT_OUTPUT).parquet(str(root/'events'))
        windows=spark.read.schema(WINDOW_OUTPUT).parquet(str(root/'windows'))
        pointer=Path((root/'anomalies-current').read_text().strip())
        anomalies=spark.read.parquet(str(pointer))
        result=dict(classified=classified.count(),quarantined=classified.filter('not valid').count(),
            validated=events.count(),source_counts={r.source:r['count'] for r in events.groupBy('source').count().collect()},
            source_id_counts={r.source_id:r['count'] for r in events.groupBy('source_id').count().collect()},
            finalized_windows=windows.count(),window_events=windows.agg(F.sum('event_count')).first()[0] or 0,
            detected_anomalies=anomalies.filter('is_anomaly').count(),
            acoustic_windows=[r.asDict(recursive=True) for r in windows.filter("source_id='p5-acoustic'").orderBy('category').limit(20).collect()],
            observed_at=datetime.now(timezone.utc).isoformat())
        # JSON-safe timestamps for bounded output evidence.
        result=json.loads(json.dumps(result,default=str))
        for key,value in expected.items():
            if key in result: assert result[key]==value,(key,result[key],value)
        if expected.get('check_acoustic'):
            actual={r['category']:r['event_count'] for r in result['acoustic_windows']}
            assert actual=={'ALARM':4,'GLASS':2,'IMPACT':2},actual
            alarm=anomalies.filter("source_id='p5-acoustic' AND category='ALARM'").first()
            assert alarm.is_anomaly and alarm.event_count==4 and alarm.threshold_exceeded
        atomic_json(root/'output-summary.json',result)
        atomic_json(args.output,result)
        if args.export: export(spark,root,args.export,streaming=True)
    finally:
        spark.stop()


if __name__=='__main__': main()
