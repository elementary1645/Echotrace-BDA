"""Bounded demonstration assertions and inspectable ingestion rejection counts."""
import argparse
import json
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from bda.streaming.transforms import CLASSIFIED, EVENT_OUTPUT, WINDOW_OUTPUT, marginal


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--root',required=True)
    p.add_argument('--synthetic',action='store_true')
    p.add_argument('--expected-late', action='store_true')
    p.add_argument('--session')
    p.add_argument('--manifest')
    args = p.parse_args()
    spark = SparkSession.builder.appName('EchoTrace-inspect').config('spark.sql.session.timeZone','UTC').getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    root = Path(args.root)
    try:
        classified = spark.read.schema(CLASSIFIED).parquet(str(root/'classified'))
        accepted = spark.read.schema(EVENT_OUTPUT).parquet(str(root/'events'))
        windows = spark.read.schema(WINDOW_OUTPUT).parquet(str(root/'windows'))
        print('INGESTION COUNTS')
        classified.groupBy('valid').count().show()
        classified.filter('not valid').select('topic','partition','offset','validation.error','raw').show(20,truncate=120)
        accepted.groupBy('source','source_id','category').count().show()
        windows.orderBy('window').show(20,truncate=False)
        for dimensions in (['category'], ['source_id'], ['source'], ['zone']):
            print('FINALIZED DISTRIBUTION: ' + ','.join(dimensions))
            marginal(windows, dimensions).orderBy('window').show(20,truncate=False)
        if args.synthetic:
            assert classified.count() == (21 if args.expected_late else 20)
            assert classified.filter('not valid').count() == 1
            assert accepted.count() == 18
            rows = windows.orderBy('window').limit(6).collect()
            assert [r.event_count for r in rows] == [2,2,2,10,1]
            assert all(abs(r.avg_confidence-.8) < 1e-9 for r in rows)
            current = Path((root/'anomalies-current').read_text().strip())
            anomalies = spark.read.parquet(str(current))
            assert anomalies.filter('is_anomaly').count() == 1
            assert anomalies.filter('is_anomaly').first().event_count == 10
        if args.session:
            assert accepted.filter(F.col('session_id') == args.session).count() == 8
            assert accepted.filter(F.col('session_id') == args.session).filter("source != 'SIMULATED'").count() == 0
        if args.manifest:
            manifest = json.loads(Path(args.manifest).read_text())
            matching = accepted.filter(F.col('session_id') == manifest['session']).filter("source_id = 'phase2-simulation'")
            assert matching.count() == len(manifest['records'])
            assert matching.filter("source != 'SIMULATED'").count() == 0
            result = windows.filter("source_id = 'phase2-simulation'").limit(100).collect()
            actual = {}
            for row in result:
                key = json.dumps([row.window.start.isoformat()+'+00:00',row.source,row.source_id,row.category,row.zone])
                actual[key] = row
            assert set(actual) == set(manifest['expected'])
            for key, expected in manifest['expected'].items():
                assert actual[key].event_count == expected['count']
                assert abs(actual[key].avg_confidence - expected['confidence_sum']/expected['count']) < 1e-9
        print('VERIFIED')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
