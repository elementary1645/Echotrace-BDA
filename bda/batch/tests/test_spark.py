import json
import os
from datetime import datetime
import pytest
pytestmark = pytest.mark.skipif(os.getenv('RUN_BATCH_SPARK_TESTS')!='1',reason='set RUN_BATCH_SPARK_TESTS=1')


@pytest.fixture(scope='module')
def spark():
    from pyspark.sql import SparkSession
    s = SparkSession.builder.master('local[2]').config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','2').getOrCreate()
    s.sparkContext.setLogLevel('ERROR')
    yield s
    s.stop()


@pytest.fixture
def events(spark):
    # Exact boundary fixture, unknown/null localization, mixed real classifications.
    return spark.createDataFrame([
        ('2026-01-01T00:00:00Z','A','s1','LIVE',.6,'LEFT'),
        ('2026-01-01T00:59:59Z','A','s1','LIVE',1.,None),
        ('2026-01-01T01:00:00Z','B','s2','RECORDED',.5,'unmapped'),
        ('2026-01-02T00:00:00Z','A','s2','SIMULATED',.9,'RIGHT')],
        'event_time string,category string,source_id string,source string,confidence double,zone string')


def test_all_queries(spark,events):
    from bda.batch.queries import query
    assert query(events,'total').first().event_count == 4
    assert query(events,'total',end='2026-01-02T00:00:00Z').first().event_count == 3
    assert sorted(r.event_count for r in query(events,'hourly').collect()) == [1,1,2]
    assert {str(r.event_date):r.event_count for r in query(events,'daily').collect()} == {'2026-01-01':3,'2026-01-02':1}
    categories = {r.category:r for r in query(events,'categories').collect()}
    assert query(events,'categories').first().category == 'A'
    assert categories['A'].event_count == 3
    assert categories['A'].avg_confidence == pytest.approx(2.5/3)
    assert {r.zone:r.event_count for r in query(events,'zones').collect()} == {'LEFT':1,'UNKNOWN':2,'RIGHT':1}
    assert {r.source_id:r.event_count for r in query(events,'sources').collect()} == {'s1':2,'s2':2}
    assert {r.source:r.event_count for r in query(events,'classification').collect()} == {'LIVE':2,'RECORDED':1,'SIMULATED':1}
    assert query(events,'peaks').first().event_count == 2
    comparisons = {r.source_id:r for r in query(events,'source_comparison').collect()}
    assert comparisons['s1'].events_per_active_day == 2
    assert comparisons['s2'].events_per_active_day == 1
    assert query(events,'trends').count() == 3
    assert all(r.history_count == 0 for r in query(events,'rolling').collect())
    from pyspark.sql import functions as F
    anomaly_rows = events.withColumn('window',F.window(F.to_timestamp('event_time'),'1 hour')).withColumn('is_anomaly',F.col('confidence')>.8)
    result = query(events,'anomalies',anomalies=anomaly_rows)
    assert sum(r.anomalous_windows for r in result.collect()) == 2
    for name in ('hourly','daily','categories','zones','sources','classification','peaks','source_comparison','trends','rolling'):
        assert query(events.limit(0),name).count() == 0
    assert query(events.limit(0),'total').first().event_count == 0
    assert query(events.limit(0),'anomalies',anomalies=anomaly_rows.limit(0)).count() == 0


def test_rolling_excludes_future(spark):
    from bda.batch.queries import query
    rows = [('2026-01-01T00:00:00Z','A','s','SIMULATED',.8,'LEFT')]*2 + [('2026-01-01T01:00:00Z','A','s','SIMULATED',.8,'LEFT')]*3 + [('2026-01-01T02:00:00Z','A','s','SIMULATED',.8,'LEFT')]*20
    df = spark.createDataFrame(rows,'event_time string,category string,source_id string,source string,confidence double,zone string')
    result = query(df,'rolling').orderBy('hour').collect()
    assert result[0].baseline_mean is None
    assert result[1].baseline_mean == 2
    assert result[2].baseline_mean == 2.5


def test_lake_baseline_roundtrip(spark,tmp_path):
    from bda.batch.generate import generate,Config
    from bda.batch.lake import build
    from bda.batch.baseline import summary,expected,assert_equivalent
    from bda.batch.queries import benchmark_summary,query
    source,target = tmp_path/'raw',tmp_path/'lake'
    metadata = generate(source,Config(count=100,sources=2,days=2,chunk=30,anomaly_every=10))
    result = build(spark,source,target,threshold=5)
    assert result['validated_count'] == 100
    assert (target/'_COMPLETE').exists()
    from pyspark.sql import functions as F
    bucket = next(iter(metadata['injected_anomalies'])).split('|')[0]
    detected = spark.read.parquet(str(target/'anomalies')).filter(
        (F.col('window.start')==F.to_timestamp(F.lit(bucket))) &
        (F.col('source_id')=='synthetic-node-00000') & (F.col('category')=='ALARM')).first()
    assert detected.is_anomaly and detected.event_count >= metadata['injected_count']
    stored = spark.read.parquet(str(target/'validated'))
    actual = sorted([r.asDict() for r in benchmark_summary(stored).collect()],key=lambda r:(r['dimension'],r['key']))
    assert_equivalent(actual,summary(source,metadata))
    assert_equivalent(actual,expected(metadata))
    assert {r.source for r in stored.select('source').distinct().collect()} == {'SIMULATED'}
    assert spark.read.parquet(str(target/'quarantine')).count() == 0
    assert stored.select('record_id').distinct().count() == 100
    assert {str(r.event_date):r.event_count for r in query(stored,'daily').collect()} == metadata['expected']['daily']


def test_quality_lake(spark,tmp_path):
    from bda.batch.fixtures import generate
    from bda.batch.lake import build
    from bda.batch.baseline import manifest
    source,target = tmp_path/'quality',tmp_path/'quality-lake'
    generate(source)
    with pytest.raises(FileNotFoundError):
        manifest(source)
    result = build(spark,source,target,fixture=True)
    assert result['origin']=='synthetic-quality'
    assert result['validated_count']==3
    assert spark.read.parquet(str(target/'quarantine')).count()==2
    assert spark.read.parquet(str(target/'validated')).filter("event_date = '2025-01-01'").count()==1


def test_dashboard_export(spark,tmp_path):
    import sqlite3
    from bda.analytics.export import export
    from bda.batch.generate import generate,Config
    from bda.batch.lake import build
    source,target = tmp_path/'raw-export',tmp_path/'lake-export'
    meta=generate(source,Config(count=100,sources=2,days=2,chunk=30,anomaly_every=10))
    build(spark,source,target,threshold=5)
    output=tmp_path/'history.sqlite'
    export(spark,target,output)
    db=sqlite3.connect(output)
    assert db.execute('SELECT sum(event_count) FROM cells').fetchone()[0]==100
    assert 'cells_category_cover' in {r[1] for r in db.execute("PRAGMA index_list('cells')")}
    assert dict(db.execute('SELECT category,sum(event_count) FROM cells GROUP BY category'))==meta['expected']['categories']
    assert db.execute('SELECT count(*) FROM anomalies').fetchone()[0]>0
    assert json.loads(db.execute("SELECT value FROM metadata WHERE key='source'").fetchone()[0])=='SIMULATED'
    assert db.execute('SELECT sum(confidence_sum)/sum(event_count) FROM cells').fetchone()[0]>0
    db.close()
    with pytest.raises(ValueError,match='already exists'):
        export(spark,target,output)
    with pytest.raises(ValueError,match='complete'):
        export(spark,tmp_path/'missing',tmp_path/'missing.sqlite')

    from bda.streaming.transforms import aggregate
    aggregate(spark.read.parquet(str(target/'validated')),'1 hour').write.parquet(str(target/'windows'))
    (target/'configuration.json').write_text('{}')
    (target/'anomalies-current').write_text(str(target/'anomalies'))
    live=tmp_path/'streaming.sqlite'
    export(spark,target,live,streaming=True)
    db=sqlite3.connect(live)
    assert db.execute('SELECT sum(event_count) FROM cells').fetchone()[0]==100
    assert 'cells_category_cover' in {r[1] for r in db.execute("PRAGMA index_list('cells')")}
    assert json.loads(db.execute("SELECT value FROM metadata WHERE key='threshold'").fetchone()[0]) is None
    db.close()
