"""Container/local Spark tests; explicitly enabled to keep fast tests independent."""
import json
import os
from datetime import datetime
import pytest
pytestmark = pytest.mark.skipif(os.getenv('RUN_SPARK_TESTS') != '1', reason='set RUN_SPARK_TESTS=1')


@pytest.fixture(scope='module')
def spark():
    from pyspark.sql import SparkSession
    s = SparkSession.builder.master('local[2]').config('spark.sql.shuffle.partitions','2').config('spark.sql.session.timeZone','UTC').getOrCreate()
    s.sparkContext.setLogLevel('ERROR')
    yield s
    s.stop()


def test_transformations(spark):
    from bda.streaming.tests.test_validation import payload
    from bda.streaming.transforms import classify, events, aggregate, anomalies, marginal
    data = [payload(event_id=str(i), confidence=c,zone=z) for i,c,z in [(1,.6,'LEFT'),(2,1.,'LEFT'),(3,.5,'RIGHT')]]
    rows = [('topic',0,i,datetime(2026,1,1),d['record_id'].encode(),json.dumps(d).encode()) for i,d in enumerate(data)]
    rows.append(('topic',0,3,datetime(2026,1,1),b'x',b'{'))
    df = spark.createDataFrame(rows,'topic string, partition int, offset long, timestamp timestamp, key binary, value binary')
    checked = classify(df)
    assert checked.filter('not valid').count() == 1
    parsed = events(checked)
    assert parsed.count() == 3
    result = aggregate(parsed).orderBy('zone').collect()
    assert result[0].event_count == 2
    assert result[0].avg_confidence == pytest.approx(.8)
    assert result[0].window.start == datetime(2026,1,1)
    assert result[1].event_count == 1
    combined = marginal(aggregate(parsed), ['category']).first()
    assert combined.event_count == 3
    assert combined.avg_confidence == pytest.approx(.7)
    assert anomalies(aggregate(parsed), threshold=3).first().is_anomaly
    assert aggregate(parsed.limit(0)).count() == 0


def test_baseline(spark):
    from pyspark.sql import functions as F
    from bda.streaming.transforms import anomalies
    rows = [(datetime(2026,1,1,0,i),n,'SIMULATED','test','IMPACT') for i,n in enumerate([2,2,2,10])]
    df = spark.createDataFrame(rows,'event_time timestamp,event_count long,source string,source_id string,category string').withColumn('window',F.window('event_time','1 minute'))
    result = anomalies(df,threshold=100).orderBy('window').collect()
    assert not result[0].is_anomaly
    assert result[-1].is_anomaly
    assert result[-1].baseline_mean == 2


def test_state_restart(spark, tmp_path):
    from bda.streaming.tests.test_validation import payload
    from bda.streaming.transforms import EVENT
    from pyspark.sql import functions as F
    incoming = tmp_path/'input'
    incoming.mkdir()
    output = str(tmp_path/'out')
    checkpoint = str(tmp_path/'checkpoint')
    def put(name, entries):
        (incoming/name).write_text('\n'.join(json.dumps(payload(event_id=e,event_time=t)) for e,t in entries))
    def run():
        df = spark.readStream.schema(EVENT).json(str(incoming)).withColumn('event_time',F.to_timestamp('event_time'))
        q = df.withWatermark('event_time','2 minutes').dropDuplicatesWithinWatermark(['record_id']).writeStream.format('parquet').option('path',output).option('checkpointLocation',checkpoint).trigger(availableNow=True).start()
        q.awaitTermination()
        assert q.exception() is None
    put('a.json',[('a','2026-01-01T00:00:10Z'),('a','2026-01-01T00:00:10Z'),('b','2026-01-01T00:00:05Z')])
    run()
    assert spark.read.parquet(output).count() == 2
    put('b.json',[('a','2026-01-01T00:00:10Z'),('future','2026-01-01T00:10:00Z')])
    run()
    put('c.json',[('late','2026-01-01T00:00:00Z')])
    run()
    assert {r.event_id for r in spark.read.parquet(output).select('event_id').collect()} == {'a','b','future'}


def test_window_finality_and_boundaries(spark, tmp_path):
    from bda.streaming.tests.test_validation import payload
    from bda.streaming.transforms import EVENT, aggregate
    from pyspark.sql import functions as F
    incoming = tmp_path/'windows-input'
    incoming.mkdir()
    output = str(tmp_path/'windows-out')
    checkpoint = str(tmp_path/'windows-checkpoint')
    def run():
        df = spark.readStream.schema(EVENT).json(str(incoming)).withColumn('event_time',F.to_timestamp('event_time'))
        q = aggregate(df).writeStream.format('parquet').option('path',output).option('checkpointLocation',checkpoint).trigger(availableNow=True).start()
        q.awaitTermination()
        assert q.exception() is None
    def put(name, times):
        (incoming/name).write_text('\n'.join(json.dumps(payload(event_id=name+str(i),event_time=t)) for i,t in enumerate(times)))
    put('a.json',['2026-01-01T00:00:59Z','2026-01-01T00:01:00Z','2026-01-01T00:00:50Z'])
    run()
    # No window finalized yet; a future batch advances event-time watermark.
    put('b.json',['2026-01-01T00:10:00Z'])
    run()
    result = spark.read.parquet(output).orderBy('window').collect()
    assert [r.event_count for r in result] == [2,1]
    assert result[0].window.end == result[1].window.start == datetime(2026,1,1,0,1)
    put('c.json',['2026-01-01T00:00:10Z'])
    run()
    assert spark.read.parquet(output).count() == 2
