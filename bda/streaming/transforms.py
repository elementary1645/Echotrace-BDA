"""Explicit schemas and built-in analytical transformations."""
from pyspark.sql import functions as F, types as T, Window
from bda.streaming.validation import validate_record

EVENT = T.StructType([
    T.StructField(k, T.DoubleType() if k in {'t','t_start','t_end','confidence','angle_deg'} else T.StringType())
    for k in ('schema_version','record_type','record_id','source_id','session_id','event_id',
              'source','event_time','t','t_start','t_end','category','label','confidence',
              'zone','zone_confidence','angle_deg')])
RESULT = T.StructType([T.StructField('canonical', T.StringType()), T.StructField('error', T.StringType())])


def classify(df):
    # One Python boundary is intentional: identical Pydantic/Unicode identity semantics.
    checked = df.select('topic','partition','offset', F.col('timestamp').alias('kafka_time'),
                        F.col('key').cast('string').alias('key'),
                        F.col('value').cast('string').alias('raw'))
    return checked.withColumn('validation', F.udf(validate_record, RESULT)('raw','key')).select(
        '*', F.col('validation.error').isNull().alias('valid'),
        F.to_date('kafka_time').alias('ingest_date'))


def events(df):
    return df.filter('valid').select('topic','partition','offset','kafka_time',
        F.from_json('validation.canonical', EVENT).alias('event')).select(
        'topic','partition','offset','kafka_time','event.*').withColumn(
        'event_time', F.to_timestamp('event_time')).withColumn('event_date', F.to_date('event_time'))


def aggregate(df, duration='1 minute', watermark='2 minutes'):
    # Fine-grained rows can be summed for category/source/zone marginals; averages need count weighting.
    return (df.withWatermark('event_time', watermark)
        .groupBy(F.window('event_time', duration), 'source', 'source_id', 'category', 'zone')
        .agg(F.count('*').alias('event_count'), F.avg('confidence').alias('avg_confidence'))
        .withColumn('events_per_second', F.col('event_count') / F.lit(_seconds(duration))))


def _seconds(duration):
    amount, unit = duration.split()
    return float(amount) * {'second':1,'seconds':1,'minute':60,'minutes':60,'hour':3600,'hours':3600}[unit]


def anomalies(df, threshold=20, history=10, minimum=3, z_threshold=3.0):
    # First collapse zone rows, so a window has one baseline observation per category/source.
    counts = df.groupBy('window','source','source_id','category').agg(F.sum('event_count').alias('event_count'))
    prior = Window.partitionBy('source','source_id','category').orderBy('window').rowsBetween(-history, -1)
    return (counts.withColumn('history_count', F.count('*').over(prior))
        .withColumn('baseline_mean', F.avg('event_count').over(prior))
        .withColumn('baseline_stddev', F.stddev_samp('event_count').over(prior))
        .withColumn('z_score', F.when(F.col('baseline_stddev') > 0,
            (F.col('event_count') - F.col('baseline_mean')) / F.col('baseline_stddev')))
        .withColumn('threshold_exceeded', F.col('event_count') >= threshold)
        .withColumn('baseline_exceeded', (F.col('history_count') >= minimum) &
            (F.col('event_count') > F.col('baseline_mean')) &
            ((F.col('z_score') >= z_threshold) | (F.col('baseline_stddev') == 0)))
        .withColumn('is_anomaly', F.col('threshold_exceeded') | F.coalesce('baseline_exceeded', F.lit(False))))

CLASSIFIED = T.StructType([
    T.StructField('topic',T.StringType()), T.StructField('partition',T.IntegerType()),
    T.StructField('offset',T.LongType()), T.StructField('kafka_time',T.TimestampType()),
    T.StructField('key',T.StringType()), T.StructField('raw',T.StringType()),
    T.StructField('validation',RESULT), T.StructField('valid',T.BooleanType()),
    T.StructField('ingest_date',T.DateType())])
EVENT_OUTPUT = T.StructType(CLASSIFIED.fields[:4] + [
    T.StructField(f.name, T.TimestampType() if f.name == 'event_time' else f.dataType)
    for f in EVENT.fields] + [T.StructField('event_date',T.DateType())])
WINDOW_OUTPUT = T.StructType([
    T.StructField('window',T.StructType([T.StructField('start',T.TimestampType()),T.StructField('end',T.TimestampType())])),
    *[T.StructField(k,T.StringType()) for k in ('source','source_id','category','zone')],
    T.StructField('event_count',T.LongType()), T.StructField('avg_confidence',T.DoubleType()),
    T.StructField('events_per_second',T.DoubleType())])


def marginal(df, dimensions):
    """Combine finalized window cells without biasing confidence toward small groups."""
    return df.groupBy('window', *dimensions).agg(
        F.sum('event_count').alias('event_count'),
        (F.sum(F.col('avg_confidence') * F.col('event_count')) /
         F.sum('event_count')).alias('avg_confidence'),
        F.sum('events_per_second').alias('events_per_second'))
