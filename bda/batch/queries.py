"""Historical SQL query interface. All time is UTC stream-position wall time."""
from pyspark.sql import functions as F

QUERIES = {
 'total': 'SELECT count(*) AS event_count FROM history',
 'hourly': "SELECT date_trunc('hour',event_time) AS hour, count(*) AS event_count FROM history GROUP BY 1",
 'daily': 'SELECT event_date, count(*) AS event_count FROM history GROUP BY 1',
 'categories': 'SELECT category, count(*) AS event_count, avg(confidence) AS avg_confidence FROM history GROUP BY category ORDER BY event_count DESC,category',
 'zones': 'SELECT zone, count(*) AS event_count FROM history GROUP BY zone',
 'sources': 'SELECT source_id, count(*) AS event_count, avg(confidence) AS avg_confidence FROM history GROUP BY source_id',
 'classification': 'SELECT source, count(*) AS event_count FROM history GROUP BY source',
 'peaks': "SELECT date_trunc('hour',event_time) AS hour, count(*) AS event_count FROM history GROUP BY 1 ORDER BY event_count DESC,hour LIMIT 24",
 'source_comparison': "SELECT source_id,count(*) AS event_count,count(DISTINCT event_date) AS active_days,count(*)/count(DISTINCT event_date) AS events_per_active_day FROM history GROUP BY source_id",
 'trends': 'SELECT event_date,category,count(*) AS event_count,avg(confidence) AS avg_confidence FROM history GROUP BY event_date,category',
 'rolling': """WITH hourly AS (
     SELECT source_id,category,date_trunc('hour',event_time) AS hour,count(*) AS event_count
     FROM history GROUP BY 1,2,3)
     SELECT *,count(*) OVER prior AS history_count,avg(event_count) OVER prior AS baseline_mean,
       stddev_samp(event_count) OVER prior AS baseline_stddev
     FROM hourly WINDOW prior AS (PARTITION BY source_id,category ORDER BY hour ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING)""",
 'anomalies': """SELECT event_date,source,category,count(*) AS evaluated_windows,
      sum(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomalous_windows
      FROM historical_anomalies GROUP BY event_date,source,category""",
}


def prepare(df, start=None, end=None):
    df = df.withColumn('event_time',F.col('event_time').cast('timestamp'))
    if start:
        df = df.filter(F.col('event_time') >= F.to_timestamp(F.lit(start)))
    if end:
        df = df.filter(F.col('event_time') < F.to_timestamp(F.lit(end)))
    return df.withColumn('event_date',F.to_date('event_time')).withColumn('zone',
        F.when(F.col('zone').isin('LEFT','CENTRE','RIGHT'),F.col('zone')).otherwise(F.lit('UNKNOWN')))


def query(df, name, start=None, end=None, anomalies=None):
    if name not in QUERIES:
        raise ValueError('unknown query: '+name)
    prepare(df,start,end).createOrReplaceTempView('history')
    if name == 'anomalies':
        if anomalies is None:
            raise ValueError('anomalies query needs finalized anomaly rows')
        selected = anomalies
        if start:
            selected = selected.filter(F.col('window.start') >= F.to_timestamp(F.lit(start)))
        if end:
            selected = selected.filter(F.col('window.start') < F.to_timestamp(F.lit(end)))
        selected.withColumn('event_date',F.to_date('window.start')).createOrReplaceTempView('historical_anomalies')
    return df.sparkSession.sql(QUERIES[name])


def benchmark_summary(df):
    prepared = prepare(df).withColumn('hour',F.date_format('event_time',"yyyy-MM-dd'T'HH':00:00Z'"))
    prepared.createOrReplaceTempView('benchmark_events')
    return df.sparkSession.sql("""SELECT
      CASE WHEN grouping(category)=0 THEN 'category' WHEN grouping(hour)=0 THEN 'hour' ELSE 'source' END AS dimension,
      coalesce(category,hour,source_id) AS key,count(*) AS event_count,
      CASE WHEN grouping(category)=0 THEN avg(confidence) ELSE NULL END AS avg_confidence
      FROM benchmark_events GROUP BY GROUPING SETS ((category),(hour),(source_id))""")
