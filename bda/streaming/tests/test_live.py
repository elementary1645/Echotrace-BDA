"""Opt-in real Kafka-to-Parquet pipeline and source offset restart verification."""
import os
import subprocess
import sys
from uuid import uuid4
import pytest
pytestmark = [pytest.mark.integration,pytest.mark.skipif(os.getenv('RUN_SPARK_INTEGRATION') != '1',reason='set RUN_SPARK_INTEGRATION=1')]


def test_kafka_restart(tmp_path):
    from confluent_kafka.admin import AdminClient, NewTopic
    topic = 'echotrace-phase2-test-' + uuid4().hex
    broker = os.getenv('SPARK_TEST_BROKERS','kafka:29092')
    admin = AdminClient({'bootstrap.servers':broker})
    for future in admin.create_topics([NewTopic(topic,num_partitions=3,replication_factor=1)]).values():
        future.result(30)
    def command(args):
        subprocess.run(args,check=True,timeout=180)
    submit = ['/opt/spark/bin/spark-submit','--master','local[2]','--driver-memory','1g',
        '--conf','spark.jars.ivy=/opt/spark/.ivy2','--packages','org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1']
    root = str(tmp_path/'analytics')
    try:
        command([sys.executable,'-m','bda.streaming.demo','--brokers',broker,'--topic',topic])
        job = submit + ['/workspace/bda/streaming/job.py','--brokers',broker,'--topic',topic,'--root',root]
        command(job)
        command(submit + ['/workspace/bda/streaming/report.py','--root',root,'--synthetic'])
        # Advance Kafka offsets after the checkpoint; this valid arrival is older than its watermark.
        command([sys.executable,'-m','bda.streaming.demo','--brokers',broker,'--topic',topic,'--late-only'])
        command(job)
        command(submit + ['/workspace/bda/streaming/report.py','--root',root,'--synthetic','--expected-late'])
    finally:
        for future in admin.delete_topics([topic]).values():
            future.result(30)
