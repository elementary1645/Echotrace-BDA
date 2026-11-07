"""Isolated Kafka/Spark/API verification and bounded streaming campaign.

python -m bda.reliability.run --output artifacts/phase5/run-UNIQUE [--campaign]
Never targets or recreates the application broker. Retains every run directory/volume.
"""
import argparse
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import threading
from uuid import uuid4

from bda.reliability.workload import acoustic, message, paced, producer, resource_guard, wait_until, percentiles
from bda.streaming.telemetry import atomic_json

REPO=Path(__file__).resolve().parents[2]


def application_broker_identity():
    observed=subprocess.run(['docker','inspect','echotrace-bda-kafka'],capture_output=True,text=True)
    return json.loads(observed.stdout)[0]['Id'] if observed.returncode==0 else None


class Run:
    def __init__(self,output,port):
        self.output=Path(output).resolve()
        self.project='echotrace-p5-'+uuid4().hex[:12]
        self.port=port;self.brokers=f'127.0.0.1:{port}'
        self.env={**os.environ,'P5_RUN_DIR':str(self.output),'P5_KAFKA_PORT':str(port),'P5_UID':str(os.getuid()),'P5_GID':str(os.getgid())}
        self.base=['docker','compose','-p',self.project,'-f',str(REPO/'infrastructure/reliability/compose.yml')]
        self.receipt={'project':self.project,'source':'SIMULATED','started_at':datetime.now(timezone.utc).isoformat(),'steps':[]}

    def command(self,args,label,timeout=300):
        before=time.monotonic()
        with (self.output/(label+'.log')).open('w') as out:
            p=subprocess.run(args,cwd=REPO,env=self.env,stdout=out,stderr=subprocess.STDOUT,timeout=timeout)
        self.receipt['steps'].append({'name':label,'exit_code':p.returncode,'seconds':time.monotonic()-before})
        self.save()
        if p.returncode: raise RuntimeError(f'{label} failed; inspect retained log')

    def compose(self,args,label,timeout=300): self.command(self.base+args,label,timeout)

    def spark_args(self,script,args):
        return self.base+['run','--rm','--no-deps','spark','--master','local[2]','--driver-memory','1g',
            '--conf','spark.jars.ivy=/run/ivy','--conf','spark.sql.shuffle.partitions=2',
            '--packages','org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1',script]+args

    def job(self,root,topic,label,stage='all'):
        self.command(self.spark_args('/workspace/bda/streaming/job.py',
            ['--root','/run/'+root,'--brokers','kafka:29092','--topic',topic,'--stage',stage,'--threshold','3']),label)

    def verify(self,root,expected,label,export=False):
        atomic_json(self.output/(label+'-expected.json'),expected)
        args=['--root','/run/'+root,'--expected','/run/'+label+'-expected.json','--output','/run/'+label+'.json']
        if export: args+=['--export','/run/streaming.sqlite']
        self.command(self.spark_args('/workspace/bda/reliability/verify.py',args),label)
        return json.loads((self.output/(label+'.json')).read_text())

    def save(self): atomic_json(self.output/'receipt.json',self.receipt)

    def new_topic(self,label):
        from confluent_kafka.admin import AdminClient,NewTopic
        topic='echotrace-p5-'+label+'-'+uuid4().hex[:12]
        admin=AdminClient({'bootstrap.servers':self.brokers})
        for future in admin.create_topics([NewTopic(topic,3,1)]).values(): future.result(30)
        return topic

    def raw_publish(self,topic,records):
        from confluent_kafka import Producer
        p=Producer({'bootstrap.servers':self.brokers,'acks':'all','enable.idempotence':True});errors=[]
        for key,value in records: p.produce(topic,key=key,value=value,on_delivery=lambda e,m: errors.append(str(e)) if e else None)
        assert p.flush(30)==0 and not errors

    def consume(self,topic,count):
        from confluent_kafka import Consumer,TopicPartition
        c=Consumer({'bootstrap.servers':self.brokers,'group.id':self.project+'-verifier','enable.auto.commit':False})
        try:
            c.assign([TopicPartition(topic,i,0) for i in range(3)])
            received=[];deadline=time.monotonic()+30
            while len(received)<count and time.monotonic()<deadline:
                m=c.poll(.2)
                if m and not m.error(): received.append(m.value())
            assert len(received)==count,(len(received),count)
            return received
        finally: c.close()

    def failures(self):
        # Only this run's broker is stopped/restarted; its volume is retained.
        from bda.schema import AnalyticsEvent
        topic=self.new_topic('recovery')
        w=producer(self.brokers,topic,'p5-recovery',self.project,timeout=10000,queue_size=8);w.start()
        try:
            assert w.submit(message('before-outage'),1)
            wait_until(lambda:w.snapshot().get('delivered',0)==1)
            self.compose(['stop','-t','15','kafka'],'isolated-broker-stop')
            assert w.submit(message('during-outage'),1)
            wait_until(lambda:w.snapshot().get('delivery_failed',0)>=1,20)
            failed=w.snapshot()
            self.compose(['up','-d','--no-recreate','--wait','kafka'],'isolated-broker-restart')
            assert w.submit(message('after-outage'),1)
            wait_until(lambda:w.snapshot().get('delivered',0)==2,30)
            recovered=w.snapshot()
        finally:
            w.close()
        observed=[AnalyticsEvent.model_validate_json(v).event_id for v in self.consume(topic,2)]
        assert set(observed)=={'before-outage','after-outage'}
        self.receipt['broker_outage']={'during':failed,'recovered':recovered,'after_shutdown':w.snapshot(),
            'observed_event_ids':observed,'expected_lost':'during-outage; delivery timeout, no durable outbox'}
        self.save()

    def end_to_end(self):
        from bda.schema import AnalyticsEvent
        from bda.event_adapter import adapt_event
        topic=self.new_topic('acoustic');self.receipt['e2e_topic']=topic
        source=acoustic(self.brokers,topic,self.project,self.output/'acoustic.json')
        fixture=producer(self.brokers,topic,'p5-fixture',self.project);fixture.start()
        fixture_messages=[]
        # Deliberate event-time disorder within the allowed horizon.
        for minute in [0,2,1,3,4]:
            count=[2,2,2,10,1][minute]
            for i in range(count):
                msg=message(f'fixture-{minute}-{i}',minute*60+i+1)
                fixture_messages.append(msg);assert fixture.submit(msg,1)
        fixture.close();assert fixture.snapshot().get('delivered')==17
        control=producer(self.brokers,topic,'p5-watermark-control',self.project);control.start()
        assert control.submit(message('advance',600,'CONTROL'),1);control.close()
        assert control.snapshot().get('delivered')==1
        self.raw_publish(topic,[(b'malformed',b'{')])
        received=self.consume(topic,28)
        valid=[];bad=0
        for value in received:
            try: valid.append(AnalyticsEvent.model_validate_json(value))
            except ValueError: bad+=1
        assert bad==1 and len(valid)==27 and {r.source for r in valid}=={'SIMULATED'}
        expected=dict(classified=28,quarantined=1,validated=26,window_events=25,detected_anomalies=2,check_acoustic=True)
        self.job('acoustic',topic,'e2e-first-job')
        first=self.verify('acoustic',expected,'e2e-first',True)
        # Restart with a genuinely new late identity plus a duplicate behind the watermark.
        late=producer(self.brokers,topic,'p5-fixture',self.project);late.start()
        assert late.submit(message('late-after-watermark',1),1)
        assert late.submit(fixture_messages[0],1)
        late.close();assert late.snapshot().get('delivered')==2
        self.job('acoustic',topic,'e2e-checkpoint-restart')
        expected['classified']=30
        second=self.verify('acoustic',expected,'e2e-restart')
        self.receipt['end_to_end']={'source_events':source['acoustic_events'],'kafka_records_initial':28,
            'raw_valid_initial':27,'initial':first,'checkpoint_restart':second,
            'enqueue_to_ack':source['enqueue_to_ack'],'new_offsets_after_restart':2}
        self.save()

    def campaign(self):
        topic=self.new_topic('rate100');self.receipt['campaign_topic']=topic
        root='campaign100'
        args=self.spark_args('/workspace/bda/streaming/job.py',['--root','/run/'+root,'--brokers','kafka:29092',
            '--topic',topic,'--stage','ingest','--continuous','--stop-file','/run/'+root+'/stop.request','--max-offsets','10000'])
        log=(self.output/'campaign-ingest.log').open('w')
        job=subprocess.Popen(args,cwd=REPO,env=self.env,stdout=log,stderr=subprocess.STDOUT)
        try:
            wait_until(lambda:(self.output/root/'telemetry/classified.json').exists() or job.poll() is not None,90)
            assert job.poll() is None,'continuous ingestion failed to start'
            result=paced(self.brokers,topic,self.project,rate=100,warmup=15,duration=60)
            # Drain discovered Kafka records; progress is actual Spark input, not producer counters.
            def drained():
                p=self.output/root/'progress.jsonl'
                if not p.exists(): return False
                return sum(json.loads(line).get('numInputRows',0) for line in p.read_text().splitlines())>=result['total_records']
            wait_until(drained,180)
            result['continuous_ingestion_drained_at']=datetime.now(timezone.utc).isoformat()
        finally:
            # The operator marker asks query.stop(), allowing sink/checkpoint commit before exit.
            (self.output/root/'stop.request').touch()
            try: job.wait(timeout=45)
            except subprocess.TimeoutExpired:
                # Retain evidence and report the failed graceful shutdown; do not hide it.
                job.terminate();job.wait(timeout=10)
                raise RuntimeError('continuous query did not acknowledge graceful stop marker')
            log.close()
        result['ingest_wrapper_exit_code']=job.returncode
        progress=[json.loads(line) for line in (self.output/root/'progress.jsonl').read_text().splitlines()]
        batches=[r for r in progress if r.get('numInputRows',0)>0]
        result['ingest_batches']=[{k:r.get(k) for k in ('batchId','numInputRows','inputRowsPerSecond','processedRowsPerSecond','durationMs')} for r in batches]
        # Advance event time only in this isolated topic; finalized data remains synthetic.
        w=producer(self.brokers,topic,'p5-watermark-control',self.project);w.start();assert w.submit(message('advance',600,'CONTROL'),1);w.close()
        self.job(root,topic,'campaign-drain')
        checked=self.verify(root,{'classified':result['total_records']+1,'quarantined':0,'validated':result['total_records']+1,
                                 'window_events':result['total_records']},'campaign-verified')
        result['validated_output']=checked
        result['consumer_group_lag']=None
        result['scope']='60-second paced producer plus concurrent ingest; downstream stages drained sequentially, not sustained concurrent full-pipeline throughput'
        self.receipt['campaign']=result;self.save()

    def monitor(self,stop):
        with (self.output/'resources.jsonl').open('a') as out:
            while not stop.is_set():
                try:
                    ids=subprocess.check_output(['docker','ps','-q','--filter',f'label=com.docker.compose.project={self.project}'],text=True,timeout=10).split()
                    if ids:
                        raw=subprocess.check_output(['docker','stats','--no-stream','--format','{{json .}}']+ids,text=True,timeout=10)
                        rows=[json.loads(line) for line in raw.splitlines()]
                        safe=[{k:r[k] for k in ('Name','CPUPerc','MemUsage','MemPerc')} for r in rows]
                        out.write(json.dumps({'observed_at':datetime.now(timezone.utc).isoformat(),'containers':safe})+'\n');out.flush()
                except (subprocess.SubprocessError,ValueError): pass
                stop.wait(5)

    def run(self,campaign=False):
        if not self.output.is_relative_to(REPO/'artifacts/phase5'): raise ValueError('output must be a new artifacts/phase5 child')
        self.output.mkdir(parents=True,exist_ok=False)
        with socket.socket() as s:
            s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
            s.bind(('127.0.0.1',self.port))
        self.receipt['preflight']=resource_guard(self.output)
        # Read-only identity evidence; no application service action is performed.
        self.receipt['application_broker_before']=application_broker_identity()
        self.save()
        self.command(['docker','image','inspect','echotrace-spark:4.0.1'],'spark-image-ready')
        # Copy cached dependencies from the existing Ivy volume read-only into this new run.
        cache=subprocess.run(['docker','volume','inspect','echotrace-bda_spark-ivy'],capture_output=True)
        if cache.returncode==0:
            self.command(['docker','run','--rm','--user',f'{os.getuid()}:{os.getgid()}','--entrypoint','python3','-v','echotrace-bda_spark-ivy:/cache:ro',
                '-v',str(self.output)+':/run','echotrace-spark:4.0.1','-c',
                "import shutil;shutil.copytree('/cache','/run/ivy')"],'copy-ivy-readonly')
        else:
            (self.output/'ivy').mkdir()
            self.receipt['connector_cache']='empty; first resolution requires Maven network access'
        unavailable=producer(self.brokers,'echotrace-p5-unavailable','p5-startup',self.project,timeout=1000)
        unavailable.start();assert unavailable.submit(message('broker-not-started'),1)
        wait_until(lambda:unavailable.snapshot().get('delivery_failed',0)>=1,20)
        unavailable.close();self.receipt['producer_before_broker']=unavailable.snapshot();self.save()
        monitor_stop=threading.Event()
        monitor=threading.Thread(target=self.monitor,args=(monitor_stop,),daemon=True);monitor.start()
        try:
            self.compose(['up','-d','--wait','kafka'],'isolated-start')
            self.failures();self.end_to_end()
            if campaign: self.campaign()
            self.receipt['status']='passed'
        except BaseException as e:
            self.receipt['status']='failed';self.receipt['failure_type']=type(e).__name__;raise
        finally:
            monitor_stop.set();monitor.join(timeout=15)
            self.receipt['application_broker_after']=application_broker_identity()
            assert self.receipt['application_broker_before']==self.receipt['application_broker_after']
            # Stop leftover one-off Spark containers only from this generated project.
            owned=subprocess.check_output(['docker','ps','-q','--filter',f'label=com.docker.compose.project={self.project}',
                '--filter','label=com.docker.compose.service=spark'],text=True).split()
            if owned:
                self.command(['docker','stop','-t','30']+owned,'owned-spark-shutdown',timeout=60)
            # Stop only this unique project; retain disposable volume and all evidence.
            self.compose(['stop','-t','30'],'isolated-stop')
            self.save()


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',required=True)
    p.add_argument('--port',type=int,default=19095);p.add_argument('--campaign',action='store_true')
    args=p.parse_args()
    if not 1024<=args.port<=65535 or args.port in (9092,19092): p.error('use a dedicated non-application port')
    sys.path.insert(0,str(REPO/'backend'))
    Run(args.output,args.port).run(args.campaign)


if __name__=='__main__':main()
