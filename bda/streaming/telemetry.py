"""Small atomic telemetry snapshots. No Kafka administration or application imports."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import threading
import time


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path, value):
    path=Path(path)
    path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_name(path.name+f'.{os.getpid()}.{threading.get_ident()}.tmp')
    with temporary.open('w') as out:
        json.dump(value,out,allow_nan=False)
        out.flush()
        os.fsync(out.fileno())
    os.replace(temporary,path)


def offset_backlog(sources):
    """Discovered latest minus processed end offsets; NOT consumer-group lag."""
    total=0
    try:
        for source in sources:
            latest=source.get('latestOffset'); end=source.get('endOffset')
            if isinstance(latest,str): latest=json.loads(latest)
            if isinstance(end,str): end=json.loads(end)
            if not isinstance(latest,dict) or not isinstance(end,dict): return None
            for topic,parts in latest.items():
                for partition,offset in parts.items():
                    total+=max(0,int(offset)-int(end[topic][partition]))
        return total if sources else None
    except (ValueError,TypeError,KeyError,AttributeError):
        return None


class Telemetry:
    """Heartbeat means writer alive; progress timestamp separately measures data freshness."""
    def __init__(self,root,interval=5):
        self.root=Path(root)/'telemetry'; self.interval=interval
        self.lock=threading.RLock();self.records={};self.stop_event=threading.Event()
        self.thread=threading.Thread(target=self._heartbeat,daemon=True,name='spark-telemetry')
        self.thread.start()

    def _write(self,name):
        row=self.records[name]
        row['observed_at']=utc_now()
        atomic_json(self.root/(name+'.json'),row)

    def started(self,name,query_id):
        if name not in ('classified','events','windows'): return
        with self.lock:
            previous={}
            path=self.root/(name+'.json')
            try:
                if path.is_file() and path.stat().st_size<=65536:
                    candidate=json.loads(path.read_text())
                    if candidate.get('query_id')==str(query_id): previous=candidate
            except (OSError,ValueError,TypeError,AttributeError):
                pass
            self.records[name]=dict(schema_version=1,name=name,query_id=str(query_id),state='running',
                started_at=utc_now(),last_progress_at=previous.get('last_progress_at'),progress=previous.get('progress'),error=False)
            self._write(name)

    def progress(self,raw):
        name=raw.get('name')
        with self.lock:
            if name not in self.records: return
            sources=raw.get('sources',[])
            row=self.records[name]
            row.update(last_progress_at=utc_now(),progress={
                'batch_id':raw.get('batchId'),'num_input_rows':raw.get('numInputRows'),
                'input_rows_per_second':raw.get('inputRowsPerSecond'),
                'processed_rows_per_second':raw.get('processedRowsPerSecond'),
                'batch_duration_ms':raw.get('durationMs',{}).get('triggerExecution'),
                'watermark':raw.get('eventTime',{}).get('watermark'),
                'discovered_offset_backlog':offset_backlog(sources),
                'rows_dropped_by_watermark':sum(s.get('numRowsDroppedByWatermark',0) for s in raw.get('stateOperators',[])),
                'state_memory_bytes':sum(s.get('memoryUsedBytes',0) for s in raw.get('stateOperators',[]))})
            self._write(name)

    def terminated(self,query_id,error):
        with self.lock:
            for name,row in self.records.items():
                if row['query_id']==str(query_id):
                    row.update(state='error' if error else 'stopped',error=bool(error))
                    self._write(name)

    def _heartbeat(self):
        while not self.stop_event.wait(self.interval):
            with self.lock:
                for name in self.records:
                    self._write(name)

    def close(self):
        self.stop_event.set();self.thread.join(timeout=self.interval+1)
        with self.lock:
            for name,row in self.records.items():
                if row['state']=='running': row['state']='stopped'
                self._write(name)
