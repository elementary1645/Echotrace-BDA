"""Versioned read-only analytics APIs. No Spark runtime or arbitrary paths in requests."""
from contextlib import contextmanager
from datetime import datetime, timezone
from functools import lru_cache
import json
import math
import os
from pathlib import Path
import sqlite3
import time
from typing import Annotated, Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field, model_validator

from echotrace import config

REPORTS = config.REPO_DIR / 'bda/reports/phase3'
DATABASE = config.REPO_DIR / 'bda/reports/phase4/history.sqlite'

def history_path():
    return Path(os.environ.get('BDA_HISTORY_DATABASE',str(DATABASE))).resolve()

def streaming_path():
    return Path(os.environ.get('BDA_STREAMING_DATABASE',str(config.REPO_DIR/'artifacts/phase3/streaming.sqlite'))).resolve()


class Filters(BaseModel):
    start: datetime | None = None
    end: datetime | None = None
    source_id: str | None = Field(None, max_length=80, pattern=r'^[\w.-]+$')
    category: str | None = Field(None, max_length=40, pattern=r'^[A-Z_]+$')

    @model_validator(mode='after')
    def valid_range(self):
        for value in (self.start, self.end):
            if value and (value.utcoffset() is None or value.astimezone(timezone.utc).minute or value.second or value.microsecond):
                raise ValueError('use timezone-aware whole-hour boundaries')
        if self.start and self.end:
            if not 0 < (self.end-self.start).total_seconds() <= 366*86400:
                raise ValueError('end must follow start by at most 366 days')
        return self


class Provenance(BaseModel):
    dataset_id: str | None = Field(None, pattern=r'^[a-f0-9]{64}$')
    origin: str
    source: str
    engine: str
    exported_at: datetime
    granularity: str
    event_count: int = Field(ge=0)
    threshold: int | None
    schema_version: Literal[1]

    @model_validator(mode='after')
    def valid_provenance(self):
        if self.exported_at.utcoffset() is None:
            raise ValueError('export time must include timezone')
        if self.origin == 'synthetic' and (self.source != 'SIMULATED' or not self.dataset_id):
            raise ValueError('synthetic provenance must identify its simulated dataset')
        return self


class Envelope(BaseModel):
    schema_version: Literal['1'] = '1'
    provenance: Provenance
    served_at: datetime
    stale: bool
    total: int
    offset: int = 0
    limit: int = 200
    rows: list[dict]


class BenchmarkTrial(BaseModel):
    engine: Literal['python','spark']
    experiment: str
    record_count: int = Field(gt=0)
    duration_seconds: float = Field(gt=0,allow_inf_nan=False)
    records_per_second: float = Field(gt=0,allow_inf_nan=False)
    master: str
    input_partitions: int
    shuffle_partitions: int
    cpu_seconds: float = Field(ge=0,allow_inf_nan=False)
    sampled_peak_container_bytes: int | None
    warmup: bool
    run_id: str


class BenchmarkResponse(BaseModel):
    schema_version: Literal['1']
    origin: str
    artifact: str
    rows: list[BenchmarkTrial]
    environments: dict[str,dict]
    storage: list[dict]
    methodology: str


class HealthResponse(BaseModel):
    schema_version: Literal['1']
    observed_at: datetime
    acoustic: Literal['running','stopped']
    source: Literal['LIVE','RECORDED','SIMULATED']
    service_error: bool
    dropped_blocks: int
    kafka: dict
    spark: dict
    historical_export: Literal['available','unavailable']
    analytical_output: dict


@contextmanager
def database(path=None):
    path = path or history_path()
    if not path.is_file():
        raise HTTPException(503, 'Historical export unavailable; run the documented offline export')
    db = None
    try:
        db = sqlite3.connect(f'{path.as_uri()}?mode=ro&immutable=1', uri=True)
        db.row_factory = sqlite3.Row
        deadline = time.monotonic() + 2
        db.set_progress_handler(lambda: int(time.monotonic() > deadline), 10000)
        yield db
    except (sqlite3.Error, ValueError, KeyError, TypeError):
        raise HTTPException(503, 'Analytical export is invalid; regenerate it offline') from None
    finally:
        if db:
            db.close()


def metadata(db):
    try:
        return Provenance.model_validate({r['key']: json.loads(r['value']) for r in db.execute('SELECT * FROM metadata')})
    except Exception:
        raise HTTPException(503, 'Analytical metadata is invalid') from None


def selection(f):
    clauses, values = [], []
    for key, op, value in [('hour','>=', f.start), ('hour','<',f.end),
                           ('source_id','=',f.source_id), ('category','=',f.category)]:
        if value is not None:
            clauses.append(f'{key} {op} ?')
            values.append(value.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:00:00Z') if isinstance(value,datetime) else value)
    return (' WHERE ' + ' AND '.join(clauses) if clauses else ''), values


def envelope(db, rows, total=None, offset=0, limit=200):
    meta = metadata(db)
    now = datetime.now(timezone.utc)
    return Envelope(provenance=meta, served_at=now,
                    stale=(now-meta.exported_at).total_seconds() > 86400,
                    rows=rows,total=len(rows) if total is None else total,offset=offset,limit=limit)


def router(engine):
    api = APIRouter(prefix='/api/v1/analytics', tags=['analytics'])

    @api.get('/overview', response_model=Envelope)
    def overview():
        with database() as db:
            row = dict(db.execute('''SELECT coalesce(sum(event_count),0) AS event_count,
                count(DISTINCT source_id) AS sources,min(hour) AS start,max(hour) AS last_hour,
                sum(confidence_sum)/sum(event_count) AS avg_confidence FROM cells''').fetchone())
            row['anomalies'] = db.execute('SELECT count(*) FROM anomalies').fetchone()[0]
            row['categories'] = [dict(r) for r in db.execute('SELECT category AS label,sum(event_count) AS value FROM cells GROUP BY category ORDER BY value DESC')]
            return envelope(db,[row])

    @api.get('/catalog', response_model=Envelope)
    def catalog():
        with database() as db:
            return envelope(db,[{'sources':[r[0] for r in db.execute('SELECT DISTINCT source_id FROM cells ORDER BY source_id LIMIT 100')],
                                 'categories':[r[0] for r in db.execute('SELECT DISTINCT category FROM cells ORDER BY category LIMIT 50')]}])

    @api.get('/historical', response_model=Envelope)
    def historical(filters: Annotated[Filters, Query()], dimension: Literal['hour','day','category','source_id','zone','confidence_bin']='hour',
                   offset: int = Query(0,ge=0,le=100000),limit: int = Query(200,ge=1,le=500)):
        # Only allowlisted identifiers enter SQL; values are always bound parameters.
        group = "substr(hour,1,10)" if dimension == 'day' else dimension
        where, values = selection(filters)
        with database() as db:
            total = db.execute(f'SELECT count(*) FROM (SELECT {group} FROM cells{where} GROUP BY {group})',values).fetchone()[0]
            rows = [dict(r) for r in db.execute(f'''SELECT {group} AS label,sum(event_count) AS value,
                sum(confidence_sum)/sum(event_count) AS avg_confidence FROM cells{where}
                GROUP BY {group} ORDER BY {group} LIMIT ? OFFSET ?''',values+[limit,offset])]
            return envelope(db,rows,total,offset,limit)

    @api.get('/anomalies', response_model=Envelope)
    def anomalies(filters: Annotated[Filters, Query()],offset: int = Query(0,ge=0,le=100000), limit: int = Query(50,ge=1,le=200)):
        where, values = selection(filters)
        with database() as db:
            total = db.execute('SELECT count(*) FROM anomalies'+where,values).fetchone()[0]
            rows = [dict(r) for r in db.execute('SELECT * FROM anomalies'+where+' ORDER BY hour DESC,source_id,category LIMIT ? OFFSET ?', values+[limit,offset])]
            meta = metadata(db)
            threshold = meta.threshold
            for row in rows:
                row['explanation'] = '; '.join(filter(None,[(f'count >= {threshold}' if threshold is not None else 'Spark absolute-threshold flag set; threshold not recorded') if row['threshold_exceeded'] else '',
                    ('Spark baseline-exceeded flag set; policy not recorded' if meta.origin=='streaming-finalized' else 'increase above prior observed-window baseline; z >= 3 or constant baseline') if row['baseline_exceeded'] else '']))
                row['kind'] = 'detected'
            return envelope(db,rows,total,offset,limit)

    @api.get('/ground-truth')
    def ground_truth():
        try:
            manifest = json.loads((REPORTS/'datasets.json').read_text())['raw-100k']
            with database() as db:
                selected=metadata(db).dataset_id
            if selected!=manifest['dataset_id']:
                return {'schema_version':'1','origin':'Ground truth unavailable for selected snapshot','source':'unavailable','dataset_id':selected,'rows':[]}
            rows = []
            for key, value in manifest['injected_anomalies'].items():
                hour, source_id, category = key.split('|')
                rows.append(dict(hour=hour,source_id=source_id,category=category,injected_events=value))
            return {'schema_version':'1','origin':'generator manifest; injected ground truth, not detected anomalies',
                    'source':'SIMULATED','dataset_id':manifest['dataset_id'],'rows':rows[:200]}
        except (OSError,ValueError,KeyError,TypeError):
            raise HTTPException(503,'Ground-truth manifest unavailable') from None

    @api.get('/streaming', response_model=Envelope)
    def streaming():
        # Fixed offline export location; query status remains unknown even when present.
        with database(streaming_path()) as db:
            rows = [dict(r) for r in db.execute("""SELECT hour,source,source_id,category,zone,
                sum(event_count) AS event_count,sum(confidence_sum)/sum(event_count) AS avg_confidence
                FROM cells GROUP BY hour,source,source_id,category,zone
                ORDER BY hour DESC,source,source_id,category,zone LIMIT 200""")]
            return envelope(db, rows, limit=200)

    @api.get('/benchmarks', response_model=BenchmarkResponse)
    def benchmarks():
        return benchmark_report()

    @api.get('/health', response_model=HealthResponse)
    def health():
        producer = engine.analytics.snapshot() if engine.analytics is not None else None
        status = engine.status_data()
        kafka = producer.get('connectivity','unavailable') if producer else 'unavailable'
        from bda.analytics.observability import read_telemetry
        spark = read_telemetry()
        def output_status(path):
            try:
                with database(path) as db:
                    meta=metadata(db)
                    return {'state':'available','exported_at':meta.exported_at,
                            'stale':(datetime.now(timezone.utc)-meta.exported_at).total_seconds()>86400}
            except HTTPException:
                return {'state':'unavailable','exported_at':None,'stale':None}
        historical=output_status(history_path())
        streaming=output_status(streaming_path())
        # Never expose mode paths, device details, or raw error messages.
        return {'schema_version':'1','observed_at':datetime.now(timezone.utc),
                'acoustic': 'running' if status.get('running') else 'stopped',
                'source':status['source'],'service_error':bool(status.get('error')),
                'dropped_blocks':status['dropped_blocks'],'kafka':{'state':kafka,'metrics':producer},
                'spark':spark,'analytical_output':{'historical':historical,'streaming':streaming},
                'historical_export':historical['state']}

    return api


@lru_cache(maxsize=1)
def benchmark_report():
    try:
        trials_path = REPORTS/'trials.json'
        if trials_path.stat().st_size > 2_000_000:
            raise ValueError()
        raw = json.loads(trials_path.read_text())
        if not isinstance(raw,list) or len(raw)>500:
            raise ValueError()
        keys = ['engine','experiment','record_count','duration_seconds','records_per_second','master',
                'input_partitions','shuffle_partitions','cpu_seconds','sampled_peak_container_bytes','warmup','run_id']
        rows = []
        for r in raw:
            if not isinstance(r,dict) or r.get('correct') is not True or r.get('status') != 'success':
                raise ValueError()
            for k in ('duration_seconds','records_per_second','record_count'):
                if not isinstance(r[k],(int,float)) or not math.isfinite(r[k]) or r[k]<=0:
                    raise ValueError()
            rows.append(BenchmarkTrial.model_validate({k:r.get(k) for k in keys}).model_dump())
        env = json.loads((REPORTS/'environments.json').read_text())
        safe_env = {key:{k:v for k,v in row.items() if k in ('container','cpu_count','cpu_limit','memory_limit_bytes','python_version','java_version','spark_version','master','setup_seconds')} for key,row in env.items()}
        storage = json.loads((REPORTS/'storage.json').read_text())
        datasets = json.loads((REPORTS/'datasets.json').read_text())
        raw_sizes = {r['dataset_id']:r['total_bytes'] for r in datasets.values()}
        return {'schema_version':'1','origin':'recorded Phase 3 synthetic benchmark trials',
                'artifact':'bda/reports/phase3/trials.json','rows':rows,'environments':safe_env,
                'storage':[{**v,'label':k,'raw_bytes':raw_sizes.get(v['dataset_id'])} for k,v in storage.items()],
                'methodology':'Three measured trials per engine/configuration; warmup separate. Setup excluded. Local Spark only; two CPU quota, 3 GiB RAM, 1 GiB heap. Uncontrolled OS cache. No verified multi-machine scaling.'}
    except (OSError,ValueError,KeyError,TypeError):
        raise HTTPException(503,'Benchmark artifacts missing or malformed') from None
