"""Bounded, allowlisted telemetry reader; no Spark imports or administrative calls."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from pydantic import BaseModel, ConfigDict, Field


class Progress(BaseModel):
    model_config=ConfigDict(extra='ignore',allow_inf_nan=False)
    batch_id: int | None=None
    num_input_rows: int | None=Field(None,ge=0)
    input_rows_per_second: float | None=Field(None,ge=0)
    processed_rows_per_second: float | None=Field(None,ge=0)
    batch_duration_ms: float | None=Field(None,ge=0)
    watermark: str | None=None
    discovered_offset_backlog: int | None=Field(None,ge=0)
    rows_dropped_by_watermark: int | None=Field(None,ge=0)
    state_memory_bytes: int | None=Field(None,ge=0)


def read_telemetry(root=None,now=None):
    now=now or datetime.now(timezone.utc)
    if root is None:
        configured=os.getenv('BDA_TELEMETRY_ROOT')
        root=Path(configured) if configured else None
    if root is None:
        return {'state':'unavailable','reason':'No telemetry root configured','queries':[], 'consumer_group_lag':None}
    root=Path(root)
    rows=[]
    for name in ('classified','events','windows'):
        path=root/'telemetry'/(name+'.json')
        if not path.is_file(): continue
        try:
            if path.stat().st_size>65536: raise ValueError('too large')
            raw=json.loads(path.read_text())
            if raw['schema_version']!=1 or raw['name']!=name or raw['state'] not in ('running','stopped','error'): raise ValueError()
            observed=datetime.fromisoformat(raw['observed_at'].replace('Z','+00:00'))
            if observed.utcoffset() is None or (observed-now).total_seconds()>60: raise ValueError()
            age=max(0,(now-observed).total_seconds())
            last=raw.get('last_progress_at')
            last_time=datetime.fromisoformat(last.replace('Z','+00:00')) if last else None
            if last_time and last_time.utcoffset() is None: raise ValueError()
            stale_progress=last_time is None or (now-last_time).total_seconds()>60
            progress=Progress.model_validate(raw.get('progress') or {}).model_dump()
            state='unknown; heartbeat stale' if age>15 and raw['state']=='running' else raw['state']
            rows.append(dict(name=name,state=state,heartbeat_stale=age>15,progress_stale=stale_progress,
                observed_at=observed.isoformat(),last_progress_at=last,
                checkpoint_present=(root/'checkpoints'/name).is_dir(),progress=progress))
        except (OSError,ValueError,KeyError,TypeError,AttributeError):
            rows.append(dict(name=name,state='unavailable; invalid telemetry'))
    state='unavailable' if not rows else ('error' if any(r['state']=='error' for r in rows) else
           'running' if any(r['state']=='running' for r in rows) else
           'stopped' if all(r['state']=='stopped' for r in rows) else 'unknown')
    output=None
    summary=root/'output-summary.json'
    try:
        if summary.is_file() and summary.stat().st_size<=65536:
            raw=json.loads(summary.read_text())
            keys=('classified','quarantined','validated','finalized_windows','window_events','detected_anomalies')
            if not all(isinstance(raw.get(k),int) and raw[k]>=0 for k in keys): raise ValueError()
            recorded=datetime.fromisoformat(raw['observed_at'].replace('Z','+00:00'))
            if recorded.utcoffset() is None: raise ValueError()
            output={k:raw[k] for k in keys}
            output.update(observed_at=recorded.isoformat(),origin='offline verified Parquet summary; not live counters')
    except (OSError,ValueError,KeyError,TypeError):
        output={'state':'unavailable; invalid output summary'}
    return {'state':state,'reason':'Recorded query telemetry; inspect heartbeat and progress freshness separately',
            'queries':rows,'verified_output':output,'consumer_group_lag':None,
            'lag_definition':'Discovered latest minus processed end offsets when Spark supplies both; consumer-group lag unavailable'}
