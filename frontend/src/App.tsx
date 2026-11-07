import { lazy, Suspense, useEffect, useState } from 'react';
import { setMode } from './api';
import { useEchoTrace } from './useEchoTrace';
import { benchmarkGroups, countBy, download, read, type Envelope, type Row } from './analyticsData';
import './analytics.css';
const AcousticConsole = lazy(()=>import('./AcousticConsole'));
const sections = ['Overview','Live Analytics','Historical Analytics','Anomaly Intelligence','Performance Lab','System Health','Acoustic Monitor'];
const fmt = (n: number | null | undefined) => n == null ? '—' : new Intl.NumberFormat('en',{maximumFractionDigits:2}).format(n);

function Bars({rows, title, unit='events'}: {rows: Row[]; title: string; unit?: string}) {
  if (!rows.length) return <Empty text={`No ${title.toLowerCase()} available for this selection.`}/>;
  const max = Math.max(...rows.map(r=>Number(r.value)),1);
  return <div className="a-bars" role="img" aria-label={`${title}, ${unit}`}>
    {rows.map((r,i)=><div className="a-bar-row" key={`${r.label}-${i}`} title={`${r.label}: ${fmt(r.value)} ${unit}`}>
      <span>{r.label}</span><div className="a-track"><i style={{width:`${Math.max(0,Number(r.value)/max*100)}%`}}/></div><strong>{fmt(r.value)}</strong>
    </div>)}
  </div>;
}
function Trend({rows, title, unit='events'}: {rows: Row[]; title: string; unit?: string}) {
  if (!rows.length) return <Empty text="No time-series data in this selection."/>;
  const max = Math.max(...rows.map(r=>Number(r.value)),1);
  return <><svg className="a-trend" viewBox="0 0 800 190" role="img" aria-label={title}>
    <title>{title}: {rows.length} intervals; maximum {fmt(max)} {unit}</title>
    {[0,1,2,3].map(n=><g key={n}><line x1="45" y1={20+n*45} x2="790" y2={20+n*45}/><text x="0" y={24+n*45}>{fmt(unit==='confidence'?max*(1-n/3):Math.round(max*(1-n/3)))}</text></g>)}
    {rows.map((r,i)=><rect key={i} x={45+i*745/rows.length} y={155-r.value/max*135} width={Math.max(1,745/rows.length-1)} height={r.value/max*135}><title>{r.label}: {fmt(r.value)} {unit}</title></rect>)}
    <text x="45" y="182">{rows[0].label}</text><text x="790" y="182" textAnchor="end">{rows[rows.length-1].label}</text>
  </svg><details><summary>Accessible chart data · {rows.length} intervals</summary><Table rows={rows}/></details></>;
}
function Table({rows}: {rows: Row[]}) {
  if (!rows.length) return <Empty text="No records available."/>;
  const columns = Object.keys(rows[0]);
  return <div className="a-table"><table><thead><tr>{columns.map(c=><th key={c} scope="col">{c.replace(/_/g,' ')}</th>)}</tr></thead><tbody>{rows.map((r,i)=><tr key={i}>{columns.map(c=><td key={c}>{r[c] == null ? '—' : typeof r[c]==='number' ? fmt(r[c]) : String(r[c])}</td>)}</tr>)}</tbody></table></div>;
}
function Empty({text}: {text: string}) { return <div className="a-empty"><span>◇</span><p>{text}</p></div>; }
function Panel({title,subtitle,children}: {title:string;subtitle?:string;children:React.ReactNode}) { return <section className="a-panel"><header><h2>{title}</h2>{subtitle&&<p>{subtitle}</p>}</header>{children}</section>; }
function Provenance({data}: {data:Envelope}) { return <div className="a-provenance"><b>{data.provenance.source} · {data.provenance.origin}</b><span>{data.provenance.engine} · {data.provenance.granularity} aggregates</span><span>Exported {new Date(data.provenance.exported_at).toLocaleString()}</span><code>Dataset {data.provenance.dataset_id}</code>{data.stale&&<strong>Snapshot older than 24 hours · recorded history, not current activity</strong>}</div>; }

export default function App() {
  const [section,setSection] = useState(()=>{const n=Number(location.hash.slice(1));return Number.isInteger(n)&&n>=0&&n<sections.length?n:0;});
  useEffect(()=>{const update=()=>{const n=Number(location.hash.slice(1));if(Number.isInteger(n)&&n>=0&&n<sections.length)setSection(n);};window.addEventListener('hashchange',update);return()=>window.removeEventListener('hashchange',update);},[]);
  return <div className="analytics-shell"><aside className="a-sidebar"><a className="a-brand" href="#0"><span>◈</span><div>ECHOTRACE<small>ANALYTICS COMMAND CENTER</small></div></a><p className="a-nav-label">WORKSPACE / PHASE 04</p><nav aria-label="Workspace">{sections.map((name,i)=><a href={`#${i}`} key={name} aria-current={section===i?'page':undefined} onClick={()=>setSection(i)}><span>{['◫','⌁','▥','◇','↗','◎','◉'][i]}</span>{name}</a>)}</nav><div className="a-side-note"><b>Evidence, in every view.</b><p>Acoustic signals. Streaming intelligence. Reproducible analytics.</p><small>LOCAL DATA PLATFORM · v1</small></div></aside><div className="a-workspace"><div className="a-topbar"><span>EchoTrace BDA <span className="a-dim">/ {sections[section]}</span></span><span className="a-chip">PROJECT ANALYTICS</span></div>{section===6?<Suspense fallback={<p>Loading acoustic monitor…</p>}><AcousticConsole/></Suspense>:<Analytics section={section}/>}</div></div>;
}

function Analytics({section}: {section:number}) {
  const {state,connection,lastReceived} = useEchoTrace();
  const [clock,setClock]=useState(Date.now());
  useEffect(()=>{if(section!==1)return;const timer=window.setInterval(()=>setClock(Date.now()),5000);return()=>window.clearInterval(timer);},[section]);
  const [data,setData] = useState<Envelope|null>(null);
  const [extra,setExtra] = useState<Row>({});
  const [catalog,setCatalog] = useState<Row>({sources:[],categories:[]});
  const [loadedKey,setLoadedKey]=useState('');
  const [error,setError] = useState(''); const [loading,setLoading]=useState(true); const [revision,retry]=useState(0);
  const [start,setStart] = useState('2026-01-01'); const [end,setEnd]=useState('2026-01-08');
  const [source,setSource]=useState(''); const [category,setCategory]=useState(''); const [offset,setOffset]=useState(0);
  const [experiment,setExperiment]=useState('5m-local2-p4-s4'); const [daily,setDaily]=useState(false);
  const [demoMessage,setDemoMessage]=useState('');
  useEffect(()=>{const c=new AbortController();read<Envelope>('catalog',c.signal).then(r=>setCatalog(r.rows[0])).catch(()=>{});return()=>c.abort();},[]);
  const params = new URLSearchParams({start:`${start}T00:00:00Z`,end:`${end}T00:00:00Z`,...(source?{source_id:source}:{}),...(category?{category}:{} )});
  const query = params.toString();
  const loadKey=`${section}|${query}|${offset}|${revision}`;
  useEffect(()=>{
    const c=new AbortController();setLoading(true);setError('');setData(null);setExtra({});
    const run=async()=>{
      if(section===1){try{setExtra({spark:await read<Envelope>('streaming',c.signal)});}catch(e){if(c.signal.aborted)throw e;setExtra({sparkError:String(e)});}}
      else if(section===4){setExtra(await read<Row>('benchmarks',c.signal));}
      else if(section===5){setExtra(await read<Row>('health',c.signal));}
      else if(section===2){
        const dimensions=['hour','day','category','source_id','zone','confidence_bin'];
        const results=await Promise.all(dimensions.map(d=>read<Envelope>(`historical?${query}&dimension=${d}&limit=500`,c.signal)));
        const anomalies=await read<Envelope>(`anomalies?${query}&limit=1`,c.signal);
        setData(results[0]);setExtra({...Object.fromEntries(dimensions.map((d,i)=>[d,results[i]])),anomalies});
      }else if(section===3){const [anomalies,truth]=await Promise.all([read<Envelope>(`anomalies?${query}&offset=${offset}&limit=20`,c.signal),read<Row>('ground-truth',c.signal)]);setData(anomalies);setExtra({truth});}
      else if(section===0){
        const [overview,trend,anomalies,health]=await Promise.all([read<Envelope>('overview',c.signal),read<Envelope>('historical?dimension=day',c.signal),read<Envelope>('anomalies?limit=5',c.signal),read<Row>('health',c.signal)]);
        setData(overview);setExtra({trend,anomalies,health});
      }
    };
    run().catch(e=>{if(!c.signal.aborted)setError(e.message);}).finally(()=>{if(!c.signal.aborted){setLoadedKey(loadKey);setLoading(false);}});return()=>c.abort();
  },[section,query,offset,revision]);
  const events=state.events;
  const liveCategories=countBy(events,e=>e.category), liveSources=countBy(events,e=>e.source);
  const rolling=events.filter(e=>e.t_end>=state.now-60);
  const filters=<div className="a-filters"><label>From (UTC)<input type="date" value={start} onChange={e=>{setStart(e.target.value);setOffset(0);}}/></label><label>Until (exclusive, UTC)<input type="date" value={end} onChange={e=>{setEnd(e.target.value);setOffset(0);}}/></label><label>Source<select aria-label="Source" value={source} onChange={e=>{setSource(e.target.value);setOffset(0);}}><option value="">All sources</option>{catalog.sources.map((s:string)=><option key={s}>{s}</option>)}</select></label><label>Category<select aria-label="Category" value={category} onChange={e=>{setCategory(e.target.value);setOffset(0);}}><option value="">All categories</option>{catalog.categories.map((s:string)=><option key={s}>{s}</option>)}</select></label></div>;
  const overview=data?.rows[0];
  return <main className="a-content"><div className="a-heading"><div><p className="a-eyebrow">SIGNAL → INSIGHT</p><h1>{sections[section]}</h1><p>{['A clear view of your acoustic data platform.','Observe the current acoustic session as events arrive.','Explore a verified historical snapshot.','Understand flagged windows and their observed baselines.','Measured performance. Reproducible results.','Operational evidence from connected services.'][section]}</p></div><button onClick={()=>retry(r=>r+1)}>↻ Refresh</button></div>
    {section===1&&((lastReceived!==null&&clock-lastReceived>10000)||connection==='closed')&&<div className="a-notice" role="status">Live feed is stale or disconnected. Buffered events remain visible; automatic reconnect is enabled.</div>}
    {section===1?<div className="a-notice">WebSocket {connection} · {state.status?.source??'Source unknown'} · Browser session buffer, maximum 200 events. These counts are not Spark aggregates. Last signal: {state.status?.wall??'unavailable'}</div>:null}
    {(section===2||section===3)&&filters}
    {loading||loadedKey!==loadKey?<div className="a-skeleton" role="status">Loading analytical evidence…</div>:error?<div className="a-error" role="alert"><h2>Analytics unavailable</h2><p>{error}</p><button onClick={()=>retry(r=>r+1)}>Retry</button></div>:<>
    {data&&<Provenance data={data}/>}
    {section===0&&overview&&<><div className="a-kpis">{[['Analyzed events',fmt(overview.event_count),'Historical snapshot only'],['Monitoring sources',fmt(overview.sources),'Distinct historical source IDs'],['Detected anomalies',fmt(overview.anomalies),'Flagged historical windows'],['Mean confidence',fmt(overview.avg_confidence),'Count-weighted event confidence']].map(([label,value,note])=><div key={label}><small>{label}</small><strong>{value}</strong><span>{note}</span></div>)}</div><div className="a-grid"><Panel title="Activity over time" subtitle={`${overview.start} → ${overview.last_hour} (UTC, final hour included)`}><Trend rows={extra.trend?.rows??[]} title="Daily historical event counts"/></Panel><Panel title="Category distribution" subtitle="One historical population"><Bars rows={overview.categories} title="Category distribution"/></Panel></div><Panel title="Recent detected anomalies" subtitle="Historical snapshot · latest flagged windows"><Table rows={(extra.anomalies?.rows??[]).map((r:Row)=>({time:r.hour,source:r.source_id,category:r.category,events:r.event_count,z_score:r.z_score}))}/></Panel><Panel title="Pipeline health" subtitle="Current process observations · separate from the historical population"><Table rows={extra.health?[{acoustic:extra.health.acoustic,source:extra.health.source,kafka:extra.health.kafka.state,spark:extra.health.spark.state,observed_at:extra.health.observed_at}]:[]}/></Panel><div className="a-notice">Processing rate and current pipeline status are available in Live Analytics and System Health. Historical totals are never combined with live counts.</div></>}
    {section===1&&<><div className="a-kpis">{[['Buffered events',events.length],['Events in last 60 stream seconds',rolling.length],['Buffered rate / second',fmt(rolling.length/60)],['Connection',connection]].map(([k,v])=><div key={k}><small>{k}</small><strong>{v}</strong><span>{state.status?.source??'Unavailable'} · current browser session</span></div>)}</div><Panel title="Rolling event counts" subtitle="Six ten-second bins · current stream clock · buffered events only"><Trend rows={Array.from({length:6},(_,i)=>({label:`${Math.max(0,state.now-60+i*10).toFixed(0)}s`,value:events.filter(e=>e.t_end>state.now-60+i*10&&e.t_end<=state.now-50+i*10).length}))} title="Rolling buffered event counts"/></Panel><div className="a-grid"><Panel title="Category distribution"><Bars rows={liveCategories} title="Live categories"/></Panel><Panel title="Localization"><Bars rows={countBy(events,e=>e.zone)} title="Live localization"/></Panel><Panel title="Confidence trend" subtitle="Per buffered event; confidence is not anomaly severity"><Trend rows={events.map(e=>({label:`${e.t_end}s`,value:e.confidence}))} title="Event confidence (0–1)" unit="confidence"/></Panel><Panel title="Source activity" subtitle="WebSocket provides source mode, not a device source ID"><Bars rows={liveSources} title="Source activity"/></Panel></div><Panel title="Live event timeline" subtitle="Latest 50 buffered detections"><Table rows={[...events].reverse().slice(0,50).map(e=>({time:e.wall,category:e.category,confidence:e.confidence,zone:e.zone,origin:e.source}))}/></Panel><Panel title="Spark streaming aggregates">{extra.spark?<><Provenance data={extra.spark}/><Table rows={extra.spark.rows}/></>:<Empty text={extra.sparkError??'No Spark serving export connected. WebSocket detections do not establish Spark query health.'}/>}</Panel><button onClick={async()=>{try{await setMode('SIMULATED');setDemoMessage('SIMULATED pipeline started. Open Acoustic Monitor for scenario controls.');}catch(e){setDemoMessage(String(e));}}}>Start explicitly simulated demonstration</button><p role="status">{demoMessage}</p></>}
    {section===2&&extra.hour&&<><div className="a-actions"><button onClick={()=>setDaily(!daily)}>{daily?'Switch to hourly':'Switch to daily'}</button><button onClick={()=>download([{provenance:data?.provenance,filters:{start,end,source,category},results:Object.fromEntries(Object.entries(extra).map(([k,v])=>[k,v.rows]))}],'echotrace-historical.json')}>Export filtered results</button><span>{fmt(extra.day.rows.reduce((s:number,r:Row)=>s+r.value,0))} events in selection · {extra.anomalies.total} detected anomaly windows</span></div><Panel title={daily?'Daily trend':'Hourly trend'} subtitle="Whole-hour filters · UTC · source and category filters applied to every chart"><Trend rows={extra[daily?'day':'hour'].rows} title="Historical counts"/>{extra.hour.total>500&&<p>Showing first 500 hourly intervals. Narrow the date range; daily chart covers the selection.</p>}</Panel><div className="a-grid">{[['category','Category mix'],['source_id','Source comparison'],['zone','Localization'],['confidence_bin','Confidence distribution']].map(([d,t])=><Panel key={d} title={t} subtitle={d==='confidence_bin'?'Bins 0–9 represent [0.0,0.1) through [0.9,1.0]':undefined}><Bars rows={extra[d].rows} title={t}/></Panel>)}</div><Panel title="Peak activity periods"><Table rows={[...extra.hour.rows].sort((a,b)=>b.value-a.value).slice(0,5)}/></Panel></>}
    {section===3&&data&&<><div className="a-notice">{data.total} detected windows in selection. Threshold: {data.provenance.threshold}. Baselines use prior observed windows; empty windows are not materialized. Injected ground truth is not a detected anomaly.</div><Panel title="Detected anomaly timeline"><Table rows={data.rows}/></Panel>{extra.truth&&<Panel title="Injected ground truth" subtitle="Separate generator manifest · full snapshot, independent of the detection filters"><Table rows={extra.truth.rows}/><p className="a-dim">{extra.truth.origin} · {extra.truth.source} · {extra.truth.dataset_id}</p></Panel>}<div className="a-actions"><button disabled={offset===0} onClick={()=>setOffset(Math.max(0,offset-20))}>Previous</button><span>{data.total?offset+1:0}–{Math.min(offset+20,data.total)} of {data.total}</span><button disabled={offset+20>=data.total} onClick={()=>setOffset(offset+20)}>Next</button><button onClick={()=>download([{provenance:data.provenance,rows:data.rows}],'echotrace-anomalies-page.json')}>Export page</button></div></>}
    {section===4&&extra.rows&&<><div className="a-notice">{extra.origin} · {extra.artifact}<p>{extra.methodology}</p></div><label className="a-config">Configuration<select value={experiment} onChange={e=>setExperiment(e.target.value)}>{Array.from(new Set<string>(extra.rows.map((r:Row)=>r.experiment))).map(s=><option key={s}>{s}</option>)}</select></label><div className="a-grid">{[['duration_seconds','Median execution time','seconds'],['records_per_second','Median throughput','events / second'],['cpu_seconds','Measured CPU consumption','CPU seconds'],['sampled_peak_container_bytes','Sampled container peak memory','bytes']].map(([metric,title,unit])=><Panel key={metric} title={title} subtitle="Measured trials only · warmups excluded"><Bars rows={benchmarkGroups(extra.rows,experiment,metric)} title={title} unit={unit}/></Panel>)}</div><Panel title="Dataset size and Spark configuration comparisons" subtitle="Each bar uses recorded measured-trial medians; configurations remain separate"><Bars rows={Array.from(new Set<string>(extra.rows.map((r:Row)=>r.experiment))).flatMap(exp=>benchmarkGroups(extra.rows,exp,'duration_seconds').map(r=>({...r,label:`${exp} / ${r.label}`})))} title="Size and configuration versus execution time" unit="seconds"/></Panel><Panel title="Dataset size versus throughput"><Bars rows={Array.from(new Set<string>(extra.rows.map((r:Row)=>r.experiment))).flatMap(exp=>benchmarkGroups(extra.rows,exp,'records_per_second').map(r=>({...r,label:`${exp} / ${r.label}`})))} title="Size versus throughput" unit="events/s"/></Panel><Panel title="Parquet storage efficiency" subtitle="No five-million-event Parquet snapshot is claimed"><Table rows={extra.storage.map((r:Row)=>({snapshot:r.label,raw_bytes:r.raw_bytes,validated_parquet_bytes:r.validated_bytes,ratio:r.validated_bytes/r.raw_bytes}))}/></Panel><Panel title="Benchmark environment"><Table rows={[{experiment,...extra.environments[experiment]}]}/></Panel><Panel title="Raw repeated measurements" subtitle="Includes the separately labeled warmup"><Table rows={extra.rows.filter((r:Row)=>r.experiment===experiment)}/></Panel></>}
    {section===5&&extra.kafka&&<><div className="a-kpis">{[['Acoustic pipeline',extra.acoustic],['Kafka producer',extra.kafka.state],['Spark query',extra.spark.state],['Historical export',extra.historical_export]].map(([k,v])=><div key={k}><small>{k}</small><strong className="a-health-value">{v}</strong><span>Observed {new Date(extra.observed_at).toLocaleTimeString()}</span></div>)}</div><Panel title="Kafka producer counters" subtitle="Cumulative process-local observations; accepting does not prove broker connectivity"><Table rows={extra.kafka.metrics?[Object.fromEntries(['accepting','queue_depth','enqueued','delivered','delivery_failed','send_failed','dropped_queue_full','dropped_stale','worker_failed','broker_errors'].map(k=>[k,extra.kafka.metrics[k]??'not observed']))]:[]}/></Panel><Panel title="Pipeline observability"><p>{extra.spark.reason}</p><p>Checkpoint telemetry and last successful streaming update: unavailable.</p><p>Acoustic dropped blocks: {extra.dropped_blocks} · Service error reported: {String(extra.service_error)} · Source: {extra.source}</p></Panel><p className="a-notice">Health is fetched on entry and on Refresh. A historical export does not prove that Kafka or Spark is currently running.</p></>}
    </>}
    <footer className="a-footer">ECHOTRACE / BIG DATA ANALYTICS <span>Traceable data. Measured claims.</span></footer>
  </main>;
}
