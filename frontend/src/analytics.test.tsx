// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from 'vitest';
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { benchmarkGroups, countBy, median, read } from './analyticsData';
import { isMessage, useEchoTrace } from './useEchoTrace';
import App from './App';

class Socket {
  static instances: Socket[]=[];
  onopen: (()=>void)|null=null; onclose:(()=>void)|null=null; onerror:(()=>void)|null=null;
  onmessage: ((e:{data:string})=>void)|null=null;
  constructor(){ Socket.instances.push(this); }
  close(){this.onclose?.();}
}
function Probe(){const {state,connection}=useEchoTrace();return <div>{connection} events:{state.events.length}</div>;}
afterEach(()=>{cleanup();vi.useRealTimers();vi.unstubAllGlobals();Socket.instances=[];location.hash='';});
describe('analytical transformations',()=>{
  it('computes actual measured medians, omitting warmups',()=>{
    expect(median([7,2,4,8])).toBe(5.5);
    expect(benchmarkGroups([{experiment:'x',engine:'spark',warmup:true,duration:100},{experiment:'x',engine:'spark',warmup:false,duration:3}], 'x','duration')[1].value).toBe(3);
    expect(countBy(['a','b','a'],x=>x)).toEqual([{label:'a',value:2},{label:'b',value:1}]);
  });
  it('surfaces structured API errors',async()=>{
    vi.stubGlobal('fetch',vi.fn().mockResolvedValue({ok:false,status:503,json:async()=>({error:'Dataset missing'})}));
    await expect(read('overview')).rejects.toThrow('Dataset missing');
  });
});
describe('WebSocket lifecycle',()=>{
  it('rejects malformed wire messages',()=>{ expect(isMessage({type:'event',data:{}})).toBe(false); expect(isMessage(null)).toBe(false); });
  it('reconnects after close and cancels reconnect on unmount',()=>{
    vi.useFakeTimers();vi.stubGlobal('WebSocket',Socket);
    const {unmount}=render(<Probe/>);
    expect(Socket.instances).toHaveLength(1);
    Socket.instances[0].close();
    vi.advanceTimersByTime(500);
    expect(Socket.instances).toHaveLength(2);
    unmount();vi.advanceTimersByTime(6000);
    expect(Socket.instances).toHaveLength(2);
  });
  it('deduplicates events and retains source classification',async()=>{
    vi.stubGlobal('WebSocket',Socket);render(<Probe/>);
    const msg={type:'event',data:{source:'SIMULATED',id:'m1-e1',t:1,t_start:0,t_end:1,category:'ALARM',confidence:.8,wall:'2026-01-01T00:00:00Z',zone:'LEFT'}};
    Socket.instances[0].onmessage?.({data:JSON.stringify(msg)});
    Socket.instances[0].onmessage?.({data:JSON.stringify(msg)});
    await waitFor(()=>expect(screen.getByText(/events:1/)).toBeTruthy());
  });
});
describe('dashboard rendering',()=>{
  it('offers navigation and retry for missing datasets',async()=>{
    vi.stubGlobal('WebSocket',Socket);
    vi.stubGlobal('fetch',vi.fn().mockResolvedValue({ok:false,status:503,json:async()=>({error:'Dataset missing'})}));
    render(<App/>);
    expect(screen.getByRole('navigation')).toBeTruthy();
    expect(await screen.findByRole('alert')).toBeTruthy();
    expect(screen.getByText('Dataset missing')).toBeTruthy();
    fireEvent.click(screen.getByRole('button',{name:'Retry'}));
    expect(await screen.findByRole('alert')).toBeTruthy();
  });
  it('does not render health telemetry as streaming provenance on navigation',async()=>{
    location.hash='#5';vi.stubGlobal('WebSocket',Socket);
    vi.stubGlobal('fetch',vi.fn().mockImplementation(async(url:string)=>({ok:!url.includes('streaming'),status:503,json:async()=>url.includes('health')?{kafka:{state:'unavailable',metrics:null},spark:{state:'unavailable',reason:'No telemetry'},observed_at:'2026-01-01T00:00:00Z',acoustic:'stopped'}:url.includes('catalog')?{rows:[{sources:[],categories:[]}]}:{error:'No streaming export'}})));
    render(<App/>);
    expect(await screen.findByText('Pipeline observability')).toBeTruthy();
    fireEvent.click(screen.getByRole('link',{name:/Live Analytics/}));
    expect(await screen.findByText('Spark streaming aggregates')).toBeTruthy();
    expect(screen.getByText(/No streaming export/)).toBeTruthy();
  });
  it('renders empty historical results with provenance',async()=>{
    location.hash='#2';vi.stubGlobal('WebSocket',Socket);
    vi.stubGlobal('fetch',vi.fn().mockImplementation(async(url:string)=>({ok:true,json:async()=>({rows:url.includes('catalog')?[{sources:[],categories:[]}]:[],total:0,provenance:{source:'SIMULATED',origin:'synthetic',engine:'Spark',exported_at:'2026-01-01T00:00:00Z',dataset_id:'verified'},stale:true})})));
    render(<App/>);
    expect(await screen.findByText('SIMULATED · synthetic')).toBeTruthy();
    expect(screen.getByText(/No time-series data/)).toBeTruthy();
    expect(screen.getByRole('button',{name:'Export filtered results'})).toBeTruthy();
  });
});
