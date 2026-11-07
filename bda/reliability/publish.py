"""Publish new Phase 5 evidence without overwriting prior reports."""
import argparse
import json
from pathlib import Path
import platform
import shutil
import sys


def main():
    p=argparse.ArgumentParser();p.add_argument('--run',required=True);p.add_argument('--output',required=True)
    p.add_argument('--optimization',required=True);args=p.parse_args()
    root=Path(args.run).resolve();out=Path(args.output);out.mkdir(parents=True,exist_ok=True)
    receipt=json.loads((root/'receipt.json').read_text());presentation=json.loads((root/'presentation.json').read_text())
    if receipt['status']!='passed' or presentation['status']!='passed':raise ValueError('both pipeline and presentation must have actually passed')
    mapping={'receipt.json':'run-receipt.json','presentation.json':'presentation.json','resources.jsonl':'resources.jsonl',
             'e2e-first.json':'e2e-first.json','e2e-restart.json':'e2e-restart.json','browser.log':'pipeline-browser.log'}
    for src,dst in mapping.items():
        target=out/dst
        if target.exists():raise ValueError('report exists; publish a new evidence directory')
        shutil.copyfile(root/src,target)
    target=out/'optimization.json'
    if target.exists():raise ValueError('optimization report exists')
    shutil.copyfile(Path(args.optimization)/'result.json',target)
    environment={'host_python':platform.python_version(),'platform':platform.platform(),
                 'spark':'4.0.1','spark_python':'3.10.12','spark_java':'17.0.18','topology':'local[2]',
                 'spark_cpu_quota':2,'spark_memory_bytes':3*1024**3,'spark_heap':'1g',
                 'broker':'Kafka 4.1.2; one broker; one CPU quota; 1 GiB limit; 256 MiB heap',
                 'preflight':receipt['preflight'],'producer_workload':'100 events/s; 15 s warmup, 60 s measured' if 'campaign' in receipt else 'not executed',
                 'limitations':'No multi-machine scale or full concurrent downstream throughput claim'}
    (out/'environment.json').write_text(json.dumps(environment,indent=2)+'\n')
    samples=[json.loads(line) for line in (root/'resources.jsonl').read_text().splitlines()]
    summary={}
    def memory_bytes(value):
        # Docker uses e.g. "941.6MiB / 3GiB".
        import re
        m=re.match(r'([\d.]+)([KMGT]?i?B)',value.split(' / ')[0]);return float(m[1])*{'B':1,'KiB':1024,'MiB':1024**2,'GiB':1024**3}[m[2]]
    for kind in ('spark','kafka'):
        rows=[r for sample in samples for r in sample['containers'] if '-'+kind+'-' in r['Name']]
        summary[kind]={'samples':len(rows),'max_sampled_memory_bytes':max(memory_bytes(r['MemUsage']) for r in rows),
                       'max_sampled_cpu_percent':max(float(r['CPUPerc'].strip('%')) for r in rows)}
    (out/'resource-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    render(out,receipt,json.loads(target.read_text()))


def render(out,receipt,optimization):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    if 'campaign' not in receipt:
        return
    c=receipt['campaign'];batches=c['ingest_batches']
    fig,ax=plt.subplots(figsize=(10,4.5))
    ax.plot([b['batchId'] for b in batches],[b['processedRowsPerSecond'] for b in batches],marker='o',label='Spark processed rows/s (active batch)')
    ax.plot([b['batchId'] for b in batches],[b['inputRowsPerSecond'] for b in batches],marker='.',label='Spark input rows/s')
    ax.axhline(c['requested_rate'],color='grey',linestyle='--',label='Requested producer rate')
    ax.set(xlabel='Nonempty ingestion batch ID (includes warmup and drain)',ylabel='Records / second',title='Measured isolated 100-events/s workload · local[2]')
    ax.legend(fontsize=8);ax.grid(alpha=.2);fig.tight_layout();fig.savefig(out/'streaming-rates.png',dpi=150);plt.close(fig)
    names=list(optimization['before']);fig,ax=plt.subplots(figsize=(8,4))
    x=list(range(len(names)))
    ax.bar([i-.18 for i in x],[optimization['before'][k]['median_ms'] for k in names],width=.36,label='Before indexes')
    ax.bar([i+.18 for i in x],[optimization['after'][k]['median_ms'] for k in names],width=.36,label='Covering indexes')
    ax.set_xticks(x,[k.replace('_',' ') for k in names]);ax.set(ylabel='Median SQL execution (ms)',title='Identical aggregate results · 20 measured reads after two warmups')
    ax.legend();fig.tight_layout();fig.savefig(out/'serving-indexes.png',dpi=150);plt.close(fig)


if __name__=='__main__':main()
