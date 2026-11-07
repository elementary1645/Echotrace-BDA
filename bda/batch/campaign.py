"""Sequential controlled campaign; every attempted experiment and exit status is retained."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--output',required=True,help='new child name inside artifacts/phase3')
    p.add_argument('--lake-1m',default='lake-1m')
    p.add_argument('--include-5m',action='store_true')
    args = p.parse_args()
    if Path(args.output).name != args.output or Path(args.lake_1m).name != args.lake_1m:
        p.error('output and lake-1m must be direct child names')
    repo = Path(__file__).resolve().parents[2]
    artifacts = repo/'artifacts'/'phase3'
    for size,lake in [('100k','lake-100k'),('1m',args.lake_1m)]:
        if not (artifacts/f'raw-{size}'/'manifest.json').is_file() or not (artifacts/lake/'_COMPLETE').is_file():
            p.error('dataset or complete lake missing for '+size)
    if args.include_5m and not (artifacts/'raw-5m'/'manifest.json').is_file():
        p.error('5m manifest missing')
    target = artifacts/args.output
    target.mkdir(exist_ok=False)
    outcomes = []
    experiments = [(size,master,parts,shuffle) for size in ('100k','1m')
                   for master,parts,shuffle in [('local[1]',2,2),('local[2]',2,2),('local[2]',2,4),('local[2]',4,4)]]
    if args.include_5m:
        experiments.append(('5m','local[2]',4,4))
    for size,master,parts,shuffle in experiments:
        label = f'{size}-local{1 if master=="local[1]" else 2}-p{parts}-s{shuffle}'
        command = [str(repo/'infrastructure/batch/run.sh'),'benchmark','--input',f'/artifacts/raw-{size}',
            '--output',f'/artifacts/{args.output}/{label}','--input-partitions',str(parts),'--shuffle',str(shuffle),
            '--runs','3','--warmups','1']
        if size!='5m':
            lake = 'lake-100k' if size=='100k' else args.lake_1m
            command += ['--storage-manifest',f'/artifacts/{lake}/manifest.json']
        print('RUN '+label,flush=True)
        started = time.perf_counter()
        with (target/f'{label}.log').open('w') as log:
            result = subprocess.run(command,env=dict(os.environ,BATCH_MASTER=master),cwd=repo,stdout=log,stderr=subprocess.STDOUT)
        outcomes.append(dict(label=label,master=master,command=command,exit_code=result.returncode,
                             process_seconds=time.perf_counter()-started,log=f'{label}.log'))
        (target/'campaign.json').write_text(json.dumps(outcomes,indent=2)+'\n')
        print(f'END {label}: {result.returncode}',flush=True)
    if any(r['exit_code'] for r in outcomes):
        raise SystemExit('campaign has failed experiments; consult campaign.json and retained logs')


if __name__ == '__main__':
    main()
