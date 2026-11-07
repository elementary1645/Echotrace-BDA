"""Dry-run by default. Remove only a stopped, receipt-owned disposable stack.

All run files remain. Volume removal requires a separate explicit --delete-volume.
"""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess


def main():
    p=argparse.ArgumentParser();p.add_argument('--run',required=True);p.add_argument('--execute',action='store_true')
    p.add_argument('--delete-volume',action='store_true');args=p.parse_args()
    root=Path(args.run).resolve();project=json.loads((root/'receipt.json').read_text())['project']
    if not re.fullmatch(r'echotrace-p5-[0-9a-f]{12}',project):raise ValueError('not a generated disposable project')
    ids=subprocess.check_output(['docker','ps','-aq','--filter',f'label=com.docker.compose.project={project}'],text=True).split()
    for identifier in ids:
        row=json.loads(subprocess.check_output(['docker','inspect',identifier],text=True))[0]
        if row['State']['Running']:raise ValueError('stop the disposable project before cleanup')
        if row['Config']['Labels'].get('com.docker.compose.project')!=project:raise ValueError('ownership mismatch')
    volume=project+'_kafka-data'
    check=subprocess.run(['docker','volume','inspect',volume],capture_output=True,text=True)
    if check.returncode==0:
        labels=json.loads(check.stdout)[0].get('Labels') or {}
        if labels.get('com.docker.compose.project')!=project or labels.get('echotrace.phase5.disposable')!='true':
            raise ValueError('refusing a volume without both disposable and project ownership labels')
    print(json.dumps({'project':project,'stopped_containers':ids,'volume':volume,'delete_volume':args.delete_volume,'execute':args.execute}))
    if not args.execute:return
    repo=Path(__file__).resolve().parents[2]
    env={**os.environ,'P5_RUN_DIR':str(root)}
    subprocess.run(['docker','compose','-p',project,'-f',str(repo/'infrastructure/reliability/compose.yml'),'down'],check=True,env=env)
    if args.delete_volume and check.returncode==0:subprocess.run(['docker','volume','rm',volume],check=True)


if __name__=='__main__':main()
