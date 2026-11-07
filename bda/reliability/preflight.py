"""Read-only deployment checks; refuses automatic infrastructure repair."""
import json
from pathlib import Path
import shutil
import subprocess
from bda.reliability.workload import resource_guard


def main():
    result=resource_guard(Path.cwd())
    for command in ('docker','node','npm'):
        if not shutil.which(command):raise SystemExit(f'{command} required; see Phase 5 operations guide')
    for image in ('echotrace-spark:4.0.1','apache/kafka:4.1.2'):
        p=subprocess.run(['docker','image','inspect',image],capture_output=True,text=True)
        if p.returncode:raise SystemExit(f'Cached image {image} missing; prepare explicitly with documented build/pull commands')
        result[image]=json.loads(p.stdout)[0]['Id']
    for p in ('frontend/node_modules','bda/reports/phase4/history.sqlite','bda/reports/phase3/trials.json'):
        if not Path(p).exists():raise SystemExit(f'Missing {p}; run documented setup')
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
