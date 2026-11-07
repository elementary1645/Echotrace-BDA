"""Read-only release checks for the project README, BDA and academic/portfolio Markdown.

Run from any directory: python3 docs/academic/check_delivery.py
No network, Docker administration, dataset generation or application mutations.
"""
from collections import Counter
import ast
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import statistics
import subprocess
import sys
from urllib.parse import unquote, urlsplit

REPO=Path(__file__).resolve().parents[2]
DOCS=[REPO/'README.md',*sorted((REPO/'bda').rglob('*.md')),*sorted((REPO/'docs').rglob('*.md'))]


def anchors(text):
    result=set(re.findall(r'<a\s+(?:id|name)=["\']([^"\']+)',text))
    counts=Counter()
    for line in text.splitlines():
        m=re.match(r'^#{1,6}\s+(.+?)\s*#*$',line)
        if not m:continue
        title=re.sub(r'<[^>]+>','',m[1]).lower().replace('`','')
        title=re.sub(r'[^\w\- ]','',title).replace(' ','-')
        number=counts[title];counts[title]+=1
        result.add(title+(f'-{number}' if number else ''))
    return result


def check_links():
    errors=[];links=0;bash=0;python=0;diagrams=0
    for file in DOCS:
        if not file.is_file():errors.append(f'missing document: {file.relative_to(REPO)}');continue
        text=file.read_text()
        for language,block in re.findall(r'^```([^\n]*)\n(.*?)^```\s*$',text,re.M|re.S):
            if language.strip()=='mermaid':diagrams+=1
            elif language.strip()=='bash':
                result=subprocess.run(['bash','-n'],input=block,text=True,capture_output=True)
                bash+=1
                if result.returncode:errors.append(f'{file.name}: bash syntax: {result.stderr.strip()}')
                # Check referenced entrypoints without executing install/service commands.
                for path in re.findall(r'(?<![\w/])(?:infrastructure/|backend/scripts/|docs/academic/)[\w./-]+\.(?:sh|py)',block):
                    if not (REPO/path).is_file():errors.append(f'{file.name}: missing command path {path}')
            elif language.strip()=='python':
                python+=1
                try:ast.parse(block)
                except SyntaxError as exc:errors.append(f'{file.name}: Python block: {exc}')
        prose=re.sub(r'^```[^\n]*\n.*?^```\s*$','',text,flags=re.M|re.S)
        for target in re.findall(r'!?\[[^\]\n]*\]\(([^)\n]+)\)',prose):
            target=target.strip().strip('<>');parts=urlsplit(target)
            if parts.scheme or target.startswith('//'):continue
            links+=1
            resolved=(file.parent/unquote(parts.path)).resolve() if parts.path else file.resolve()
            if not resolved.is_relative_to(REPO):errors.append(f'{file.name}: path leaves repository: {target}');continue
            if not resolved.exists():errors.append(f'{file.name}: missing link {target}');continue
            if parts.fragment and resolved.suffix=='.md':
                if unquote(parts.fragment) not in anchors(resolved.read_text()):errors.append(f'{file.name}: missing anchor {target}')
    return {'documents':len(DOCS),'local_links':links,'bash_blocks':bash,'python_blocks':python,'mermaid_blocks':diagrams},errors


def check_evidence():
    p3=REPO/'bda/reports/phase3';p5=REPO/'bda/reports/phase5'
    trials=json.loads((p3/'trials.json').read_text())
    assert len(trials)==72 and sum(bool(r['warmup']) for r in trials)==18
    assert all(r['status']=='success' and r['correct'] is True for r in trials)
    medians={engine:statistics.median(r['duration_seconds'] for r in trials
        if r['record_count']==5000000 and r['engine']==engine and not r['warmup']) for engine in ('python','spark')}
    assert abs(medians['python']-70.68902700700073)<1e-9
    assert abs(medians['spark']-32.70021507800084)<1e-9
    assert round(medians['python']/medians['spark'],2)==2.16
    datasets=json.loads((p3/'datasets.json').read_text())
    assert datasets['raw-5m']['record_count']==5000000
    assert datasets['raw-5m']['source_classification']=='SIMULATED'
    storage=json.loads((p3/'storage.json').read_text())
    assert storage['lake-1m-retry']['validated_count']==1000000
    assert storage['lake-1m-retry']['validated_bytes']==113032004
    aggregate=json.loads((p3/'sample-aggregates-1m.json').read_text())
    for dimension in ('category','hour','source'):
        assert sum(r['event_count'] for r in aggregate if r['dimension']==dimension)==1000000
    hourly=json.loads((p3/'historical-hourly.json').read_text())
    assert len(hourly)==48 and sum(r['event_count'] for r in hourly)==28426
    p4=REPO/'bda/reports/phase4';receipt=json.loads((p4/'export-receipt.json').read_text())
    assert hashlib.sha256((p4/'history.sqlite').read_bytes()).hexdigest()==receipt['sha256']
    with sqlite3.connect(f'{(p4/"history.sqlite").as_uri()}?mode=ro&immutable=1',uri=True) as db:
        assert db.execute('SELECT sum(event_count),count(*) FROM cells').fetchone()==(100000,68160)
        assert db.execute('SELECT count(*) FROM anomalies').fetchone()[0]==477
    run=json.loads((p5/'run-receipt.json').read_text());presentation=json.loads((p5/'presentation.json').read_text())
    assert run['status']==presentation['status']=='passed'
    first=run['end_to_end']['initial'];restart=run['end_to_end']['checkpoint_restart']
    assert (first['classified'],first['validated'],first['window_events'],first['detected_anomalies'])==(28,26,25,2)
    assert (restart['classified'],restart['validated'],restart['window_events'],restart['detected_anomalies'])==(30,26,25,2)
    c=run['campaign'];assert (c['requested_rate'],c['warmup_count'],c['produced'],c['acknowledged'],c['measured_seconds'])==(100,1500,6000,6000,60)
    assert c['validated_output']['validated']==7501 and c['validated_output']['window_events']==7500
    opt=json.loads((p5/'optimization.json').read_text());assert opt['results_identical']
    assert round(opt['before']['categories']['median_ms'],2)==31.27
    assert round(opt['after']['categories']['median_ms'],2)==5.88
    assert opt['before']['categories']['result']==opt['after']['categories']['result']
    questions=re.findall(r'^### \d+\.',(REPO/'docs/academic/VIVA_QUESTIONS.md').read_text(),re.M)
    slides=re.findall(r'^## Slide \d+', (REPO/'docs/academic/PRESENTATION.md').read_text(),re.M)
    assert len(questions)>=30 and len(slides)==12
    times=re.findall(r'\*\*Time:\*\* (\d+):(\d+)\.',(REPO/'docs/academic/PRESENTATION.md').read_text())
    assert sum(int(m)*60+int(s) for m,s in times)==780
    return {'recorded_trials':72,'measured_trials':54,'warmups':18,'five_million_medians_seconds':medians,
            'dashboard_events':100000,'historical_lake_events':1000000,'viva_questions':len(questions),'slides':len(slides)}


def main():
    result,errors=check_links()
    try:result['evidence']=check_evidence()
    except (AssertionError,OSError,ValueError,KeyError,sqlite3.Error) as exc:
        errors.append(f'evidence check failed: {type(exc).__name__}: {exc}')
    result['errors']=errors
    print(json.dumps(result,indent=2))
    return bool(errors)


if __name__=='__main__':sys.exit(main())
