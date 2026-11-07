"""Comparable read-only SQL index experiment on copies; never edits prior evidence."""
import argparse
import json
from pathlib import Path
import sqlite3
import statistics
import time

QUERIES={
    'categories':'SELECT category,sum(event_count) AS value FROM cells GROUP BY category ORDER BY value DESC',
    'source_count':'SELECT count(DISTINCT source_id) FROM cells',
}
INDEXES=['CREATE INDEX cells_category_cover ON cells(category,event_count)',
         'CREATE INDEX cells_source_cover ON cells(source_id)']


def measure(path):
    db=sqlite3.connect(f'{path.as_uri()}?mode=ro',uri=True)
    result={}
    for name,sql in QUERIES.items():
        samples=[]
        for _ in range(22):
            before=time.perf_counter();rows=db.execute(sql).fetchall();samples.append((time.perf_counter()-before)*1000)
        result[name]={'median_ms':statistics.median(samples[2:]),'min_ms':min(samples[2:]),'max_ms':max(samples[2:]),
                      'samples':20,'warmups_excluded':2,'result':rows,
                      'plan':[r[3] for r in db.execute('EXPLAIN QUERY PLAN '+sql)]}
    db.close();return result


def main():
    p=argparse.ArgumentParser();p.add_argument('--output',required=True);args=p.parse_args()
    root=Path(args.output).resolve();root.mkdir(parents=True,exist_ok=False)
    source=Path('bda/reports/phase4/history.sqlite').resolve()
    candidate=root/'candidate.sqlite'
    original=sqlite3.connect(f'{source.as_uri()}?mode=ro',uri=True);target=sqlite3.connect(candidate)
    original.backup(target);original.close();target.close()
    baseline=measure(candidate);before_bytes=candidate.stat().st_size
    target=sqlite3.connect(candidate)
    for sql in INDEXES:target.execute(sql)
    target.commit();target.close()
    after=measure(candidate)
    for key in QUERIES:assert baseline[key]['result']==after[key]['result']
    result={'population':'68,160 aggregate cells, same SQLite copy before/after index creation; sequential warm-cache local reads',
            'baseline_bytes':before_bytes,'indexed_bytes':candidate.stat().st_size,'indexes':INDEXES,
            'before':baseline,'after':after,'results_identical':True,
            'speedup':{k:baseline[k]['median_ms']/after[k]['median_ms'] for k in QUERIES}}
    (root/'result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))


if __name__=='__main__':main()
