"""Separate data-quality fixtures. These are never benchmark datasets."""
import argparse
import json
from pathlib import Path
from bda.batch.generate import Config, records


def generate(output):
    root = Path(output)
    root.mkdir(parents=True,exist_ok=False)
    valid = [r for r,_ in records(Config(count=4,days=1))]
    invalid = dict(valid[1],confidence=2)
    # Line order demonstrates late arrival; historical batch queries intentionally reorder by event time.
    late = dict(valid[2],event_time='2025-01-01T00:00:00Z')
    rows = [valid[0],valid[0],invalid,late,valid[3]]
    (root/'quality.jsonl').write_text('\n'.join(json.dumps(r) for r in rows)+'\n{\n')
    (root/'fixture.json').write_text(json.dumps({'fixture_only':True,'records':6,'invalid':2,'duplicates':1,'late_ids':[late['record_id']]},indent=2))


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--output',required=True)
    generate(p.parse_args().output)
