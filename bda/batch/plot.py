"""Export charts from successful measured trials; disclose every other trial separately."""
import argparse
from collections import defaultdict
import json
import os
import tempfile
from pathlib import Path
import statistics
from bda.batch.benchmark import records


def load(paths):
    rows = []
    for path in paths:
        for row in records(path):
            rows.append(dict(row,journal=str(path)))
    return rows


def render(paths, output):
    os.environ.setdefault('MPLCONFIGDIR',tempfile.mkdtemp(prefix='echotrace-phase3-mpl-'))
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    target = Path(output)
    target.mkdir(parents=True,exist_ok=False)
    all_rows = load(paths)
    (target/'trial-status.json').write_text(json.dumps(all_rows,sort_keys=True,indent=2)+'\n')
    rows = [r for r in all_rows if r.get('status')=='success' and not r['warmup'] and r.get('correct')]
    if not rows:
        raise ValueError('no successful measured trials; status report retained')
    def label(r):
        return 'Python' if r['engine']=='python' else f"Spark {r['master']} input={r['requested_input_partitions']} shuffle={r['shuffle_partitions']}"
    def save(name,xlabel,ylabel,series,bar=False):
        fig,ax = plt.subplots(figsize=(9,5))
        for name_,points in sorted(series.items()):
            ordered = sorted(points.items())
            xs = [x for x,_ in ordered]
            ys = [statistics.median(v) for _,v in ordered]
            if bar:
                ax.bar([str(x) for x in xs],ys,label=name_)
            else:
                ax.plot(xs,ys,marker='o',label=name_)
        ax.set(xlabel=xlabel,ylabel=ylabel)
        ax.set_title('EchoTrace Phase 3 — measured medians (OS cache uncontrolled)')
        ax.legend(fontsize=7)
        ax.grid(axis='y',alpha=.3)
        fig.tight_layout()
        for extension in ('png','svg'):
            fig.savefig(target/f'{name}.{extension}',dpi=160)
        plt.close(fig)
    for name,metric,ylabel in [('execution-time','duration_seconds','Analytical execution time (s)'),
                               ('throughput','records_per_second','Records processed per second'),
                               ('spark-versus-python','duration_seconds','Processing time (s)')]:
        series = defaultdict(lambda:defaultdict(list))
        for row in rows:
            series[label(row)][row['record_count']].append(row[metric])
        save(name,'Input events (records)',ylabel,series)
    series = defaultdict(lambda:defaultdict(list))
    for row in rows:
        if row['engine']=='spark':
            key = f"{row['master']} / in {row['input_partitions']} / shuffle {row['shuffle_partitions']}"
            series[str(row['record_count'])+' records'][key].append(row['duration_seconds'])
    save('partition-runtime','Master / actual input partitions / shuffle partitions','Execution time (s)',series)
    series = defaultdict(lambda:defaultdict(list))
    for row in rows:
        if row.get('validated_bytes') is not None:
            series['Validated Parquet'][row['input_bytes']/1024**2].append(row['validated_bytes']/1024**2)
    if series:
        save('storage','Raw JSONL input (MiB)','Validated Parquet storage (MiB)',series)
    else:
        (target/'storage-unavailable.txt').write_text('No validated_bytes measurements in supplied trials.\n')
    return len(rows)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--results',nargs='+',required=True)
    p.add_argument('--output',required=True)
    args = p.parse_args()
    print(render(args.results,args.output))
