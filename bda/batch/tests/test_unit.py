import json
from pathlib import Path
import pytest
from bda.batch.generate import Config, generate, records, checksum, resource_check
from bda.batch.baseline import manifest, summary, expected, assert_equivalent
from bda.schema import AnalyticsEvent


@pytest.mark.parametrize('format',['jsonl','jsonl.gz'])
def test_generation(tmp_path,format):
    config = Config(count=103,sources=3,chunk=25,format=format,anomaly_every=10)
    a,b = tmp_path/'a',tmp_path/'b'
    first,second = generate(a,config),generate(b,config)
    assert first == second
    assert first['file_count'] == 5
    assert first['total_bytes'] == sum((a/f['path']).stat().st_size for f in first['files'])
    assert manifest(a) == first
    assert_equivalent(summary(a,first),expected(first))
    emitted = list(records(config))
    assert len({r['record_id'] for r,_ in emitted}) == 103
    for row,_ in emitted:
        assert AnalyticsEvent.model_validate(row).source == 'SIMULATED'
    assert sum(flag for _,flag in emitted) == first['injected_count'] == 11
    assert list(records(Config(count=4,seed=43))) != list(records(Config(count=4,seed=42)))
    with pytest.raises(FileExistsError):
        generate(a,config)


def test_manifest_corruption(tmp_path):
    generate(tmp_path/'a',Config(count=4))
    file = next((tmp_path/'a').glob('part-*'))
    file.write_text('{}\n')
    with pytest.raises(ValueError,match='mismatch'):
        manifest(tmp_path/'a')


def test_incomplete_results(tmp_path):
    from bda.batch.benchmark import records as parse
    path = tmp_path/'results.jsonl'
    path.write_text(json.dumps(dict(record_type='started',run_id='a'))+'\n'+
        json.dumps(dict(record_type='started',run_id='b'))+'\n'+
        json.dumps(dict(record_type='result',run_id='b',status='failed'))+'\n'+'{')
    assert [r['status'] for r in parse(path)] == ['incomplete','failed','invalid_journal_line']


def test_resource_guard(tmp_path,monkeypatch):
    from collections import namedtuple
    import bda.batch.generate as module
    monkeypatch.setattr(module.shutil,'disk_usage',lambda _:namedtuple('D','free')(100))
    with pytest.raises(RuntimeError,match='insufficient disk'):
        resource_check(tmp_path,10000000)


def test_fixture_and_cleanup(tmp_path):
    from bda.batch.fixtures import generate as fixture
    from bda.batch.cleanup import checked
    fixture(tmp_path/'fixture')
    assert json.loads((tmp_path/'fixture'/'fixture.json').read_text())['invalid'] == 2
    with pytest.raises(ValueError):
        checked('/tmp')
    with pytest.raises(ValueError):
        checked(Path.cwd()/'artifacts'/'phase3')


@pytest.mark.parametrize('config',[Config(count=0),Config(sources=0),Config(peak_hour=25),Config(start='2026-01-01'),Config(anomaly_every=-1)])
def test_config(config):
    with pytest.raises(ValueError):
        config.validate()


def test_quality_fixture_validation(tmp_path):
    from bda.batch.fixtures import generate as fixture
    from bda.streaming.validation import validate_record
    fixture(tmp_path/'quality')
    accepted,rejected = [],[]
    for raw in (tmp_path/'quality'/'quality.jsonl').read_text().splitlines():
        try:
            key = json.loads(raw)['record_id']
        except ValueError:
            key = None
        canonical,error = validate_record(raw,key)
        (rejected if error else accepted).append(error or json.loads(canonical))
    assert len(accepted)==4 and len(rejected)==2
    assert len({r['record_id'] for r in accepted})==3
    assert sum(r['event_time'].startswith('2025-') for r in accepted)==1


def test_event_metrics(tmp_path):
    from bda.batch.benchmark import event_metrics
    events = [dict(Event='SparkListenerJobStart',**{'Job ID':0,'Stage IDs':[0],'Properties':{'spark.jobGroup.id':'trial'}}),
        dict(Event='SparkListenerTaskEnd',**{'Stage ID':0,'Task Metrics':{'Executor Run Time':4,
            'Input Metrics':{'Bytes Read':100},'Shuffle Write Metrics':{'Shuffle Bytes Written':25}}})]
    (tmp_path/'events_1').write_text('\n'.join(json.dumps(e) for e in events))
    result = event_metrics(tmp_path)['trial']
    assert result['tasks']==1 and result['input_bytes']==100 and result['shuffle_write_bytes']==25
