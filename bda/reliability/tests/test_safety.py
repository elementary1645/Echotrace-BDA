import pytest
from bda.reliability.run import Run


def test_runner_refuses_non_disposable_output(tmp_path):
    with pytest.raises(ValueError,match='artifacts/phase5'):
        Run(tmp_path/'unsafe',19095).run()


def test_runner_refuses_existing_output(tmp_path,monkeypatch):
    from bda.reliability import run
    monkeypatch.setattr(run,'REPO',tmp_path)
    target=tmp_path/'artifacts/phase5/existing';target.mkdir(parents=True)
    with pytest.raises(FileExistsError):
        Run(target,19095).run()


def test_workload_requires_dedicated_topic():
    from bda.reliability.workload import producer,paced
    with pytest.raises(ValueError,match='dedicated'):
        producer('127.0.0.1:9092','echotrace.events.v1','source','session')
    with pytest.raises(ValueError,match='safe campaign'):
        paced('127.0.0.1:9092','unused','session',rate=10000)


def test_missing_application_broker_is_supported(monkeypatch):
    from bda.reliability import run
    from types import SimpleNamespace
    monkeypatch.setattr(run.subprocess,'run',lambda *a,**k:SimpleNamespace(returncode=1,stdout=''))
    assert run.application_broker_identity() is None
