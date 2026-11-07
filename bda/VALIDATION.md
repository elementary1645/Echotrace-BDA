# Phase 1 validation — 2026-11-07

Branch: `feat/bda-foundation`. The initial working tree contained untracked
`bda/event_adapter.py`, package/test initializers, and `infrastructure/kafka/compose.yml`.
These were inspected and extended; no unrelated tracked changes were present.

## Audit and decisions

Inspected `events.py`, `schemas.py`, `pipeline.py`, `api/server.py`, backend tests,
requirements, script import bootstrapping, and Compose before implementation.
The generation-filtered broadcaster is the integration boundary. Kafka uses a
separate bounded worker queue; no acoustic-worker Kafka calls were introduced.
The server now tracks its initial source-start task so shutdown cannot race a
late initial pipeline start. WebSocket payloads and latency calculation remain intact.
Timestamp semantics and source-switch delivery limits are in `README.md` here.

## Commands and results

A local `.venv` was created with Python 3.14.2. Installed Pydantic 2.14.3,
confluent-kafka 2.16.1, pytest 9.2.0, FastAPI 0.162.1, and scientific/audio
runtime packages. Torch/PANNs weights were not installed. Initial sandbox pip
DNS and Docker permission failures were environmental; authorized commands outside
the sandbox succeeded. Asyncio/TestClient checks also ran outside sandbox restrictions.

- `python -m pytest bda/tests -m 'not integration' -q`: 28 passed,
  one live integration test deselected.
- `python -m pytest backend/tests -q`: 157 passed, 20 skipped with missing assets.
- With the upstream AudioSet label CSV downloaded to `/tmp/echotrace-class-labels.csv`,
  reran the full backend suite: 174 passed, 3 skipped (model/weight dependent).
  This includes the existing FastAPI/WebSocket/mode-switch/reset/latency tests.
  No source or user home model assets were changed. Exact invocation:

  ```bash
  PYTHONPATH=backend .venv/bin/python - <<'PY'
  from pathlib import Path
  import pytest
  from echotrace import config
  config.LABELS_FILE = Path('/tmp/echotrace-class-labels.csv')
  raise SystemExit(pytest.main(['backend/tests', '-q']))
  PY
  ```

- `RUN_KAFKA_INTEGRATION=1 python -m pytest bda/tests/test_kafka_integration.py -q -s`:
  one passed in 3.57 s. Actual finite EchoTrace simulated pipeline produced eight
  event records. All eight were enqueued, submitted, acknowledged, consumed, and
  schema-validated; queue depth was zero after shutdown. Verification session:
  `64c24352-846e-40d7-a7c2-5842544e4504` (effective suffix `/g1`).
- Kafka topic describe: three partitions, replication factor one, broker/leader one.
- Final combined suite with the label CSV override: 202 passed, 4 skipped
  (three model-dependent tests and the opt-in Kafka test).
- `docker compose -f infrastructure/kafka/compose.yml config --quiet`: passed.
  Also started an isolated fresh stack with project `echotrace-phase1-validation`,
  container `echotrace-bda-validation-kafka`, and host port 19092. It became healthy,
  accepted topic creation, and retained its three-partition topic after `down`
  followed by `up --wait` using the same named volume. The temporary stack and
  its task-created volume were removed afterward; the original broker was unchanged.
- `python -m compileall -q bda backend/echotrace backend/scripts`: passed.
- `git diff --check`: passed. No formatter/linter configuration was found.

The installed Starlette emits a deprecation warning about its httpx-based TestClient;
it does not fail the tests. The client briefly logged IPv6 localhost connection
refusals before connecting over IPv4; real delivery still succeeded.

## Scope and limits of verification

Live delivery used the pre-existing Kafka 4.1.2 container. Fresh Compose startup
and persistence were separately verified in an isolated temporary stack. The
original container was not recreated or its earlier smoke data migrated. See the
migration warning in the setup guide.
No GPU, neural-inference smoke, microphone hardware, production failover, sustained
throughput benchmark, or Spark behavior was verified. There is no end-to-end
exactly-once guarantee. Unit failure tests use fakes; no deliberate outage was
imposed on the user's running broker. Full details of loss/backpressure semantics
are documented in the setup guide.
