# Phase 2 validation — 2026-11-07

Branch `feat/spark-streaming`; initial working tree clean. Host: Python 3.14.2,
Java 25, 7.8 GiB RAM, no swap; Docker 29.9.2. `rg` was unavailable, so source
inspection used find/grep/cat. No AGENTS.md was found. Reviewed Phase 1 README,
validation, schema, adapter, producer, Kafka Compose and existing tests before coding.
Official Spark documentation was consulted for the pinned runtime and streaming APIs.
No neural weights were installed or used.

## Executed checks

- `.venv/bin/python -m pytest bda/tests backend/tests -q` outside sandbox socket
  restrictions: **185 passed, 21 skipped**, 4.55 s. The first sandbox attempt
  stalled in socket-dependent tests and was interrupted; the unrestricted run passed.
- `.venv/bin/python -m pytest bda/streaming/tests/test_validation.py -q`:
  **15 passed**, 0.27 s after final storage locking changes.
- Combined regression invocation with only the upstream public AudioSet CSV in `/tmp`:

  ```bash
  .venv/bin/python - <<'PY'
  import urllib.request
  urllib.request.urlretrieve('https://storage.googleapis.com/us_audioset/youtube_corpus/v1/csv/class_labels_indices.csv', '/tmp/echotrace-class-labels.csv')
  PY
  PYTHONPATH=backend .venv/bin/python - <<'PY'
  from pathlib import Path
  import pytest
  from echotrace import config
  config.LABELS_FILE = Path('/tmp/echotrace-class-labels.csv')
  raise SystemExit(pytest.main(['bda/tests', 'bda/streaming/tests', 'backend/tests', '-q']))
  PY
  ```

  **217 passed, 9 skipped**, 20.54 s. Skips: three model/weight dependent,
  four opt-in local Spark, and two opt-in live Kafka tests. One existing Starlette
  TestClient deprecation warning. Before the label fixture: **200 passed, 26 skipped**.
- `RUN_KAFKA_INTEGRATION=1 .venv/bin/python -m pytest bda/tests/test_kafka_integration.py -q -s`:
  **1 passed**, 4.05 s. Actual EchoTrace breakin simulation emitted eight events,
  all acknowledged and consumed; session `9bfe06ff-50e1-4f54-8e55-a1cd0fcbaba7/g1`.
  Final producer metrics: enqueued/submitted/delivered 8, queue_depth 0.
  localhost IPv6 connection failures were logged before successful IPv4 delivery.
- `infrastructure/spark/run.sh build`: final digest-pinned Docker image and complete
  dependency lock built successfully. Runtime inspection reported Spark/PySpark
  4.0.1, Python 3.10.12 and Temurin Java 17.0.18+8.
- Both normal merged Compose and the isolated validation overlay passed
  `docker compose ... config --quiet`.
- `python -m compileall -q bda/streaming`, `bash -n infrastructure/spark/run.sh`,
  and `git diff --check`: passed.

## Actual Kafka-to-Parquet demonstrations

Executed dedicated synthetic topic creation with three partitions, then:

```bash
.venv/bin/python -m bda.streaming.demo --topic echotrace.phase2.synthetic
infrastructure/spark/run.sh run --topic echotrace.phase2.synthetic --root /data/synthetic-v1
infrastructure/spark/run.sh inspect --root /data/synthetic-v1 --synthetic
```

Report printed VERIFIED. Actual stored records: 20 classified, one invalid JSON
quarantine with Kafka coordinates/raw payload, 18 distinct validated analytical
events. The repeated identity did not inflate counts. Finalized counts were
**2, 2, 2, 10, 1**, all average confidence **0.8**; the ten-event window was the
single anomaly. A minute-10 event advanced the watermark and remained unfinalized.
The same-root job was restarted successfully using saved source offsets and state.

For actual EchoTrace simulation, executed:

```bash
PYTHONPATH=backend .venv/bin/python -m bda.streaming.simulate \
  --topic echotrace.phase2.simulated --manifest /tmp/echotrace-simulated-expected.json
# Copied the generated manifest into ignored artifacts/phase2 for the container mount.
infrastructure/spark/run.sh run --topic echotrace.phase2.simulated --root /data/simulated-v1
infrastructure/spark/run.sh inspect --root /data/simulated-v1 \
  --manifest /workspace/artifacts/phase2/simulated-expected.json
```

Report printed VERIFIED. Session `simulation-d355d6e4eeb442bfba2a566da2975d8a`:
eight actual events plus one separately labeled watermark control record. Spark
counts and confidence means matched independent Python calculations from the input
manifest. Output included ALARM/CENTRE count 4, mean 0.5760000000000001 and
IMPACT/RIGHT count 2, mean 0.585, alongside the remaining source events. All
retained SIMULATED provenance. Control records were confined to the demo topic.

## Measurements, not a benchmark campaign

From `/data/simulated-v1/progress.jsonl`, under concurrent development load:
Kafka classification batch 0 processed 9 rows, triggerExecution **17,579 ms**,
processedRowsPerSecond **0.5118**. Dedupe batch 0 processed 9 rows in **6,868 ms**,
reported **1.3104 rows/s**, and state memoryUsedBytes **2,800** for nine state rows.
Initial inputRowsPerSecond was 0 (first-batch reporting), not an absence of records.
`docker stats --no-stream` samples during verification showed Spark containers
around 0.28–1.02 GiB within their 3 GiB limit; these are samples, not peak tracking.
No sustained-throughput, latency percentile, speedup, failover, or multi-machine
scalability result is claimed. Logs expose offsets/backlog where available,
watermarks, rejected ingestion counts via reports, and state metrics.

## Failures and preservation incident

Initial container tests exposed a missing direct-Python Spark import path; explicit
Spark/Py4J Python paths fixed it. First live submission exposed Ivy's nonexistent
home path; the persistent explicit Ivy location fixed it. Progress dictionary UUIDs
required serialization conversion. A report originally named inspect.py shadowed
Python's stdlib inspect module and was renamed report.py. One test helper was edited
while a shell was running it, causing a post-test shell parse error even though
pytest passed; later verification uses the stable script. The clean-test host-port
override initially used the host advertised address for its container health check;
the committed validation overlay checks the internal listener instead.

**Kafka data preservation was not fully achieved.** I recreated the pre-existing
legacy broker before checking its actual mounts/log directory. The documented
named `echotrace-bda_kafka-data` volume was retained, but the earlier application
topic was absent afterward. The Phase 1 documentation had explicitly warned that
legacy container data was not automatically migrated. The application topic was
recreated and live delivery reverified. A read-only audit of all retained anonymous
Docker volumes found them empty; no earlier records were recovered. This cannot be
represented as preservation of the legacy records. No `down -v` was run. Startup
now refuses legacy containers lacking the required persistent mount, but this guard
does not undo the earlier mistake.

## Delivery and operational limits

Graceful checkpoint/offset restart is tested; abrupt process-kill/storage-failure
fault injection is not. Spark native sink/checkpoint recovery does not repair
Phase 1 producer loss. Deduplication is watermark-bounded. Trailing windows wait
for later event times. Active data/log retention needs monitoring; cleanup is only
for explicitly retired whole roots, and anomaly snapshots have configurable retention.
No production security, replicated broker durability, neural inference, microphone,
or sustained performance campaign was verified.

## Final verification additions

- `infrastructure/spark/run.sh test`: **19 passed, 1 skipped**, 183.45 s.
  Four real local Spark tests include parsing/aggregation/weighted marginals,
  anomaly history, dedupe/late/out-of-order checkpoint restart, and half-open window
  boundaries/finalization/late-data rejection across aggregator restarts. The one
  skipped test is opt-in live Kafka.
- Clean-start project `echotrace-phase2-clean`, using the committed validation overlay:
  broker started healthy on localhost:19092; the live Kafka integration test passed
  **1 test in 386.26 s** under concurrent verification load. It used an actual unique
  three-partition topic, parsed Kafka records, checked quarantine/window/anomaly
  results, restarted all queries with unchanged checkpoints, and checked unchanged
  committed output counts. The test-created topic was deleted afterward.
- `infrastructure/spark/run.sh run` against the default `echotrace.events.v1` topic:
  exit zero. `run.sh inspect --root /data/analytics --session
  9bfe06ff-50e1-4f54-8e55-a1cd0fcbaba7/g1` printed VERIFIED: eight persisted source
  events, all SIMULATED; no finalized trailing windows without future event times.
- Published a real Kafka late arrival using
  `.venv/bin/python -m bda.streaming.demo --topic echotrace.phase2.synthetic --late-only`,
  restarted the same `/data/synthetic-v1` root, and ran
  `infrastructure/spark/run.sh inspect --root /data/synthetic-v1 --synthetic --expected-late`:
  VERIFIED. Classified count 21, analytical count 18, unchanged five finalized
  windows and anomaly. This validates recovery of new offsets as well as exclusion
  of a deliberately late event from analytical state. The integrated test now
  includes this same post-checkpoint late-arrival sequence.
- Final integrated test revision, including publishing a new late record between
  restarts, reran against the isolated broker: **1 passed in 153.00 s**. Both
  before/after reports printed VERIFIED; no source or output was substituted with
  an internal fake stream. The isolated validation project was shut down with
  `docker compose -p echotrace-phase2-clean -f infrastructure/kafka/compose.yml
  -f infrastructure/spark/compose.yml -f infrastructure/spark/validation.compose.yml down`
  (no volume deletion). The application broker was left running.
