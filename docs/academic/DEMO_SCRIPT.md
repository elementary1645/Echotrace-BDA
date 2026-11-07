# Professor demonstration script

**Goal:** show source provenance, analytical correctness and the evidence behind
claims. Plan a 10–15 minute interactive walkthrough after setup. A fresh isolated
Kafka/Spark verification takes additional minutes; prepare it before class or allow
that time explicitly. Commands below run from the repository root unless noted.

Choose a track and say which is being shown:

| Track | What it establishes | What it does not establish |
| --- | --- | --- |
| A — recorded-output fallback + current acoustic simulation | Current UI/API behavior, actual scripted acoustic events, committed historical/benchmark evidence | A fresh Kafka/Spark end-to-end pass |
| B — new isolated pipeline run | Actual acoustic simulation → producer → Kafka → Spark → Parquet → API → browser, with recovery/workload checks | Microphone inference, multi-machine scale or long-duration capacity |
| C — optional existing application services | Current application producer and finite Spark activity under existing configuration | A replacement for isolated failure tests or permission to reset data |

Screenshots/JSON from previous phases remain **recorded evidence** even when a new
run is also demonstrated. Do not label them live.

## 1. Fresh checkout and environment checks

```bash
git clone https://github.com/elementary1645/Echotrace-BDA.git
cd Echotrace-BDA
git status --short
python3 --version
node --version
npm --version
```

The recorded BDA host used Linux, Python 3.14.2 and Node 24.22.2. The exact
Python-3.14 host dependency subset is in
[infrastructure/reliability/requirements-py314.lock](../../infrastructure/reliability/requirements-py314.lock).
Spark is isolated in its pinned Python 3.10.12 image; do not substitute host Java
25/Python 3.14 into the Spark verification environment.

On Debian/Ubuntu, install system audio libraries if missing:

```bash
sudo apt-get update
sudo apt-get install libportaudio2 libsndfile1
```

These are dependency-setup commands, not commands to run blindly during class or
on an offline machine. Use the equivalent supported package manager elsewhere.
If Python/Node/system libraries are unavailable, use a prepared machine or the
committed screenshots/reports and disclose that no current runtime was verified.

## 2. Track A: reliable weight-free fallback

Install dependencies/build before the demonstration; internet is needed for first
installation, not for the already prepared local recorded-output UI:

```bash
infrastructure/dashboard/run.sh setup
# Ensure this fallback cannot accidentally submit to an inherited Kafka setting.
KAFKA_ENABLED=false infrastructure/dashboard/run.sh serve
```

Open **http://127.0.0.1:8000/**. The launcher runs the original `SimSource`/Pipeline
break-in scenario and suppresses neural preloading. No ignored data directories,
neural weights, Kafka or Docker are needed. The committed historical SQLite and
benchmark artifacts are the declared data sources. Use Ctrl-C for graceful shutdown.
If port 8000 is occupied, choose an unused port with `serve --port 8006`; do not
stop an unknown listener. In another terminal:

```bash
curl --fail http://127.0.0.1:8000/api/status
curl --fail http://127.0.0.1:8000/api/v1/analytics/overview
curl --fail http://127.0.0.1:8000/api/v1/analytics/health
```

**Say:** “This is a current acoustic simulation and a recorded analytical-output
fallback. I am not claiming a new Kafka/Spark pass.” Do not click LIVE/RECORDED and
expect neural inference from the weight-free launcher; use the original backend
setup for those modes, as linked in the README.

## 3. Walk through the interface

### Overview — 1 minute

Show **100,000** analyzed events, **16 historical source IDs**, **477 detected
windows** and the SIMULATED/synthetic provenance line. Explain that these are the
reviewed 100k historical population, not active-device counts, the 1m lake or 5m
benchmark. Open the dataset identity/export timestamp. A stale warning is valid
for old recorded history; do not suppress it to make the page look live.

### Current acoustic simulation and original monitor — 2 minutes

Open **Live Analytics**. Confirm the WebSocket status and source **SIMULATED**.
The 60-second break-in loop emits events around seconds 4–23, so wait for that
part of the loop if the page is quiet. Inspect category, localization and confidence
charts, explaining their bounded browser-buffer population. The displayed buffered
rate is not end-to-end Spark throughput.

Open **Acoustic Monitor** to show the preserved map, sequence explanations,
source controls, timeline and meters. The simulation uses scripted labels/scores,
not microphone inference. Do not represent approximate horizontal localization
as identity tracking or a physically validated trajectory.

### Historical Analytics — 2 minutes

Use From **2026-01-01**, Until (exclusive) **2026-01-03**, all sources/categories.
Expected total **28,426**. Category **ALARM** gives **1,493**. Try a source filter,
switch hourly/daily views and export bounded results. Date controls are UTC calendar
bounds; the API also accepts validated timezone-aware whole UTC hours.

Equivalent checked API query:

```bash
curl --fail 'http://127.0.0.1:8000/api/v1/analytics/historical?dimension=day&start=2026-01-01T00:00:00Z&end=2026-01-03T00:00:00Z'
```

Do not claim the web request launches Spark: these are read-only reviewed aggregate
exports from actual prior Spark output. [E06](EVIDENCE_INDEX.md#e06),
[E07](EVIDENCE_INDEX.md#e07).

### Anomaly Intelligence — 1 minute

Show observed count, previous baseline, flags and nullable z-score. Explain the
absolute-count and preceding-observed-window rule. The default historical injected
ground truth includes **1,000 ALARM events** at Jan 4 noon for
`synthetic-node-00000`; it is not the detected-window count and is shown separately.
Restore full date/category filters when needed to see that period. No trained
severity/model probability is claimed.

### Performance Lab — 2 minutes

Select the default `5m-local2-p4-s4` experiment. Compare recorded measured medians
**70.6890 s Python** and **32.7002 s Spark**, about **2.16x**, excluding setup.
Show raw warmup labels and environment. Explain **72 results = 54 measured + 18
warmups** and **22.1031 s** separately recorded setup for that configuration.
Python was faster at 100k; no 10m timing or complete 5m Parquet size is available.

Show [original charts](../../bda/reports/phase3/charts/) and
[serving-index evidence](../../bda/reports/phase5/optimization.json) as different
experiments. Do not add measured latencies from different populations.

### System Health — 1 minute

Use Refresh. In Track A, stopped/unavailable producer or Spark telemetry is expected.
A historical export can be available while streaming is stopped. Explain current
process counters, writer heartbeat, processing progress, observed backlog and
export freshness separately. Consumer-group lag stays unavailable when not measured.

## 4. Track B: prepare a fresh isolated Kafka/Spark run

Before class, install the tested host dependencies and browser support. Avoid
reinitializing an environment containing unrelated user work; use a dedicated clone
or the existing reviewed virtual environment.

```bash
python3.14 -m venv .venv
.venv/bin/python -m pip install -r infrastructure/reliability/requirements-py314.lock
npm --prefix frontend ci
# Downloads browser and, when needed, installs OS packages; run during preparation.
(cd frontend && npx playwright install --with-deps chromium)
docker version
docker compose version
free -h
df -h .
```

Check cached runtime images; prepare explicitly if absent:

```bash
docker image inspect apache/kafka:4.1.2
docker image inspect echotrace-spark:4.0.1
# Only when missing:
# docker pull apache/kafka:4.1.2
# infrastructure/spark/run.sh build
infrastructure/reliability/run.sh preflight
```

Preflight requires about 3 GiB available RAM and 2 GiB free disk plus cached images
and committed outputs. This is not a guarantee that arbitrary larger workloads fit.
First connector resolution needs Maven access unless the copied Ivy cache supplies
it. No public Spark UI is required. Existing services are only inspected, not reset.

Run with a **new** name and unused dedicated loopback port:

```bash
infrastructure/reliability/run.sh verify artifacts/phase5/professor-001 --campaign --port 19096
```

**Actual actions:** a generated disposable broker project; real topic creation and
acknowledgement; deliberate owned-broker outage; actual finite EchoTrace acoustic
simulation and fixtures; Kafka consumption; streaming validation/deduplication/
windows/anomaly outputs; checkpoint restart with new late input; offline export;
a 100-events/s target experiment; production build; real HTTP and Chromium checks.
The owned broker stops afterward. Run artifacts and its new persisted Kafka volume
remain available. The existing application broker is not killed, replaced or reset.

If this command fails, inspect that run's retained logs and `receipt.json`. Do not
reuse the output directory, delete checkpoints, or edit a failed receipt to passed.
Fallback to Track A and state which current test did not complete.

## 5. Inspect Kafka, Spark and recovery evidence

Inspect the newly generated run with a bounded local report rather than relying
on pre-existing topic records:

```bash
.venv/bin/python - <<'PY'
import json
from pathlib import Path
root = Path('artifacts/phase5/professor-001')
r = json.loads((root / 'receipt.json').read_text())
print('Run:', r['project'], 'status:', r['status'])
print('New dedicated topic:', r['e2e_topic'])
print('First Parquet verification:', r['end_to_end']['initial'])
print('After restart:', r['end_to_end']['checkpoint_restart'])
print('Measured workload:', {k: r['campaign'][k] for k in ('requested_rate', 'measured_seconds', 'produced', 'acknowledged', 'actual_produced_rate', 'acknowledged_rate')})
print('Browser/API status:', json.loads((root / 'presentation.json').read_text())['status'])
PY
```

Expected finite verification: **28 classified, 1 quarantine, 26 unique validated**;
**25 finalized events**, acoustic subset **8**, **2 anomalies**. After two new late
Kafka records: **30 classified**, other totals unchanged. Explain the control record
as a separate event-time advancement device. Actual producer-before/outage counters
are in the same receipt. The during-outage record timed out; that loss is expected
and disclosed, not hidden by an exactly-once claim.

The workload should independently verify 7,500 synthetic workload events plus one
control after 15-second warmup and 60-second measurement. New machine timings can
differ; the historical reported numbers are not pass/fail latency targets.

## 6. Present the newly generated Spark export

The verification server used by the harness shuts down after its browser check.
For an interactive display of the same newly generated output, start an owned demo
server using declared operator paths:

```bash
BDA_STREAMING_DATABASE="$PWD/artifacts/phase5/professor-001/streaming.sqlite" \
BDA_TELEMETRY_ROOT="$PWD/artifacts/phase5/professor-001/acoustic" \
KAFKA_ENABLED=false infrastructure/dashboard/run.sh serve
```

Open Live Analytics and Refresh. The **Spark streaming aggregates** section must
show `streaming-finalized` origin and rows for `p5-acoustic`; the sum of that subset
is eight. The same page's WebSocket charts belong to a current separate acoustic
simulation. Do not pool them with the finalized export.

```bash
curl --fail http://127.0.0.1:8000/api/v1/analytics/streaming
curl --fail http://127.0.0.1:8000/api/v1/analytics/health
```

System Health should show recorded **stopped** queries/checkpoint presence, their
last progress/freshness, verified output counts and available export time. That
is correct because the finite run completed. It is not an active query claim.
The default historical page still uses the committed 100k snapshot, keeping its
population distinct. Screenshots from the prior verified run are in
[Phase 5 evidence](../../bda/reports/phase5/README.md).

## 7. Optional Track C: existing application services

Use this only when the existing stack is yours to operate and current state has
been evaluated. Do not run failure injection against it. Read the retained
[Phase 2 incident and safeguards](../../bda/PHASE2_VALIDATION.md) first.

```bash
docker ps -a --format '{{.Names}} {{.Status}}'
# Inspect storage ownership before an existing legacy stack is changed:
docker inspect echotrace-bda-kafka
# Supported startup runs its preservation guard; stop if that guard refuses:
infrastructure/spark/run.sh start
# On a fresh supported broker only, create the application topic idempotently:
docker compose -f infrastructure/kafka/compose.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic echotrace.events.v1 --partitions 3 --replication-factor 1
# In another terminal, opt in to the real producer:
infrastructure/dashboard/run.sh serve --kafka
# Allow acoustic events, then run finite Spark stages:
infrastructure/spark/run.sh run
infrastructure/spark/run.sh inspect --root /data/analytics
```

Default finite windows may not finalize until later genuine event times arrive.
Do not inject artificial future controls into the application topic. Exporting
finalized application rows requires an operator-chosen existing Spark data volume
mounted read-only, as described in the [streaming export guide](../../bda/analytics/DEMO.md).
These commands are supported interfaces, not a claim that Track C was re-executed
in this documentation phase.

## 8. Safe shutdown and retained evidence

- Interactive demo: Ctrl-C; confirm `Application shutdown complete.` in the log.
- Isolated runner: normally stops only its own stack. If interrupted, use the generated
  project name from its receipt to inspect owned resources; never substitute the
  application project. Consult the operations guide before manual intervention.
- Existing Track C services: `infrastructure/spark/run.sh stop` only when intentionally
  stopping the supported application stack. It retains storage.
- Do not use `down -v`, global pruning, application topic resets or checkpoint deletion.

Optional **read-only** cleanup review for a completed isolated run:

```bash
.venv/bin/python -m bda.reliability.cleanup --run artifacts/phase5/professor-001
```

No destructive cleanup is necessary for assessment. The helper's explicit execute/
volume-deletion flags are documented in the operations guide for intentionally
disposable resources; run files/checkpoints/evidence stay retained.

## 9. Release and evidence checks

```bash
python3 docs/academic/check_delivery.py
.venv/bin/python -m pytest -q
npm --prefix frontend test
npm --prefix frontend run build
# Cached runtime only; no expensive 5m campaign:
infrastructure/batch/run.sh test
```

For the default browser suite, keep Track A running in a separate terminal, then
use `npm --prefix frontend run test:e2e -- e2e/dashboard.spec.ts`. The generated-output
browser case is run by Track B. Tests requiring neural assets, live integrations
or ignored generated-run roots skip unless their prerequisites/flags are supplied;
see [final release record](../../bda/PHASE6_VALIDATION.md).

**Closing statement:** “The new isolated run, if performed, verifies the stated
path and counts. Otherwise the dashboard demonstrates a current simulation with
previously validated recorded outputs. Neither track establishes production HA,
field acoustic accuracy or end-to-end exactly-once delivery.”
