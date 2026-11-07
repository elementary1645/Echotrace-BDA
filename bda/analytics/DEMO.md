# Professor demonstration

## Reproducible weight-free local demonstration

Prerequisites: Linux/macOS with Python 3.10+ (this session uses 3.14), Node 20.19+
(this session uses 24), npm, and PortAudio/libsndfile system libraries. On Debian:
`sudo apt-get install libportaudio2 libsndfile1`. No neural weights, Kafka, Docker
or existing ignored dataset directories are required for the default demo. The
reviewed aggregate serving snapshot and original benchmark reports are in Git.

From a fresh checkout:

```bash
git switch feat/bda-analytics-dashboard
infrastructure/dashboard/run.sh setup
infrastructure/dashboard/run.sh serve
```

Open http://127.0.0.1:8000/ . The simulation uses the original deterministic
`SimSource`/Pipeline break-in scenario and is visibly **SIMULATED**. This demo
launcher suppresses neural preload and intentionally rejects LIVE/RECORDED mode;
use `backend/scripts/serve.py` with full `backend/requirements.txt`, AudioSet
labels, neural weights and appropriate audio hardware to demonstrate inference.
The existing normal launcher and source controls are preserved.

1. Overview: verify 100,000 historical simulated events, 16 historical source IDs,
   477 detected windows and the dataset ID. These are independent of the live buffer.
2. Live Analytics: inspect WebSocket status, current simulated events, rolling
   buffered counts, confidence, categories and localization. Wait 4–23 seconds
   within the 60-second loop for break-in events. Source activity is source mode.
3. Acoustic Monitor: original map, controls, linked sequences, timeline and meters.
4. Historical Analytics: Jan 1 inclusive to Jan 3 exclusive gives **28,426** events.
   Category ALARM gives **1,493** in that interval. Try source filters and switch
   hourly/daily view. Export downloads bounded JSON analytical results.
5. Anomaly Intelligence: inspect actual flagged windows, baseline and explanations;
   the separate full-snapshot injected-ground-truth table contains 1,000 events
   at Jan 4 12:00 UTC for ALARM / synthetic-node-00000. Injected count is not a
   detected-window count. Detected rows follow the filters; ground truth does not.
6. Performance Lab: default 5m configuration shows original measured Python median
   **70.6890 s** and Spark **32.7002 s**; setup excluded. Inspect raw trials, change
   configuration, compare local throughput/resource/storage observations. No
   five-million-record Parquet storage or multi-machine scaling claim is made.
7. System Health: absent Kafka/Spark telemetry remains unavailable/stopped.
   Refresh explicitly; the UI does not imply all services are running.

## Optional real Kafka and finalized Spark outputs

Do this only when demonstrating the infrastructure, using the existing pinned
runtime and storage safeguards. Never use `down -v`. Existing Kafka installations
without persisted storage must follow the migration instructions in Phase 2;
startup refuses unsafe recreation.

```bash
infrastructure/spark/run.sh build
infrastructure/spark/run.sh start
# Fresh broker only: idempotently create the application topic.
docker compose -f infrastructure/kafka/compose.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic echotrace.events.v1 --partitions 3 --replication-factor 1
# Start the demo in another terminal, opting in to actual producer delivery:
infrastructure/dashboard/run.sh serve --kafka
# Let events arrive, then run finite stages on the retained application root:
infrastructure/spark/run.sh run
infrastructure/spark/run.sh inspect --root /data/analytics
```

Finalized windows depend on event-time watermark advancement; idle wall time does
not finalize trailing windows. Publish a serving snapshot after a successful
anomaly stage. Discover the existing named volume with
`docker volume ls --filter label=com.docker.compose.volume=spark-data`; set its
exact name below (the batch helper mounts it read-only):

```bash
BATCH_PHASE2_VOLUME=YOUR_EXISTING_SPARK_DATA_VOLUME infrastructure/batch/run.sh export \
  --streaming --input /data/analytics --output /artifacts/streaming-new.sqlite
# After successful export, atomically publish into the fixed serving location:
mv artifacts/phase3/streaming-new.sqlite artifacts/phase3/streaming.sqlite
```

Live Analytics → Refresh now shows finalized Spark rows with export provenance.
This is a recorded snapshot of finalized streaming output, not an active query
status signal. Repeat the export into a **new** name for updates. Stream roots with
no successful anomaly snapshot are refused. Checkpoints/offsets remain private and
unavailable to the dashboard. Producer acceptance alone is not broker health;
inspect delivered/failed counters and use the supported infrastructure health tools.
For ongoing stages see Phase 2; do not create multiple writers per root/stage.

## Rebuild the historical serving artifact independently

To reproduce without pre-existing ignored state, generate and validate the 100k
snapshot as documented in Phase 3, using new paths if outputs already exist:

```bash
infrastructure/spark/run.sh build
infrastructure/batch/run.sh generate --count 100000 --seed 42 --output artifacts/phase3/raw-100k
infrastructure/batch/run.sh lake --input /artifacts/raw-100k --output /artifacts/lake-100k
infrastructure/batch/run.sh export --input /artifacts/lake-100k --output /artifacts/dashboard-new.sqlite
```

Compare aggregate totals/category/day marginals to
`bda/reports/phase3/datasets.json`, then atomically copy the reviewed export into
`bda/reports/phase4/history.sqlite`. The export receipt provides the checked-in
file's checksum and dataset identity. Fresh rebuild timestamps and SQLite file
layout can differ; compare data, not a newly generated file's binary hash.
No benchmark is rerun by the dashboard. Use Phase 3's campaign instructions to
independently regenerate benchmark artifacts; do not replace measurements with
estimates.

## Tests and shutdown

```bash
.venv/bin/python -m pip install pytest httpx
.venv/bin/python -m pytest -q
infrastructure/batch/run.sh test
(cd frontend && npm test && npm run build)
# With the demo server running; first-time Chromium setup:
(cd frontend && npx playwright install --with-deps chromium && npm run test:e2e)
.venv/bin/python -m bda.analytics.measure
```

Existing label/model-dependent tests may skip on a clean minimal demo setup. Use
the original model setup workflow for those checks; no weights are needed for the
new analytics API tests. Browser tests use actual APIs and recorded artifacts,
plus one deliberately intercepted 503 to verify the retry state. Screenshots are
written to `bda/reports/phase4/`. Stop the demo with Ctrl-C; optional services stop
with `infrastructure/spark/run.sh stop`, retaining their data.
