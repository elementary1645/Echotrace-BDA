# Phase 2: Spark Structured Streaming

## Architecture and pinned runtime

Kafka 4.1.2 → classified Parquet (valid + quarantine) → bounded event-ID
deduplication → event-time window Parquet → anomaly snapshots.
Every streaming stage has its own durable checkpoint and Spark native file sink.
Anomaly calculation is an explicit bounded-history batch computation over finalized
window output, rather than an unsafe second stateful operation inside foreachBatch.

The official Spark 4.0.1 image is pinned by SHA-256 in `infrastructure/spark/Dockerfile`.
It contains PySpark 4.0.1, Python 3.10.12, Temurin Java 17.0.18+8, Hadoop 3.4.1,
and Scala 2.13. The Kafka connector is
`org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1` (Kafka client 3.9.1).
Python dependencies, including transitives, are pinned in `requirements.lock`.
The host's Python 3.14 and Java 25 are deliberately not used for Spark.

Official compatibility references:
[4.0.1 runtime](https://spark.apache.org/docs/4.0.1/),
[Kafka connector](https://spark.apache.org/docs/4.0.1/streaming/structured-streaming-kafka-integration.html),
[streaming APIs, watermarks and recovery](https://spark.apache.org/docs/4.0.1/streaming/apis-on-dataframes-and-datasets.html).

`local[2]` is a deliberate equivalent topology for this single Codespace: a real
Spark driver/executor runtime using two local worker threads, limited to two CPUs
and 3 GiB container RAM (1 GiB JVM heap). It demonstrates partitioned Spark work,
not multi-machine scale or fault tolerance. No administrative ports are published.
Parallelism is configurable; host CPU and RAM should be checked before raising it.
The Kafka overlay adds `INTERNAL://kafka:29092`; host applications retain localhost:9092.
It keeps the original Compose project and `kafka-data` volume.

## Startup, health and shutdown

From repository root, with Docker Compose v2:

```bash
infrastructure/spark/run.sh build
infrastructure/spark/run.sh start
# Create the existing application topic if this is a fresh data directory:
docker compose -f infrastructure/kafka/compose.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic echotrace.events.v1 --partitions 3 --replication-factor 1
infrastructure/spark/run.sh run
infrastructure/spark/run.sh inspect --root /data/analytics
infrastructure/spark/run.sh stop
```

`start` verifies the broker health check. A finite job exits zero only when all
stages finish successfully; `inspect` checks the data, and `docker stats --no-stream`
reports container resources while it runs. Initial connector download needs Maven
internet access; its persistent Ivy cache supports subsequent launches.

**Legacy container migration:** `start` refuses to recreate an existing Kafka
container without a mount at `/var/lib/kafka/data`. Export needed records before
any recreation, preserve that export, and replay into the persisted broker. There
is no automatic migration. Do not bypass this check with raw Compose commands.
Never use `down -v` for this stack. `stop` retains containers and all volumes.
See the validation record for the storage migration mistake encountered this session.

The one-shot default uses `availableNow`, i.e. finite conventional micro-batches
up to discovered offsets. For ongoing processing run three separate terminals:

```bash
infrastructure/spark/run.sh run --stage ingest --continuous
infrastructure/spark/run.sh run --stage events --continuous
infrastructure/spark/run.sh run --stage windows --continuous
# Periodic snapshot calculation after windows exist:
infrastructure/spark/run.sh run --stage anomalies
```

Each process uses its own stage checkpoint. **Only one writer per root/stage** is
allowed. Start ingest first. Three concurrent JVMs can exceed a small Codespace's
RAM: use periodic finite `run` invocations there. SIGINT/SIGTERM stops active
queries and the Spark session; outputs/checkpoints remain available.

## Configuration and schemas

Run `run.sh run --help`. Defaults: brokers `kafka:29092`, topic
`echotrace.events.v1`, root `/data/analytics`, watermark `2 minutes`, tumbling
window `1 minute`, shuffle parallelism 2, maximum 10,000 Kafka offsets per trigger.
`--window` accepts a positive number of seconds/minutes/hours; watermark uses
Spark duration syntax. Threshold 20, historical window count 10, minimum history 3,
z threshold 3, snapshot retention 3. Use a new root when changing schema, Kafka
subscription, state keys, watermark, window, or parallelism; checkpoint compatibility
is constrained by Spark. A root belongs to one immutable configuration.

Explicit schemas are in `bda/streaming/transforms.py`:

| Dataset | Contents |
| --- | --- |
| classified | topic, partition, offset, Kafka timestamp, key, raw JSON, validation canonical JSON/error, valid flag, ingestion date |
| events | Kafka coordinates/timestamp plus all v1 event fields, UTC event_time, event_date |
| windows | window {start,end}, source, source_id, category, zone, event_count, avg_confidence, events_per_second |
| anomaly snapshot | window/category/source counts, history_count, baseline_mean/stddev, z_score, threshold/baseline flags, is_anomaly |

Classified output partitions by `valid` and `ingest_date`: `valid=false` is the
inspectable quarantine, retaining payload and exact Kafka coordinates. Events
partition by event date. Windows are deliberately not partitioned on high-cardinality
keys. Native schemas are fixed; no schema inference from incoming JSON. One
intentional Python UDF reuses Phase 1 Pydantic validation for Unicode record hashes,
finite values, required fields, timestamp offsets, intervals, unknown fields and
source provenance. Analytics use built-in DataFrame functions, without Python UDFs.
Missing/unsupported version/type or a Kafka key different from record_id is invalid.

Category counts, source/device counts, zone distribution, LIVE/RECORDED/SIMULATED
distribution and category/source breakdown are **marginals of the window table**:
sum event_count across the unwanted dimensions. Frequency is event_count divided
by window seconds. To combine confidence means, use
`sum(avg_confidence * event_count) / sum(event_count)`, not an unweighted mean.
Events are preserved with original source; synthetic fixtures use dedicated topics
and source IDs. No unrestricted collect/toPandas is used; reports collect at most
6 synthetic rows or 100 simulation comparison rows.

## Event time, deduplication and finality

`event_time` remains EchoTrace's emitted **stream-position wall timestamp**,
not acoustic onset. Offsets are normalized to UTC by the wire contract. Windows
are half-open `[start,end)`. `kafka_time`/ingestion date are separate ingestion
metadata, not the analytical clock (Kafka timestamp may be producer create time).

Events stage uses `withWatermark(...).dropDuplicatesWithinWatermark(record_id)`.
Duplicate IDs within the supported watermark horizon do not inflate counts.
First accepted content wins for reused identities. State is evicted as event time
advances: this is not a permanent global ID registry. Replays with changed times
outside the horizon are not guaranteed to deduplicate. Out-of-order records within
the delay are supported; older records may be dropped, with state/watermark metrics.
All validated arrivals remain in classified storage for diagnosis, even when dropped
from the analytical event sink.

Windows run in a separate checkpointed query to avoid chained stateful query/sink
constraints. Append output contains only finalized windows after watermark progress;
no provisional window table is claimed. The watermark is based on the maximum
observed event time and is applied at micro-batch boundaries. Idle streams do not
advance it with wall time. Final trailing windows may remain in state indefinitely
until later events arrive. Watermark delay is a lower bound on tolerated lateness,
not a promise to accept every older record. Separate stages can each reject late
input; pathological future timestamps can advance a global watermark across sources.
There is no per-device watermark isolation. Fixture control events advance event
time only in dedicated test topics; never inject them into real-world statistics.

## Explainable anomaly baseline

For each category, source and source_id, combine zones into one count per finalized
window. A positive anomaly is either count >= configured absolute threshold or,
with enough prior observations, count above the previous N-window mean and
z >= configured limit. Constant prior histories flag any increase (standard
 deviation zero, z_score null); insufficient history uses only the absolute threshold.
Only preceding finalized rows enter the baseline, never the current or future row.
Missing empty windows are not materialized as zeros: history means previous N
**observed** windows. This is a transparent baseline, not a trained detector.
Snapshot recomputation scans retained windows; it is not a scalable historical
warehouse solution. `anomalies-current` atomically points to an immutable successful
Parquet snapshot on the shared local filesystem. Failed unpublished snapshots are
harmless; later success retains only `--snapshot-retention` latest snapshots.

## Recovery, storage ownership and retention

`/data` is the `spark-data` named volume. Checkpoints are under ROOT/checkpoints;
never edit/delete them independently of the associated outputs. Spark Kafka offsets
come from checkpoints, not consumer group commits; startingOffsets=earliest applies
only to a fresh query. failOnDataLoss=true prevents silently resetting missing
retained offsets. The native Parquet sink's metadata log coordinates committed
batch files with checkpoints. Read outputs using Spark at the **dataset root** so
it honors `_spark_metadata`; recursive glob readers can expose uncommitted files.
The pipeline does not use foreachBatch and claims no custom transaction protocol.
No end-to-end exactly-once guarantee: the Phase 1 producer has no durable outbox,
can lose events, and can submit application duplicates; deduplication is bounded.

Files are capped at 100,000 records; low shuffle parallelism and five-second
continuous triggers limit writer fanout. Low-volume streams still create small
files. Do not compact, prune date partitions, or apply object-store TTL in place
while downstream file streams/checkpoints reference them. Retain all active run
roots by default. To enforce configurable retention safely: stop all writers and
readers, archive the entire root (including checkpoints/metadata), mark that archived
root with `.retired`, then use the dry-run cleanup utility:

```bash
python -m bda.streaming.retention --parent /path/to/archived-runs --keep-days 30
# After reviewing candidates, same command with --execute deletes retired roots.
```

A new active epoch needs a fresh root and an explicit source offset strategy to
avoid replaying retained Kafka history. Automated active-data retention/compaction
and incremental anomaly history are deferred; disk use must be monitored.

## Tests and reproducible demonstrations

```bash
.venv/bin/python -m pytest bda/streaming/tests -q
infrastructure/spark/run.sh test
infrastructure/spark/run.sh live-test
.venv/bin/python -m pytest bda/tests backend/tests -q
```

Live tests explicitly opt in with RUN_SPARK_INTEGRATION=1 (set by the helper), create
an isolated unique three-partition topic, publish real Kafka records, execute all
stages twice with the same checkpoints, assert exact persisted output counts and
anomalies, then delete only the test-created topic. Local Spark tests cover parser,
confidence, UTC, provenance, windows, anomalies, empty transforms, out-of-order,
duplicates, late records and state checkpoint restart. No neural weights needed.

For a visible controlled workload, create a dedicated topic and run:

```bash
docker compose -f infrastructure/kafka/compose.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic echotrace.phase2.synthetic --partitions 3 --replication-factor 1
.venv/bin/python -m bda.streaming.demo --topic echotrace.phase2.synthetic
infrastructure/spark/run.sh run --topic echotrace.phase2.synthetic --root /data/synthetic-v1
infrastructure/spark/run.sh inspect --root /data/synthetic-v1 --synthetic
```

Expected: 20 Kafka records, 1 malformed quarantine, 18 unique events, finalized counts
2,2,2,10,1 and confidence .8, one anomaly at minute 3. The event at minute 10 advances
the watermark and remains an unfinalized trailing window. Rerunning the job with
the same root must preserve these counts. Publish fixtures once per topic: republishing
is an intentional replay, not the same test fixture. Use a new topic/root to start over.

Actual EchoTrace simulated scenario (existing backend environment needs its normal
scientific/audio dependencies but no model weights):

```bash
mkdir -p artifacts/phase2
docker compose -f infrastructure/kafka/compose.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic echotrace.phase2.simulated --partitions 3 --replication-factor 1
PYTHONPATH=backend .venv/bin/python -m bda.streaming.simulate \
  --topic echotrace.phase2.simulated --manifest artifacts/phase2/simulated-expected.json
infrastructure/spark/run.sh run --topic echotrace.phase2.simulated --root /data/simulated-v1
infrastructure/spark/run.sh inspect --root /data/simulated-v1 \
  --manifest /workspace/artifacts/phase2/simulated-expected.json
```

This runs `Pipeline(SimSource(...scenario='breakin'))`, preserves its eight actual
events, and independently calculates expected counts/confidence from the source
records. A separately labeled control source advances the watermark. The manifest
includes actual inputs and is ignored by Git. Use a fresh topic/root for each new
simulation to avoid comparing multiple sessions together.

## Measurement methodology and limits

ROOT/progress.jsonl records progress as micro-batches complete, including
inputRowsPerSecond, processedRowsPerSecond, numInputRows, durationMs, source
start/end/latest offsets, eventTime watermark, stateOperators memory/state counts
and rows dropped by watermark. Kafka connector source metrics include backlog
where supported. Per-stage recent progress is also saved on termination. The bounded
Spark recentProgress buffer is not a complete audit; use the listener log for history.
`inspect` reports ingestion valid/rejected counts separately from event-time outputs.
Measure host/container resources with `docker stats --no-stream` while running,
recording limits, input size, batch trigger, partitions, warm/cold cache and runtime.
Compare independent expected counts before timing; do not conflate startup/download
cost with steady-state throughput. No throughput superiority or multi-machine
scalability claim is made. Progress logs and active data need operational rotation.

To test a fresh isolated broker without changing the application stack:

```bash
docker compose -p echotrace-phase2-clean \
  -f infrastructure/kafka/compose.yml -f infrastructure/spark/compose.yml \
  -f infrastructure/spark/validation.compose.yml up -d --wait kafka
docker compose -p echotrace-phase2-clean \
  -f infrastructure/kafka/compose.yml -f infrastructure/spark/compose.yml \
  -f infrastructure/spark/validation.compose.yml run --rm \
  -e RUN_SPARK_INTEGRATION=1 --entrypoint python3 spark \
  -m pytest bda/streaming/tests/test_live.py -q -s -p no:cacheprovider
docker compose -p echotrace-phase2-clean \
  -f infrastructure/kafka/compose.yml -f infrastructure/spark/compose.yml \
  -f infrastructure/spark/validation.compose.yml down
```

This validation overlay publishes only localhost:19092 and checks health via the
internal listener. Its named volumes survive shutdown. For an explicit real Kafka
late-arrival exercise after the synthetic demonstration has finalized:

```bash
.venv/bin/python -m bda.streaming.demo --topic echotrace.phase2.synthetic --late-only
infrastructure/spark/run.sh run --topic echotrace.phase2.synthetic --root /data/synthetic-v1
infrastructure/spark/run.sh inspect --root /data/synthetic-v1 --synthetic --expected-late
```

Expected classified count becomes 21, with 18 analytical events and unchanged
finalized results. The live integration test includes this arrival after restart.
Source/state configuration is persisted in each root and incompatible reuse is
rejected; local filesystem locks enforce a single writer per stage.
