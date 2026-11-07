# Resume and technical portfolio material

Use only if these statements accurately describe the submitting person's contribution.
The original EchoTrace acoustic platform and PANNs model must retain their attribution;
do not imply sole authorship of pre-existing code. Measurements below are historical
project observations, not newly repeated results on every reader's machine.
[Evidence and boundaries](../academic/EVIDENCE_INDEX.md).

## One-line project description

**EchoTrace-BDA:** a reproducible local acoustic-event analytics platform extending
EchoTrace with Kafka, Spark streaming/SQL, Parquet history, safe analytical APIs and
a provenance-aware React dashboard.

## Three ATS-friendly resume bullets

- Extended EchoTrace with Kafka, Spark Structured Streaming, Parquet and FastAPI/React;
  verified simulated event flow through the browser, checkpoint recovery, duplicate
  suppression and invalid-record quarantine. [E08](../academic/EVIDENCE_INDEX.md#e08)
- Generated and benchmarked 5M synthetic event records and completed a 1M-record
  Parquet lake; observed about 2.16x local Spark/Python speedup for one checked
  aggregation workload, excluding setup. [E02–E05](../academic/EVIDENCE_INDEX.md#e02)
- Reduced category aggregate-query median from 31.27 to 5.88 ms with covering indexes
  and identical outputs; documented the additional storage cost and reproducible
  before/after methodology. [E11](../academic/EVIDENCE_INDEX.md#e11)

**Compact technology line:** Python · FastAPI · Pydantic · Apache Kafka 4.1.2 ·
Apache Spark 4.0.1 · Structured Streaming · Spark SQL · Apache Parquet · SQLite
serving exports · React · TypeScript · Docker Compose · pytest · Vitest · Playwright.

## Longer technical portfolio description

EchoTrace-BDA extends an existing local acoustic monitoring application into an
inspectable data platform. A bounded asynchronous Kafka side channel preserves
versioned source/session/event identity without putting broker I/O on the acoustic
worker. Spark Structured Streaming validates records into accepted/quarantine
storage, performs watermark-bounded deduplication and publishes finalized event-time
aggregates. Historical Parquet builds, allowlisted Spark SQL and explicit baseline/
count anomaly outputs support analytical exploration. Reviewed aggregate exports
serve a six-view React workspace through bounded FastAPI endpoints, alongside the
original Acoustic Monitor.

Evaluation separates actual acoustic simulation, deterministic synthetic benchmark
metadata, recorded historical exports and process telemetry. The project generated
and benchmarked five million synthetic records, completed a one-million-record
historical lake, and retained 72 benchmark results: 54 measured and 18 warmup.
For one five-million-record analytical bundle, Python/Spark medians were
70.6890/32.7002 seconds, about 2.16x, excluding setup in a local two-CPU-quota Spark
environment. This is not a multi-machine scale or universal speedup result.

An isolated broker/Spark experiment verified the actual simulated acoustic path
through Parquet, API and browser, including duplicate, malformed and late records,
checkpoint restart and a deliberately timed-out submission during broker outage.
A separate 100-events/s target used a 15-second warmup and 60-second measured
producer/concurrent-ingest interval, with downstream stages drained sequentially.
Telemetry distinguishes writer heartbeat, processing progress and export freshness.
A measured serving-index experiment reduced category-query median 31.27 → 5.88 ms
with a larger export file. The resulting portfolio emphasizes reproducible evidence
and explicit delivery/security limits rather than end-to-end exactly-once or HA claims.

**Links:** [report](../academic/PROJECT_REPORT.md),
[architecture](../academic/ARCHITECTURE.md), [demo](../academic/DEMO_SCRIPT.md),
[raw measurements](../../bda/reports/phase3/trials.json),
[reliability evidence](../../bda/reports/phase5/README.md).

## 30-second interview pitch

“I extended the original EchoTrace acoustic application into a traceable analytics
platform using Kafka, Spark, Parquet, FastAPI and React. I verified simulated events
through the actual pipeline and browser, including checkpoint recovery and failure
cases. The project benchmarked five million synthetic records and completed a
million-record lake. I can explain the exact workload behind its measured results,
why local Spark is not a multi-machine scaling claim, and where the bounded producer
can still lose data.”

## Two-minute technical explanation

“EchoTrace already detected and linked acoustic events. My extension focuses on
what happens to event metadata after detection: stable identity, durable analytical
history, event-time semantics and observable failure boundaries. It preserves the
original acoustic interface and credits the pretrained PANNs model separately.

“The server submits current-generation records to a bounded producer queue. Kafka
I/O stays on a worker thread, so broker trouble is isolated from acoustic work.
The tradeoff is best-effort delivery: queue-full and timeout losses are visible,
but there is no durable outbox.

“Spark uses separate checkpointed ingest, event-deduplication and window queries.
Malformed records remain in quarantine. Watermarks bound state and late-data handling;
idle wall time does not finalize trailing windows. Historical Parquet and Spark SQL
provide hour, category and source analysis, while explainable anomaly flags use
count thresholds and preceding observed-window baselines.

“For evaluation, I kept the populations distinct. Five million synthetic metadata
records were generated and benchmarked; one million completed the historical lake.
One local aggregation bundle measured about 2.16x Spark/Python median speedup excluding
setup, and smaller inputs favored Python. I also measured a category-query index
improvement with a documented storage tradeoff.

“An isolated real broker/Spark run verified acoustic simulation through generated
outputs, the API and browser. It checked duplicates, quarantine and late arrivals
after checkpoint restart. A broker outage deliberately lost a timed-out event, so
I do not claim end-to-end exactly once. My next evaluations would address durable
replay, secured replicated deployment and longer multi-machine workloads.”

## Keyword reference and claims to avoid

**Implemented keywords:** event streaming, KRaft development broker, record identity,
schema validation, bounded asynchronous producer, event-time windows, watermarks,
late-data handling, bounded deduplication, checkpoint recovery, ETL, quarantine,
columnar storage, Parquet partitions, Spark SQL, deterministic data generation,
repeated benchmarking, query indexing, provenance, freshness, read-only API,
responsive dashboard, integration testing, failure injection, reproducibility.

Do not add Kubernetes, cloud services, production HA, trained anomaly models,
replicated multi-broker results, multi-machine scaling, end-to-end exactly-once,
10m execution or field acoustic accuracy. Do not call the 72 results 72 non-warmup
measured trials, or the five million records five million microphone detections.
The [evidence index](../academic/EVIDENCE_INDEX.md) gives the supported wording.
