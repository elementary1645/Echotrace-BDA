# Viva and technical interview preparation

Answers describe the implemented project, not every capability of its frameworks.
For numbers and qualifications, keep the [evidence index](EVIDENCE_INDEX.md) open.

## Architecture and scope

### 1. What did you build beyond original EchoTrace?

A versioned Kafka side channel, Spark streaming validation/state/aggregates,
historical Parquet/SQL workflows, deterministic datasets and benchmarks, analytical
APIs/dashboard and an isolated recovery/evaluation harness. The original acoustic
engine, CNN14 usage, localization and operational console were retained; PANNs was
not newly trained by this extension. [Architecture](ARCHITECTURE.md).

### 2. Does the five-million-record result mean five million sounds were detected?

No. Five million SIMULATED event-metadata records were generated and benchmarked.
The largest complete validated Parquet lake was one million events, and the default
UI serves a separate 100k snapshot. Those are deliberately different populations.
[E02](EVIDENCE_INDEX.md#e02), [E05](EVIDENCE_INDEX.md#e05), [E07](EVIDENCE_INDEX.md#e07).

### 3. Why use Kafka instead of writing directly from the microphone loop?

The asynchronous side channel separates analytical transport from acoustic work.
The server submits current-generation events to a bounded queue; a worker handles
Kafka I/O. This reduces coupling to slow/unavailable transport, while making the
best-effort loss boundary explicit. [Producer](../../bda/producer.py).

### 4. Is the platform a tested multi-machine cluster?

No. Spark `local[1]`/`local[2]` used local worker threads in one host, with a two-CPU
quota/3 GiB Spark container cap. Task/shuffle/partition behavior is real, but
multi-machine scheduling, network scale and independent failure domains were not
measured. [E04](EVIDENCE_INDEX.md#e04).

## Kafka and record semantics

### 5. What is the Kafka deployment architecture here?

A development single KRaft broker/controller, version 4.1.2. Test/application topics
use three partitions and replication factor one. KRaft avoids a separate ZooKeeper
service; one broker still does not provide replicated availability. Inspect
[Compose](../../infrastructure/kafka/compose.yml).

### 6. Do three topic partitions mean three brokers?

No. Partitions are independent ordered logs and can all reside on the one broker.
They permit partitioned work but do not establish three failure domains. The
reliability test creates its own three-partition topics on a new disposable broker.

### 7. How is a record keyed, and what ordering does that imply?

The key is the stable record identity derived from source ID, session ID and event
ID. Identical keys target consistent partition assignment, but distinct events can
land on different partitions. Global event-time ordering is not guaranteed, so
Spark handles event-time windows and permitted disorder explicitly.
[Schema](../../bda/schema.py).

### 8. What does `acks=all` guarantee in this project?

The producer requests acknowledgements from the current in-sync replicas according
to Kafka's protocol. With replication factor one here, that is not independent
replicated durability. It also says nothing about records dropped before Kafka or
about application-level re-submission. [E09](EVIDENCE_INDEX.md#e09).

### 9. Why enable producer idempotence and still perform deduplication?

Kafka producer idempotence addresses supported producer-retry duplication. Replaying
the same application event can still create multiple log records. Spark separately
suppresses stable event identities within its watermark horizon. These mechanisms
operate at different boundaries.

### 10. What happens when the application producer queue fills?

Nonblocking submission drops the newest event and increments `dropped_queue_full`.
It does not block the acoustic path indefinitely or spill to a durable outbox.
A blocked-worker test with capacity one verifies the rejection and later draining.
[Reliability test](../../bda/reliability/tests/test_reliability.py).

### 11. Can you claim at-least-once or exactly-once for every source event?

Not end to end. Queue drops, delivery timeouts, stale-generation filtering and process
loss can remove events before Kafka. Native sink/checkpoint behavior and bounded
identity state do not repair that upstream loss. The outage experiment explicitly
lost one timed-out submission. [E09](EVIDENCE_INDEX.md#e09).

### 12. How do consumer offsets differ from Spark checkpoints?

The standalone development consumer explicitly commits its processed-message offset
and reports invalid records before advancing by policy. Spark's Kafka/file-source
progress and state are maintained in its own checkpoint directories; it does not
rely on those development consumer commits. [Consumer](../../bda/consumer.py),
[job](../../bda/streaming/job.py).

## Spark execution and event time

### 13. Why are ingest, events and windows separate stages?

They have distinct source/sink and stateful commit boundaries. Ingest preserves
validation outcomes; events perform watermark-bounded identity deduplication;
windows perform aggregation/finalization. Separate native queries/checkpoints avoid
assuming arbitrary chained stateful/custom-batch processing is safe.
[Stage diagram](ARCHITECTURE.md#3-structured-streaming-stages).

### 14. What is the event-time field?

It is EchoTrace's emitted stream-position wall timestamp, normalized to UTC by the
wire adapter. It is not necessarily actual acoustic onset or current ingestion
wall time. Kafka timestamps and observer monotonic durations are separate clocks.
[Adapter](../../bda/event_adapter.py).

### 15. What is a watermark, and what are the defaults here?

It bounds retained event-time state and informs late-data/finalization decisions.
The streaming job defaults to a two-minute delay with one-minute windows. It is
based on observed event-time progression; an idle stream does not finalize windows
just because two wall-clock minutes passed. [Transforms](../../bda/streaming/transforms.py).

### 16. Does the watermark promise every late event will be accepted?

No. Events within the supported delay are supported under the documented query
semantics; sufficiently old arrivals may be rejected, and multiple stages can
apply lateness handling. A future timestamp can advance the shared watermark.
Tests verify particular late/out-of-order cases, not every possible arrival pattern.

### 17. What happens to an event exactly on a window end?

Windows are half-open `[start,end)`. That boundary event belongs to the next window,
not both. Actual Spark boundary/finalization tests cover this behavior.
[Streaming tests](../../bda/streaming/tests/test_spark.py).

### 18. Is deduplication permanent?

No. Stable IDs identify records, but streaming deduplication state is evicted under
the watermark. A reused identity outside the supported horizon is not guaranteed
to be suppressed forever. Historical builds separately select unique record IDs
within their finite input population.

### 19. What does a checkpoint preserve, and when is it unsafe to reuse?

It records source progress and relevant operator state/commit information for its
query epoch. The project checks compatible source/state configuration and exclusive
stage ownership. Changed schema/source/state settings should use a new root rather
than editing checkpoint files. [Storage guard](../../bda/streaming/storage.py).

### 20. What did the real restart test establish?

After a first actual Kafka/Spark pass, two newly published late records advanced
classified count from 28 to 30; unique analytical count stayed 26, finalized count
25 and anomaly count two. The restart used existing checkpoints and genuine new
Kafka offsets. It does not prove recovery from physical storage destruction.
[E09](EVIDENCE_INDEX.md#e09).

## Historical storage, SQL and anomalies

### 21. Why Parquet rather than only JSONL?

JSONL is convenient inspectable input; Parquet provides typed columnar analytical
storage and event-date partition discovery for the lake. In the measured 1m snapshot,
validated Parquet used about 23.43% of the raw JSONL bytes. That ratio is specific
to these event records, not an audio-compression claim. [E06](EVIDENCE_INDEX.md#e06).

### 22. Does the completion marker make the lake a distributed ACID database?

No. It marks a successful immutable build with manifest/provenance. It prevents
consumers from treating a known incomplete build as finished; it is not a claimed
distributed transaction/table protocol. Native streaming sink metadata and
checkpoint consistency also require reading the proper dataset roots.

### 23. What Spark SQL queries are implemented?

Totals, hourly/daily activity, category confidence means, sources, zones,
classification, peaks, source comparisons, trends, preceding-window statistics
and anomaly summaries. Their names/SQL are allowlisted. The Jan 1–3 UTC example
was independently checked at 48 hourly rows and 28,426 events.
[Queries](../../bda/batch/queries.py).

### 24. How do you combine confidence averages across groups?

Use `sum(group_mean * group_count) / sum(group_count)`. An unweighted mean biases
the result toward small groups. The serving export retains sums/counts so filters
can produce correct weighted results. [Exporter](../../bda/analytics/export.py).

### 25. Is anomaly detection a trained machine-learning model?

No. It is an explainable positive-count detector: absolute threshold or a sufficiently
supported increase over preceding observed-window statistics. The current window
and future observations do not enter its own baseline; empty missing windows are
not automatically zeros. [Anomaly transform](../../bda/streaming/transforms.py).

### 26. Why can an anomaly have a null z-score?

There may be insufficient or zero-variance history. The implemented zero-variance
rule can flag an increase without inventing a numeric z-score. The dashboard exposes
the available flags/history/baseline and labels unrecorded policy rather than
fabricating severity.

### 27. Are injected anomalies and detected anomalies the same count?

No. The generator records injected event ground truth; Spark flags analytical
windows. Original acoustic risk sequences are a third concept. The default historical
UI's 1,000 injected ALARM events must not be equated with 477 detected windows.
[E07](EVIDENCE_INDEX.md#e07).

## Serving, measurements and reliability

### 28. Why serve SQLite exports instead of launching Spark on every HTTP request?

The web layer needs predictable bounded reads. Controlled offline exports preserve
Spark output/provenance, while FastAPI validates filters and paginates aggregates.
This avoids unrestricted expensive jobs and loading millions of events into the
application process. It introduces an explicit publication/freshness step.

### 29. How are API requests bounded and validated?

Pydantic validates identifiers, dimensions, date boundaries and pagination. SQL
identifiers come from allowlists and values are bound parameters. Historical pages
cap at 500 rows, anomaly pages at 200, and SQLite work has a two-second instruction
progress budget. No request accepts arbitrary paths or SQL. [API](../../backend/echotrace/api/analytics.py).

### 30. What does the 2.16x result actually compare?

Medians of three measured Python/Spark repetitions for a specific 5m raw JSONL
aggregation bundle: category count/mean plus UTC hourly/source counts. Times are
70.6890/32.7002 s, with setup excluded. It is local execution, not general Spark
superiority; Python was faster at 100k. [E04](EVIDENCE_INDEX.md#e04).

### 31. Why report both 72 results and 54 measured trials?

Nine configuration/size combinations × two engines × four repetitions produce
72 result records. One repetition per engine/configuration is warmup: 18 warmups
and 54 measured results. Charts exclude warmups from measured medians, and the
raw records remain inspectable. [E03](EVIDENCE_INDEX.md#e03).

### 32. What did the 100-events/s campaign establish?

After a 15-second warmup, 6,000 records were submitted in 60 measured seconds at
about 100/s and all acknowledged/validated. Ingestion ran concurrently; downstream
stages drained sequentially. It did not establish long-duration, fully concurrent
pipeline capacity or performance at higher suggested rates. [E10](EVIDENCE_INDEX.md#e10).

### 33. How was indexing justified, and what did it cost?

The same aggregate copy was measured before/after covering category/count and
source-ID indexes, with identical query results. Category median fell 31.27 → 5.88 ms,
while file size grew 10,285,056 → 13,340,672 bytes. Indexes apply to new exports;
this is a query-specific warm-cache tradeoff. [E11](EVIDENCE_INDEX.md#e11).

### 34. What distinguishes process health from data freshness?

Writer heartbeat, successful progress timestamp, export publication time and current
producer acknowledgement are separate observations. A running-but-idle query can
have stale processing; a stopped query can have an available old export. Missing
telemetry is unavailable, not automatically healthy. Consumer-group lag remains
unknown when only discovered-offset backlog is available.

### 35. Can you add acknowledgement, HTTP and browser p95 values into end-to-end p95?

No. They are different boundaries and populations, and the browser interval has
only one observation. The enqueue-to-ack timing uses one process's monotonic clock;
per-record Kafka-to-Spark transport was not correlated. Event-time lag is not
transport latency. [Latency definitions](../../bda/reliability/LATENCY.md).

### 36. What are the main bottlenecks, risks and next steps?

Small-workload setup/planning and some micro-batches exceeded the configured trigger;
SQL grouping/source scans benefited from measured indexes. Shared CPUs, validation,
state, small files and offline export may matter, but unprofiled causes are not
claimed proven. Next evaluations include durable outbox, secured replicated operation,
longer/multi-machine workloads and independent acoustic testing. The earlier legacy
broker data-loss incident is retained as an operational lesson, not erased by later
successful tests. [E15](EVIDENCE_INDEX.md#e15).
