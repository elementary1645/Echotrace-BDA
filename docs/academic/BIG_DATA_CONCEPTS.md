# Big Data concepts: implementation reference

Use this sheet during the professor demonstration. A technology's general ability
to run on a cluster is not evidence that this project measured a multi-machine
cluster. Quantitative observations are mapped in the [evidence inventory](EVIDENCE_INDEX.md).

| Concept | Where EchoTrace-BDA demonstrates it | Inspectable implementation or test | Boundary to explain |
| --- | --- | --- | --- |
| Volume | Deterministic metadata datasets up to 5m; completed lake up to 1m | [generator](../../bda/batch/generate.py), [manifests](../../bda/reports/phase3/datasets.json), [storage](../../bda/reports/phase3/storage.json) | Synthetic event rows, not millions of audio classifications; 10m was not executed |
| Velocity | Kafka arrivals and micro-batch ingestion; 100-events/s controlled target | [paced producer](../../bda/reliability/workload.py), [campaign receipt](../../bda/reports/phase5/run-receipt.json) | 60 measured seconds after warmup; downstream stages drained sequentially |
| Variety | Source classifications, categories, confidence, localization, timestamps and valid/invalid records | [v1 schema](../../bda/schema.py), [validation](../../bda/streaming/validation.py) | A structured event schema, not unrestricted multimodal/raw-audio analytics |
| Veracity | Strict validation, identity checks, quarantine, deterministic checksums and independent totals | [validation tests](../../bda/streaming/tests/test_validation.py), [batch baseline](../../bda/batch/baseline.py), [quality fixture](../../bda/batch/fixtures.py) | Validation establishes contract consistency, not acoustic truth |
| Value | Source/category/time exploration, explainable anomalies, measured tradeoffs | [dashboard](../../frontend/src/App.tsx), [SQL](../../bda/batch/queries.py) | Technical utility is demonstrated; business/response outcome improvement is not measured |
| Distributed-processing model | Spark partitions, tasks, grouping/shuffle and local concurrency comparisons | [benchmark](../../bda/batch/benchmark.py), [task metrics](../../bda/reports/phase3/task-metrics.json) | Executed `local[1]`/`local[2]`; no separate-machine worker/failure-domain scaling |
| Event streaming | Optional asynchronous event publication to a Kafka topic and streaming readers | [producer](../../bda/producer.py), [stream job](../../bda/streaming/job.py), [real pipeline runner](../../bda/reliability/run.py) | Kafka I/O is a side channel; its failure need not stop the acoustic source |
| Partitioning | Kafka topic partitions; event-date Parquet directories; configurable Spark input/shuffle work | [Kafka Compose](../../infrastructure/kafka/compose.yml), [lake](../../bda/batch/lake.py), [benchmark](../../bda/batch/benchmark.py) | Three Kafka partitions do not mean three brokers; requested and actual Spark input partitions can differ |
| Windowing | One-minute streaming groups and one-hour historical anomaly groups, half-open intervals | [aggregate](../../bda/streaming/transforms.py), [Spark boundary tests](../../bda/streaming/tests/test_spark.py) | Finalized window rows differ from acoustic sequence patterns and live frontend counters |
| Watermarks | Event-time progress bounds late-state retention and finalization | [stream stages](../../bda/streaming/job.py), [watermark/restart tests](../../bda/streaming/tests/test_spark.py) | Idle wall time does not advance them; trailing windows can remain pending |
| Deduplication | Stable SHA-256 record identity and watermark-bounded state; historical unique-ID selection | [identity/schema](../../bda/schema.py), [transforms/job](../../bda/streaming/job.py), [restart output](../../bda/reports/phase5/e2e-restart.json) | Streaming suppression is not a permanent global dedupe guarantee |
| ETL | Raw JSON/Kafka → validate/canonicalize → accepted/quarantine → typed event/aggregate storage | [adapter](../../bda/event_adapter.py), [validation](../../bda/streaming/validation.py), [lake builder](../../bda/batch/lake.py) | Quarantine is retained and inspectable, not silently deleted |
| Data lake | Immutable completed historical run roots with validated Parquet, provenance and analytical tables | [lake](../../bda/batch/lake.py), [historical validation](../../bda/PHASE3_VALIDATION.md) | Local files and build guards; not a claimed distributed ACID table format |
| Spark SQL | Allowlisted totals, hourly/daily/source/category/zone/trend/rolling/anomaly queries | [SQL definitions](../../bda/batch/queries.py), [executed example](../../bda/reports/phase3/historical-hourly.json), [SQL tests](../../bda/batch/tests/test_spark.py) | HTTP serving uses reviewed SQLite aggregates and does not launch arbitrary Spark SQL |
| Batch versus stream | Streaming state/append outputs; finite historical SQL, anomaly recomputation and benchmark scans | [stream job](../../bda/streaming/job.py), [batch lake](../../bda/batch/lake.py), [benchmark](../../bda/batch/benchmark.py) | `availableNow` is a finite streaming execution; anomaly snapshot calculation is a separate batch |
| Fault tolerance | Tested stage restart, native sink commits and explicit detection of unavailable/stale services | [storage guards](../../bda/streaming/storage.py), [matrix](../../bda/reliability/MATRIX.md), [outage receipt](../../bda/reports/phase5/run-receipt.json) | Single broker, producer loss windows and no physical-storage/replicated-failover validation |
| Checkpointing | Separate ingest/events/windows progress and state under compatible run roots | [job](../../bda/streaming/job.py), [checkpoint tests](../../bda/streaming/tests/test_spark.py), [recovery diagram](ARCHITECTURE.md#6-recovery-and-checkpoints) | Not development consumer-group commits; do not delete a checkpoint independently of its outputs |
| Performance benchmarking | Repeated correct-output trials, warmup labels, environment/cgroup observations and setup exclusion | [raw trials](../../bda/reports/phase3/trials.json), [benchmark](../../bda/batch/benchmark.py), [index experiment](../../bda/reliability/optimize.py) | 54 measured + 18 warmup results; query-specific ratios, uncontrolled OS cache, no universal speedup |
| Observability and freshness | Producer acknowledgements, safe Spark heartbeat/progress, verified output summaries and export timestamps | [telemetry writer](../../bda/streaming/telemetry.py), [reader](../../bda/analytics/observability.py), [System Health](../../frontend/src/App.tsx) | Process alive, progress fresh and data current are different claims; true consumer-group lag may be unavailable |

## Explaining local Spark precisely

Spark's logical plan, task execution, partitions, shuffle and checkpoint semantics
are concrete parts of the implementation. `local[2]` supplies two local worker
threads inside one host's execution environment. The recorded Spark container has
a two-CPU quota, a 3 GiB memory cap and a 1 GiB heap; these do not describe reserved
independent machines. Local[1]/local[2] timings examine local concurrency and setup
amortization. They do not measure inter-host network transfer, replicated broker
failure, cluster scheduler behavior or horizontal scaling.

A defensible explanation is: “I implemented an architecture using distributed-
processing abstractions and validated it locally with measured resource limits.”
Avoid: “I proved multi-node distributed scaling” or “Kafka plus Spark made every
application event exactly once.” See [E04](EVIDENCE_INDEX.md#e04),
[E09](EVIDENCE_INDEX.md#e09), [E10](EVIDENCE_INDEX.md#e10).

## Three useful cross-checks during viva

1. **Population:** Is this value an acoustic buffer, finalized window, historical
   snapshot, benchmark record count or telemetry observation?
2. **Time:** Is this event time, processing progress, export time, enqueue-to-ack
   monotonic duration or observed UI presentation time?
3. **Guarantee:** Is the statement implemented, actually tested under a specified
   scenario, or proposed as future work?

The answer should identify a file/receipt above. A framework's advertised general
capability is not sufficient evidence for a project-specific experimental claim.
