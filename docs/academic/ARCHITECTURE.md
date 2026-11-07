# Implementation architecture

These diagrams describe the merged implementation, not a proposed cluster.
Execution is local Spark in Docker; Kafka is a single KRaft broker with three-partition
application/test topics and replication factor one. Path ownership and lineage matter
more than the visual separation of boxes. [Evidence inventory](EVIDENCE_INDEX.md#e01).

## 1. Full system

```mermaid
flowchart LR
    A["LIVE microphone or RECORDED WAV"] --> B["Original EchoTrace pipeline"]
    S["Clearly SIMULATED acoustic source"] --> B
    B --> W["Acoustic WebSocket and existing APIs"]
    W --> U["React analytics and Acoustic Monitor"]
    B --> P["Bounded EventProducer side channel"]
    P --> K["Kafka event topic"]
    K --> C["Spark classified and quarantine"]
    C --> E["Spark deduped events"]
    E --> F["Finalized event-time windows"]
    F --> N["Offline anomaly snapshot"]
    E --> H["Optional historical lake import"]
    G["Deterministic synthetic JSONL"] --> H
    H --> Q["Historical Spark SQL"]
    F --> X["Controlled offline SQLite export"]
    N --> X
    H --> X
    X --> R["Read-only FastAPI analytics"]
    M["Recorded benchmark reports"] --> R
    T["Safe telemetry snapshots"] --> R
    R --> U
```

**Code:** [pipeline](../../backend/echotrace/pipeline.py),
[server bridge](../../backend/echotrace/api/server.py), [streaming job](../../bda/streaming/job.py),
[lake](../../bda/batch/lake.py), [exporter](../../bda/analytics/export.py).
The engine's broadcaster is the actual Kafka submission boundary. The direct
pipeline-to-producer arrow denotes this side-channel integration, not Kafka I/O on
the acoustic worker thread. Historical and streaming exports are selected operator
workflows, not automatic lake refresh on every event.

## 2. Kafka producer and consumer flow

```mermaid
flowchart LR
    A["Emitted acoustic event"] --> B["Generation check and v1 adaptation"]
    B --> Q["Bounded in-memory queue"]
    Q --> P["Kafka worker: idempotence and acks all"]
    P --> K["Topic partitions keyed by record_id"]
    K --> S["Spark Kafka source"]
    K --> D["Optional development consumer"]
    P --> M["Acknowledged and failed counters"]
    Q --> X["Queue-full drop counter"]
    B --> I["Invalid-event counter"]
    D --> V["Schema validation and display"]
    V --> O["Explicit development offset commit"]
```

**Code:** [adapter](../../bda/event_adapter.py), [producer](../../bda/producer.py),
[consumer](../../bda/consumer.py), [settings](../../bda/config.py).
The producer has no durable outbox. `acks=all` cannot provide replicated durability
with a single broker and replication factor one. The development consumer reports
invalid input then advances its offset by policy; Spark uses its own checkpoints,
not those development-consumer commits.

## 3. Structured Streaming stages

```mermaid
flowchart TD
    K["Kafka raw value and coordinates"] --> I["Ingest query: strict validation"]
    I --> C["Classified Parquet: accepted and quarantine"]
    I -.-> CI["Ingest checkpoint"]
    C --> D["Events query: event-time watermark and bounded ID dedupe"]
    D --> E["Events Parquet partitioned by event date"]
    D -.-> CD["Events checkpoint"]
    E --> A["Windows query: count and confidence mean"]
    A --> W["Append-only finalized window Parquet"]
    A -.-> CW["Windows checkpoint and state"]
    W --> N["Bounded-history batch anomaly calculation"]
    N --> Z["Immutable snapshot and atomic current pointer"]
    I --> T["Progress journal and atomic telemetry"]
    D --> T
    A --> T
```

**Code:** [job](../../bda/streaming/job.py), [transforms](../../bda/streaming/transforms.py),
[ownership guards](../../bda/streaming/storage.py), [listener](../../bda/streaming/metrics.py).
Separate processes may run continuous stages, but the recorded resource-bounded
campaign used concurrent ingestion and sequential downstream draining. Anomaly
calculation is a batch over finalized windows, not a second stateful streaming
operation. Idle wall time does not advance event-time watermarks.

## 4. Historical processing

```mermaid
flowchart LR
    G["Seeded synthetic generator"] --> J["Chunked JSONL and manifest"]
    P["Existing Phase 2 events root"] --> L["New immutable historical build"]
    J --> L
    L --> V["Validated deduped Parquet"]
    L --> Q["Quarantine and provenance manifest"]
    V --> A["Hourly and daily aggregates"]
    V --> N["Hourly-window anomaly table"]
    V --> M["Completion marker after successful build"]
    V --> SQL["Allowlisted Spark SQL queries"]
    M --> SQL
    SQL --> O["Bounded reviewed query exports"]
    J --> B["Separate checked Python and Spark benchmark"]
    B --> R["Trial journals and environment records"]
```

**Code:** [generator](../../bda/batch/generate.py), [lake builder](../../bda/batch/lake.py),
[query definitions](../../bda/batch/queries.py), [benchmark](../../bda/batch/benchmark.py).
The completion marker is a build-level publication guard, not a distributed ACID
transaction protocol. Large raw/Parquet datasets remain outside Git.

## 5. Dashboard serving

```mermaid
flowchart LR
    P["Completed historical or finalized streaming Parquet"] --> E["Operator-run Spark exporter"]
    E --> S["Immutable SQLite aggregates and provenance"]
    S --> API["Versioned read-only analytics API"]
    B["Committed raw benchmark measurements"] --> API
    T["Allowlisted heartbeat and progress files"] --> API
    C["Current producer process observations"] --> API
    API --> U["Six analytics views"]
    W["Original acoustic WebSocket"] --> U
    W --> O["Original Acoustic Monitor"]
```

**Code:** [API](../../backend/echotrace/api/analytics.py),
[telemetry reader](../../bda/analytics/observability.py),
[React app](../../frontend/src/App.tsx), [original console](../../frontend/src/AcousticConsole.tsx).
HTTP requests do not launch Spark. SQL identifiers are allowlisted, values bound,
responses paginated, and SQLite work budgeted. WebSocket charts use a bounded
browser buffer; their population is distinct from Spark-exported statistics.

## 6. Recovery and checkpoints

```mermaid
flowchart TD
    A["Existing run root with committed files and checkpoints"] --> B["Restart with same compatible configuration"]
    B --> C["Exclusive stage lock and configuration guard"]
    C --> D["Kafka/file offsets and operator state restored"]
    D --> E["Process newly available input"]
    E --> F["Native file sink commits and checkpoint progress"]
    F --> G["Verify counts, provenance and finality"]
    B --> X["Changed source or state settings"]
    X --> Y["Refuse reuse; choose a new root"]
    E --> L["Late input may be rejected by watermark"]
    P["Producer timeout or queue drop before Kafka"] --> N["Not recoverable from Spark checkpoint"]
```

**Code/tests:** [claim guard](../../bda/streaming/storage.py),
[streaming recovery tests](../../bda/streaming/tests/test_spark.py),
[real isolated restart verification](../../bda/reliability/run.py).
Checkpoint recovery was tested; abrupt disk failure, replicated broker failover
and end-to-end exactly-once delivery were not established. Read datasets at their
Spark roots so native metadata logs are honored. Never delete checkpoints separately
from the outputs and source epoch they describe.
