# Professor presentation specification

**Format:** 12 slides, approximately **13 minutes**, excluding the
separate hands-on demonstration. No PPTX is claimed or required. Use the supplied
original diagrams and committed screenshots/charts, with readable labels and a small
source footer. Fill institution/student/guide placeholders before submission.
All quantitative statements refer to historical committed evidence unless the
speaker explicitly identifies a newly reproduced run.

## Slide 1 — EchoTrace-BDA

**Time:** 0:30.

**Main content:** “Acoustic event streaming, historical analytics and measurable
reliability.” Student/institution/guide: placeholders. One sentence: an analytics
extension of the original EchoTrace acoustic system, not a newly trained audio model.

**Suggested visual:** project title plus three blocks: acoustic events → analytical
pipeline → evidence-backed dashboard. Avoid deployment/client logos suggesting adoption.

**Verified support:** [E01](EVIDENCE_INDEX.md#e01), [E14](EVIDENCE_INDEX.md#e14).

**Speaker notes:** “This project connects sound-event metadata to a reproducible
analytics workflow. I will separate the original acoustic capabilities from the
BDA extension and distinguish measured results from future work.”

## Slide 2 — From individual sounds to patterns over time

**Time:** 0:55.

**Main content:** a single detection gives limited temporal context; retained event
history enables activity comparisons, unusual-count windows and source analysis.
The engineering problem includes identity, bad input, late data and service failure.
Human judgment remains necessary; no emergency-response accuracy claim is made.

**Suggested visual:** one event card versus a short timeline and category distribution.
Use actual [overview](../../bda/reports/phase5/overview-desktop.png) rather than invented values.

**Verified support:** [original pipeline](../../backend/echotrace/pipeline.py),
[E07](EVIDENCE_INDEX.md#e07), [report problem statement](PROJECT_REPORT.md#2-problem-statement).

**Speaker notes:** “The use case motivates analysis, but this work does not measure
responder outcomes. My evaluation concerns whether the data path is correct,
inspectable and reproducible.”

## Slide 3 — Why a Big Data Analytics architecture?

**Time:** 1:00.

**Main content:** volume: up to 5m generated/benchmarked synthetic records; velocity:
a controlled 100-events/s target; variety: categories/sources/confidence/location;
veracity: validation/quarantine/deduplication; value: historical and anomaly exploration.

**Suggested visual:** five rows mapping each V to a repository artifact. Label volume
“SIMULATED metadata,” and velocity “60-second measured producer/concurrent-ingest experiment.”

**Verified support:** [E02](EVIDENCE_INDEX.md#e02), [E10](EVIDENCE_INDEX.md#e10),
[concept mapping](BIG_DATA_CONCEPTS.md).

**Speaker notes:** “A small workstation does not reproduce a data center. It can
still demonstrate event-time state, partitioned processing and reproducible large
record counts. I will not call local worker threads separate machines.”

## Slide 4 — Existing system and BDA extension

**Time:** 1:00.

**Main content:** retain CNN14 tagging, approximate direction, sequence/risk logic
and Acoustic Monitor. Add strict event contracts, Kafka side channel, streaming
state, historical lake/SQL, safe analytical APIs and reliability evidence.

**Suggested visual:** two-column comparison, with retained components visually distinct
from extensions. Use [original console source](../../frontend/src/AcousticConsole.tsx)
and the analytics overview screenshot as implementation references.

**Verified support:** [E01](EVIDENCE_INDEX.md#e01), [E14](EVIDENCE_INDEX.md#e14).

**Speaker notes:** “The BDA contribution is not ownership of PANNs or a new CNN.
The imported EchoTrace history, contributors and MIT attribution remain intact.
A good extension preserves working functionality and makes its boundary explicit.”

## Slide 5 — Technology choices and resource limits

**Time:** 0:45.

**Main content:** Python/FastAPI/Pydantic; Kafka 4.1.2; Spark 4.0.1 Structured
Streaming/SQL; Parquet; SQLite reviewed aggregates; React/TypeScript; Docker Compose.
Spark experiment: local threads, two CPU quota, 3 GiB RAM, 1 GiB heap.

**Suggested visual:** layer table with a “why” column, not a wall of logos.

**Verified support:** [recorded environment](../../bda/reports/phase3/environments.json),
[Phase 5 environment](../../bda/reports/phase5/environment.json),
[report technology tradeoffs](PROJECT_REPORT.md#8-technologies-and-justification).

**Speaker notes:** “SQLite serves bounded reviewed aggregates; it is not replacing
the Parquet lake. HTTP requests do not launch uncontrolled Spark jobs. Resource
limits are part of the methodology, not incidental setup details.”

## Slide 6 — Complete system architecture

**Time:** 1:15.

**Main content:** trace the acoustic WebSocket path and the separate producer/Kafka/
Spark/Parquet/export/API path. Historical generation and recorded benchmarks remain
separate inputs. The original acoustic view stays available.

**Suggested visual:** [full architecture diagram](ARCHITECTURE.md#1-full-system).
Highlight one path at a time; state which arrows represent an offline operator step.

**Verified support:** [E01](EVIDENCE_INDEX.md#e01), [E08](EVIDENCE_INDEX.md#e08),
[API implementation](../../backend/echotrace/api/analytics.py).

**Speaker notes:** “WebSocket charts are current browser-buffer counts. Spark
charts come from finalized exported windows. A shared dashboard must not erase
that difference or silently combine incompatible populations.”

## Slide 7 — Kafka and event-time Spark processing

**Time:** 1:30.

**Main content:** stable identity and original provenance; bounded queue and Kafka
worker; classified/quarantine → deduped events → finalized windows; checkpoint per
stage. Watermarks progress with event time. Trailing windows can remain unfinalized.

**Suggested visual:** [streaming-stage diagram](ARCHITECTURE.md#3-structured-streaming-stages)
plus one half-open window boundary example. Do not depict anomalies as a trained model.

**Verified support:** [schema](../../bda/schema.py), [transforms](../../bda/streaming/transforms.py),
[real restart outputs](../../bda/reports/phase5/e2e-restart.json).

**Speaker notes:** “A watermark bounds state and lateness behavior; it is not a
wall-clock timer. Native sink/checkpoint recovery only helps after records reach
the relevant durable boundary. It cannot restore a producer queue lost before Kafka.”

## Slide 8 — Historical SQL and explainable anomalies

**Time:** 1:15.

**Main content:** largest complete lake: 1m validated events. Default UI: a separate
100k snapshot. Hour/day/source/category analysis; count-weighted confidence means.
Anomaly: absolute threshold or a preceding-observed-window baseline condition;
injected ground truth is separate from detected flags.

**Suggested visual:** historical date-filter result and [anomaly/source values](../../bda/reports/phase5/e2e-first.json).
Display Jan 1 inclusive–Jan 3 exclusive UTC: 28,426 events in the default snapshot.

**Verified support:** [E05](EVIDENCE_INDEX.md#e05), [E06](EVIDENCE_INDEX.md#e06),
[E07](EVIDENCE_INDEX.md#e07), [SQL](../../bda/batch/queries.py).

**Speaker notes:** “The historical detector uses previous observed windows, not
zero-filled empty hours. A null z-score can be mathematically meaningful. I expose
recorded baselines and flags instead of inventing severity or model confidence.”

## Slide 9 — Dashboard demonstration preview

**Time:** 1:00.

**Main content:** six analytical views plus Acoustic Monitor; visible provenance,
loading/empty/error states and refresh; current process observations versus recorded
exports. Indicate whether the upcoming demonstration is recorded-output fallback
or a newly generated isolated pipeline run.

**Suggested visual:** [generated pipeline browser screenshot](../../bda/reports/phase5/pipeline-dashboard.png)
and [health screenshot](../../bda/reports/phase5/system-health.png).

**Verified support:** [E07](EVIDENCE_INDEX.md#e07), [E08](EVIDENCE_INDEX.md#e08),
[browser transcript](../../bda/reports/phase5/pipeline-browser.txt), [demo script](DEMO_SCRIPT.md).

**Speaker notes:** “A stopped query and available recorded export can both be true.
A healthy-looking page does not prove Kafka or Spark is live. The later hands-on
script shows the actual data source before interpreting a number.”

## Slide 10 — Measured performance, with boundaries

**Time:** 1:40.

**Main content:** 72 recorded results = 54 measured + 18 warmups. At 5m for one raw
aggregation bundle: Python 70.6890 s, Spark 32.7002 s, about 2.16x, setup excluded.
Python faster at 100k. Category SQL on the aggregate copy: 31.27 → 5.88 ms after
indexes, with extra storage. The 100-events/s experiment is a different workload.

**Suggested visual:** [original Spark/Python chart](../../bda/reports/phase3/charts/spark-versus-python.png)
plus a small [serving-index chart](../../bda/reports/phase5/serving-indexes.png).
Keep axes, units, repetitions and source footer visible.

**Verified support:** [E03](EVIDENCE_INDEX.md#e03), [E04](EVIDENCE_INDEX.md#e04),
[E10](EVIDENCE_INDEX.md#e10), [E11](EVIDENCE_INDEX.md#e11).

**Speaker notes:** “The ratio is not a universal recommendation. Startup recorded
22.10 seconds separately at the largest configuration. The index result concerns
SQLite read queries, not Spark. I do not add separate latency percentiles into an
invented end-to-end percentile or extrapolate an unexecuted 10m result.”

## Slide 11 — Reliability findings and honest failure semantics

**Time:** 1:30.

**Main content:** actual Kafka-to-browser verification; duplicate/quarantine checks;
checkpoint restart 28 → 30 classified but 26 unique/25 finalized unchanged. Outage:
two surrounding records delivered, one during-outage timeout lost. Stale heartbeat,
progress, checkpoint presence and export freshness are distinct observations.

**Suggested visual:** [recovery diagram](ARCHITECTURE.md#6-recovery-and-checkpoints)
and a three-row expected/observed failure table from the matrix.

**Verified support:** [E08](EVIDENCE_INDEX.md#e08), [E09](EVIDENCE_INDEX.md#e09),
[E12](EVIDENCE_INDEX.md#e12), [reliability matrix](../../bda/reliability/MATRIX.md).

**Speaker notes:** “Idempotent Kafka submission and checkpointed Spark do not remove
the producer's best-effort loss window. Earlier legacy data loss is disclosed in
Phase 2 rather than erased. All destructive failure injection here used a newly
owned broker, not the existing application service.”

## Slide 12 — Conclusion and next verification steps

**Time:** 0:40.

**Main content:** achieved: traceable local data flow, checked historical/benchmark
outputs, bounded serving and observed recovery behavior. Future: durable outbox,
secured replicated deployment, longer/multi-machine experiments and remaining
per-record latency instrumentation. Thank reviewers; direct them to the evidence index.

**Suggested visual:** three achieved outcomes, three future evaluations, repository
URL and [report/evidence links](EVIDENCE_INDEX.md). Avoid “production ready” badges.

**Verified support:** [report limitations](PROJECT_REPORT.md#23-limitations-and-threats-to-validity),
[future scope](PROJECT_REPORT.md#24-future-scope), [release checks](../../bda/PHASE6_VALIDATION.md).

**Speaker notes:** “The project's strongest result is an inspectable, reproducible
engineering demonstration with explicit limits. I can show the exact input
population, raw measurement and recovery outcome behind each headline.”

## Presenter checklist

Use the [demo script](DEMO_SCRIPT.md) to prepare dependencies and choose the demo
track before class. Keep the evidence index open for questions, preserve original
chart labels, disclose SIMULATED data on every relevant slide, and fill student/
institution placeholders without inventing approval. The separate live run takes
minutes; do not promise it can complete inside a one-minute slide slot.
