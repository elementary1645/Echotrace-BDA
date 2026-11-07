# Phase 5 validation — November 7, 2026

## Initial state and preservation

Initial branch `feat/bda-reliability`, clean at `59f4039` (merged Phase 4); no
AGENTS.md present. Reviewed Phase 1–4 code, deployment/configuration and validation,
including the prior Kafka preservation incident. Host preflight: 2 CPUs, 7.8 GiB
RAM, about 5.1 GiB available, about 8.2 GiB disk free, no swap. Docker 29.9.2;
Spark/PySpark 4.0.1 with Python 3.10.12/Java 17.0.18 in the existing pinned image;
host Python 3.14.2 and Node 24.22.2. Exact image IDs and preflight values are in
the evidence environment/receipt.

The application broker stayed running and retained container identity
`9e59a3133bf966ca4aa0b4d02748a279ec4592fbbccfee67c5bb2f5e1a9891c6` before/after
the isolated run. No application topic, volume or checkpoint was reset, deleted or
recreated. Existing Ivy artifacts were copied from a read-only mount. Phase 3
reports, Phase 4 SQLite and original screenshots remain unchanged. New artifacts
and screenshots use `bda/reports/phase5` and ignored `artifacts/phase5` run roots.

## Actual integration commands and outcomes

Executed the full runner and presentation commands separately (the supplied
`infrastructure/reliability/run.sh verify` wrapper combines them with a build):

```bash
.venv/bin/python -m bda.reliability.run \
  --output artifacts/phase5/run-20261107-d --port 19096 --campaign
npm --prefix frontend run build
.venv/bin/python -m bda.reliability.presentation --run artifacts/phase5/run-20261107-d
```

Isolated project `echotrace-p5-56d29fa11f14`, new three-partition topics, new run
roots/checkpoints and new persisted Kafka volume. All run/presentation steps passed.
The test broker was stopped after each use; its volume/evidence were retained.
A dry-run cleanup ownership audit passed; no cleanup destruction was executed.

| Stage | Actual observation |
| --- | --- |
| Actual finite EchoTrace break-in simulation | 8 emitted acoustic events, source SIMULATED |
| Normal EventProducer | 9 acknowledgements including one same-ID replay |
| Combined dedicated-topic wire population | 28 records: acoustic 9, fixture 17, control 1, malformed 1 |
| Independent Kafka consumer | 28 received; 27 schema-valid SIMULATED, 1 malformed |
| Spark classified/quarantine | 28 classified, 1 quarantined |
| Spark validated/deduped | 26 unique, including the separate trailing control |
| Finalized window population | 25 events in 8 cells, acoustic ALARM=4 / GLASS=2 / IMPACT=2 |
| Detected anomalies | 2 windows with configured threshold 3; actual acoustic ALARM=4 is flagged |
| Restart and genuinely new Kafka offsets | 2 additional late records; classified 30, quarantine 1, unique events 26, finalized events 25, anomalies 2 |
| API generated export | Provenance streaming-finalized; 25 events, acoustic subset 8; no population substitution |
| Chromium generated-data presentation | 1 passed, exact source rows/provenance and stopped Spark telemetry visible; no page errors |

The minute-3 fixture anomaly is retained at `2026-01-01T00:03:00Z`, independently
checked through the API. This caught/fixed the old exporter rounding streaming
anomaly times to the hour. Unknown streaming anomaly policy is labeled unrecorded,
not assigned a fabricated threshold or model severity.

Job timings include setup: first full streaming job 61.40 s, first verification/
export 39.31 s, checkpoint-restart job 46.79 s, restart verification 37.74 s.
They are orchestration durations, not per-event latency percentiles.

## Controlled failure results

- Producer before broker startup: 1 accepted submission, 1 eventual delivery
  failure, no acknowledgement; worker shut down with queue zero.
- Temporary owned-broker outage: before-outage and after-outage events acknowledged
  and consumed (2); during-outage event timed out (1 failure) and was not consumed.
  The same worker recovered to `recent acknowledgement` while retaining error
  counters. It ended stopped with worker_alive false. This demonstrates a real
  best-effort loss window, not exactly-once delivery.
- Queue saturation: a blocked real worker with queue capacity 1 accepted queued
  work, rejected the next submission, incremented dropped_queue_full, then drained
  two successes after release. Executed in the reliability unit suite.
- Replayed duplicate, deliberately reordered fixture minutes and post-watermark
  late arrivals were exercised through actual Kafka/Spark, not a fake source.
  Restart telemetry recorded two rows dropped by watermark in the event stage.
- Missing/corrupt/empty serving exports and renamed/restored SQLite files were
  tested with sanitized 503/zero/empty outcomes. Chromium offline/online API recovery
  verified error → Retry → successful data. Reconnect hook tests still pass.
- Writer heartbeat/progress freshness, malformed/oversized telemetry, unavailable
  consumer-group lag and safe response redaction were tested separately.
- Continuous ingestion acknowledged the stop marker and exited **0**. Producer
  workers joined; API logs recorded `Application shutdown complete.` on shutdown.
  Uvicorn's process exit after signal was -15 after its graceful shutdown log;
  this is recorded rather than represented as an exit-zero observation.

Three initial harness attempts are retained and summarized in `harness-failures.json`:
(a) temporary AdminClient lifetime destroyed pending topic futures; fixed by holding
its reference, (b) a 1-second healthy-path timeout was too short for coordinator
startup; restored 10 seconds for healthy delivery while retaining short deliberate
failure tests, (c) port preflight refused an address-in-use condition; no listener
was stopped and the next attempt used another dedicated port. None is counted as
a passing end-to-end run. Keyboard test development also exposed incorrect native
select assumptions; final test waits for options/results and uses ArrowDown/Enter.

## Measured 100-events/s workload

Requested rate **100 events/s**, 15 s warmup (1,500 records), then 60 s measured
(6,000 records). Measured enqueue interval **60.000084 s**, actual produced rate
**99.99986 events/s**. All 6,000 acknowledged; acknowledgement/drain interval
**60.050219 s**, acknowledged rate **99.91637 events/s**. Entire workload: 7,500
enqueued/submitted/delivered, zero observed error/drop counters, queue zero at close.

Continuous Kafka ingestion observed all 7,500. A separate watermark control made
7,501 classified/validated records at final verification, zero quarantine, exactly
7,500 finalized workload events. Source IDs separate the control from the workload.
One high-count window was detected by the unchanged configured anomaly definition.

Fifteen nonempty ingestion batches include warmup and drain. Spark input rates
were near 100 during the paced interval; processed rates vary with active-batch
execution and must not be confused with sustained arrival capacity. The initial
batch took 16.94 s and another took 5.34 s against a 5 s trigger. Raw measurements
and [rate chart](reports/phase5/streaming-rates.png) retain these slower batches.
The downstream finite drain took 59.43 s and verification 36.34 s, including setup.

Docker sampling across the isolated run observed Spark up to about **1.01 GiB**
and **172.5% of one CPU**, Kafka about **461.6 MiB** and **103.25%**. These are
samples derived from rounded Docker readings, not profiler peaks or exclusive
hardware allocation. Limits: Spark 2 CPU/3 GiB/1 GiB heap; broker 1 CPU/1 GiB/256 MiB
heap. Consumer-group lag is unavailable; last discovered-offset backlog was zero
where reported. Higher suggested rates were not executed or extrapolated.

This is a 60-second paced producer/concurrent-ingest experiment with sequential
completion of downstream stages. No sustained full concurrently running platform
capacity, long-duration endurance or multi-machine scaling claim is made.

## Latency evidence

Definitions and unavailable boundaries: [LATENCY.md](reliability/LATENCY.md).
Successful enqueue-to-ack population: 9 acoustic submissions, p50 **996.40 ms**,
p95 **997.91 ms**, p99 **997.97 ms**; includes coordinator/queue effects, not source
creation time. Workload's bounded last 4,096 measured acknowledgements: p50
**10.26 ms**, p95 **29.99 ms**, p99 **105.43 ms**. Failed submissions are not silently
included as successes. The first 1,500 warmup records are excluded from these
workload percentiles.

Generated-export real HTTP requests: 20 measured after one warmup, p50 **3.11 ms**,
p95 **4.30 ms**, p99 **4.54 ms**. Owned server start to first verified API response
was **1,017.34 ms**. Browser response-received to exact row visible was **173.00 ms**
in **one** observation (navigation to row visible 633.43 ms); no browser percentile
is inferred. No incompatible event-time/monotonic clocks were subtracted.

## Justified optimization

Executed, after the streaming campaign finished:

```bash
.venv/bin/python -m bda.reliability.optimize --output artifacts/phase5/index-experiment-001
```

Same 68,160-cell SQLite copy, same SQL/results, before and after adding covering
category/event-count and source-ID indexes; 20 measured sequential reads after two
warmups per query. Category median **31.27 → 5.88 ms** (5.32x), distinct-source count
**14.77 → 3.81 ms** (3.88x). Query plans confirm covering scans and removal of the
category group temporary B-tree. Bytes **10,285,056 → 13,340,672** (+29.7%).

Indexes now apply to newly generated exports and are integration-tested. No
historical report/export was edited. These are query-specific warm-cache local
improvements with storage/preparation cost, not a Spark or full API speedup claim.
No speculative producer/Spark tuning changed the analytical definitions.

## Regression and build checks

Actual executed results:

- `PHASE5_RUN_DIR="$PWD/artifacts/phase5/run-20261107-d" .venv/bin/python -m pytest -q`:
  **271 passed, 14 skipped in 18.22 s**, existing Starlette deprecation warning.
  Includes generated-output API checks. Skips: old opt-in Kafka tests (2), local
  Spark tests (4), batch Spark tests (5), and weight-dependent tests (3). Actual
  Spark/Kafka checks run independently below; no neural weights/microphone were used.
- Isolated pinned container with `RUN_SPARK_TESTS=1`, repository mounted read-only
  and **no application data volumes**: **19 passed, 1 skipped in 51.32 s**. Includes
  actual transformation/state/watermark/checkpoint recovery tests. The skipped
  legacy full Kafka integration is superseded in scope by the separately executed
  Phase 5 run, not counted as passing.
- `infrastructure/batch/run.sh test`: **20 passed in 94.95 s**, including export
  indexes, historical SQL and validated lake correctness in the pinned runtime.
- Existing real Kafka test on a new topic in the owned disposable broker, with
  `RUN_KAFKA_INTEGRATION=1 KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19096`:
  **1 passed in 4.00 s**, eight actual pipeline events acknowledged/consumed;
  the test broker was stopped again afterward.
- Reliability/generated-output focused run: **16 passed in 1.50 s**.
- Frontend components/hooks: **8 passed**. Production TypeScript/Vite build succeeded,
  main JS about 168.40 kB (54.93 kB gzip), lazy acoustic console 18.72 kB (6.86 kB gzip).
- Chromium default-view/mobile/error/recovery/keyboard: **5 passed in 7.7 s**; the separate genuine pipeline-output browser test
  **1 passed in 4.1 s**.
- `npm audit --audit-level=moderate`: **0 vulnerabilities** for the unchanged lock.
- Python compileall, shell syntax and `git diff --check`: passed.

## Limitations

No end-to-end exactly-once, durable producer outbox, physical storage-failure test,
HA, remote-machine failover, neural inference or microphone accuracy validation is
claimed. Telemetry is local atomic filesystem observation, not an authenticated
production monitoring service. Heartbeat/progress can become stale; output counts
are timestamped offline verifications. No per-record Spark transport latency or
true committed consumer-group lag was established. Plaintext single-broker local
development security and retained-artifact disk growth remain operator concerns.
See the complete [operations/safety guide](reliability/OPERATIONS.md) and
[scenario matrix](reliability/MATRIX.md). All prior limitations and the documented
Phase 2 preservation incident remain intact.

## Final release checks

- Final default-dashboard Chromium suite: **5 passed in 7.7 s**, including real
  navigation/filter data, mobile bounds, deliberate 503, offline/online API recovery,
  and native keyboard navigation/filter activation. The separate generated-output
  browser check passed as recorded above. Running all browser files without the
  dedicated generated-output URL intentionally skips that one opt-in test.
- Last-successful telemetry progress is retained across the same checkpoint query
  identity, with a regression that rejects carrying it across a different identity.
- A presentation receipt and published report files refuse overwriting. Raw failed
  attempts, disposable volumes and run checkpoints remain retained.
- No Phase 3/4 report files differ from the merged base. Previous screenshots were
  restored from Git after the inherited browser test initially targeted Phase 4;
  all new screenshot targets now use Phase 5.
