# Phase 5 — integration and reliability plan

Initial audit: clean `feat/bda-reliability` at merged Phase 4 commit `59f4039`.
Reviewed prior validation reports (including the Phase 2 preservation incident),
Kafka/Spark launchers and settings, producer/consumer, checkpoint/output contracts,
SQLite export/API, WebSocket state and React views. Host: 2 CPUs, 7.8 GiB RAM,
about 5.1 GiB available, 8.2 GiB disk free. Docker's original application broker is
healthy, backed by its existing volume. Cached Spark 4.0.1 runtime is available.

Plan before implementation:

1. Keep the application broker, all existing volumes/checkpoints and historical
   evidence untouched. Start a uniquely named disposable broker with a loopback
   port and a new project-owned persistent volume. Bound Spark to one 3 GiB JVM.
2. Exercise real finite EchoTrace simulation through EventProducer, Kafka, streaming
   validation/dedupe/window/anomaly outputs, offline SQLite export, API and browser.
   Use unique topics/roots, compare counts/provenance, then restart checkpoints.
3. Inject failures only into the disposable broker; record drops/timeouts/recovery.
   Cover queue bounds, duplicates, malformed/out-of-order/late records and API loss.
4. Add safe atomic progress summaries with explicit stopped/error/stale states,
   watermark/rates/backlog where reported, output freshness and producer observations.
5. Run a resource-guarded, paced isolated workload with separate warmup and a
   60-second measured interval. Record actual counts, rates, acknowledgement timings
   and sampled resource data. Keep finite drain time separate from steady ingestion.
6. Optimize only against comparable measured baselines, recheck correctness, run
   regressions/build/browser checks, publish evidence and push logical commits.

Detailed results, reproduction, safety boundaries and limitations are in
`PHASE5_VALIDATION.md` and `reliability/OPERATIONS.md` after execution.

## Implemented architecture

`bda.reliability.run` owns a generated Compose project, dedicated loopback broker,
unique topics and run root. It publishes actual finite `Pipeline(SimSource)` events
through the normal bounded `EventProducer`, alongside clearly isolated fixtures
and watermark controls. Real Structured Streaming writes classified/quarantined,
deduped event, finalized window and anomaly Parquet. A separate Spark verifier
checks counts/provenance and exports SQLite. `bda.reliability.presentation` serves
that generated SQLite through the real FastAPI app, verifies HTTP responses and
runs Chromium against the real React data path. The wrapper combines these steps.
No mocked Kafka/Spark output substitutes for the integration test.

Producer observability adds bounded enqueue-to-ack samples, last acknowledgement/
error timestamps and recovery-aware evidence states. Spark's listener writes atomic
allowlisted telemetry with a separate writer heartbeat and progress timestamp.
The API distinguishes unavailable, stopped, error and stale/unknown query states;
System Health shows rates, durations, watermarks, checkpoint presence, discovered
backlog where available, verified quarantine/output counts and export freshness.
Consumer-group lag remains explicitly unavailable rather than inferred.

Correctness hardening fixes an empty-snapshot null count, preserves minute-level
streaming anomaly timestamps, and prevents configured alternative snapshots from
borrowing injected ground truth from a different dataset. Missing/corrupt outputs
and telemetry fail safely. Alternative serving paths are trusted process environment
configuration, never request parameters or response fields.

Covering indexes for category marginals and source IDs were adopted for **new**
exports after an identical-data before/after experiment. The original Phase 4
SQLite and Phase 3 benchmark evidence remain byte-for-byte unchanged. Read latency
improvement trades about 2.91 MiB additional index storage in the tested copy;
this is not an end-to-end pipeline or Spark speedup claim.

## Executed integration and workload

The successful isolated run verified eight real simulated acoustic events, one
intentional duplicate, a separately identified synthetic fixture, one malformed
record and a watermark control. Initial wire count 28 → 26 unique analytical
events, one quarantine, 25 finalized events in eight cells, two anomalies. After
restart, two newly published late records increased classified count to 30 but
left analytical totals unchanged. API and browser read those newly generated rows.

The controlled workload produced 1,500 warmup plus 6,000 measured-phase records
at a 100-events/s target. All 7,500 were acknowledged, ingested and validated, with
no workload producer failures/drops. An extra watermark control is tracked separately.
Ingestion ran concurrently; downstream event/window/anomaly stages were drained
sequentially to bound memory. This establishes the executed 60-second producer/
ingest workload and subsequent output correctness, not sustained full concurrent
pipeline capacity or capacity at higher suggested rates.

See [executed validation](PHASE5_VALIDATION.md), [reproduction and troubleshooting](reliability/OPERATIONS.md),
[reliability matrix](reliability/MATRIX.md), [latency definitions](reliability/LATENCY.md),
and [machine-readable evidence](reports/phase5/README.md).
