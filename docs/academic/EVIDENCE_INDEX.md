# Evidence inventory and claim boundaries

Audit baseline: `33ce36e`, merged local `main` and `origin/main`, November 7, 2026.
The delivery branch is `docs/bda-final-delivery`; initial working tree was clean.
All five BDA phase tips are ancestors of main. Earlier acoustic-development phase
numbers in original commits describe the original EchoTrace project, not these
five BDA extension phases. This index is the claim authority for the new report,
slides, README and portfolio. Results below are historical committed observations;
Phase 6 release checks are recorded separately in [PHASE6_VALIDATION](../../bda/PHASE6_VALIDATION.md).

## How to read the evidence

A source implementation establishes what is implemented; an executed validation
report establishes what was tested. A saved measurement establishes its measured
population, not general capacity. Large raw datasets and Parquet/checkpoints are
ignored local artifacts; their manifests, selected outputs, journals and validation
receipts are committed. New evaluators can reproduce them using the documented
commands rather than assuming those ignored directories exist.

<a id="e01"></a>
## E01 — Integrated implementation and provenance

**Claim:** Kafka ingestion, streaming, historical analytics, dashboard and reliability
work are merged. Merge commits: `651e428`, `3871ef0`, `71c25fe`, `59f4039`, `33ce36e`.
Tips checked with `git merge-base --is-ancestor`: `d012585`, `aaf8d6a`, `de207f7`,
`91b7f83`, `56c9288`.

**Sources:** [wire schema](../../bda/schema.py), [adapter](../../bda/event_adapter.py),
[producer](../../bda/producer.py), [API](../../backend/echotrace/api/analytics.py),
[React workspace](../../frontend/src/App.tsx).
**Boundary:** merged code alone does not prove every runtime mode was exercised.

<a id="e02"></a>
## E02 — Generated dataset volume

**Claim:** deterministic **100,000**, **1,000,000** and **5,000,000** event-record
datasets were generated. All are **SIMULATED synthetic metadata**, not five million
neural detections or audio clips. The 5m raw JSONL dataset has **2,411,926,657 bytes**.

**Evidence:** [dataset manifests](../../bda/reports/phase3/datasets.json),
[executed generation record](../../bda/PHASE3_VALIDATION.md),
[generator and resource guard](../../bda/batch/generate.py).
5m dataset ID: `acd0fb5afb346f9a7bc54b9bc49670c729d32b354f6db61daab41131fab7042f`.
**Boundary:** the 10m request was refused for insufficient disk; no 10m result exists.

<a id="e03"></a>
## E03 — Benchmark trial population

**Claim:** **72 successful recorded results = 54 measured trials + 18 warmups**,
across nine dataset/configuration combinations and two engines. Each engine has
one warmup plus three measured repetitions per configuration.

**Evidence:** [all results](../../bda/reports/phase3/trials.json),
[original journals](../../bda/reports/phase3/journals/),
[campaign commands](../../bda/reports/phase3/campaign.json),
[benchmark implementation](../../bda/batch/benchmark.py).
**Boundary:** never describe all 72 as measured non-warmup trials or independent
hardware experiments. Min/max bars are observed ranges, not confidence intervals.

<a id="e04"></a>
## E04 — Workload-specific Spark/Python comparison

**Claim:** at 5m records, median Python **70.689027 s**, Spark **32.700215 s**:
ratio **2.1617**, reported as **about 2.16x**. Workload: category count/mean,
UTC hourly count and source count, using the same raw JSONL and independently
checked outputs. Spark `local[2]`, requested input partitions 4, shuffle partitions 4.

**Evidence:** `5m-local2-p4-s4` rows in [trials](../../bda/reports/phase3/trials.json),
[environment](../../bda/reports/phase3/environments.json),
[validation analysis](../../bda/PHASE3_VALIDATION.md),
[original chart](../../bda/reports/phase3/charts/spark-versus-python.png).
Measured Python samples: 68.482873, 71.538943, 70.689027 s. Spark: 34.781886,
32.589476, 32.700215 s. Recorded setup **22.103096 s** is excluded from those medians.

**Boundary:** generation, lake conversion, setup and validation overhead are not
included in this speedup. OS cache was uncontrolled; no persisted benchmark
DataFrame. Two CPU quota, 3 GiB container limit, 1 GiB heap; local execution only.
Python was faster at 100k. This is not universal superiority or multi-machine scaling.

<a id="e05"></a>
## E05 — Completed historical lake

**Claim:** largest completed wire-validated historical Parquet snapshot:
**1,000,000 unique events**, `lake-1m-retry`. Its category, hour and source aggregate
marginals each independently total one million.

**Evidence:** [storage receipt](../../bda/reports/phase3/storage.json),
[million-event aggregate sample](../../bda/reports/phase3/sample-aggregates-1m.json),
[executed lake validation](../../bda/PHASE3_VALIDATION.md),
[lake builder](../../bda/batch/lake.py).
**Boundary:** do not sum category/hour/source marginals as three million events.
An interrupted initial 1m build is not complete. No complete 5m lake is claimed.

<a id="e06"></a>
## E06 — Historical storage and query example

**Claim:** validated 1m Parquet bytes **113,032,004**, raw JSONL **482,385,125**
(about **23.43%**). At 100k, **11,390,571 / 48,238,572** bytes (about **23.61%**).
These are validated-Parquet/raw ratios, not all-output or lossless-audio ratios.
The 100k Jan 1 inclusive–Jan 3 exclusive UTC query returns **48 hourly rows** and
**28,426 events**.

**Evidence:** [storage](../../bda/reports/phase3/storage.json),
[raw manifests](../../bda/reports/phase3/datasets.json),
[hourly SQL output](../../bda/reports/phase3/historical-hourly.json),
[execution receipt](../../bda/reports/phase3/historical-hourly-execution.json),
[SQL definitions](../../bda/batch/queries.py).
**Boundary:** query/write timing is a separate observation, not a campaign sample.

<a id="e07"></a>
## E07 — Default dashboard population

**Claim:** the committed read-only serving snapshot represents **100,000**
SIMULATED historical events, **16** source IDs, **68,160 aggregate cells** and
**477 detected windows**, Jan 1–7 2026 UTC. Six analytics views plus original
Acoustic Monitor; eight versioned read-only analytics endpoints.

**Evidence:** [export receipt](../../bda/reports/phase4/export-receipt.json),
[serving snapshot](../../bda/reports/phase4/history.sqlite),
[Phase 4 validation](../../bda/PHASE4_VALIDATION.md),
[API contract](../../bda/analytics/API.md),
[desktop screenshot](../../bda/reports/phase5/overview-desktop.png).
Snapshot SHA-256: `d19231017e5cb4cd787a588bc85dfde0689bc52cba8092fb26123017a1805acf`.
**Boundary:** historical source IDs are not active devices; this is not the 1m lake
or the 5m benchmark population. WebSocket buffered counts are not Spark aggregates.

<a id="e08"></a>
## E08 — Actual pipeline through API and browser

**Claim:** Phase 5 exercised actual finite EchoTrace simulation → normal producer
→ isolated Kafka → Spark → Parquet → generated SQLite → FastAPI → React/Chromium.
Acoustic subset **8 events**; combined initial wire **28**, unique validated **26**,
quarantine **1**, finalized events **25**, finalized cells **8**, detected anomalies **2**.
The combined population includes dedicated fixtures/control; it is not 28 acoustic detections.

**Evidence:** [acoustic records](../../bda/reports/phase5/acoustic-manifest.json),
[first output](../../bda/reports/phase5/e2e-first.json),
[run receipt](../../bda/reports/phase5/run-receipt.json),
[HTTP verification](../../bda/reports/phase5/presentation.json),
[browser transcript](../../bda/reports/phase5/pipeline-browser.txt),
[generated-data screenshot](../../bda/reports/phase5/pipeline-dashboard.png),
[runner](../../bda/reliability/run.py), [presentation verifier](../../bda/reliability/presentation.py).
**Boundary:** recorded finite simulation, not microphone inference or a continuously
live full-stage dashboard feed. Original phase validation details remain authoritative.

<a id="e09"></a>
## E09 — Reliability and delivery limitations

**Claim:** checkpoint restart ingested two new late Kafka records: classified
**28 → 30**, validated **26 → 26**, finalized events **25 → 25**, anomalies **2 → 2**.
A broker-outage test consumed two acknowledged events and observed one timed-out
submission that was not delivered. Queue saturation and API restoration are tested.

**Evidence:** [restart output](../../bda/reports/phase5/e2e-restart.json),
[receipt/outage counters](../../bda/reports/phase5/run-receipt.json),
[scenario matrix](../../bda/reliability/MATRIX.md),
[reliability tests](../../bda/reliability/tests/test_reliability.py),
[Phase 5 validation](../../bda/PHASE5_VALIDATION.md).
**Boundary:** bounded best effort at the producer, watermark-bounded deduplication,
not end-to-end exactly-once or verified replicated fault tolerance.

<a id="e10"></a>
## E10 — Controlled streaming workload

**Claim:** target **100 events/s**; warmup **15 s / 1,500** records; measured
**60 s / 6,000** records. Actual produced **99.99986 events/s**, acknowledged
**99.91637 events/s** over the measured acknowledgement/drain interval.
All **7,500** workload records validated; the additional control is separate.

**Evidence:** campaign section of [run receipt](../../bda/reports/phase5/run-receipt.json),
[batch-rate plot](../../bda/reports/phase5/streaming-rates.png),
[resource samples](../../bda/reports/phase5/resources.jsonl),
[paced workload](../../bda/reliability/workload.py).
**Boundary:** concurrent producer/ingest, downstream stages drained sequentially.
Not long-duration sustained full-platform capacity, and no higher-rate result.

<a id="e11"></a>
## E11 — Measured serving-index improvement

**Claim:** on the same 68,160-cell SQLite copy, category-query median
**31.2651 → 5.8753 ms**, distinct-source-count **14.7713 → 3.8053 ms**; identical
outputs. Twenty measured reads after two warmups, per query and variant.
Bytes **10,285,056 → 13,340,672**. Indexes are applied to new exports only.

**Evidence:** [before/after values and plans](../../bda/reports/phase5/optimization.json),
[comparison chart](../../bda/reports/phase5/serving-indexes.png),
[experiment](../../bda/reliability/optimize.py), [exporter](../../bda/analytics/export.py).
**Boundary:** query-specific warm-cache local SQL result, not a whole-API or Spark
speedup. The original Phase 4 snapshot is unchanged; storage/preparation costs exist.

<a id="e12"></a>
## E12 — Latency and observability

**Claim:** last 4,096 successful measured workload enqueue-to-ack samples:
p50 **10.26**, p95 **29.99**, p99 **105.43 ms**. Twenty sequential same-host HTTP
reads after one warmup: p50 **3.11**, p95 **4.30**, p99 **4.54 ms**. Browser
response-received → row visible: **173 ms**, one observation only.

**Evidence:** [run](../../bda/reports/phase5/run-receipt.json),
[HTTP samples](../../bda/reports/phase5/presentation.json),
[browser observation](../../bda/reports/phase5/pipeline-browser.txt),
[clock/population definitions](../../bda/reliability/LATENCY.md),
[telemetry reader](../../bda/analytics/observability.py).
**Boundary:** these are different populations/boundaries; never add their percentiles
or call them complete per-event latency. Per-record Spark transport and true committed
consumer-group lag remain unavailable. Heartbeat freshness is separate from progress.

<a id="e13"></a>
## E13 — Test provenance

Historical Phase 4: 252 Python, 20 batch/Spark, 8 frontend, 3 browser checks.
Historical Phase 5 final: 271 Python passed/14 skipped with generated-output opt-in;
19 streaming passed/1 skipped; 20 batch passed; 1 real Kafka smoke; 8 frontend;
5 default browser checks plus 1 separate generated-output browser check.

**Evidence:** [Phase 4 record](../../bda/PHASE4_VALIDATION.md),
[Phase 5 record](../../bda/PHASE5_VALIDATION.md),
[current Phase 6 release record](../../bda/PHASE6_VALIDATION.md).
**Boundary:** suites overlap; never sum them into a unique-test count. A clean clone
without ignored run outputs skips generated-output tests; model assets and integration
flags also affect counts. Phase 6 reruns are reported independently.

<a id="e14"></a>
## E14 — Original EchoTrace attribution and acoustic evaluation

**Sources:** [MIT license/copyright](../../LICENSE),
[original acoustic guide](../backend_readme_section.md),
[evaluation disclosure](../metrics.md), [metrics](../metrics.json),
[original pipeline](../../backend/echotrace/pipeline.py).
Original project history imported at `c76d4f5`; original acoustic commits include
Chinmay1881 and frontend contribution by Aditya Karadi. Individual Git history and
LICENSE remain the authority for attribution; institutional/student names are placeholders.

**Boundary:** the older 10-clip evaluation reports 9/10 sequence matches and 46/56
zone matches after tuning on those same clips. It is not independent evidence of
field safety, BDA throughput or new model accuracy. Original neural inference was
not rerun in the final BDA delivery. Weights are separately obtained from PANNs.

<a id="e15"></a>
## E15 — Preserved incident record and operational safety

**Evidence:** the data-preservation incident and guard in
[Phase 2 validation](../../bda/PHASE2_VALIDATION.md),
[startup preflight](../../infrastructure/spark/preflight.py),
[isolated reliability Compose](../../infrastructure/reliability/compose.yml),
[safe cleanup](../../bda/reliability/cleanup.py).

The earlier legacy broker recreation lost records; it is explicitly documented and
was not repaired by later tests. Later isolated tests preserved application broker
identity and avoided resetting its volumes. Do not claim the entire project history
had zero data loss. Do not use `down -v`, global pruning or automatic unsafe repair.

## Reconciled wording and outstanding limits

- Replace the old README's “documentation lands in the final phase” with actual links.
- Treat old “dashboard outside scope” and “Spark telemetry unavailable” statements as
  phase-scoped historical records; later phases add those capabilities, with opt-in telemetry.
- Replace “72 measured trials” with “72 recorded results, including 18 warmups.”
- Describe all multi-million results as synthetic **event metadata** analytics.
- Do not promote a short local experiment into cluster scale, full real-time capacity,
  end-to-end exactly-once delivery or field-tested acoustic safety.
- The original Windows guide's Node 18 recommendation predates the current frontend
  toolchain; new instructions use the tested Node 24 host and separate inference setup.
- Keep the original evaluation's tuning disclosure and the Phase 2 loss incident visible.
