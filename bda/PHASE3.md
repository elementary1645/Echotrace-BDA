# Phase 3: historical analytics and reproducible benchmarks

Phase 3 adds dataset engineering, immutable historical Parquet snapshots, a SQL
query interface and checked Python/Spark comparisons. It keeps Phase 1/2 source,
Kafka storage, checkpoints, and the documented data-preservation incident intact.
No Kafka service startup/recreation is needed by the batch runner.

## Runtime and resource controls

Use the existing digest-pinned Spark/PySpark 4.0.1 image (Python 3.10.12, Java
17.0.18, Scala 2.13). If missing, build it with `infrastructure/spark/run.sh build`.
Batch commands run a non-root container limited to two CPUs, 3 GiB memory and
1 GiB Spark heap. Linux/Codespaces UID/group files are mounted read-only so Hadoop
can resolve the host-mapped user, and generated artifacts remain user-owned.
`BATCH_MASTER=local[1]` or `local[2]` controls local worker threads. No UI is published.
The batch runner does not attach to, restart, or delete Kafka storage.

Large artifacts belong under ignored `artifacts/phase3`. The generator reserves
1,500 bytes/event plus 2 GiB free space before writing: this is a conservative raw,
Parquet and temporary-space estimate, independent of likely gzip compression.
Inspect `free -h`, `df -h .`, `docker stats --no-stream` and `docker system df`
before increasing workloads. Spark can spill to its container temporary disk;
monitor Docker capacity too. Run benchmark containers **sequentially**, with no
concurrent conversion/generation/test jobs. Never delete user data to meet a target.

## Deterministic dataset generation

From repository root, using the existing backend virtual environment:

```bash
infrastructure/batch/run.sh generate --count 100000 --seed 42 --sources 16 \
  --output artifacts/phase3/raw-100k
infrastructure/batch/run.sh generate --count 1000000 --seed 42 --sources 16 \
  --output artifacts/phase3/raw-1m
# Larger supported targets, subject to the same resource guard:
infrastructure/batch/run.sh generate --count 5000000 --seed 42 \
  --output artifacts/phase3/raw-5m
infrastructure/batch/run.sh generate --count 10000000 --seed 42 \
  --output artifacts/phase3/raw-10m
```

Existing output directories are refused. Failed generation leaves an incomplete
root without a completion manifest; consumers refuse it. `--format jsonl.gz` adds
deterministic gzip (mtime zero, no embedded path); default is JSONL. `--chunk`
(default 100,000) bounds files, not a materialized memory buffer: records are emitted
one at a time. Supports seed, count, source count, days (maximum 366), explicit UTC-midnight
start, peak hour and `--anomaly-every` (zero disables injections). All outputs use
the v1 `AnalyticsEvent` contract and `SIMULATED` source classification.

Categories follow weighted activity; confidence follows a bounded beta distribution.
Night hours have lower activity, and the configured peak hour has higher weight.
Each source has a deterministic session identity; every event has a unique indexed
ID and the existing SHA-256 source/session/event record ID. Every Nth record is
moved into an ALARM burst at noon on the middle day at synthetic-node-00000.
The manifest records **injected** counts independently of natural background counts;
it does not claim every injection is an independently detected anomaly. Events are
emitted in deterministic randomized arrival order, not sorted event time.

`manifest.json` records generator/schema version, complete config, dataset identity,
seed, record/file counts, byte size, each file's SHA-256, expected category counts,
confidence sums, hourly/source/zone/day counts, daily category trends and injected
anomaly bucket counts. Dataset identity hashes version/config; benchmark results
also record the exact manifest SHA-256, binding measurements to file checksums.
Reproducibility applies to this generator version and pinned Python runtime; Python
random implementation changes require renewed verification/versioning.

Data quality fixtures stay separate:

```bash
infrastructure/batch/run.sh fixtures --output artifacts/phase3/quality
```

This emits six arrivals: one exact duplicate, one confidence-invalid record, one
malformed JSON line, and a valid old event time arriving later in the file. There
are four valid arrivals and three unique identities. `fixture.json` describes the
expected quality outcomes. It is deliberately **not** a benchmark manifest, so
benchmark input validation refuses it. Historical batch time is not a watermark:
late data is included when valid; Phase 2 tests demonstrate streaming rejection.

## Historical layers and provenance

```bash
infrastructure/batch/run.sh lake --input /artifacts/raw-100k --output /artifacts/lake-100k
infrastructure/batch/run.sh lake --input /artifacts/raw-1m --output /artifacts/lake-1m
```

Paths `/artifacts` map to host `artifacts/phase3`. Conversion verifies all input
checksums outside analytical benchmarks, reuses Phase 2's Pydantic validator,
quarantines malformed records, deduplicates record IDs, writes event-date-partitioned
Parquet, historical anomaly windows and hourly/daily/trend aggregate tables.
`--partitions` controls writer/shuffle parallelism; files cap at 250,000 records.
Partitioning by date avoids per-device directories and avoids copying raw files.
Validation results are temporarily persisted with DISK_ONLY so valid/quarantine
writes reuse one wire-validation pass without retaining a million rows in heap;
the cache is unpersisted after routing and is not a permanent layer.
Low-volume days can still have small files. This phase does not implement automatic
compaction. Exact duplicates are supported; conflicting payloads sharing one valid
identity are outside the generated workload and should not be used as a reproducible
benchmark (Spark deduplication does not specify which conflicting payload wins).

| Layer | Schema/ownership |
| --- | --- |
| Raw | Versioned v1 wire events, JSONL/gzip, generator-owned immutable files and manifest |
| Validated | V1 fields with UTC timestamp `event_time`, `event_date`, `dataset_origin`, and synthetic `dataset_id`; Phase 2 imports additionally retain Kafka coordinates/time |
| Quarantine | Original raw JSON string `value`, validator `error`; empty for valid generated datasets |
| Aggregated | Hourly/daily count tables and day/category count/confidence tables |
| Anomalies | Phase 2 anomaly schema: window, source/source_id/category, count, prior-history mean/stddev, flags and score |

A historical root is written once and is published by `_COMPLETE` only after all
outputs and the provenance manifest succeed. Readers reject incomplete roots.
Failed roots remain inspectable and are never silently overwritten. Concurrent
writers to the same root are refused by exclusive directory creation. This is a
local-filesystem snapshot publication convention, not an object-store transaction.
The raw reference and original dataset manifest are retained; full raw data is not
copied. V1 is the only accepted wire schema. Additive historical metadata must be
versioned/documented; incompatible type/semantic changes require new roots and an
explicit migration. Existing streaming sink schemas are unchanged.

To reuse Phase 2 without modifying its files:

```bash
BATCH_PHASE2_VOLUME=echotrace-bda_spark-data infrastructure/batch/run.sh lake \
  --phase2 --input /data/analytics/events --output /artifacts/lake-phase2
```

The named volume must already exist and is mounted **read-only** at its original
`/data` location because file-sink metadata contains absolute paths. Spark reads the
dataset root to honor `_spark_metadata`. `dataset_origin=application` means the
application pipeline produced the record, not that the observation was LIVE;
original LIVE/RECORDED/SIMULATED provenance remains unchanged. An imported snapshot
includes only events actually retained by the Phase 2 event sink, not its dropped
late events or quarantined arrivals.

## Historical SQL interface

All analytical timestamps are the existing **stream-position wall timestamp**, not
acoustic onset. UTC conversion is explicit. Periods are `[start,end)`, with start
inclusive and end exclusive; unknown/null/unmapped zones normalize to UNKNOWN for
queries. Accepted wire events themselves still require a legal zone.

```bash
infrastructure/batch/run.sh lake --input /artifacts/lake-100k \
  --query hourly --start 2026-01-01T00:00:00Z --end 2026-01-03T00:00:00Z \
  --output /artifacts/query-hourly
```

Exports contain Spark JSON part files plus `_schema.json` (ignored by Spark data readers). Every query accepts the
validated event schema above and optional UTC bounds; the anomalies query also reads
the snapshot's finalized anomaly rows. A future dashboard can read these exports
without changing query definitions. No frontend is implemented in this phase.

| Query | Purpose and output columns |
| --- | --- |
| total | Period total: event_count (long; 0 for empty input) |
| hourly | UTC hour (timestamp), event_count |
| daily | event_date (date), event_count |
| categories | category (string), event_count, avg_confidence (double), descending count order |
| zones | normalized zone (string), event_count |
| sources | source_id (string), event_count, avg_confidence |
| classification | original source classification (string), event_count |
| peaks | Top 24 observed hours: hour, event_count; descending count, hour tie-break |
| source_comparison | source_id, event_count, active_days (long), events_per_active_day (double) |
| rolling | source_id, category, hour, event_count, history_count, baseline_mean/stddev |
| trends | event_date, category, event_count, avg_confidence |
| anomalies | event_date, source, category, evaluated_windows, anomalous_windows (long) |

All grouped queries return zero rows on empty input; total returns one row containing
zero. No zero-activity buckets are fabricated. Rolling history is the previous 24
**observed** hourly buckets per source/category, excluding current/future rows, within
the selected period; insufficient history produces null means/stddevs. Historical
anomalies aggregate zones before applying the Phase 2 prior-only statistical
baseline, with a configurable absolute `--threshold` (default 100), history 24 and
minimum history 3. Anomaly period filters select windows by window.start; partial hours are not
recomputed from individual events. Historical anomaly frequency counts evaluated source/category
windows rather than claiming every acoustic event is an anomaly.

Small deterministic numerical expectations are tested in `bda/batch/tests/test_spark.py`:
four events at UTC 00:00, 00:59:59, 01:00 and next-day 00:00 yield hourly counts
2/1/1, daily counts 3/1; category A has count 3 and confidence mean 2.5/3; source
counts are 2/2; zones include two UNKNOWNs; LIVE/RECORDED/SIMULATED counts are 2/1/1.
A separate 2/3/20 hourly fixture yields prior means null/2/2.5, excluding the burst.

## Benchmark methodology

Run **after** generation/conversion and other tests finish:

```bash
BATCH_MASTER='local[1]' infrastructure/batch/run.sh benchmark \
  --input /artifacts/raw-100k --storage-manifest /artifacts/lake-100k/manifest.json \
  --input-partitions 2 --shuffle 2 --warmups 1 --runs 3 --output /artifacts/bench-100k-local1
BATCH_MASTER='local[2]' infrastructure/batch/run.sh benchmark \
  --input /artifacts/raw-100k --storage-manifest /artifacts/lake-100k/manifest.json \
  --input-partitions 2 --shuffle 2 --warmups 1 --runs 3 --output /artifacts/bench-100k-local2
```

Repeat with raw-1m/lake-1m and new output roots for additional sizes. To isolate
partition settings, keep local[2] and dataset fixed and vary `--input-partitions`
and `--shuffle`. Actual input partition counts are recorded: requested split sizes
are not promises about physical file splitting, especially with unsplittable gzip.

Both engines read the **identical raw files** and compute the same category counts,
category confidence means, UTC hourly counts and source counts. Python is a separate
single-process implementation using JSON parsing, datetime and dictionaries; Spark
uses explicit-schema JSON reads and one SQL GROUPING SETS scan/shuffle. Each result
is checked against independently accumulated manifest statistics (absolute confidence
tolerance 1e-8). No full-event driver collection occurs. At most 100,000 aggregate
rows may return to the driver, with a fail-fast cap.

Timers include file reads, parsing and the complete analytical bundle. They exclude
manifest verification, Spark startup, generation, result correctness comparison and
JSON result serialization. Setup duration (manifest verification plus Spark startup) is reported separately. Spark read
planning/RDD partition enumeration is included in its analytical timer; both engines
construct their result data structures in the timer. This is an end-to-end bundle
comparison, not proof that every individual operation has identical overhead.
Python runs first while the Spark JVM is idle in the same container. Container CPU
and sampled-memory metrics therefore include that idle JVM; they are not isolated
Python-process memory peaks. Resource samples are taken every 50 ms, so brief peaks
can be missed. No cache flushing is attempted: warmups are labeled and excluded
from measured medians, and operating-system cache remains uncontrolled. No persisted
DataFrame cache is used. JVM JIT/cache evolution can affect later runs.

Each run first journals `started`, then success/failure with duration, throughput,
CPU seconds, sampled container memory high-water, actual partitions, input/output
bytes and correctness. Interrupted starts are reported as incomplete; malformed
journal lines and failed runs stay visible. All paths refuse overwrites; reruns get
new result directories. `environment.json` records runtime versions, master, CPU/RAM
limits, complete Spark configuration, setup time and container runner configuration.
`task-metrics.json` joins Spark event-log tasks to trial job groups, exposing task
counts, executor CPU/run time, input bytes, shuffle bytes and spill bytes. Raw Spark
event logs remain available for stage inspection. They add overhead equally across
Spark trials. `input_mib_per_second` is logical input bytes divided by analytical
time, not a physical disk-bandwidth measurement. Historical builds also expose
validated read/validate/shuffle/write duration and output bytes/s, not isolated disk
write speed. Query JSON serialization is outside the comparison timer.

Standalone independent baseline:

```bash
infrastructure/batch/run.sh baseline --input artifacts/phase3/raw-100k \
  --output artifacts/phase3/python-standalone-100k.json
```

## Reports and charts

Install the pinned plotting tool into the host environment:

```bash
.venv/bin/pip install -r infrastructure/batch/requirements-plot.txt
infrastructure/batch/run.sh plot --results artifacts/phase3/bench-*/trials.jsonl \
  --output artifacts/phase3/charts
```

Matplotlib exports PNG and SVG charts: dataset size/time, dataset size/throughput,
Spark/Python time, partition configuration/runtime and raw-input/validated-Parquet
storage. Values come only from successful correctness-checked measured trials.
`trial-status.json` discloses warmups, failures and incomplete runs, rather than
silently replacing them. Curves show medians with min/max observed ranges (not confidence intervals); Python trials across configurations
are pooled in charts, while raw journals preserve each experiment's samples.
Storage charts require `--storage-manifest`; absent data produces an explicit
unavailable notice instead of invented storage measurements. Labels identify units,
master and partition configuration. See PHASE3_VALIDATION.md for actual measurements,
chart interpretation and largest executed workload.

## Tests and safe cleanup

```bash
.venv/bin/python -m pytest bda/batch/tests -q
infrastructure/batch/run.sh test
infrastructure/spark/run.sh test
.venv/bin/python -m pytest bda/tests bda/streaming/tests bda/batch/tests backend/tests -q
```

Fast tests never require Spark; Spark tests explicitly opt in via
RUN_BATCH_SPARK_TESTS=1 (set by the helper). Small fixtures test determinism,
checksums, identities, schema validation, injected ground truth, malformed/duplicate/
late fixture contents, journals, resource guards, SQL, null zones, period boundaries,
empty inputs, prior-only baselines and Parquet/Python/Spark consistency. Normal CI
does not generate a million-record dataset. Phase 2 live Kafka tests remain separate.

Review dry-run cleanup candidates before deleting only a generated artifact subtree:

```bash
infrastructure/batch/run.sh clean artifacts/phase3/raw-100k
# Same command with --execute removes only that checked subtree.
```

The cleanup utility refuses the artifact root itself, paths outside it and symlink
targets. It never deletes volumes. Do not delete raw data referenced by a retained
manifest or report unless intentionally retiring that experiment as a whole.

## Big Data concepts and limits

Volume is demonstrated by the sizes actually recorded in validation; larger command
support is not evidence of execution. Velocity is Phase 2 micro-batch streaming,
while Phase 3 batch analytics scans historical snapshots. Variety includes category,
confidence, localization, source provenance and malformed quality fixtures, rather
than unsupported claims of multiple unrelated media pipelines. Spark partitions,
shuffles and task metrics demonstrate distributed-processing architecture locally;
local[1]/local[2] comparisons measure local concurrency, not multi-machine scale.
Partitioned Parquet demonstrates columnar storage and date pruning. Validation,
quarantine, deterministic manifests and independent calculations demonstrate data
quality and reproducibility. Scalability remains a measured resource/data-size
tradeoff, not a production capability claim. Spark startup can dominate small
workloads; Python may be faster. Production security, HA, remote object stores,
multi-node failover, a dashboard and cloud deployment are outside this phase.

For the full sequential campaign, after required raw and lake roots are complete:

```bash
.venv/bin/python -m bda.batch.campaign --output campaign --include-5m
# If reproducing the validation session's explicit failed-conversion retry path:
# add --lake-1m lake-1m-retry
```

The campaign compares local[1]/local[2] with input=2/shuffle=2, then holds local[2]
and input=2 fixed while varying shuffle=2/4, then holds shuffle=4 fixed while
varying input=2/4. It attempts every experiment sequentially and records exit codes,
including failures, in campaign.json. The optional 5m comparison uses local[2],
input=4/shuffle=4; its lake storage size is omitted because that conversion is not
part of the campaign. Individual historical query exports additionally include
`_execution.json` with query-and-JSON-write wall time and output bytes; this timing
is distinct from the checked raw-input comparison bundle.

API references: [Spark 4.0.1 GROUP BY/GROUPING SETS](https://spark.apache.org/docs/4.0.1/sql-ref-syntax-qry-select-groupby.html),
[Parquet sources and partition discovery](https://spark.apache.org/docs/4.0.1/sql-data-sources-parquet.html),
and [event logs and task metrics](https://spark.apache.org/docs/4.0.1/monitoring.html).

To exercise historical data quality explicitly (kept out of benchmark manifests):

```bash
infrastructure/batch/run.sh fixtures --output artifacts/phase3/quality
infrastructure/batch/run.sh lake --fixture --input /artifacts/quality \
  --output /artifacts/lake-quality
```

This separate `synthetic-quality` snapshot must contain three unique validated
records, two quarantined records and the valid old event. Without `--fixture`, the
quality root is refused. Invalid records never enter the historical aggregates.

Package small reproducible evidence and all five charts after a successful campaign:

```bash
.venv/bin/python -m bda.batch.publish --campaign artifacts/phase3/campaign \
  --output artifacts/phase3/reproduced-report
```

The output path must be new. This copies journals, manifests, environment and task
metrics, plus one actual million-record aggregate result; it never copies event
datasets. Published evidence from this implementation is in
[reports/phase3](reports/phase3/README.md). The validation record also includes the
independently checked historical hourly CLI export.
