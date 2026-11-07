# Phase 3 validation — 2026-11-07

Branch `feat/bda-batch-benchmarks`, initially clean, based on merged Phase 2 commit
`3871ef0`. No AGENTS.md was present. Inspected Phase 1/2 documentation, v1 schema,
streaming transforms, sink/checkpoint layout, Docker runtime and resource limits.
No frontend, Kafka configuration, broker storage, streaming source or incident
safeguard was changed. All timestamps in analytical data and logs below are UTC.

## Environment and resource decisions

Initial audit: host Python 3.14.2, Java 25, 7.8 GiB RAM, roughly 5.2 GiB available,
no swap, 13 GiB workspace disk free; Docker 29.9.2. The application Kafka broker
was already healthy. `docker system df` reported about 2.83 GB images and 1.58 GB
volumes. Batch jobs reused the pinned Phase 2 image: Spark/PySpark 4.0.1, Python
3.10.12, Java 17.0.18; two CPUs, 3 GiB container RAM and 1 GiB JVM heap. Dataset
and plot generation used the existing host virtual environment. Official Spark
4.0.1 GROUP BY/GROUPING SETS, Parquet and monitoring documentation was consulted.

Before larger generation, the resource guard refused 10,000,000 records:
`insufficient disk: need 17147483648 bytes including reserve, have 12872003584`.
No 10m output directory was created by that check. The 5m guard passed; that
workload was generated. No user data or volume was deleted to free space.

## Dataset commands actually executed

```bash
.venv/bin/python -m bda.batch.generate --count 100000 --seed 42 \
  --output artifacts/phase3/raw-100k > /tmp/phase3-generate-100k.json
.venv/bin/python -m bda.batch.generate --count 1000000 --seed 42 \
  --output artifacts/phase3/raw-1m > /tmp/phase3-generate-1m.json
.venv/bin/python -m bda.batch.generate --count 5000000 --seed 42 \
  --output artifacts/phase3/raw-5m > /tmp/phase3-generate-5m.json
```

All three completed with checksummed manifests; default configuration was 16 sources,
7 days, 100k records/file, JSONL and generator version 1.0.0. Each dataset labels
all records SIMULATED and records injected anomaly ground truth independently.

| Records | Files | Raw bytes | Dataset identity |
| ---: | ---: | ---: | --- |
| 100,000 | 1 | 48,238,572 | 22a52e6d117aca0478238e99a4962f561d6d3f7773305435671edcbeedcd5a93 |
| 1,000,000 | 10 | 482,385,125 | 5a393bae3c6ca42deafb9aa6875b253da63fcf18634e350579d7c9b9d1c36fc1 |
| 5,000,000 | 50 | 2,411,926,657 | acd0fb5afb346f9a7bc54b9bc49670c729d32b354f6db61daab41131fab7042f |

The 100k manifest independently recorded category counts ALARM 5,987, AMBIENT
24,818, IMPACT 19,852, MUSIC 14,778 and SPEECH 34,565. Exactly 1,000 records were
injected into the Jan 4 noon ALARM/source-0 burst, in addition to background events.
Generation duration was not captured for these initial runs and is not inferred
from analytical benchmark timing. Large data remains ignored by Git.

## Historical conversions and recovery of preparation failures

Executed `infrastructure/batch/run.sh lake --input /artifacts/raw-100k
--output /artifacts/lake-100k`: completed with **100,000 validated events**,
**11,390,571 bytes** of validated Parquet and **11,647,847 bytes** of total outputs
before the final manifest/completion marker. No quarantine records were expected.

An initial million-record conversion at `/artifacts/lake-1m` was deliberately
stopped after observing duplicated validation work. Its incomplete directory and
log were retained; it is not counted as a successful conversion. The implementation
now temporarily persists validation results with DISK_ONLY, avoiding repeated wire
validation for accepted/quarantine sinks without a million-row heap cache. The retry
uses `/artifacts/lake-1m-retry`, never overwriting the failed root.

The existing Phase 2 event sink was imported with:

```bash
BATCH_PHASE2_VOLUME=echotrace-bda_spark-data infrastructure/batch/run.sh lake \
  --phase2 --input /data/analytics/events --output /artifacts/lake-phase2-retry
```

Completed with **8 validated events**, **8,403 bytes** of validated Parquet and
**16,970 bytes** total outputs before manifest/completion. The volume was mounted
read-only at its original `/data` path. An initial `/phase2` mount failed because
Spark file-sink metadata contains original absolute `/data/...` filenames; that
failed output was retained and the original volume was never edited. Import
preserved source provenance and Kafka coordinates.

Other resolved preparation failures: Hadoop could not resolve a host-mapped numeric
UID until the Linux user/group files were mounted read-only; Ivy needed an explicit
writable `/tmp/ivy` path. Pytest initially reported a duplicate test_spark.py import
name; the new tests now have their own package namespace. These failures were not
substituted with fabricated successful measurements.

## Executed regression checks

- First batch suite: `infrastructure/batch/run.sh test`: **14 passed in 118.78 s**,
  including three real Spark historical SQL/round-trip tests and 11 fast tests.
- After quality-fixture and metric parser checks were added:
  `.venv/bin/python -m pytest bda/batch/tests/test_unit.py -q`: **13 passed**.
- Combined existing regression and batch fast tests with the pre-existing public
  AudioSet label fixture in `/tmp`: **229 passed, 12 skipped**, 29.33 s. The command:

  ```bash
  PYTHONPATH=backend .venv/bin/python - <<'PY'
  from pathlib import Path
  import pytest
  from echotrace import config
  config.LABELS_FILE = Path('/tmp/echotrace-class-labels.csv')
  raise SystemExit(pytest.main(['bda/tests','bda/streaming/tests','bda/batch/tests','backend/tests','-q']))
  PY
  ```

  One existing Starlette TestClient deprecation warning. Skips were opt-in Spark/
  Kafka tests and unavailable neural-model assets, not failed analytics assertions.
- `infrastructure/spark/run.sh test`: **19 passed, 1 skipped in 231.74 s**.
  This reverified Phase 2 parsing, deduplication, watermark/late behavior, aggregation
  and checkpoint restart. Live Kafka tests were not rerun in this phase; Kafka and
  streaming code remained unchanged.
- `python -m compileall -q bda/batch`, `bash -n infrastructure/batch/run.sh`,
  `git diff --check`: passed during implementation.
- Matplotlib 3.10.7 imported and rendered successfully. Dependencies are pinned in
  `infrastructure/batch/requirements-plot.txt`; writable plot cache location is
  specified for the actual report commands.

## Final preparation results

The optimized million-record retry completed with **1,000,000 validated events**,
**113,032,004 bytes** validated Parquet and **113,322,842 bytes** total outputs
before the final manifest/completion marker. Its recorded build duration was
**230.686 s**, including validation, quarantine routing and aggregate/anomaly table
writes. The validated read/validate/shuffle/write action took **204.847 s**, giving
**551,788 output bytes/s**. Preparation overlapped generation/tests and these
figures are not a controlled cache or deduplication-speedup comparison.

The final batch Spark suite, including the disk-persisted validation path and known
injected anomaly detection: **16 passed in 125.04 s**. `infrastructure/batch/run.sh test`
was the executed command. All generated datasets remained separate from application
statistics and every imported streaming volume mount was read-only.

The sequential campaign was started only after generation, historical conversion
and Spark test jobs finished:

```bash
.venv/bin/python -m bda.batch.campaign --output campaign \
  --lake-1m lake-1m-retry --include-5m
```

This invokes the exact commands retained in `campaign.json`, with one warmup and
three measured trials per engine/experiment. CPU/thread/input/shuffle controls,
correctness-checked results, warmups, journals, setup/environment and Spark task
metrics are retained rather than replacing unsuccessful samples.

## Measured benchmark campaign

All **9 experiments** exited zero. All **72 trials** passed independent aggregate checks: **18 warmups** and **54 measured trials**. No failed or incomplete trial was replaced or hidden.

| Events | Master | Requested / actual input partitions | Shuffle | Python median s | Spark median s | Spark events/s |
| ---: | --- | --- | ---: | ---: | ---: | ---: |
| 100,000 | local[1] | 2 / 2 | 2 | 1.0972 | 1.3280 | 75,303 |
| 100,000 | local[2] | 2 / 2 | 2 | 2.0053 | 2.4054 | 41,574 |
| 100,000 | local[2] | 2 / 2 | 4 | 1.0292 | 1.8568 | 53,856 |
| 100,000 | local[2] | 4 / 4 | 4 | 1.8948 | 2.4260 | 41,220 |
| 1,000,000 | local[1] | 2 / 3 | 2 | 13.0701 | 6.6923 | 149,425 |
| 1,000,000 | local[2] | 2 / 3 | 2 | 12.8820 | 6.2467 | 160,086 |
| 1,000,000 | local[2] | 2 / 3 | 4 | 12.9112 | 4.5445 | 220,046 |
| 1,000,000 | local[2] | 4 / 5 | 4 | 13.1049 | 7.1973 | 138,940 |
| 5,000,000 | local[2] | 4 / 5 | 4 | 70.6890 | 32.7002 | 152,904 |

Setup (manifest verification + Spark initialization) ranged **6.290–22.103 s**, separate from analytical timings. Maximum sampled container memory across all trials was **2069.6 MiB** within the **3,072 MiB** limit. These are cgroup samples, not complete process RSS peaks.

Limits and raw CPU seconds per trial are retained in environments.json/trials.json. Python resource samples include the idle Spark JVM. Original task metrics and job-group joins are packaged in task-metrics.json; full Spark event logs remain in the ignored campaign directories.


## Charts and interpretation

Executed:

```bash
.venv/bin/python -m bda.batch.publish --campaign artifacts/phase3/campaign \
  --output bda/reports/phase3
```

Packaged 72 actual trial outcomes, original journals, environment, task metrics,
dataset manifests/checksums and a real million-record aggregate sample. The latter
was independently compared again with the million-record manifest and matched.
`issues.json` is empty. No event dataset was copied into Git. Matplotlib exported
all five required PNG/SVG charts. After visual inspection, categorical labels were
wrapped and ranges added; the identical measurements were re-rendered using
`bda.batch.plot.render` into `/tmp/echotrace-phase3-charts-final` and copied over only
the generated chart files. No trial values were changed.

- **Dataset size versus execution time:** Python was faster at 100k in all measured
  configurations. Spark was faster for this specific checked bundle at 1m and 5m.
  This does not compare every historical query or prove Spark is universally faster.
- **Size versus throughput:** observed Spark throughput varied from about 41k to
  220k events/s across measured configurations/sizes. Larger input amortized SQL
  planning/task-launch overhead. Cache/JIT and scheduling effects remain visible.
- **Spark versus Python:** at 5m, local[2]/requested input=4/shuffle=4 measured
  70.6890 s Python versus 32.7002 s Spark, about 152,904 Spark events/s and
  70.34 logical input MiB/s. Excluded setup must be added when evaluating a one-off
  invocation; setup is not silently included in the steady-state speed comparison.
- **Partition configuration versus runtime:** at 1m, local[2]/input=2/shuffle=4 had
  a 4.5445 s Spark median, versus 7.1973 s with requested input=4/shuffle=4. More
  partitions/threads did not consistently improve runtime. Actual input counts were
  3 and 5 respectively because JSONL file packing differs from requested targets.
  Python's 100k medians varied 1.03–2.01 s on the same capped environment, showing
  scheduling/cache noise; three samples do not establish strong causal performance
  rankings. Charts show observed min/max, not confidence intervals.
- **Input versus storage size:** validated Parquet used 11,390,571 / 48,238,572 bytes
  at 100k and 113,032,004 / 482,385,125 bytes at 1m (roughly 23–24% of raw JSONL).
  This includes validated provenance and file metadata. No 5m Parquet size is
  invented: the largest full historical snapshot executed was 1m.

Across 36 Spark warmup/measured job groups, event-log parsing retained **428 task
attempts** and **592,616 shuffle-written bytes**, plus input/spill/CPU counters.
Full metrics remain inspectable per run; task-count overhead is not a throughput
speedup claim. Maximum sampled container memory was about 2.02 GiB under the 3 GiB
cap. These are local, repeated, warm-OS-cache-oriented experiments, not verified
multi-machine scaling or isolated hardware measurements.

## Machine-readable historical export

Executed:

```bash
infrastructure/batch/run.sh lake --input /artifacts/lake-100k --query hourly \
  --start 2026-01-01T00:00:00Z --end 2026-01-03T00:00:00Z \
  --output /artifacts/query-hourly
```

JSON part files plus `_schema.json` and `_execution.json` were produced. Independent
Python comparison against the generator manifest matched **48 hourly rows** and
**28,426 events**, including half-open period boundaries. This sample and its schema/
execution receipt are packaged as historical-hourly*.json in the report directory.
Query-and-JSON-write duration was **13.1998 s**, measured under concurrent final
verification load; it is not mixed with the controlled raw-input benchmark bundle.

Standalone Python CLI also executed successfully on the same 100k dataset:
`infrastructure/batch/run.sh baseline --input artifacts/phase3/raw-100k
--output artifacts/phase3/python-standalone-100k.json`. It checked correct output
and reported **6.4804 s**. This used host Python 3.14 under final verification load,
so it is deliberately not pooled with controlled container Python 3.10 trials.

## Boundaries of verification

Largest generated and benchmarked dataset: **5,000,000 records**. Largest complete
wire-validated Parquet/aggregate/anomaly historical snapshot: **1,000,000 records**.
10m generation capability and resource refusal are tested; an actual 10m dataset
was **not** generated or benchmarked. Five-million-record raw files were verified by
checksums and independent Python/Spark aggregate agreement, not by a full 5m lake
conversion. No neural model, microphone, cloud/multi-node deployment, production HA,
security hardening, storage-failure injection or final React dashboard was tested.
Live Kafka was left healthy and untouched; its data-preservation incident record
and startup safeguards remain unchanged. Benchmark ratios describe this workload
and environment only; no extrapolated 10m performance appears in this project.

## Final release checks

- Final combined regression command shown above: **232 passed, 13 skipped in
  18.77 s**, with the existing Starlette deprecation warning. This includes the
  added explicit quality-lake test as a skip in the fast suite. Expected skips:
  four batch Spark tests, four Phase 2 Spark tests, two live Kafka tests, three
  model/weight dependent backend tests.
- `infrastructure/batch/run.sh test`: **19 passed in 52.67 s**. This includes 15
  fast checks and four actual Spark tests. Earlier 18-test release run passed in
  76.80 s before the final quality-lake test was added.
- Quality CLI commands executed:

  ```bash
  infrastructure/batch/run.sh fixtures --output artifacts/phase3/quality
  infrastructure/batch/run.sh lake --fixture --input /artifacts/quality \
    --output /artifacts/lake-quality
  ```

  Completed with **3 unique validated events**, **2 quarantined records** (also
  asserted by the Spark test), duplicate suppression and one valid 2025 late event
  retained in historical analytics. Snapshot origin is `synthetic-quality`; it is
  not a benchmark manifest. Published output sizes: 13,804 validated bytes and
  30,073 total output bytes before the completion manifest.
- In the pinned Python 3.10 image, generated and schema-validated an event using
  UTC `Z` start syntax; result was timezone-aware UTC. Config start is now required
  to be midnight so configured activity hours retain their clock-hour meaning.
  This restriction does not alter any published default-seed dataset.
- Result-parser hardening tests verify malformed/primitive journal records are
  reported and a duplicate successful outcome cannot replace a previous failure.
  Category query results explicitly rank by descending count. Neither change
  altered the timed benchmark aggregation definitions or the completed journals.
- All 72 packaged results, original journals and the exported million-record
  aggregate sample were rechecked against the manifest/statistical reference.
  All five charts were re-rendered after label corrections; execution-time and
  partition-runtime PNGs were visually inspected.
- Final `python -m compileall -q bda/batch`,
  `bash -n infrastructure/batch/run.sh`, and `git diff --check`: passed.

Additional resource interpretation: largest-workload measured CPU usage had median
90.4% of one core for Python and 116.4% for Spark, from cgroup CPU-time/wall-time
ratios (the container quota was two cores). Across all Spark trial tasks the logged
input-byte counter totaled 18,140,794,824 bytes, with 592,616 shuffle bytes and zero
reported memory/disk spill bytes. Small shuffle volume suggests scan/parsing and
query/task overhead are useful next profiling targets; this is an inference from
counters, not a profiler-proven breakdown of wall time.

## Exact reproduction sequence

Use new output directories; existing immutable paths are deliberately refused.

```bash
infrastructure/spark/run.sh build  # only if the pinned runtime image is absent
.venv/bin/pip install -r infrastructure/batch/requirements-plot.txt
infrastructure/batch/run.sh generate --count 100000 --seed 42 --output artifacts/phase3/raw-100k
infrastructure/batch/run.sh generate --count 1000000 --seed 42 --output artifacts/phase3/raw-1m
infrastructure/batch/run.sh generate --count 5000000 --seed 42 --output artifacts/phase3/raw-5m
infrastructure/batch/run.sh lake --input /artifacts/raw-100k --output /artifacts/lake-100k
infrastructure/batch/run.sh lake --input /artifacts/raw-1m --output /artifacts/lake-1m
.venv/bin/python -m bda.batch.campaign --output campaign --include-5m
.venv/bin/python -m bda.batch.publish --campaign artifacts/phase3/campaign --output artifacts/phase3/reproduced-report
infrastructure/batch/run.sh test
```

The executed session used `--lake-1m lake-1m-retry` for its campaign because the
initial conversion was retained as incomplete. That interrupted preparation is
fully separate from the successful repeated benchmark trials. Published example
hourly exports can be regenerated with the query command above. Exact experiment
commands, including master and partitions, are retained in the report's campaign.json.
