# Phase 5 executed evidence

All data is from the isolated SIMULATED Phase 5 run, not live acoustic observations.
The original Phase 3/4 reports and serving snapshot remain unchanged.

| Artifact | Meaning |
| --- | --- |
| `run-receipt.json` | Actual steps, exit codes, isolated identities, outage outcomes, checkpoint comparisons, workload measurements |
| `acoustic-manifest.json` | Eight real finite EchoTrace simulation event records, producer metrics and successful enqueue-to-ack observations |
| `e2e-first.json`, `e2e-restart.json` | Independently read Spark Parquet counts, provenance and acoustic aggregates before/after new late Kafka records |
| `presentation.json` | Real HTTP samples, observed safe health response, generated-export validation and server shutdown exit |
| `pipeline-browser.txt` | Passing real generated-output Chromium check and one response-to-visible measurement |
| `environment.json` | Runtime/topology/resource caps and initial resource guard observations |
| `resources.jsonl`, `resource-summary.json` | Rounded Docker samples and derived maxima across the isolated run, not profiler peaks |
| `harness-failures.json` | Initial failed/refused attempts and corrections; never counted as passes |
| `optimization.json` | Identical SQL outputs and plans before/after measured covering indexes |
| `streaming-rates.png` | Actual ingestion batch rates including warmup/drain; not full-platform capacity |
| `serving-indexes.png` | Actual before/after query-specific read timing comparison |
| `pipeline-dashboard.png`, `system-health.png` | Genuine generated pipeline rows and stopped/stale-aware query observations |
| `overview-desktop.png`, `overview-mobile.png` | Default dashboard regression screenshots, written here to preserve Phase 4 evidence |

Full commands, test results and limitations: `bda/PHASE5_VALIDATION.md`.
Reproduce with `infrastructure/reliability/run.sh verify` and a new run directory;
retain its ignored Parquet/checkpoints/SQLite/logs for inspection. Publish into a
**new evidence directory** using `python -m bda.reliability.publish`; existing
report files are refused. Large runtime files remain local, not in Git.

Latency population caveats: workload enqueue-to-ack percentiles use the bounded
last 4,096 measured successful acknowledgements; 1,500 warmup records are excluded.
The browser has one observation and no claimed percentiles. Consumer-group lag,
per-record Spark transport latency, long-duration endurance and multi-machine
scaling remain unverified. See `bda/reliability/LATENCY.md`.
