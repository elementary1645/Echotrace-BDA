# Phase 4 validation — November 7, 2026

Work performed on `feat/bda-analytics-dashboard`; initial checkout was clean and
already on the requested branch. No AGENTS.md was present. No existing data,
commits, Kafka storage, Spark checkpoint or analytical definition was removed.
The Sites skill was used for the repository implementation workflow; this request
is a local repository/Git branch delivery, not a hosted Sites deployment.

## Actual data integration

Executed the existing pinned Spark container with the new controlled exporter:

```bash
infrastructure/batch/run.sh export --input /artifacts/lake-100k --output /artifacts/dashboard.sqlite
```

Completed successfully against the previously complete 100k historical snapshot.
Copied the resulting SQLite into `bda/reports/phase4/history.sqlite`; no raw events
are included. Independently checked total (100,000), every category marginal and
every daily marginal against `bda/reports/phase3/datasets.json`. Snapshot has 68,160
aggregate cells and 477 detected windows. Full identity/checksum/bytes are recorded
in [export-receipt.json](reports/phase4/export-receipt.json). The Jan 1–3 UTC
half-open API selection returns 48 hourly groups and 28,426 events, agreeing with
the earlier independently verified Phase 3 SQL export. ALARM selection returns
1,493 events. Source/category/date filters preserve equal totals across dimensions.

Benchmarks read the original 72 successful checked trial records. Tests verify six
measured 5m trials (three per engine), original provenance and the absence of
internal artifact paths in the public response. Warmup trials remain inspectable
but are excluded from chart medians. No measurements were recreated or extrapolated.

## Checks executed

- `.venv/bin/python -m pytest -q`: **252 passed, 14 skipped in 17.30 s**.
  Skips cover opt-in Spark/Kafka integration and neural-weight tests. The additional
  export test is included as a skip in this fast suite and actually executes below.
  Existing Starlette TestClient deprecation warning remains.
- `infrastructure/batch/run.sh test`: **20 passed in 59.98 s** in the pinned
  Spark/Python 3.10 container. Includes historical SQL/lake regressions, completion
  and overwrite guards, historical dashboard export count/category checks, and
  finalized streaming export with unknown threshold metadata.
- Analytics API tests: **20 passing cases** in the final combined suite (the earlier independent 17-case run passed in 6.47 s), covering genuine
  data/provenance, dates, pagination, bad inputs, empty/missing/corrupt data,
  malformed benchmark output, health and unavailable streaming exports.
- `(cd frontend && npm test)`: **8 passed**, including data transformations, API error parsing, WebSocket reconnection/deduplication/malformed messages, empty/error rendering and cross-view response isolation.
- `(cd frontend && npm run test:e2e)`: **3 passed** against the documented weight-free demo launcher, covering desktop/mobile, all views, actual filtering and deliberate HTTP error handling.
- Production build: TypeScript checking and Vite 6.4.5 build succeeded; main bundle 167.86 kB (54.76 kB gzip), lazy acoustic console 18.72 kB (6.86 kB gzip).
- Python compileall, shell syntax checks and `git diff --check` passed.
- Dependency installation audit after patching Vite/Vitest: **0 vulnerabilities**.

The official small AudioSet label CSV was fetched to enable existing acoustic
regressions. No neural weights were downloaded. Initial minimal-environment run
had 232 passed/30 skipped before labels were available; the final expanded run
above supersedes it. API test requests hung in the restricted sandbox but executed
normally outside it; no application workaround was introduced for that restriction.

## Browser verification and debugging

Installed Chromium and missing OS libraries, plus the already-declared backend
WebSocket transport dependency. Tested the actual production assets through the
actual FastAPI server. Desktop/mobile screenshots are checked in. Browser checks
exercise Overview, historical dates/category selection, anomaly table, original
five-million-record benchmark values, health, live socket and the original acoustic
monitor. Another test intercepts only the overview response with a deliberate 503
and verifies the retry/error presentation. Mobile checks assert no document overflow.

Browser verification exposed two defects that were corrected: ambiguous nested
select labels and stale health response data being rendered as streaming provenance
when navigating quickly between views. A request identity gate and a component
regression test cover the latter. Original acoustic behavior remains covered by
existing API/pipeline tests and the browser monitor navigation check. Actual
microphone/weight-backed inference is outside this session's verification.

## Measured API performance

Executed `.venv/bin/python -m bda.analytics.measure`; retained raw results in
[api-timings.json](reports/phase4/api-timings.json). Ten sequential ASGI requests
per route after one excluded warmup: median overview **101.34 ms**, hourly history
**43.07 ms**, anomalies page **1.60 ms**, cached benchmark report **1.08 ms**.
Payloads were 735 / 13,987 / 8,237 / 28,509 bytes respectively. This is local
in-process timing under concurrent verification load, excluding HTTP network and
Spark setup; it is not a load-test/latency SLO or a new Spark throughput benchmark.

## Verification boundaries

All complete default views use committed genuine project outputs. The optional
streaming exporter is integration-tested against actual Spark DataFrames/Parquet;
a live application Kafka-to-Spark-to-dashboard session was not rerun here. Existing
infrastructure remains untouched. Active Spark liveness, checkpoints, offsets and
last successful streaming update remain explicitly unavailable. Health uses actual
producer observations rather than inferring service health from page rendering.
See [limitations](PHASE4.md) and [full reproduction guide](analytics/DEMO.md).
