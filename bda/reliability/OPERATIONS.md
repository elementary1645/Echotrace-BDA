# Phase 5 reproduction and operational safety

## Dependency and resource preflight

The tested host is Linux x86_64, Python 3.14.2, Node 24.22.2 and Docker Compose v2.
The host lock is `infrastructure/reliability/requirements-py314.lock`; the existing
Spark image retains its separate Python 3.10.12 / Spark 4.0.1 / Java 17 lock and
resource limits. Other host Python versions require resolving the existing ranges
and rerunning tests; the Python 3.14 lock is not asserted portable to them.

```bash
python3.14 -m venv .venv
.venv/bin/python -m pip install -r infrastructure/reliability/requirements-py314.lock
(cd frontend && npm ci && npx playwright install --with-deps chromium)
# Prepare explicitly if missing; these are downloads/builds, not broker recreation:
docker pull apache/kafka:4.1.2
infrastructure/spark/run.sh build
infrastructure/reliability/run.sh preflight
```

PortAudio/libsndfile system libraries are needed by acoustic module imports even
for simulation. On Debian use `libportaudio2 libsndfile1`; browser setup also needs
Chromium libraries. No neural weights are needed. Preflight requires >=3 GiB
available RAM and >=2 GiB free disk, cached images, dependencies and the committed
Phase 3/4 evidence. Spark uses two local worker threads, two CPU quota, 3 GiB memory
and a 1 GiB heap. The isolated broker uses one CPU quota and 1 GiB memory, a 256 MiB
heap. Containers share host CPUs; quotas are not reserved dedicated hardware.

## One-command isolated verification

Use a new output directory every time, from repository root:

```bash
infrastructure/reliability/run.sh verify artifacts/phase5/evaluator-001 --campaign --port 19096
```

The wrapper runs actual Kafka/Spark failure/integration tests, the paced workload,
a production frontend build, generated-output API checks and the Chromium test.
It creates a new unique Compose project and new dedicated topic identities, keeps
all output/checkpoints in that run directory, and stops only its own broker. It
never starts/stops/recreates the application's broker. Its new Kafka volume is
retained. Existing Ivy cache is copied read-only when available; a missing cache
requires Maven network access for the documented connector. No public Spark UI or
administration endpoint is exposed.

The runner refuses occupied ports and existing output directories. Pick another
unused loopback port rather than stopping an unknown service. Do not pass the
application broker port. Readiness requires Compose health and actual successful
record acknowledgements; coordinator initialization may trail topic readiness.
Do not shorten the healthy-path delivery timeout to the deliberately failing
startup test's timeout.

For individual steps (a retained successful run):

```bash
.venv/bin/python -m bda.reliability.presentation --run artifacts/phase5/evaluator-001
# New output directory, only after other timed workloads finish:
.venv/bin/python -m bda.reliability.optimize --output artifacts/phase5/index-experiment-001
```

The presentation verifier starts and shuts down its own loopback demo server on
8005, checks real HTTP output and starts Chromium against it. It is separate from
an already-running application. `receipt.json`, per-step logs, expected/output
JSON, Parquet, checkpoints, immutable SQLite, progress journals and sampled Docker
resources are retained. Failures stay visible; rerun with a new directory after
fixing the cause. Do not relabel a failed receipt as passed.

## Dashboard observability

The default dashboard still uses the reviewed Phase 4 historical snapshot. To
observe a generated run without replacing it, configure trusted **server-side**
paths, then start the weight-free demo:

```bash
BDA_STREAMING_DATABASE="$PWD/artifacts/phase5/evaluator-001/streaming.sqlite" \
BDA_TELEMETRY_ROOT="$PWD/artifacts/phase5/evaluator-001/acoustic" \
  infrastructure/dashboard/run.sh serve
```

Live Analytics shows finalized Spark rows separately from WebSocket buffer counts.
System Health reads bounded, allowlisted telemetry and offline verified output
summaries. No request accepts file paths, SQL or administrative options. Optional
`BDA_HISTORY_DATABASE` selects a reviewed alternative historical serving snapshot;
keep original committed evidence unchanged. Replace serving exports atomically,
not in place. These paths are operator configuration and are not exposed in API
responses. Invalid/missing outputs report unavailable and data requests return 503.

Telemetry definitions:

- Writer heartbeat records `observed_at`; >15 seconds without a heartbeat changes
  a previously running writer to unknown. A stopped record remains stopped.
- `last_progress_at` is callback observation of successful Spark progress, with
  a separate >60-second stale-progress indicator. Running-but-idle is not fresh data.
- Input/processed rates and batch duration are Spark-reported micro-batch metrics,
  not inferred from frontend counts. Watermark is event-time finalization progress.
- Discovered-offset backlog is latest discovered Kafka offsets minus processed end
  offsets when both are supplied. It is not committed consumer-group lag. Genuine
  consumer-group lag stays null/unavailable where not observed.
- Quarantine/validated/anomaly counters come from the explicit Parquet verifier,
  timestamped as an offline summary. They are not live per-batch counters.
- Export freshness is export publication time, not active query liveness or the
  timestamp of the last acoustic event. Producer counters are process cumulative;
  last acknowledgement/error observations determine the displayed evidence state.

## Shutdown and safe cleanup

Normal runner completion stops its disposable services while preserving data.
The continuous-ingest campaign creates an operator marker that invokes
`StreamingQuery.stop()` and waits for checkpoint/sink shutdown. For an operator-
launched continuous stage, `--stop-file ROOT/stop.request` must be inside its root;
create it to request a clean exit. Use a new marker name/remove only your own
previous marker before intentionally starting another controlled continuous run.
SIGINT/SIGTERM support remains; abrupt process death can leave telemetry stale.

Cleanup is a separate, explicit action. Default is a read-only dry run:

```bash
.venv/bin/python -m bda.reliability.cleanup --run artifacts/phase5/evaluator-001
# Only when the listed resources are intentionally disposable:
.venv/bin/python -m bda.reliability.cleanup --run artifacts/phase5/evaluator-001 --execute
# Optional additional destruction of that run's disposable Kafka volume only:
.venv/bin/python -m bda.reliability.cleanup --run artifacts/phase5/evaluator-001 --execute --delete-volume
```

The cleanup helper verifies generated project identity, stopped containers and
both volume ownership labels. It retains run files/checkpoints/evidence, refuses
running resources, and never invokes a global prune or `down -v`. Do not delete
application volumes or checkpoints. Original Kafka startup/migration safeguards
and the Phase 2 data-loss incident record remain applicable and unchanged.

## Troubleshooting

| Symptom | Action |
| --- | --- |
| Port occupied | Choose an unused dedicated port; do not stop the existing listener |
| Spark image/cache missing | Explicit build; first connector resolution needs Maven access |
| Resource preflight refusal | Reduce unrelated workloads or use another machine; never delete user data |
| Topic ready but initial record times out | Inspect coordinator startup and delivery errors; healthy path uses 10-second timeout |
| Export missing/corrupt | Inspect producer/job/verifier logs; publish a new complete export atomically |
| Stream stopped but health says unknown | Writer died before termination telemetry; inspect heartbeat age and retained logs |
| No finalized windows | Advance genuine event time; idle wall time cannot advance watermarks. Only isolated synthetic tests use control records |
| Invalid record | Inspect isolated quarantine with Kafka coordinates; never silently skip it |
| API inaccessible | Check loopback server process/configuration; UI exposes retry and stale buffered data |
| Graceful stop times out | Retain logs and failed receipt; inspect only the generated project's processes before manual intervention |

## Guarantees and security limits

The producer is bounded best effort, not a durable outbox. Queue-full, stale-source,
send failure, delivery timeout and forced shutdown can lose records; counters make
these windows visible. Idempotent Kafka producer retries do not deduplicate
application re-submission. Spark's native file sinks/checkpoints and watermark-
bounded event-ID state provide tested restart/deduplication behavior within the
recorded assumptions, not an end-to-end exactly-once guarantee. Unfinalized trailing
windows may wait indefinitely; late events beyond the horizon may be rejected.

This is a local academic development stack: plaintext Kafka, one broker, replication
factor one, local Spark and no web authentication/authorization hardening. Ports
bind loopback; internal services use their isolated Docker network. Do not expose
these services to untrusted networks. No credentials are configured or returned by
the telemetry API. Subprocess arguments use arrays, not shell interpolation; no
user-controlled SQL/paths enter HTTP operations. Raw quarantine and diagnostic
logs can contain source payloads and should remain local to authorized operators.

A completed presentation receipt is immutable: the verifier refuses to overwrite
`presentation.json`. Inspect its existing result or run a new isolated pipeline
for another complete observation. A same-checkpoint Spark query restart preserves
the previous successful progress observation until a new batch arrives; it does
not misrepresent an idle restarted writer as fresh processing.
