# EchoTrace-BDA Phase 1

Kafka is an optional analytics side channel. Spark is intentionally outside this phase.
Run commands from the repository root. Python 3.11+ and Docker Compose v2 are
recommended; the existing application also needs PortAudio/libsndfile and its usual
model assets for LIVE/RECORDED inference. SIMULATED needs no neural model weights.

## Install and start

```bash
python -m venv .venv
source .venv/bin/activate
# Install the appropriate CPU/GPU torch wheel first if using actual inference.
pip install -r backend/requirements.txt -r bda/requirements.txt

docker compose -f infrastructure/kafka/compose.yml config --quiet
docker compose -f infrastructure/kafka/compose.yml up -d --wait
docker compose -f infrastructure/kafka/compose.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic echotrace.events.v1 --partitions 3 --replication-factor 1
docker compose -f infrastructure/kafka/compose.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic echotrace.events.v1
```

`--if-not-exists` does not repair a differently configured existing topic; inspect
its description. The expected result is three partitions, replication factor one,
and leader/broker ID one. The Compose file pins Kafka 4.1.2, persists broker data in
a named volume, disables automatic topic creation, and provides a health check.
The localhost advertised listener supports applications on the Docker host only.
This is single-broker, plaintext development infrastructure, without production
security or redundancy. The earlier Compose file did not mount persistent storage;
recreating an old container with this file starts a new data directory. Export any
old records you need before recreating that container. No automatic migration is provided.

```bash
export KAFKA_ENABLED=true
export BDA_SOURCE_ID=lab-array-01
# Explicit timezone of the host that generates EchoTrace's naive local wall times.
export BDA_SOURCE_TIMEZONE=Asia/Kolkata
python backend/scripts/serve.py --source sim
```

Use the actual producer host timezone, e.g. `UTC` on a UTC host. Kafka disabled is
the default. No Kafka client import or connection occurs when disabled. Missing
Kafka dependencies or invalid configuration are logged without stopping EchoTrace.

In another terminal with the same virtual environment, from the repository root:

```bash
python -m bda.consumer
# To replay using a new development consumer group:
python -m bda.consumer --group echotrace-bda-review-01
```

The default group is `echotrace-bda-dev-v1`; new groups read from earliest retained
offsets, existing groups resume committed offsets. Accepted records print as JSON.
Invalid records print the topic, partition, offset, reason, and raw payload to stderr.
This development consumer commits synchronously after reporting each record,
including invalid records. It supplies no quarantine store or downstream transaction.
SIGINT/SIGTERM closes it cleanly. Consumers with the same group share partitions.

## Environment

| Variable | Default | Meaning |
| --- | --- | --- |
| `KAFKA_ENABLED` | `false` | Opt in to analytics publishing |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker list |
| `KAFKA_TOPIC` | `echotrace.events.v1` | Existing destination topic |
| `BDA_SOURCE_ID` | `echotrace-local` | Stable deployment/device identity; set uniquely for each source |
| `BDA_SESSION_ID` | random UUID per application lifespan | Base session identity; do not reuse across restarts |
| `BDA_SOURCE_TIMEZONE` | unset | IANA timezone required for naive input timestamps |
| `KAFKA_QUEUE_SIZE` | `1024` | Application record queue and Kafka client message count limit |
| `KAFKA_DELIVERY_TIMEOUT_MS` | `10000` | Client retry/delivery deadline, minimum 1000 ms |
| `KAFKA_FLUSH_TIMEOUT` | `5` | Final client flush budget in seconds |

Effective session IDs append `/g<generation>` at every successful source start,
including switches back to the same mode. Random base sessions prevent event ID
reuse after server restarts. An explicitly reused session ID forfeits that isolation.
Frame, sequence, status, and snapshot messages are never published.

## Schema and timestamp semantics

`schema.py` defines the validated, frozen v1 contract. Unknown fields and versions
are rejected. Canonical UTF-8 serialization sorts keys, uses compact JSON, and
rejects NaN and infinity. The adapter leaves the original message unchanged.

| Field | Meaning |
| --- | --- |
| `schema_version`, `record_type` | Literal `1.0`, `event` |
| `record_id` | SHA-256 of the unambiguous JSON array `[source_id, session_id, event_id]` |
| `source_id`, `session_id`, `event_id` | Deployment identity, source-run identity, original EchoTrace ID |
| `source` | Original `LIVE`, `RECORDED`, or `SIMULATED` classification |
| `event_time` | UTC-normalized original message `wall`: session start wall time plus `t` |
| `t` | Analyzer stream position in session-relative seconds at event-message construction; rounded to milliseconds upstream |
| `t_start`, `t_end` | Original acoustic interval onset/end in session-relative seconds |
| `category`, `label`, `confidence` | Acoustic category, detection label, probability in [0,1] |
| `zone`, `zone_confidence`, `angle_deg` | Original LEFT/CENTRE/RIGHT/UNKNOWN zone, high/low/null confidence, optional finite angle |

The source audit found `Analyzer._base(self.t)` in `add_event()`, and `wall(t)`
calculates `start_wall + timedelta(seconds=t)`. Therefore `event_time` is neither
necessarily onset nor actual emission wall time. It normally corresponds to the
acoustic end/stream position. Internal `Event.wall` represents onset, but is not
used as the emitted message's `wall`. `capture_time` is a monotonic performance
clock for latency and is deliberately excluded from analytics. Recorded playback
wall time describes the playback session, not the original file's recording time.

Aware timestamps retain their offset meaning and normalize to UTC. Naive inputs
require an explicit IANA timezone; ambiguous and nonexistent DST local times are
rejected. No implicit UTC assumption is made. Intervals require
`0 <= t_start <= t_end <= t`, allowing 1 ms for upstream rounding of `t`.
Localization confidence is categorical in the source, so no numerical confidence
is invented. Identifiers and labels must contain a non-whitespace character.

## Architecture and delivery limits

The existing broadcaster filters stale generations first. Its analytics submission
performs validation/serialization and a nonblocking queue insertion; it never waits
for Kafka I/O. A nonblocking switch-lock acquisition avoids waiting during source
changes; events encountered while a switch holds that lock are not submitted.
The detector worker and WebSocket JSON contract remain unchanged. Existing
capture-to-WebSocket latency measurement remains in its original location.

One producer thread per enabled application lifespan constructs the Kafka client,
polls delivery callbacks, drains accepted records, and flushes on shutdown. Both
the application queue and client queue are bounded. Full queues drop the newest
record with an explicit metric/log; client queue errors are also reported. Queued
old generations are dropped before submission after switching. Records already
handed to the Kafka client cannot be retracted, and may complete after a switch;
they keep their original identity and provenance. This is a best-effort analytics
channel, with no durable outbox or end-to-end exactly-once/at-least-once guarantee.

The client uses `acks=all` and idempotent production for retries within its producer
session. Application replay is not deduplicated: identical input yields identical
IDs, permitting downstream deduplication, but repeated submissions can produce
multiple Kafka records. A single broker cannot provide replica fault tolerance.
Outages retry only within the configured client delivery deadline; failures,
overflow, process crashes, and bounded shutdown may lose records.

Metrics are available via `app.state.engine.analytics.snapshot()` and emitted as
structured JSON on errors and worker shutdown: enqueued/submitted/delivered,
invalid, dropped_queue_full, dropped_stale, send_failed, delivery_failed,
broker_errors, worker_failed, flush_incomplete/flush_failed, shutdown_timeout,
queue_depth, and accepting. Counters are local to a lifespan, not durable telemetry.
An unexpected worker failure disables acceptance for that lifespan; restart the
application after fixing the cause. Normal broker reconnection is handled by the
Kafka client. No throughput or latency performance claim has been measured.

Client callback polling and flush follow the [official Python client documentation](https://docs.confluent.io/kafka-clients/python/current/overview.html).

## Tests and actual delivery verification

```bash
python -m pytest bda/tests -m 'not integration' -q
python -m pytest backend/tests -q
RUN_KAFKA_INTEGRATION=1 python -m pytest bda/tests/test_kafka_integration.py -q -s
python -m compileall -q bda backend/echotrace backend/scripts
docker compose -f infrastructure/kafka/compose.yml config --quiet
```

Unit tests use fakes and never need Kafka. The integration test is skipped unless
explicitly enabled, then fails if the broker or topic is unavailable. It runs one
finite actual EchoTrace simulated scenario through `Pipeline`, publishes all its
events under a unique session, consumes the topic, validates every matching record,
and checks keys, provenance, IDs, and delivery acknowledgements. It does not use a
mock producer, create topics, or depend on model weights. Older unrelated smoke
records are ignored by session; malformed records are reported by the standalone
consumer. For manual verification, run that consumer and the enabled server above;
observe matching event IDs and `SIMULATED` source in both `/api/events` and Kafka.
Restart the server with Kafka disabled to check the original operation independently.

## Shutdown

Stop the server with Ctrl-C first to stop detection and drain/flush analytics; then
stop the consumer and run:

```bash
docker compose -f infrastructure/kafka/compose.yml down
```

The named volume survives `down`. `down -v` permanently deletes the development
broker data; use it only when intentionally resetting the stack.
