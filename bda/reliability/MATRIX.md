# Reliability test matrix

This matrix describes the checks implemented by the isolated runner, regression
suites and browser verifier. Executed results and report locations are recorded
in `bda/PHASE5_VALIDATION.md`; implementation alone is not evidence of execution.

| Scenario | Expected behavior | Verification source |
| --- | --- | --- |
| Producer before broker startup | Bounded nonblocking submission, eventual delivery failure, clean close; no durable replay promise | Isolated runner `producer_before_broker` |
| Broker temporarily unreachable | One explicitly timed-out event lost; previous/next acknowledged records consumed; producer remains usable | Owned broker stop/restart, `broker_outage` receipt |
| Queue capacity reached | Drop newest with counter; existing queued work drains after worker resumes | Real worker with deliberately blocked factory, unit test |
| Spark stops/restarts | Committed output remains; queries reuse checkpoints and ingest new offsets | Full runner initial/restart receipts |
| Duplicate replay within horizon | Same ID does not inflate analytical count | Acoustic duplicate: 27 valid wire records → 26 unique events |
| Out-of-order arrivals | Accepted within supported event-time horizon | Fixture minute 2 intentionally published before minute 1; exact window counts verified |
| Late beyond watermark | Classified arrival retained; deduped analytical and finalized output counts unchanged | Restart receives new late ID and duplicate after minute-10 watermark control |
| Malformed message | Retained in classified quarantine with Kafka coordinates; excluded from analytical event counts | Actual malformed Kafka JSON; quarantine count one |
| Analytical output unavailable | 503 rather than invented totals; restoration resumes reads | API test renames/restores isolated copied SQLite |
| Empty/corrupt export | Honest zero/empty state or sanitized 503 | API regressions and dashboard empty/error components |
| Dashboard API loss/restoration | Error and retry; successful data after connectivity returns | Chromium offline/online cycle against actual backend |
| WebSocket loss/restoration | Bounded reconnect, deduped buffer, cancelled reconnect on unmount | Frontend hook tests; real socket/browser regression |
| Writer exists but no progress | Running writer, separately stale/absent progress; no healthy-data inference | Telemetry tests |
| Writer dies without termination | Stale heartbeat changes running state to unknown | Timestamp-controlled telemetry test, not destructive production kill |
| Missing/malformed telemetry | Explicit unavailable, no arbitrary paths/errors leaked | Telemetry/API tests |
| Clean shutdown | Continuous `query.stop()` marker exits zero; producer worker and owned server exit | Campaign wrapper code and recorded exit; unit lifecycle/API tests |

The failure experiment deliberately acknowledges the unavoidable loss instead of
claiming exactly-once delivery. Abrupt broker storage destruction, network packet-
level fault injection, physical disk failures and multi-machine failover are not
claimed. Kafka failure injection is performed only on a new disposable broker.
