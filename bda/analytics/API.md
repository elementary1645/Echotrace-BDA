# Analytics API v1

Prefix: `/api/v1/analytics`. FastAPI OpenAPI: `/docs` and `/openapi.json`.
All routes are read-only GETs. Existing acoustic routes and WebSocket retain their
contracts. Missing/corrupt exports return 503; malformed/unsupported query inputs
return 422; unknown routes return 404. No Spark work runs on requests.

| Route | Result |
| --- | --- |
| `/overview` | Historical count, distinct source IDs, time range, weighted confidence, category counts and detected-window count |
| `/catalog` | Bounded distinct historical source IDs (100) and categories (50) |
| `/historical` | Filtered aggregate rows, weighted confidence and pagination |
| `/anomalies` | Actual detected windows, prior baseline statistics, flags, explanation and pagination |
| `/ground-truth` | Separately labeled original 100k generator injected anomalies; full snapshot, unfiltered |
| `/streaming` | Optional finalized-window export, latest 200 cells; 503 when absent |
| `/benchmarks` | All 72 original trial rows including labeled warmups, safe environment metadata, storage and methodology |
| `/health` | Process observations, producer counters and explicit unknown Spark telemetry |

Historical/anomaly responses use:

```json
{
  "schema_version": "1",
  "provenance": {
    "dataset_id": "sha256 dataset identity",
    "origin": "synthetic",
    "source": "SIMULATED",
    "engine": "Apache Spark 4.0.1",
    "exported_at": "ISO UTC timestamp",
    "granularity": "hour",
    "event_count": 100000,
    "threshold": 100,
    "schema_version": 1
  },
  "served_at": "ISO UTC timestamp",
  "stale": false,
  "total": 168,
  "offset": 0,
  "limit": 200,
  "rows": []
}
```

Provenance `event_count` is the **whole snapshot**, not the filtered selection.
`total` counts result groups/windows before pagination. Sum returned `value` for
returned-page counts. Reconcile `total` with `offset/limit` before treating a page
as a full selection. UI uses daily rows for the filtered population count.

Historical rows: `{label, value, avg_confidence}`. `dimension` allowlist:
`hour` (default), `day`, `category`, `source_id`, `zone`, `confidence_bin`.
`label` is the grouped key; `value` is event count. Count weighting applies to
confidence regardless of grouping. Confidence bins are integers 0–9.

Shared historical/anomaly filters: `start`, `end`, `source_id`, `category`.
Date bounds must include a timezone, normalize to whole UTC hours and use
`[start,end)`. An explicit pair must span >0 and <=366 days. Omitted bounds use
the retained snapshot. Identifiers are length/character constrained; unknown valid
identifiers return empty data. Historical `limit`: 1–500 (default 200); anomaly
`limit`: 1–200 (default 50). `offset`: 0–100000. Stable sort precedes pagination.

Anomaly rows: `hour, source, source_id, category, event_count, history_count,
baseline_mean, baseline_stddev, z_score, threshold_exceeded, baseline_exceeded,
explanation, kind`. Nullable baseline/z-score values mean unavailable, never zero.
`kind=detected`; flags copied from Spark. For this historical definition, absolute
threshold is manifest-defined, z threshold is 3, history is 24 observed windows.

Streaming rows: `hour, source, source_id, category, zone, event_count,
avg_confidence`. Original source labels remain on each row. Snapshot freshness
is export time, not query liveness. Streaming threshold is null when not recorded.
Health does not expose paths, credentials, mode files or raw exception details.
Producer counters retain their original names; absent counters have not yet been
observed, rather than fabricated zero-valued measurements.

```bash
curl 'http://127.0.0.1:8000/api/v1/analytics/historical?dimension=day&start=2026-01-01T00:00:00Z&end=2026-01-03T00:00:00Z'
curl 'http://127.0.0.1:8000/api/v1/analytics/anomalies?category=ALARM&limit=20&offset=0'
```
