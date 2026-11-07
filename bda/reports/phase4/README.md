# Phase 4 dashboard evidence

- `history.sqlite`: reviewed read-only serving snapshot from the completed Phase 3
  100k synthetic historical Parquet lake. Aggregate cells and detected anomalies;
  no raw audio/events. Source identity is stored in its metadata table.
- `export-receipt.json`: dataset identity, snapshot SHA-256, counts, size and
  independent comparisons against the original generator manifest.
- `api-timings.json`: actual local sequential ASGI response measurements with
  method, trial count and payload sizes. Separate from Spark campaign results.
- `overview-desktop.png`, `overview-mobile.png`: Chromium captures of production
  React assets served by real FastAPI endpoints, not fixture-only renderings.

See `bda/PHASE4_VALIDATION.md` for exact commands, results and verification limits.
Regenerate the serving snapshot using `bda/analytics/DEMO.md`; do not edit SQLite
in place while the application serves it. Original Phase 3 measurements are kept
unchanged in the adjacent `phase3/` report directory.
