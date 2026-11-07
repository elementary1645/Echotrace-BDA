# Phase 3 design

Add an isolated `artifacts/phase3` workspace and independent batch entry points;
leave Kafka, its volumes, and Phase 2 checkpoints unchanged.

Raw: immutable chunked JSONL or reproducibly gzipped JSONL, v1 events, deterministic
identities and SIMULATED provenance; checksummed completion manifest. Quality
fixtures are separate and cannot enter benchmark comparison manifests.
Validated: immutable Parquet snapshots partitioned by event_date, with dataset
identity/origin columns added without changing the Phase 2 wire or sink schemas.
Raw data is referenced instead of copied. Aggregated: hourly, daily and category
trends, plus explainable historical anomaly windows. Publish `_COMPLETE` only after
all writes and provenance metadata succeed.

Queries: total, hourly/daily, category counts/confidence, zones, monitoring source,
source classification, peak hours, source comparison, rolling prior activity,
category/day trends, and historical anomaly frequency. UTC half-open period filters
retain the Phase 1 stream-position wall-time semantics. Bounded machine-readable
aggregate exports support a later dashboard.

Comparison: identical raw files and semantic workload for one-pass Python and Spark
SQL GROUPING SETS. Verify each trial against independently accumulated manifest
statistics; keep warmups separate, run >=3 measured trials, retain failed/incomplete
journals, and capture event logs and cgroup resource samples. Exclude generation,
checksum verification, Spark startup and result checking/serialization from analytical
timing, reporting setup separately. OS cache is uncontrolled; no cold-cache claim.

Experiments: 100k and 1m records initially, local[1]/local[2], controlled input and
shuffle partitions. Larger generation requires conservative disk checks; 5m/10m
are supported targets rather than automatic promises. Keep benchmarks sequential.
