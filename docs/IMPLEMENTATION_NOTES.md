# Robust Pipeline Implementation Notes

This revision hardens the existing local pipeline without adding a new platform or service.

## Reliability changes

- Producer now sends configurable batches, defaulting to 10 events every 10 seconds.
- Producer persists its event position and resumes after restart.
- Kafka producer waits for acknowledged delivery and retries transient failures.
- Source timestamps are correctly interpreted as `America/New_York` before conversion to UTC.
- Spark derives date/hour/day attributes in the source timezone.
- PostgreSQL ingestion uses a separate append-only ingest table followed by an idempotent `ON CONFLICT DO NOTHING` merge into the primary staging table.
- Spark records recent micro-batch counts in `analytics.streaming_batches` for observability.
- Streamlit refreshes its dashboard fragment every 10 seconds.
- dbt now exposes a broader analytics layer for KPIs, time patterns, base/hour patterns, geographic grid density, and daily trends.

## Delivery semantics

Kafka + Spark checkpoints provide restart/replay behavior. PostgreSQL is explicitly made idempotent using the deterministic `trip_id` key. The HDFS Parquet sink remains append-oriented; a crash during a sink write can theoretically produce a repeated physical file on replay. The analytical PostgreSQL layer remains deduplicated, so this is not presented as end-to-end exactly-once storage.

## Existing database migration

For a database that already contains the original project tables, run `postgres/sql/004_streaming_resilience.sql` once before starting the revised Spark job. It creates only the additional ingest and observability tables and is safe to rerun.

## Data contract

The source contains only `Date/Time`, `Lat`, `Lon`, and `Base`. No fare, distance, duration, passenger, payment, driver, or revenue fields are fabricated.
