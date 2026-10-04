# PostgreSQL

Run `postgres/sql/001_create_schemas.sql`, then `002_create_staging_table.sql`, then `003_analytics_views.sql`.

`staging.uber_trips` is the durable serving boundary with a primary key on `trip_id`. Spark first appends each micro-batch to `staging.uber_trips_ingest`, then PostgreSQL merges it into `staging.uber_trips` with `ON CONFLICT (trip_id) DO NOTHING`. This makes normal Kafka/Spark replays safe at the database boundary.

`analytics.streaming_batches` records Spark batch IDs, record counts, and processing timestamps for dashboard observability. dbt builds the analytical tables in the `analytics` schema from the staging source.

Connection settings are `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_JDBC_URL`, `POSTGRES_TABLE`, `POSTGRES_INGEST_TABLE`, and `POSTGRES_CHECKPOINT_PATH`. Supply the PostgreSQL JDBC package to Spark at submission time; no JAR is committed to the repository.
