# Spark Structured Streaming

`spark_streaming/stream_all_events.py` reads Kafka values, parses the centralized schema, validates the event fields, derives time attributes using `America/New_York`, and writes the transformed stream to two checkpointed sinks:

1. HDFS Parquet, partitioned by `trip_date`.
2. PostgreSQL through an idempotent `foreachBatch` ingest/merge boundary.

The PostgreSQL boundary is intentionally separate from the final primary-key table. A replayed Spark micro-batch can therefore be retried without failing because its `trip_id` already exists.

Use the host Spark 3.5.9 installation with the matching Kafka and PostgreSQL connectors:

```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,org.postgresql:postgresql:42.7.7 spark_streaming/stream_all_events.py
```

The default trigger is 10 seconds. Defaults are `hdfs://localhost:9000/data/uber/trips`, `hdfs://localhost:9000/data/uber/checkpoints/trips`, and `hdfs://localhost:9000/data/uber/checkpoints/postgres`.

A normal Structured Streaming checkpoint must remain paired with its Kafka topic. Do not point a fresh Kafka topic at an old checkpoint unless an intentional replay/reset is being performed.
