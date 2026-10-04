# Producer

`producer/uber_producer.py` reads the FiveThirtyEight CSV with `csv.DictReader`, converts each valid row to the canonical event, and publishes JSON to Kafka.

Default live-demo behavior is **10 events every 10 seconds**. The producer persists its next event position in `data/producer_state.json`, so a restart resumes instead of starting from the first row. Kafka publishing uses `acks=all` and retries; if a process dies after Kafka accepts a batch but before the position file is updated, the deterministic `trip_id` allows downstream PostgreSQL ingestion to ignore the replay.

The source `Date/Time` values are local New York pickup timestamps. The producer converts them from `America/New_York` to UTC before putting them in the canonical `event_timestamp` field. Spark uses the same source timezone when deriving date/hour attributes.

Configuration is available through flags or environment variables: `--input`/`UBER_INPUT_PATH`, `--bootstrap-servers`/`KAFKA_BOOTSTRAP_SERVERS`, `--topic`/`KAFKA_TOPIC`, `--batch-size`/`PRODUCER_BATCH_SIZE`, `--interval-seconds`/`PRODUCER_INTERVAL_SECONDS`, `--limit`/`PRODUCER_LIMIT`, and `--state-path`/`PRODUCER_STATE_PATH`. Use `--no-resume` only when an intentional replay is required.
