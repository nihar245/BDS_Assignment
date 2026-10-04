# Troubleshooting

- Kafka connection errors: verify the broker is running, `KAFKA_BOOTSTRAP_SERVERS` is reachable, and `uber_trips` exists.
- Spark cannot read Kafka: make the matching `spark-sql-kafka-0-10` connector available to `spark-submit`.
- HDFS write failures: verify the namenode URI, permissions, and both configured paths.
- dbt connection errors: check every `POSTGRES_*` value and run from the `dbt/` directory with `--profiles-dir .`.
- Empty dashboard: load processed rows into `staging.uber_trips` and create the analytics views first.
- Missing local dataset: run `scripts/download_dataset.sh`; never commit the resulting CSV.
