"""Consume canonical Uber events from Kafka and persist them to HDFS and PostgreSQL."""

import os

from streaming_functions import (
    create_kafka_read_stream,
    create_spark_session,
    transform_uber_stream,
    write_parquet_stream,
    write_postgres_stream,
)


def main() -> None:
    spark = create_spark_session()
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "uber_trips")
    output_path = os.getenv("HDFS_OUTPUT_PATH", "hdfs://localhost:9000/data/uber/trips")
    checkpoint_path = os.getenv("SPARK_CHECKPOINT_PATH", "hdfs://localhost:9000/data/uber/checkpoints/trips")
    postgres_host = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port = os.getenv("POSTGRES_PORT", "5432")
    postgres_db = os.getenv("POSTGRES_DB", "uber_streaming")
    postgres_user = os.getenv("POSTGRES_USER", "postgres")
    postgres_password = os.getenv("POSTGRES_PASSWORD")
    postgres_url = os.getenv("POSTGRES_JDBC_URL", f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}")
    postgres_target_table = os.getenv("POSTGRES_TABLE", "staging.uber_trips")
    postgres_ingest_table = os.getenv("POSTGRES_INGEST_TABLE", "staging.uber_trips_ingest")
    postgres_checkpoint = os.getenv("POSTGRES_CHECKPOINT_PATH", "hdfs://localhost:9000/data/uber/checkpoints/postgres")
    trigger = os.getenv("STREAMING_TRIGGER", "10 seconds")
    starting_offsets = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")

    if not postgres_password:
        raise RuntimeError("POSTGRES_PASSWORD must be set for the PostgreSQL sink")

    raw_stream = create_kafka_read_stream(spark, bootstrap_servers, topic, starting_offsets)
    trips = transform_uber_stream(raw_stream)
    write_parquet_stream(trips, output_path, checkpoint_path, trigger)
    write_postgres_stream(
        trips,
        postgres_url,
        postgres_ingest_table,
        postgres_target_table,
        postgres_user,
        postgres_password,
        postgres_checkpoint,
        trigger,
    )
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
