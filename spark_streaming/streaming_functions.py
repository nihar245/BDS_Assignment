"""Reusable Spark Structured Streaming transformations and sinks."""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, dayofweek, from_json, hour, to_date, to_timestamp

try:
    from .schema import UBER_EVENT_SCHEMA
except ImportError:
    from schema import UBER_EVENT_SCHEMA


def create_spark_session(app_name: str = "Uber Trip Streaming") -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", os.getenv("SOURCE_TIMEZONE", "America/New_York"))
        .getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", os.getenv("SOURCE_TIMEZONE", "America/New_York"))
    return spark


def create_kafka_read_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    starting_offsets: str = "earliest",
) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )


def transform_uber_stream(stream: DataFrame) -> DataFrame:
    parsed = (
        stream.selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), UBER_EVENT_SCHEMA).alias("event"))
        .select("event.*")
    )
    timestamp = to_timestamp(col("event_timestamp"))
    return (
        parsed.withColumn("event_timestamp", timestamp)
        .filter(
            col("trip_id").isNotNull()
            & col("event_timestamp").isNotNull()
            & col("pickup_latitude").between(-90.0, 90.0)
            & col("pickup_longitude").between(-180.0, 180.0)
            & col("base").isNotNull()
            & (col("base") != "")
        )
        .withColumn("trip_date", to_date(col("event_timestamp")))
        .withColumn("pickup_hour", hour(col("event_timestamp")))
        .withColumn("day_of_week", dayofweek(col("event_timestamp")))
    )


def write_parquet_stream(
    stream: DataFrame,
    output_path: str,
    checkpoint_path: str,
    trigger: str = "10 seconds",
):
    return (
        stream.writeStream.format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("trip_date")
        .trigger(processingTime=trigger)
        .start()
    )


def write_postgres_stream(
    stream: DataFrame,
    jdbc_url: str,
    ingest_table: str,
    target_table: str,
    user: str,
    password: str,
    checkpoint_path: str,
    trigger: str = "10 seconds",
):
    """Write batches through an idempotent PostgreSQL staging/merge boundary."""

    def write_batch(batch: DataFrame, batch_id: int) -> None:
        records = batch.count()
        if records == 0:
            return

        (
            batch.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", ingest_table)
            .option("user", user)
            .option("password", password)
            .option("driver", "org.postgresql.Driver")
            .option("batchsize", "5000")
            .mode("append")
            .save()
        )

        import psycopg2
        from urllib.parse import urlparse

        parsed = urlparse(jdbc_url.replace("jdbc:", ""))
        connection = psycopg2.connect(
            host=parsed.hostname,
            port=parsed.port or 5432,
            dbname=parsed.path.lstrip("/"),
            user=user,
            password=password,
        )
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO {target_table}
                        (trip_id, event_timestamp, pickup_latitude, pickup_longitude,
                         base, trip_date, pickup_hour, day_of_week)
                    SELECT trip_id, event_timestamp, pickup_latitude, pickup_longitude,
                           base, trip_date, pickup_hour, day_of_week
                    FROM {ingest_table}
                    ON CONFLICT (trip_id) DO NOTHING
                    """
                )
                cursor.execute(f"TRUNCATE TABLE {ingest_table}")
                cursor.execute(
                    """
                    INSERT INTO analytics.streaming_batches
                        (batch_id, records_received, processed_at)
                    VALUES (%s, %s, now())
                    ON CONFLICT (batch_id) DO UPDATE SET
                        records_received = EXCLUDED.records_received,
                        processed_at = EXCLUDED.processed_at
                    """,
                    (batch_id, records),
                )
            connection.commit()
        finally:
            connection.close()

    return (
        stream.writeStream.foreachBatch(write_batch)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger)
        .start()
    )
