import os
import json
import time
import shutil
import subprocess

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format


# ============================================================
# CONFIGURATION
# ============================================================

SPARK_MASTER = "spark://localhost:7077"

HDFS_INPUT = "hdfs://localhost:9000/data/uber/cluster_input"

HDFS_MERGED_OUTPUT = (
    "hdfs://localhost:9000/data/uber/cluster_merged"
)

LOCAL_STREAM_OUTPUT = (
    r"D:\BDS_robust_dashboard_cluster\output\cluster_stream"
)

# Number of records sent immediately
HEADSTART_RECORDS = 50000

# After head-start:
BATCH_SIZE = 20
BATCH_INTERVAL_SECONDS = 10


# ============================================================
# START SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("UberDistributedClusterExperiment")
    .master(SPARK_MASTER)
    .config("spark.sql.shuffle.partitions", "6")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n" + "=" * 70)
print("UBER DISTRIBUTED CLUSTER EXPERIMENT")
print("=" * 70)

print(f"Spark Master : {SPARK_MASTER}")
print(f"HDFS Input   : {HDFS_INPUT}")
print(f"HDFS Output  : {HDFS_MERGED_OUTPUT}")


# ============================================================
# 1. READ DISTRIBUTED SHARDS FROM HDFS
# ============================================================

print("\n[1/5] Reading distributed HDFS shards...")

datetime_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{HDFS_INPUT}/master_datetime.csv")
    .select(
        "trip_id",
        col("Date/Time").alias("event_time")
    )
)

lat_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{HDFS_INPUT}/worker1_lat.csv")
    .select(
        "trip_id",
        col("Lat").alias("pickup_latitude")
    )
)

lon_base_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{HDFS_INPUT}/worker2_lon_base.csv")
    .select(
        "trip_id",
        col("Lon").alias("pickup_longitude"),
        "Base"
    )
)

print("  master_datetime.csv loaded")
print("  worker1_lat.csv loaded")
print("  worker2_lon_base.csv loaded")


# ============================================================
# 2. DISTRIBUTED JOIN
# ============================================================

print("\n[2/5] Performing distributed join on trip_id...")

merged_df = (
    datetime_df
    .repartition(6, "trip_id")
    .join(
        lat_df.repartition(6, "trip_id"),
        on="trip_id",
        how="inner"
    )
    .join(
        lon_base_df.repartition(6, "trip_id"),
        on="trip_id",
        how="inner"
    )
)

# Convert timestamp
merged_df = merged_df.withColumn(
    "event_timestamp",
    to_timestamp(
        col("event_time"),
        "M/d/yyyy H:mm:ss"
    )
)

merged_df = merged_df.select(
    "trip_id",
    "event_timestamp",
    "pickup_latitude",
    "pickup_longitude",
    col("Base").alias("base")
)

print("  Distributed JOIN created.")


# ============================================================
# 3. MATERIALIZE + STORE COMPLETE MERGED DATASET IN HDFS
# ============================================================

print("\n[3/5] Writing distributed merged dataset to HDFS...")


(
    merged_df
    .write
    .mode("overwrite")
    .parquet(HDFS_MERGED_OUTPUT)
)

print("  Distributed output written successfully.")
print(f"  Location: {HDFS_MERGED_OUTPUT}")


# ============================================================
# 4. VERIFY MERGED DATA
# ============================================================

print("\n[4/5] Verifying distributed result...")

merged_count = merged_df.count()

print(f"  Total merged records: {merged_count:,}")

print("\n  Sample merged records:")

(
    merged_df
    .limit(5)
    .show(truncate=False)
)


# ============================================================
# 5. RATE-CONTROLLED STREAM SIMULATION
# ============================================================

print("\n[5/5] Starting controlled cluster stream...")

# Clean local output directory
if os.path.exists(LOCAL_STREAM_OUTPUT):
    shutil.rmtree(LOCAL_STREAM_OUTPUT)

os.makedirs(LOCAL_STREAM_OUTPUT, exist_ok=True)

stream_file = os.path.join(
    LOCAL_STREAM_OUTPUT,
    "merged_events.jsonl"
)

# Read merged dataset from HDFS.
# toLocalIterator() DOES NOT collect the complete dataset
# into driver memory. Records are consumed incrementally.
stream_df = (
    spark.read
    .parquet(HDFS_MERGED_OUTPUT)
    .select(
        "trip_id",
        "event_timestamp",
        "pickup_latitude",
        "pickup_longitude",
        "base"
    )
)

iterator = stream_df.toLocalIterator()


# ------------------------------------------------------------
# HEAD-START: 50,000 records immediately
# ------------------------------------------------------------

print("\n" + "-" * 70)
print(f"HEAD-START: Writing first {HEADSTART_RECORDS:,} records")
print("-" * 70)

start_time = time.time()

written = 0

with open(
    stream_file,
    "w",
    encoding="utf-8"
) as f:

    while written < HEADSTART_RECORDS:

        try:
            row = next(iterator)
        except StopIteration:
            break

        record = {
            "trip_id": row["trip_id"],
            "event_timestamp": (
                row["event_timestamp"].isoformat()
                if row["event_timestamp"]
                else None
            ),
            "pickup_latitude": row["pickup_latitude"],
            "pickup_longitude": row["pickup_longitude"],
            "base": row["base"]
        }

        f.write(
            json.dumps(record) + "\n"
        )

        written += 1

    f.flush()

elapsed = time.time() - start_time

print(
    f"  Head-start written: {written:,} records"
)

print(
    f"  Time taken: {elapsed:.2f} seconds"
)

print(
    f"  Output: {stream_file}"
)


# ------------------------------------------------------------
# CONTINUOUS STREAM
# 20 records every 10 seconds
# ------------------------------------------------------------

print("\n" + "=" * 70)
print("CONTINUOUS STREAM STARTED")
print("=" * 70)

print(
    f"Rate: {BATCH_SIZE} records every "
    f"{BATCH_INTERVAL_SECONDS} seconds"
)

print("Press CTRL+C to stop.")
print("=" * 70)


total_streamed = written
batch_number = 0

try:

    while True:

        batch = []

        for _ in range(BATCH_SIZE):

            try:
                row = next(iterator)
            except StopIteration:
                break

            record = {
                "trip_id": row["trip_id"],
                "event_timestamp": (
                    row["event_timestamp"].isoformat()
                    if row["event_timestamp"]
                    else None
                ),
                "pickup_latitude": row["pickup_latitude"],
                "pickup_longitude": row["pickup_longitude"],
                "base": row["base"]
            }

            batch.append(record)

        # Dataset exhausted
        if not batch:
            print("\nAll merged records have been streamed.")
            break

        # Append batch to local JSONL stream
        with open(
            stream_file,
            "a",
            encoding="utf-8"
        ) as f:

            for record in batch:
                f.write(
                    json.dumps(record) + "\n"
                )

        batch_number += 1
        total_streamed += len(batch)

        print(
            f"[Batch {batch_number:05d}] "
            f"+{len(batch):02d} records | "
            f"Total streamed: {total_streamed:,}"
        )

        if len(batch) < BATCH_SIZE:
            print("\nFinal batch reached.")
            break

        time.sleep(BATCH_INTERVAL_SECONDS)

except KeyboardInterrupt:

    print("\n\nStream stopped by user.")

finally:

    spark.stop()

    print("\n" + "=" * 70)
    print("CLUSTER EXPERIMENT FINISHED")
    print("=" * 70)

    print(
        f"Total streamed records: {total_streamed:,}"
    )

    print(
        f"Local stream output: {stream_file}"
    )