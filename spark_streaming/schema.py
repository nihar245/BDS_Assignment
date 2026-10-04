"""Spark schema for canonical Uber trip events."""

from pyspark.sql.types import DoubleType, StringType, StructField, StructType


UBER_EVENT_SCHEMA = StructType(
    [
        StructField("trip_id", StringType(), False),
        StructField("event_timestamp", StringType(), False),
        StructField("pickup_latitude", DoubleType(), False),
        StructField("pickup_longitude", DoubleType(), False),
        StructField("base", StringType(), False),
    ]
)
