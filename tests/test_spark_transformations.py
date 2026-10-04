import inspect

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from spark_streaming.streaming_functions import (  # noqa: E402
    transform_uber_stream,
    write_postgres_stream,
)


def test_postgres_sink_has_separate_configuration():
    parameters = inspect.signature(write_postgres_stream).parameters
    assert list(parameters) == [
        "stream",
        "jdbc_url",
        "table",
        "user",
        "password",
        "checkpoint_path",
        "trigger",
    ]


def test_transform_adds_time_columns_and_rejects_bad_coordinates():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    try:
        rows = [
            '{"trip_id":"good","event_timestamp":"2014-04-01T12:30:00+00:00","pickup_latitude":40.7,"pickup_longitude":-73.9,"base":"B02512"}',
            '{"trip_id":"bad","event_timestamp":"2014-04-01T12:30:00+00:00","pickup_latitude":95,"pickup_longitude":-73.9,"base":"B02512"}',
        ]
        source = spark.createDataFrame([(row,) for row in rows], ["value"])
        result = transform_uber_stream(source).collect()
        assert len(result) == 1
        assert result[0]["pickup_hour"] == 12
        assert result[0]["day_of_week"] == 2
    finally:
        spark.stop()
