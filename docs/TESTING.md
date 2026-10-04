# Testing

Run `python -m pytest tests -q` and `python -m compileall -q producer spark_streaming dashboard airflow/dags tests`. Tests cover deterministic conversion, canonical fields, malformed-row skipping, coordinate validation, and the PostgreSQL sink API. The Spark transformation test is skipped when PySpark is unavailable. dbt tests cover ID uniqueness/not-null and valid timestamps, coordinates, dates, and hours when PostgreSQL is available.

The manually verified project results are 5 successful dbt models and 12/12 dbt tests. Airflow task results were `dbt_deps` success, `dbt_run` success, and `dbt_test` success.
