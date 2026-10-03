# Airflow

Airflow is optional and Dockerized separately from Kafka. Copy `.env.example` to `.env` at the repository root. The containers load that file with `env_file: ../.env` and override `POSTGRES_HOST` to `host.docker.internal`, because PostgreSQL 17 runs on the Windows host. The setup has separate Docker PostgreSQL metadata storage, a webserver, and a scheduler using LocalExecutor.

The image uses `apache/airflow:2.10.4-python3.11`, installs only the Airflow PostgreSQL provider and dbt 1.8.8/1.8.2 dependencies, and installs Git because `dbt deps` and `dbt debug` require it. The shared `AIRFLOW__WEBSERVER__SECRET_KEY` remains configured for scheduler/webserver log serving.

For a fresh Airflow metadata volume, from `airflow/` run `docker compose --env-file ../.env up airflow-init`, then `docker compose --env-file ../.env up -d`. For the existing initialized volume, start services with `docker compose --env-file ../.env up -d`; do not use `down -v`. The DAG [uber_pipeline_dag.py](../airflow/dags/uber_pipeline_dag.py) runs `dbt deps`, `dbt run`, and `dbt test` against PostgreSQL. It does not start Kafka, Spark, or HDFS and is not needed for the core streaming path.

The verified Airflow task results are `dbt_deps` success, `dbt_run` success, and `dbt_test` success.
