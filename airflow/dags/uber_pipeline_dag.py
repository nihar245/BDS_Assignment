"""Run dbt transformations and tests for the Uber streaming pipeline."""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="uber_pipeline_dbt",
    description="Build and test Uber analytics models in PostgreSQL.",
    schedule="*/1 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["uber", "dbt"],
) as dag:
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /dbt && dbt deps --profiles-dir .",
    )
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /dbt && dbt run --profiles-dir .",
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /dbt && dbt test --profiles-dir .",
    )

    dbt_deps >> dbt_run >> dbt_test
