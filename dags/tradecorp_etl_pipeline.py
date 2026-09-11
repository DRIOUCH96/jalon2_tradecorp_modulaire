"""DAG d'orchestration du pipeline ETL TradeCorp."""

from datetime import timedelta

import pendulum
from airflow import DAG


DEFAULT_ARGS = {
    "owner": "tradecorp",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="tradecorp_etl_pipeline",
    description="Orchestration quotidienne du pipeline ETL PySpark TradeCorp",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2024, 1, 1, tz="Europe/Paris"),
    schedule="0 6 * * *",
    catchup=False,
    tags=["tradecorp", "etl", "spark"],
) as dag:
    pass