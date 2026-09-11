"""DAG d'orchestration du pipeline ETL TradeCorp."""

from datetime import timedelta

import pendulum
from airflow import DAG
from docker.types import Mount


DEFAULT_ARGS = {
    "owner": "tradecorp",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

HOST_PROJECT_PATH = (
    "//c/Users/driou/Downloads/"
    "jalon2_tradecorp_modulaire"
)

SPARK_IMAGE = "tradecorp-modulaire-pyspark:latest"
DOCKER_NETWORK = "tradecorp-modulaire-network"
AIRFLOW_ENV_FILE = "/opt/airflow/.env"

DOCKER_MOUNTS = [
    Mount(
        source=f"{HOST_PROJECT_PATH}/src",
        target="/home/jovyan/src",
        type="bind",
        read_only=True,
    ),
    Mount(
        source=f"{HOST_PROJECT_PATH}/data",
        target="/home/jovyan/data",
        type="bind",
    ),
    Mount(
        source=f"{HOST_PROJECT_PATH}/.env",
        target="/home/jovyan/.env",
        type="bind",
        read_only=True,
    ),
]


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