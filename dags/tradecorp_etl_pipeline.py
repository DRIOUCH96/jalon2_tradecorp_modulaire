"""DAG d'orchestration du pipeline ETL TradeCorp."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
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
DOCKER_OPERATOR_ARGS = {
    "image": SPARK_IMAGE,
    "docker_url": "unix://var/run/docker.sock",
    "docker_conn_id": None,
    "network_mode": DOCKER_NETWORK,
    "auto_remove": "success",
    "mount_tmp_dir": False,
    "mounts": DOCKER_MOUNTS,
    "env_file": AIRFLOW_ENV_FILE,
    "environment": {
        "PYTHONPATH": "/home/jovyan/src",
    },
    "working_dir": "/home/jovyan",
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
        fetch_exchange_rates = DockerOperator(
        task_id="fetch_exchange_rates",
        command=[
            "python",
            "/home/jovyan/src/fetch_exchange_rates.py",
        ],
        **DOCKER_OPERATOR_ARGS,
        )

        reader = DockerOperator(
        task_id="reader",
        command=[
            "python",
            "/home/jovyan/src/reader.py",
        ],
        **DOCKER_OPERATOR_ARGS,
        )

        transformer = DockerOperator(
        task_id="transformer",
        command=[
            "python",
            "/home/jovyan/src/transformer.py",
        ],
        **DOCKER_OPERATOR_ARGS,
        )

        writer = DockerOperator(
        task_id="writer",
        command=[
            "python",
            "/home/jovyan/src/writer.py",
        ],
        **DOCKER_OPERATOR_ARGS,
        )

        (
        fetch_exchange_rates
        >> reader
        >> transformer
        >> writer
        )