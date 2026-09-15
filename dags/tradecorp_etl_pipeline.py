from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.filesystem import FileSensor
from docker.types import Mount


default_args = {
    "owner": "tradecorp",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
DEFAULT_PROJECT_PATH = (
    "//c/Users/driou/Downloads/"
    "jalon2_tradecorp_modulaire"
)
PROJECT_PATH = os.getenv(
    "TRADECORP_PROJECT_PATH",
    DEFAULT_PROJECT_PATH,
).rstrip("/")
MOUNTS = [
    Mount(
        source=f"{PROJECT_PATH}/src",
        target="/home/jovyan/src",
        type="bind",
        read_only=True,
    ),
    Mount(
        source=f"{PROJECT_PATH}/data",
        target="/home/jovyan/data",
        type="bind",
    ),
    Mount(
        source=f"{PROJECT_PATH}/.env",
        target="/home/jovyan/.env",
        type="bind",
        read_only=True,
    ),
]
TASK_ENVIRONMENT = {
    "AZURE_RAW_CONTAINER": os.getenv(
        "AZURE_RAW_CONTAINER",
        "raw",
    ),
    "AZURE_RAW_REFERENCE_PATH": os.getenv(
        "AZURE_RAW_REFERENCE_PATH",
        "reference",
    ),
    "AZURE_CLEAN_CONTAINER": os.getenv(
        "AZURE_CLEAN_CONTAINER",
        "clean",
    ),
    "LOCAL_TMP_DIR": "/home/jovyan/data/tmp",
    "CLEAN_OUTPUT_PATH": os.getenv(
        "CLEAN_OUTPUT_PATH",
        "orders_enriched.parquet",
    ),
    "STAGING_PARQUET_PATH": os.getenv(
        "STAGING_PARQUET_PATH",
        "/home/jovyan/data/staging/orders_enriched",
    ),
    "COUNTRY_CURRENCY_FILENAME": os.getenv(
        "COUNTRY_CURRENCY_FILENAME",
        "country_currency.csv",
    ),
    "PYTHONPATH": "/home/jovyan/src",
}


PRIVATE_ENVIRONMENT = {
    "AZURE_STORAGE_ACCOUNT_NAME": os.getenv(
        "AZURE_STORAGE_ACCOUNT_NAME",
        "",
    ),
    "AZURE_STORAGE_ACCOUNT_KEY": os.getenv(
        "AZURE_STORAGE_ACCOUNT_KEY",
        "",
    ),
}


def create_task(task_id: str, command: str) -> DockerOperator:
    """Crée une tâche DockerOperator pour Airflow."""

    return DockerOperator(
        task_id=task_id,
        image="tradecorp-modulaire-pyspark:latest",
        command=command,
        docker_url="unix://var/run/docker.sock",
        docker_conn_id=None,
        network_mode="tradecorp-modulaire-network",
        auto_remove="success",
        mount_tmp_dir=False,
        mounts=MOUNTS,
        environment=TASK_ENVIRONMENT,
        private_environment=PRIVATE_ENVIRONMENT,
        working_dir="/home/jovyan"
    )


with DAG(
    dag_id="tradecorp_etl_pipeline",
    default_args=default_args,
    description="Pipeline de traitement des données TradeCorp",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["tradecorp", "etl", "spark"],
) as dag:
    wait_for_trigger_file = FileSensor(
        task_id="wait_for_trigger_file",
        filepath="/opt/airflow/data/trigger/go.txt",
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=3600,
        mode="reschedule",
    )
    t0 = create_task(
        task_id="fetch_exchange_rates",
        command="python /home/jovyan/src/fetch_exchange_rates.py",
    )
    t1 = create_task(
        task_id="reader",
        command="spark-submit /home/jovyan/src/reader.py",
    )
    t2 = create_task(
        task_id="transformer",
        command="spark-submit /home/jovyan/src/transformer.py",
    )
    t3 = create_task(
        task_id="writer",
        command="spark-submit /home/jovyan/src/writer.py",
    )
    wait_for_trigger_file >> t0 >> t1 >> t2 >> t3
