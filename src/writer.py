import logging
import os
import tempfile
from pathlib import Path

from pyspark.sql import DataFrame

from utils import create_blob_service_client


LOGGER = logging.getLogger(__name__)


def write_parquet(
    df: DataFrame,
    output_name: str = "orders_enriched.parquet",
) -> str:
    """Écrit le DataFrame en Parquet puis l'envoie dans ADLS clean."""

    container_name = os.getenv(
        "AZURE_CLEAN_CONTAINER",
        "clean",
    )
    blob_prefix = output_name.replace("\\", "/").strip("/")

    with tempfile.TemporaryDirectory(
        prefix="tradecorp_parquet_"
    ) as temporary_directory:
        parquet_directory = (
            Path(temporary_directory) / "parquet"
        )

        LOGGER.info(
            "Écriture locale du Parquet dans %s",
            parquet_directory,
        )

        df.write.mode("overwrite").parquet(
            str(parquet_directory)
        )

        container_client = (
            create_blob_service_client()
            .get_container_client(container_name)
        )

        for local_file in parquet_directory.rglob("*"):
            if not local_file.is_file():
                continue

            if local_file.name.endswith(".crc"):
                continue

            relative_path = local_file.relative_to(
                parquet_directory
            ).as_posix()

            blob_name = f"{blob_prefix}/{relative_path}"

            with local_file.open("rb") as file_handle:
                container_client.upload_blob(
                    name=blob_name,
                    data=file_handle,
                    overwrite=True,
                )

            LOGGER.info(
                "Fichier Parquet envoyé : %s/%s",
                container_name,
                blob_name,
            )

    return f"{container_name}/{blob_prefix}"