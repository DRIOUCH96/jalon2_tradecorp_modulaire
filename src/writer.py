import logging
import os
import tempfile
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

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
        existing_blob_names = [
            blob.name
            for blob in container_client.list_blobs(
                name_starts_with=f"{blob_prefix}/"
            )
        ]

        for existing_blob_name in existing_blob_names:
            container_client.delete_blob(
                existing_blob_name
            )

            LOGGER.info(
                "Ancien fichier supprimé : %s/%s",
                container_name,
                existing_blob_name,
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


def main() -> None:
    """Lit le résultat intermédiaire et l'envoie dans ADLS clean."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    spark = None

    try:
        LOGGER.info("Démarrage de la SparkSession")

        spark = (
            SparkSession.builder
            .appName("TradeCorp Writer")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        temporary_root = Path(
            os.getenv(
                "LOCAL_TMP_DIR",
                "/home/jovyan/data/tmp",
            )
        )

        transformed_directory = (
            temporary_root
            / "airflow"
            / "transformed"
        )

        if not transformed_directory.is_dir():
            raise FileNotFoundError(
                "Parquet intermédiaire introuvable : "
                f"{transformed_directory}"
            )

        LOGGER.info(
            "Lecture du Parquet intermédiaire : %s",
            transformed_directory,
        )

        dataframe = spark.read.parquet(
            str(transformed_directory)
        )

        LOGGER.info(
            "Parquet intermédiaire : %s lignes, %s colonnes",
            dataframe.count(),
            len(dataframe.columns),
        )

        output_name = os.getenv(
            "CLEAN_OUTPUT_PATH",
            "orders_enriched.parquet",
        )

        destination = write_parquet(
            dataframe,
            output_name,
        )

        LOGGER.info(
            "Écriture terminée avec succès : %s",
            destination,
        )

    except Exception:
        LOGGER.exception(
            "Échec de l'écriture dans ADLS clean"
        )
        raise

    finally:
        if spark is not None:
            LOGGER.info("Arrêt de la SparkSession")
            spark.stop()


if __name__ == "__main__":
    main()
