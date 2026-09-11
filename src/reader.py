import json
import logging
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from utils import download_blob


LOGGER = logging.getLogger(__name__)

BUSINESS_CSV_FILES = (
    "categories.csv",
    "customers.csv",
    "employees.csv",
    "order_details.csv",
    "orders.csv",
    "products.csv",
    "shippers.csv",
    "suppliers.csv",
)

## Téléchargement des fichiers métier depuis ADLS
def download_business_csvs(
    destination: str | Path,
) -> dict[str, str]:
    """Télécharge uniquement les huit CSV métier."""

    destination_path = Path(destination)
    destination_path.mkdir(parents=True, exist_ok=True)

    local_paths = {}

    for filename in BUSINESS_CSV_FILES:
        local_path = destination_path / filename

        download_blob(
            blob_name=filename,
            destination=local_path,
        )

        table_name = Path(filename).stem
        local_paths[table_name] = str(local_path)

        LOGGER.info("Fichier métier téléchargé : %s", filename)

    return local_paths

# Lecture des fichiers métier avec Spark
def read_business_csvs(
    spark: SparkSession,
    destination: str | Path,
) -> dict[str, DataFrame]:
    """Télécharge et lit les huit CSV métier avec Spark."""

    local_paths = download_business_csvs(destination)

    return {
        table_name: spark.read.csv(
            local_path,
            header=True,
            inferSchema=True,
        )
        for table_name, local_path in local_paths.items()
    }

# Téléchargement des fichiers de référence avec Spark
def download_reference_files(
    destination: str | Path,
) -> dict[str, str]:
    """Télécharge les deux fichiers de référence depuis raw/reference."""

    destination_path = Path(destination)
    destination_path.mkdir(parents=True, exist_ok=True)

    reference_prefix = os.getenv(
        "AZURE_RAW_REFERENCE_PATH",
        "reference",
    ).strip("/")

    filenames = (
        "country_currency.csv",
        "exchange_rates.json",
    )

    local_paths = {}

    for filename in filenames:
        local_path = destination_path / filename
        blob_name = f"{reference_prefix}/{filename}"

        download_blob(
            blob_name=blob_name,
            destination=local_path,
        )

        local_paths[Path(filename).stem] = str(local_path)

        LOGGER.info("Fichier de référence téléchargé : %s", blob_name)

    return local_paths

# Lecture des fichiers de référence avec Spark
def read_reference_files(
    spark: SparkSession,
    destination: str | Path,
) -> tuple[DataFrame, dict[str, float]]:
    """Télécharge et lit le mapping pays-devise et les taux."""

    local_paths = download_reference_files(destination)

    country_currency = spark.read.csv(
        local_paths["country_currency"],
        header=True,
        inferSchema=True,
    )

    with Path(local_paths["exchange_rates"]).open(
        "r",
        encoding="utf-8",
    ) as file_handle:
        exchange_payload = json.load(file_handle)

    rates = exchange_payload.get("rates")

    if not isinstance(rates, dict):
        raise ValueError(
            "Le fichier exchange_rates.json ne contient pas de taux valides"
        )

    exchange_rates = {
        currency.upper(): float(rate)
        for currency, rate in rates.items()
    }

    return country_currency, exchange_rates
def main() -> None:
    """Télécharge, valide et conserve les fichiers pour Airflow."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    spark = None

    try:
        spark = (
            SparkSession.builder
            .appName("TradeCorp Reader")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        temporary_root = Path(
            os.getenv(
                "LOCAL_TMP_DIR",
                "/home/jovyan/data/tmp",
            )
        )

        staging_root = temporary_root / "airflow"
        business_directory = staging_root / "business"
        reference_directory = staging_root / "reference"

        staging_root.mkdir(
            parents=True,
            exist_ok=True,
        )

        LOGGER.info(
            "Dossier d'échange Airflow : %s",
            staging_root,
        )

        dataframes = read_business_csvs(
            spark,
            business_directory,
        )

        country_currency, exchange_rates = (
            read_reference_files(
                spark,
                reference_directory,
            )
        )

        for table_name, dataframe in dataframes.items():
            LOGGER.info(
                "%s : %s ligne(s)",
                table_name,
                dataframe.count(),
            )

        LOGGER.info(
            "Référence pays-devise : %s ligne(s)",
            country_currency.count(),
        )

        LOGGER.info(
            "Taux de change chargés : %s",
            len(exchange_rates),
        )

        LOGGER.info(
            "Lecture terminée : fichiers conservés dans %s",
            staging_root,
        )

    except Exception:
        LOGGER.exception(
            "Échec de la lecture des données"
        )
        raise

    finally:
        if spark is not None:
            LOGGER.info("Arrêt de la SparkSession")
            spark.stop()


if __name__ == "__main__":
    main()