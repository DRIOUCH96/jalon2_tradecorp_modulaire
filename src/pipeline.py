import logging
import os
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession

from enrichment import add_currency_column
from reader import read_business_csvs, read_reference_files
from transformer import build_enriched
from writer import write_parquet


LOGGER = logging.getLogger(__name__)


def main() -> None:
    """Orchestre le pipeline TradeCorp de bout en bout."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    spark = None

    try:
        LOGGER.info("Démarrage de la SparkSession")

        spark = (
            SparkSession.builder
            .appName("TradeCorp Data Pipeline")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        temporary_root = Path(
            os.getenv("LOCAL_TMP_DIR", "/home/jovyan/data/tmp")
        )
        temporary_root.mkdir(parents=True, exist_ok=True)

        with tempfile.TemporaryDirectory(
            prefix="tradecorp_pipeline_",
            dir=temporary_root,
        ) as temporary_directory:
            temporary_path = Path(temporary_directory)

            LOGGER.info(
                "Étape 1/4 - Lecture des huit tables métier"
            )

            dataframes = read_business_csvs(
                spark,
                temporary_path / "business",
            )

            LOGGER.info(
                "Étape 2/4 - Transformation et jointure"
            )

            transformed = build_enriched(dataframes)

            LOGGER.info(
                "Étape 3/4 - Lecture des références et enrichissement"
            )

            country_currency, exchange_rates = (
                read_reference_files(
                    spark,
                    temporary_path / "reference",
                )
            )

            enriched = add_currency_column(
                transformed,
                country_currency,
                exchange_rates,
            )

            row_count = enriched.count()

            LOGGER.info(
                "DataFrame final : %s lignes, %s colonnes",
                row_count,
                len(enriched.columns),
            )

            LOGGER.info(
                "Colonnes finales : %s",
                ", ".join(enriched.columns),
            )

            LOGGER.info(
                "Étape 4/4 - Écriture dans la zone clean"
            )

            output_name = os.getenv(
                "CLEAN_OUTPUT_PATH",
                "orders_enriched.parquet",
            )

            destination = write_parquet(
                enriched,
                output_name,
            )

            LOGGER.info(
                "Pipeline terminé avec succès : %s",
                destination,
            )

    except Exception:
        LOGGER.exception(
            "Échec du pipeline TradeCorp"
        )
        raise

    finally:
        if spark is not None:
            LOGGER.info("Arrêt de la SparkSession")
            spark.stop()


if __name__ == "__main__":
    main()