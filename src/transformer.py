import logging
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from enrichment import add_currency_column
from reader import (
    load_business_csvs,
    load_reference_files,
)
from utils import (
    add_sous_total,
    clean_customers,
    clean_employees,
    clean_order_details,
    clean_orders,
    clean_products,
)


LOGGER = logging.getLogger(__name__)


FINAL_COLUMNS = [
    "order_id",
    "customer_id",
    "employee_id",
    "product_id",
    "order_date",
    "required_date",
    "shipped_date",
    "freight",
    "is_shipped",
    "prix_unitaire",
    "quantite",
    "discount",
    "sous_total",
    "customer_name",
    "customer_country",
    "customer_city",
    "product_name",
    "category_name",
    "en_stock",
    "full_name",
    "shipper_name",
]


def build_enriched(
    dataframes: dict[str, DataFrame],
) -> DataFrame:
    """Nettoie et joint les sept tables du résultat final."""

    customers = clean_customers(
        dataframes["customers"]
    ).select(
        "customer_id",
        F.col("company_name").alias(
            "customer_name"
        ),
        F.col("country").alias(
            "customer_country"
        ),
        F.col("city").alias(
            "customer_city"
        ),
    )

    orders = clean_orders(
        dataframes["orders"]
    ).select(
        "order_id",
        "customer_id",
        "employee_id",
        "shipper_id",
        "order_date",
        "required_date",
        "shipped_date",
        "freight",
        "is_shipped",
    )

    order_details = add_sous_total(
        clean_order_details(
            dataframes["order_details"]
        )
    ).select(
        "order_id",
        "product_id",
        "prix_unitaire",
        "quantite",
        "discount",
        "sous_total",
    )

    products = clean_products(
        dataframes["products"]
    ).select(
        "product_id",
        "category_id",
        "product_name",
        "en_stock",
    )

    categories = dataframes[
        "categories"
    ].select(
        "category_id",
        "category_name",
    )

    employees = clean_employees(
        dataframes["employees"]
    ).select(
        "employee_id",
        "full_name",
    )

    shippers = dataframes[
        "shippers"
    ].select(
        "shipper_id",
        F.col("company_name").alias(
            "shipper_name"
        ),
    )

    enriched = (
        order_details
        .join(
            orders,
            on="order_id",
            how="inner",
        )
        .join(
            customers,
            on="customer_id",
            how="left",
        )
        .join(
            products,
            on="product_id",
            how="left",
        )
        .join(
            categories,
            on="category_id",
            how="left",
        )
        .join(
            employees,
            on="employee_id",
            how="left",
        )
        .join(
            shippers,
            on="shipper_id",
            how="left",
        )
    )

    return enriched.select(*FINAL_COLUMNS)


def main() -> None:
    """Transforme les données et produit le Parquet intermédiaire."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    spark = None

    try:
        LOGGER.info("Démarrage de la SparkSession")

        spark = (
            SparkSession.builder
            .appName("TradeCorp Transformer")
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
        transformed_directory = staging_root / "transformed"

        LOGGER.info(
            "Lecture des fichiers métier depuis %s",
            business_directory,
        )

        dataframes = load_business_csvs(
            spark,
            business_directory,
        )

        LOGGER.info(
            "Nettoyage et jointure des données métier"
        )

        transformed = build_enriched(dataframes)

        LOGGER.info(
            "Lecture des références depuis %s",
            reference_directory,
        )

        country_currency, exchange_rates = (
            load_reference_files(
                spark,
                reference_directory,
            )
        )

        LOGGER.info(
            "Ajout des devises et des montants locaux"
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
            "Écriture du Parquet intermédiaire dans %s",
            transformed_directory,
        )

        enriched.write.mode("overwrite").parquet(
            str(transformed_directory)
        )

        LOGGER.info(
            "Transformation terminée avec succès"
        )

    except Exception:
        LOGGER.exception(
            "Échec de la transformation"
        )
        raise

    finally:
        if spark is not None:
            LOGGER.info("Arrêt de la SparkSession")
            spark.stop()


if __name__ == "__main__":
    main()