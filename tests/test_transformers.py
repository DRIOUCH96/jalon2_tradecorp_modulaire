import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


SRC_DIRECTORY = (
    Path(__file__).resolve().parents[1] / "src"
)

sys.path.insert(0, str(SRC_DIRECTORY))

from enrichment import add_currency_column
from utils import (
    add_sous_total,
    clean_customers,
    clean_orders,
)


@pytest.fixture(scope="session")
def spark():
    """Crée une SparkSession commune aux quatre tests."""

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("TradeCorp Tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


def test_clean_orders_supprime_shipped_date_null(spark):
    """Vérifie que les commandes non livrées sont supprimées."""

    schema = StructType(
        [
            StructField("order_id", IntegerType(), False),
            StructField("order_date", StringType(), True),
            StructField("required_date", StringType(), True),
            StructField("shipped_date", StringType(), True),
            StructField("freight", StringType(), True),
            StructField("ship_via", IntegerType(), True),
        ]
    )

    dataframe = spark.createDataFrame(
        [
            (
                1,
                "1997-01-01",
                "1997-01-10",
                "1997-01-05",
                "12.50",
                1,
            ),
            (
                2,
                "1997-02-01",
                "1997-02-10",
                None,
                "8.25",
                2,
            ),
        ],
        schema,
    )

    result = clean_orders(dataframe)

    assert result.count() == 1
    assert result.filter("shipped_date IS NULL").count() == 0
    assert "shipper_id" in result.columns
    assert "ship_via" not in result.columns
    assert result.first()["is_shipped"] is True


def test_add_sous_total_calcule_correctement(spark):
    """Vérifie le calcul avec une remise de 10 %."""

    schema = StructType(
        [
            StructField("prix_unitaire", DoubleType(), False),
            StructField("quantite", IntegerType(), False),
            StructField("discount", DoubleType(), True),
        ]
    )

    dataframe = spark.createDataFrame(
        [(10.0, 2, 0.1)],
        schema,
    )

    result = add_sous_total(dataframe)

    assert result.first()["sous_total"] == pytest.approx(18.0)


def test_clean_customers_applique_trim_et_casse(spark):
    """Vérifie trim, initcap et la mise en majuscules."""

    schema = StructType(
        [
            StructField("customer_id", StringType(), False),
            StructField("company_name", StringType(), True),
            StructField("contact_name", StringType(), True),
            StructField("country", StringType(), True),
        ]
    )

    dataframe = spark.createDataFrame(
        [
            (
                "TEST1",
                "  entreprise test  ",
                "  jean dupont  ",
                "  france  ",
            )
        ],
        schema,
    )

    customer = clean_customers(dataframe).first()

    assert customer["company_name"] == "entreprise test"
    assert customer["contact_name"] == "Jean Dupont"
    assert customer["country"] == "FRANCE"


def test_add_currency_column(spark):
    """Vérifie l'enrichissement sans effectuer d'appel réseau."""

    amounts = spark.createDataFrame(
        [
            ("FRANCE", 18.0),
            ("UK", 10.0),
        ],
        [
            "customer_country",
            "sous_total",
        ],
    )

    country_currency = spark.createDataFrame(
        [
            ("FRANCE", "EUR"),
            ("UK", "GBP"),
        ],
        [
            "country",
            "currency",
        ],
    )

    exchange_rates = {
        "EUR": 0.92,
        "GBP": 0.78,
    }

    result = (
        add_currency_column(
            amounts,
            country_currency,
            exchange_rates,
        )
        .orderBy("customer_country")
        .collect()
    )

    assert result[0]["currency"] == "EUR"
    assert result[0]["sous_total_local"] == pytest.approx(16.56)

    assert result[1]["currency"] == "GBP"
    assert result[1]["sous_total_local"] == pytest.approx(7.8)