import os
from pathlib import Path

from azure.storage.blob import BlobServiceClient
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def create_blob_service_client() -> BlobServiceClient:
    """Crée le client de connexion à ADLS Gen2."""

    account_name = os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
    account_key = os.environ["AZURE_STORAGE_ACCOUNT_KEY"]

    account_url = (
        f"https://{account_name}.blob.core.windows.net"
    )

    return BlobServiceClient(
        account_url=account_url,
        credential=account_key,
    )


def download_blob(
    blob_name: str,
    destination: str | Path,
    container_name: str | None = None,
) -> str:
    """Télécharge un blob ADLS vers un fichier local."""

    container_name = container_name or os.getenv(
        "AZURE_RAW_CONTAINER",
        "raw",
    )

    destination_path = Path(destination)
    destination_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    container_client = (
        create_blob_service_client()
        .get_container_client(container_name)
    )

    blob_client = container_client.get_blob_client(
        blob_name
    )

    with destination_path.open("wb") as file_handle:
        file_handle.write(
            blob_client.download_blob().readall()
        )

    return str(destination_path)


def clean_customers(df: DataFrame) -> DataFrame:
    """Nettoie la table des clients."""

    return (
        df.withColumn(
            "company_name",
            F.trim(F.col("company_name")),
        )
        .withColumn(
            "contact_name",
            F.initcap(
                F.trim(F.col("contact_name"))
            ),
        )
        .withColumn(
            "country",
            F.upper(
                F.trim(F.col("country"))
            ),
        )
        .dropDuplicates(["customer_id"])
    )


def clean_orders(df: DataFrame) -> DataFrame:
    """Nettoie la table des commandes."""

    result = df.filter(
        F.col("shipped_date").isNotNull()
        & (F.trim(F.col("shipped_date")) != "")
    )

    for column_name in (
        "order_date",
        "required_date",
        "shipped_date",
    ):
        result = result.withColumn(
            column_name,
            F.to_date(F.col(column_name)),
        )

    return (
        result.withColumn(
            "freight",
            F.col("freight").cast("double"),
        )
        .withColumnRenamed(
            "ship_via",
            "shipper_id",
        )
        .withColumn(
            "is_shipped",
            F.col("shipped_date").isNotNull(),
        )
    )


def clean_order_details(df: DataFrame) -> DataFrame:
    """Nettoie les lignes de commande."""

    return (
        df.withColumn(
            "unit_price",
            F.col("unit_price").cast("double"),
        )
        .withColumn(
            "quantity",
            F.col("quantity").cast("integer"),
        )
        .withColumn(
            "discount",
            F.col("discount").cast("double"),
        )
        .withColumnRenamed(
            "unit_price",
            "prix_unitaire",
        )
        .withColumnRenamed(
            "quantity",
            "quantite",
        )
    )


def add_sous_total(df: DataFrame) -> DataFrame:
    """Calcule le montant d'une ligne après remise."""

    discount = F.coalesce(
        F.col("discount"),
        F.lit(0.0),
    )

    return df.withColumn(
        "sous_total",
        F.round(
            F.col("prix_unitaire")
            * F.col("quantite")
            * (F.lit(1.0) - discount),
            2,
        ),
    )


def clean_employees(df: DataFrame) -> DataFrame:
    """Sélectionne et nettoie les colonnes des employés."""

    result = df.select(
        "employee_id",
        "first_name",
        "last_name",
        "title",
        "hire_date",
        "city",
        "country",
    )

    return (
        result.withColumn(
            "first_name",
            F.trim(F.col("first_name")),
        )
        .withColumn(
            "last_name",
            F.trim(F.col("last_name")),
        )
        .withColumn(
            "hire_date",
            F.to_date(F.col("hire_date")),
        )
        .withColumn(
            "full_name",
            F.concat_ws(
                " ",
                F.col("first_name"),
                F.col("last_name"),
            ),
        )
    )


def clean_products(df: DataFrame) -> DataFrame:
    """Nettoie la table des produits."""

    result = (
        df.withColumn(
            "unit_price",
            F.col("unit_price").cast("double"),
        )
        .withColumn(
            "units_in_stock",
            F.col("units_in_stock").cast("integer"),
        )
    )

    return result.withColumn(
        "en_stock",
        F.coalesce(
            F.col("units_in_stock"),
            F.lit(0),
        )
        > 0,
    )