from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils import (
    add_sous_total,
    clean_customers,
    clean_employees,
    clean_order_details,
    clean_orders,
    clean_products,
)


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