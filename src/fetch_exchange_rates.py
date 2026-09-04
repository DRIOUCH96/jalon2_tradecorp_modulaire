import logging
import os

import requests
from azure.storage.blob import BlobServiceClient, ContentSettings


LOGGER = logging.getLogger(__name__)

EXCHANGE_RATE_URL = (
    "https://api.exchangerate-api.com/v4/latest/USD"
)


def create_blob_service_client() -> BlobServiceClient:
    """Crée un client Azure sans dépendance à Spark."""

    account_name = os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
    account_key = os.environ["AZURE_STORAGE_ACCOUNT_KEY"]

    return BlobServiceClient(
        account_url=f"https://{account_name}.blob.core.windows.net",
        credential=account_key,
    )


def fetch_exchange_rates(
    url: str = EXCHANGE_RATE_URL,
) -> bytes:
    """Télécharge la réponse JSON brute de l'API."""

    response = requests.get(
        url,
        timeout=30,
    )
    response.raise_for_status()

    payload = response.json()

    if payload.get("base") != "USD":
        raise ValueError(
            "La devise de base reçue n'est pas USD"
        )

    if not isinstance(payload.get("rates"), dict):
        raise ValueError(
            "La réponse ne contient pas de taux valides"
        )

    return response.content


def upload_exchange_rates(
    raw_json: bytes,
) -> str:
    """Dépose la réponse brute dans raw/reference."""

    container_name = os.getenv(
        "AZURE_RAW_CONTAINER",
        "raw",
    )
    reference_path = os.getenv(
        "AZURE_RAW_REFERENCE_PATH",
        "reference",
    ).strip("/")

    blob_name = f"{reference_path}/exchange_rates.json"

    blob_client = (
        create_blob_service_client()
        .get_container_client(container_name)
        .get_blob_client(blob_name)
    )

    blob_client.upload_blob(
        raw_json,
        overwrite=True,
        content_settings=ContentSettings(
            content_type="application/json"
        ),
    )

    return f"{container_name}/{blob_name}"


def main() -> None:
    """Récupère et charge le taux de change du jour."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    try:
        LOGGER.info(
            "Téléchargement des taux depuis %s",
            EXCHANGE_RATE_URL,
        )

        raw_json = fetch_exchange_rates()
        destination = upload_exchange_rates(raw_json)

        LOGGER.info(
            "Taux de change déposés dans %s",
            destination,
        )
    except Exception:
        LOGGER.exception(
            "Échec de la récupération des taux de change"
        )
        raise


if __name__ == "__main__":
    main()