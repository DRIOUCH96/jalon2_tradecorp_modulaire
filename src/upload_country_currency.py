import logging
import os
from pathlib import Path

from azure.storage.blob import (
    BlobServiceClient,
    ContentSettings,
)


LOGGER = logging.getLogger(__name__)

DEFAULT_SOURCE_PATH = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "raw"
    / "reference"
    / "country_currency.csv"
)


def create_blob_service_client() -> BlobServiceClient:
    """Crée un client Azure sans dépendance à Spark."""

    account_name = os.environ[
        "AZURE_STORAGE_ACCOUNT_NAME"
    ]
    account_key = os.environ[
        "AZURE_STORAGE_ACCOUNT_KEY"
    ]

    return BlobServiceClient(
        account_url=(
            f"https://{account_name}.blob.core.windows.net"
        ),
        credential=account_key,
    )


def upload_country_currency(
    source_path: str | Path = DEFAULT_SOURCE_PATH,
) -> str:
    """Envoie le mapping pays-devise dans ADLS."""

    source = Path(source_path)

    if not source.is_file():
        raise FileNotFoundError(
            f"Fichier de référence introuvable : {source}"
        )

    container_name = os.getenv(
        "AZURE_RAW_CONTAINER",
        "raw",
    )
    reference_path = os.getenv(
        "AZURE_RAW_REFERENCE_PATH",
        "reference",
    ).strip("/")

    blob_name = (
        f"{reference_path}/country_currency.csv"
    )

    blob_client = (
        create_blob_service_client()
        .get_container_client(container_name)
        .get_blob_client(blob_name)
    )

    with source.open("rb") as file_handle:
        blob_client.upload_blob(
            file_handle,
            overwrite=True,
            content_settings=ContentSettings(
                content_type="text/csv"
            ),
        )

    return f"{container_name}/{blob_name}"


def main() -> None:
    """Exécute l’upload unique du mapping."""

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | %(levelname)s | "
            "%(name)s | %(message)s"
        ),
    )

    try:
        destination = upload_country_currency()

        LOGGER.info(
            "Mapping pays-devise envoyé dans %s",
            destination,
        )
    except Exception:
        LOGGER.exception(
            "Échec de l’upload du mapping pays-devise"
        )
        raise


if __name__ == "__main__":
    main()