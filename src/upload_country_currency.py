import os
from pathlib import Path

from azure.storage.blob import BlobServiceClient, ContentSettings


DEFAULT_SOURCE_PATH = (
	Path(__file__).resolve().parents[1]
	/ "data"
	/ "raw"
	/ "reference"
	/ "country_currency.csv"
)


def create_blob_service_client() -> BlobServiceClient:
	"""Construit le client Azure Blob depuis les variables d'environnement."""

	account_name = os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
	account_url = (
		account_name
		if account_name.startswith("http")
		else f"https://{account_name}.blob.core.windows.net"
	)
	return BlobServiceClient(
		account_url=account_url,
		credential=os.environ["AZURE_STORAGE_ACCOUNT_KEY"],
	)


def upload_country_currency(
	source_path: str | Path | None = None,
	container_name: str | None = None,
	blob_name: str | None = None,
	client: BlobServiceClient | None = None,
) -> str:
	"""Dépose country_currency.csv dans raw/reference d'ADLS Gen2."""

	source = Path(source_path or os.getenv("COUNTRY_CURRENCY_SOURCE", DEFAULT_SOURCE_PATH))
	if not source.is_file():
		raise FileNotFoundError(f"Fichier de référence introuvable : {source}")

	container_name = container_name or os.getenv("AZURE_RAW_CONTAINER", "raw")
	reference_path = os.getenv("AZURE_RAW_REFERENCE_PATH", "reference").strip("/")
	blob_name = blob_name or f"{reference_path}/country_currency.csv"

	blob_client = (client or create_blob_service_client()).get_blob_client(
		container=container_name,
		blob=blob_name,
	)
	with source.open("rb") as file_handle:
		blob_client.upload_blob(
			file_handle,
			overwrite=True,
			content_settings=ContentSettings(content_type="text/csv"),
		)

	return f"{container_name}/{blob_name}"


def main() -> None:
	"""Exécute l'upload manuel de la référence pays-devise."""

	location = upload_country_currency()
	print(f"Référence pays-devise déposée dans ADLS Gen2 : {location}")


if __name__ == "__main__":
	main()
import logging
import os
from pathlib import Path

from azure.storage.blob import ContentSettings

from utils import create_blob_service_client


LOGGER = logging.getLogger(__name__)

DEFAULT_SOURCE_PATH = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "raw"
    / "reference"
    / "country_currency.csv"
)


def upload_country_currency(
    source_path: str | Path = DEFAULT_SOURCE_PATH,
) -> str:
    """Charge une fois le mapping pays-devise dans ADLS."""

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

    blob_name = f"{reference_path}/country_currency.csv"

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
    """Exécute l'upload unique du fichier de référence."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    try:
        destination = upload_country_currency()

        LOGGER.info(
            "Référence pays-devise déposée dans %s",
            destination,
        )
    except Exception:
        LOGGER.exception(
            "Échec de l'upload de country_currency.csv"
        )
        raise


if __name__ == "__main__":
    main()