import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# Load variables from .env
load_dotenv()

AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
INPUT_CONTAINER = os.getenv("INPUT_CONTAINER")
OUTPUT_CONTAINER = os.getenv("OUTPUT_CONTAINER")

if not AZURE_STORAGE_CONNECTION_STRING:
    raise ValueError("AZURE_STORAGE_CONNECTION_STRING not found in .env")

blob_service = BlobServiceClient.from_connection_string(
    AZURE_STORAGE_CONNECTION_STRING
)


def download_twbx(blob_path: str, local_path: str):
    blob_client = blob_service.get_blob_client(
        container=INPUT_CONTAINER,
        blob=blob_path
    )

    with open(local_path, "wb") as f:
        f.write(blob_client.download_blob().readall())

    return local_path


def upload_csv(local_path: str, blob_path: str):
    container_client = blob_service.get_container_client(
        OUTPUT_CONTAINER
    )

    try:
        container_client.create_container()
    except Exception:
        pass

    blob_client = container_client.get_blob_client(blob_path)

    with open(local_path, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)

    return blob_client.url
