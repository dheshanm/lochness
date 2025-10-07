"""
Azure Blob Storage API interactions.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from azure.core.exceptions import AzureError
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger(__name__)


def get_blob_service_client(connection_string: str) -> BlobServiceClient:
    """
    Create a BlobServiceClient using the provided connection string.

    Args:
        connection_string (str): The Azure Blob Storage connection string.

    Returns:
        BlobServiceClient: An instance of BlobServiceClient.
    """
    try:
        blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )
        return blob_service_client
    except AzureError as e:
        logger.error(f"Azure error creating BlobServiceClient: {e}")
        raise


def check_if_container_exists(
    connection_string: str,
    container_name: str,
) -> bool:
    """
    Check if an Azure Blob Storage container exists.

    Args:
        connection_string (str): The Azure Blob Storage connection string.
        container_name (str): The name of the container to check.

    Returns:
        bool: True if the container exists, False otherwise.
    """
    try:
        blob_service_client = get_blob_service_client(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        return container_client.exists()
    except AzureError as e:
        logger.error(f"Azure error checking if container exists: {e}")
        return False


def upload_to_blob(
    connection_string: str,
    container_name: str,
    blob_name: str,
    source_file_path: Path,
    tags: Optional[Dict[str, str]] = None,
) -> None:
    """
    Upload a file to an Azure Blob Storage container with optional tags.

    Args:
        connection_string (str): The Azure Blob Storage connection string.
        container_name (str): The name of the container to upload to.
        blob_name (str): The name of the blob (file) in the container.
        source_file_path (Path): The local path to the file to be uploaded.
        tags (Optional[Dict[str, str]]): Optional tags to associate with the blob.

    Returns:
        None
    """
    blob_service_client = get_blob_service_client(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    if tags is None:
        tags = {}

    tags["uploaded_by"] = "azure_blob_example_script"
    tags["source_file"] = str(source_file_path.resolve())
    tags["upload_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(file=source_file_path, mode="rb") as data:
        container_client.upload_blob(
            name=blob_name, data=data, overwrite=True, tags=tags
        )

    return None
