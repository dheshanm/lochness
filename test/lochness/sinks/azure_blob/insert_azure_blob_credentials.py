#!/usr/bin/env python
"""
Insert Azure Blob Storage credentials into the keystore for a specific project.
"""
import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
root_dir = None  # pylint: disable=invalid-name
for parent in file.parents:
    if parent.name == "lochness_v2":
        root_dir = parent

sys.path.append(str(root_dir))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import traceback
from typing import Any, Dict
import logging

from rich.logging import RichHandler

from lochness.sinks.azure_blob_storage.credentials import insert_azure_blob_cred
from lochness.helpers import config, utils

config_file = utils.get_config_file_path()

logger = logging.getLogger(__name__)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}

logging.basicConfig(**logargs)


def main():
    """
    Main function to insert Azure Blob Storage credentials.
    """

    azure_blob_creds = config.parse(config_file, 'azure-blob-datasink-test')

    connection_string: str = azure_blob_creds['connection_string']  # type: ignore

    key_name: str = azure_blob_creds['key_name']  # type: ignore
    project_id: str = azure_blob_creds['project_id']  # type: ignore
    logger.info(
        f"Attempting to insert Azure Blob Storage credentials with key_name "
        f"'{azure_blob_creds['key_name']}' for project '{project_id}'."
    )
    try:
        insert_azure_blob_cred(
            key_name=key_name,
            connection_string=connection_string,
            project_id=project_id,
        )
        logger.info("Azure Blob Storage credentials inserted successfully.")
    except Exception as e:  # pylint: disable=broad-except
        logger.info(f"An error occurred while inserting Azure Blob Storage credentials: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
