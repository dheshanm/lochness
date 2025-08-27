#!/usr/bin/env python
"""
Insert MinIO credentials into the keystore for a specific project.
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

from lochness.sinks.minio_object_store.credentials import insert_minio_cred

logger = logging.getLogger(__name__)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}

logging.basicConfig(**logargs)

# MinIO Credentials
KEY_NAME = "minio_dev"
ACCESS_KEY = "lochness-dev"
SECRET_KEY = "MCedHMB0KbPOhubVK81UcYaDy2tgFB4pxjQtrHcX"
ENDPOINT_URL = "http://pnl-minio-1.partners.org:9000"
PROJECT_ID = "Pronet"


def main():
    """
    Main function to insert MinIO credentials.
    """
    logger.info(
        f"Attempting to insert MinIO credentials with key_name "
        f"'{KEY_NAME}' for project "
        f"'{PROJECT_ID}'..."
    )
    try:
        insert_minio_cred(
            key_name=KEY_NAME,
            access_key=ACCESS_KEY,
            secret_key=SECRET_KEY,
            endpoint_url=ENDPOINT_URL,
            project_id=PROJECT_ID,
        )
        logger.info("MinIO credentials inserted successfully.")
    except Exception as e:  # pylint: disable=broad-except
        logger.info(f"An error occurred while inserting MinIO credentials: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
