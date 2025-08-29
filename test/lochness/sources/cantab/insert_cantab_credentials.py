"""
Insert CANTAB credentials into the keystore.
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

import logging
import json
from typing import Dict, Any
import os

from rich.logging import RichHandler

from lochness.helpers import utils, db, config
from lochness.models.keystore import KeyStore

MODULE_NAME = "test.lochness.sources.cantab.insert_creds"

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}

logging.basicConfig(**logargs)

# Keystore Details
KEY_NAME = "cantab_test"
PROJECT_ID = "Pronet"


def get_credentials_from_env() -> Dict[str, str]:
    """
    Retrieve CANTAB credentials from environment variables.

    Note: Need to set CANTAB_USERNAME and CANTAB_PASSWORD in your environment.
    Use export CANTAB_USERNAME='your_username'
        export CANTAB_PASSWORD='your_password'

    Returns:
        Dict[str, str]: Dictionary containing 'username' and 'password'.
    Raises:
        ValueError: If credentials are not found in environment variables.
    """
    username = os.environ.get("CANTAB_USERNAME")
    password = os.environ.get("CANTAB_PASSWORD")

    if username is None or password is None:
        logger.error(
            "CANTAB credentials not found in environment variables."
        )
        logger.error("Please set CANTAB_USERNAME and CANTAB_PASSWORD.")
        raise ValueError(
            "CANTAB credentials (username, password) not found in environment variables."
        )

    cantab_credentials = {
        "username": username,
        "password": password,
    }
    return cantab_credentials


def insert_cantab_credentials():
    """
    Insert CANTAB credentials into the keystore.
    """
    cantab_credentials = get_credentials_from_env()

    keystore_entry = KeyStore(
        key_name=KEY_NAME,
        project_id=PROJECT_ID,
        key_type="cantab",
        key_value=json.dumps(cantab_credentials),
        key_metadata={
            "description": "Test CANTAB credentials",
            "created_by": "test/lochness/sources/cantab/insert_cantab_credentials.py",
        },
    )

    config_file = utils.get_config_file_path()
    encryption_passphrase = config.get_encryption_passphrase(config_file=config_file)

    queries = [
        keystore_entry.to_sql_query(encryption_passphrase=encryption_passphrase)
    ]

    db.execute_queries(config_file, queries, show_commands=True)
    logger.info(f"Inserted CANTAB credentials with key_name '{KEY_NAME}' into the keystore.")


if __name__ == "__main__":
    insert_cantab_credentials()
