"""
Insert MindLAMP credentials into the keystore for testing purposes.
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
from typing import Dict, Any
import json

from rich.logging import RichHandler

from lochness.helpers import utils, db, config
from lochness.models.keystore import KeyStore

MODULE_NAME = "test.lochness.sources.mindlamp.insert_creds"

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}

logging.basicConfig(**logargs)

# Keystore Details
KEY_NAME = "mindlamp_prod"
PROJECT_ID = "Pronet"

# MindLAMP Credentials (replace with actual values)
ACCESS_KEY = "your_mindlamp_access_key"
SECRET_KEY = "your_mindlamp_secret_key"


def insert_mindlamp_credentials():
    """
    Insert MindLAMP credentials into the keystore.
    """

    key_values: Dict[str, str] = {
        "access_key": ACCESS_KEY,
        "secret_key": SECRET_KEY,
    }

    # Create the keystore entry
    keystore_entry = KeyStore(
        key_name=KEY_NAME,
        project_id=PROJECT_ID,
        key_type="mindlamp",
        key_value=json.dumps(key_values),
        key_metadata={
            "description": "Test MindLAMP credentials",
            "created_by": "test/lochness/sources/mindlamp/insert_mindlamp_credentials.py",
        },
    )

    config_file = utils.get_config_file_path()

    encryption_passphrase: str = config.parse(config_file, "general")[
        "encryption_passphrase"
    ]  # type: ignore

    queries = [
        keystore_entry.to_sql_query(encryption_passphrase=encryption_passphrase),
    ]

    # Insert the keystore entry into the database
    db.execute_queries(config_file, queries, show_commands=True)

    logger.debug(f"Successfully inserted MindLAMP credentials: {KEY_NAME}")
    logger.debug(f"Project: {PROJECT_ID}")
    logger.debug(f"Access Key: {ACCESS_KEY[:8]}...")
    logger.debug(f"Secret Key: {SECRET_KEY[:8]}...")


if __name__ == "__main__":
    insert_mindlamp_credentials()
