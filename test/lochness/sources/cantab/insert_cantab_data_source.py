"""
Insert a test CANTAB data source into the database.
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

from rich.logging import RichHandler

from lochness.helpers import utils, db
from lochness.models.data_source import DataSource
from lochness.sources.cantab.models.data_source import CANTABDataSourceMetadata

MODULE_NAME = "test.lochness.sources.cantab.insert_data_source"

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}

logging.basicConfig(**logargs)

# Data Source Details
DATA_SOURCE_NAME = "CANTAB_Test_DS"
SITE_ID = "CP"
PROJECT_ID = "Pronet"

# This must match the key_name used when inserting CANTAB credentials
KEYSTORE_NAME = "cantab_test"

# CANTAB API Endpoint
CANTAB_API_ENDPOINT = "https://connect-prime.int.cantab.com/api"


def insert_cantab_data_source():
    """
    Insert a test CANTAB data source into the database.
    """
    # Create the data source metadata
    data_source_metadata = CANTABDataSourceMetadata(
        keystore_name=KEYSTORE_NAME,
        api_url=CANTAB_API_ENDPOINT,
    )

    # Create the data source
    data_source = DataSource(
        data_source_name=DATA_SOURCE_NAME,
        site_id=SITE_ID,
        project_id=PROJECT_ID,
        data_source_type="cantab",
        is_active=True,
        data_source_metadata=data_source_metadata.model_dump(),
    )

    config_file = utils.get_config_file_path()
    db.execute_queries(config_file, [data_source.to_sql_query()], show_commands=True)

    logger.info(f"Successfully inserted MindLAMP data source: {DATA_SOURCE_NAME}")
    logger.info(f"Site: {SITE_ID}")
    logger.info(f"Project: {PROJECT_ID}")
    logger.info(f"Keystore Name: {KEYSTORE_NAME}")
    logger.info(f"API URL: {CANTAB_API_ENDPOINT}")


if __name__ == "__main__":
    insert_cantab_data_source()
