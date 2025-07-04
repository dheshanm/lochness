#!/usr/bin/env python
"""
XNAT module
"""

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
root_dir = None  # pylint: disable=invalid-name
for parent in file.parents:
    if parent.name == "lochness-v2":
        root_dir = parent

sys.path.append(str(root_dir))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import logging
from typing import Any, Dict, List, Optional, cast

import pandas as pd
import requests
from rich.logging import RichHandler

from lochness.helpers import logs, utils, db, config
from lochness.models.keystore import KeyStore
from lochness.models.subjects import Subject
from lochness.sources.xnat.models.data_source import XnatDataSource

MODULE_NAME = "lochness.sources.xnat.tasks.sync"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)



def insert_xnat_cred():
    config_file = utils.get_config_file_path()

    # how should we handle encryption passphrase?
    encryption_passphrase = config.parse(config_file, 'general')[
            'encryption_passphrase']

    # 2. Create a KeyStore instance
    my_key = KeyStore(
        key_name="xnat",
        key_value="secure_token_string_here",
        key_type="xnat",
        key_metadata={
            "description": "Access token for XNAT",
            "created_by": "kevin"}
    )

    insert_query = my_key.to_sql_query(
            encryption_passphrase=encryption_passphrase)

    db.execute_queries(  # type: ignore
        config_file=config_file,
        queries=[insert_query],
        show_commands=False,
    )

def get_xnat_cred():
    config_file = utils.get_config_file_path()



# def test():
    # logs.configure_logging(
        # config_file=config_file,
        # module_name=MODULE_NAME,
        # logger=logger
    # )

    # if not config_file.exists():
        # logger.error(f"Config file does not exist: {config_file}")
        # sys.exit(1)

    # logger.info("Finished syncing XNAT.")
