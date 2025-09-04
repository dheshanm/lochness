#!/usr/bin/env python
"""
Test fteching subject data
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
from typing import Any, Dict

import pytest
from rich.logging import RichHandler

from lochness.helpers import utils
from lochness.sources.cantab import api as cantab_api
from lochness.sources.cantab.models.data_source import CANTABDataSource
from lochness.helpers import config

MODULE_NAME = "test.lochness.sources.cantab.fetch_data"

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}

logging.basicConfig(**logargs)

# Data Source Details
cantab_cred = config.parse(config_file, 'cantab-test')
DATA_SOURCE_NAME = cantab_cred['data_source_name']
SITE_ID = cantab_cred['site_id']
PROJECT_ID = cantab_cred['project_id']

CANTAB_ID = cantab_cred['cantab_id']


@pytest.fixture(scope="module")
def config_file() -> Path:
    """A pytest fixture to provide the config file path to tests."""
    # This makes sure the config is only loaded once for all tests in this file.
    return utils.get_config_file_path()


@pytest.mark.cantab
@pytest.mark.integration
def test_fetch_subject_data_integration(config_file: Path) -> None:
    """
    Integration test for fetching CANTAB subject data.

    Args:
        config_file (Path): Path to the configuration file.
    """
    test_cantab_data_sources = CANTABDataSource.get(
        data_source_name=DATA_SOURCE_NAME,
        site_id=SITE_ID,
        project_id=PROJECT_ID,
        config_file=config_file,
    )

    if test_cantab_data_sources is None:
        logger.error("CANTAB Data Source not found.")
        raise AssertionError("CANTAB Data Source not found.")
    else:
        logger.info("CANTAB Data Source found.")

    cantab_data = cantab_api.get_cantab_data(
        cantab_data_source=test_cantab_data_sources,
        cantab_id=CANTAB_ID,
        config_file=config_file
    )

    if cantab_data is not None:
        logger.info(f"Success: Fetched CANTAB data for ID: {CANTAB_ID}")
    else:
        logger.error(f"Failed to fetch CANTAB data for ID: {CANTAB_ID}")
