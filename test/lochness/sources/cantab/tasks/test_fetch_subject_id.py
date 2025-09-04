#!/usr/bin/env python
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

MODULE_NAME = "test.lochness.sources.cantab.link_subject"

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

TEST_SUBJECT_ID = minio_cred['test_subject_id']
EXPECTED_CANTAB_ID = minio_cred['cantab_id']


@pytest.fixture(scope="module")
def config_file() -> Path:
    """A pytest fixture to provide the config file path to tests."""
    # This makes sure the config is only loaded once for all tests in this file.
    return utils.get_config_file_path()


@pytest.mark.cantab
@pytest.mark.integration
def test_fetch_subject_id_integration(config_file: Path) -> None:
    """
    Integration test for fetching CANTAB subject ID.

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

    cantab_id = cantab_api.fetch_cantab_id(
        cantab_data_source=test_cantab_data_sources,
        subject_id=TEST_SUBJECT_ID,
        config_file=config_file,
    )

    if cantab_id == EXPECTED_CANTAB_ID:
        logger.info(f"Success: Fetched expected CANTAB ID: {cantab_id}")
    else:
        logger.error(f"Unexpected CANTAB ID: {cantab_id}")
        raise AssertionError(f"Unexpected CANTAB ID: {cantab_id}")
