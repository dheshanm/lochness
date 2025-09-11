#!/usr/bin/env python
"""
Insert a DataSink for MinIO into the database.
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

from rich.logging import RichHandler


from lochness.helpers import db, utils
from lochness.models.data_sinks import DataSink
from lochness.helpers import config

logger = logging.getLogger(__name__)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}

logging.basicConfig(**logargs)

minio_cred = config.parse(config_file, 'datasink-test')
TEST_DATA_SINK_NAME = minio_cred['test_data_sink_name']
TEST_SITE_ID = minio_cred['test_site_id']
TEST_PROJECT_ID = minio_cred['test_project_id']

# This must match the key_name used when inserting MinIO credentials
KEYSTORE_NAME = minio_cred['keystore_name']

# MinIO specific metadata (non-sensitive, but defines the sink)
MINIO_BUCKET_NAME = minio_cred['minio_bucket_name']
MINIO_REGION = minio_cred['minio_region']  # MinIO often doesn't use regions; placeholder


def main(config_file: Path):
    """
    Main function to insert a MinIO DataSink.
    """
    logger.info(f"Ensuring MinIO data sink '{TEST_DATA_SINK_NAME}' exists...")
    data_sink_metadata_for_insert = {
        "type": "minio",
        "active": True,
        "bucket_name": MINIO_BUCKET_NAME,
        "region": MINIO_REGION,
        "keystore_name": KEYSTORE_NAME,
    }
    data_sink_obj = DataSink(
        data_sink_name=TEST_DATA_SINK_NAME,
        site_id=TEST_SITE_ID,
        project_id=TEST_PROJECT_ID,
        data_sink_metadata=data_sink_metadata_for_insert,
    )

    db.execute_queries(config_file, [data_sink_obj.to_sql_query()], show_commands=False)
    logger.info(f"MinIO data sink '{TEST_DATA_SINK_NAME}' inserted.")


if __name__ == "__main__":
    config_file = utils.get_config_file_path()
    if not config_file.exists():
        logger.error(f"ERROR: Configuration file not found at {config_file}")
        sys.exit(1)

    main(config_file=config_file)
