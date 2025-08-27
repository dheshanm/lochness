#!/usr/bin/env python
"""
Upload a file to MinIO
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

import os
import logging
from typing import Any, Dict
import traceback

from rich.logging import RichHandler

from lochness.helpers import utils, db
from lochness.sinks.data_sink_i import DataSinkI
from lochness.sinks.minio_object_store.minio_sink import MinioSink
from lochness.models.data_sinks import DataSink
from lochness.models.files import File

logger = logging.getLogger(__name__)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}

logging.basicConfig(**logargs)

TEST_DATA_SINK_NAME = "MinIO_Test_Sink"
TEST_SITE_ID = "CP"
TEST_PROJECT_ID = "Pronet"

# This must match the key_name used when inserting MinIO credentials
KEYSTORE_NAME = "minio_dev"

# MinIO specific metadata (non-sensitive, but defines the sink)
MINIO_BUCKET_NAME = "lochness-test-bucket"
MINIO_REGION = "us-east-1"  # MinIO often doesn't use regions; placeholder

# Dummy file details
TEST_FILE_NAME = "test_upload_file.txt"
TEST_FILE_CONTENT = b"This is a test file for MinIO upload.\n"


def main():
    """
    Main function to test uploading a file to MinIO via a data sink.
    """

    logger.info(
        f"Attempting to upload a test file to MinIO via data sink '{TEST_DATA_SINK_NAME}'..."
    )

    config_file = utils.get_config_file_path()
    if not config_file.exists():
        logger.error(f"ERROR: Configuration file not found at {config_file}")
        return

    # --- Insert MinIO Data Sink (ensures it exists for the test) ---
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
    logger.info(f"MinIO data sink '{TEST_DATA_SINK_NAME}' exists.")

    # 1. Create a dummy file
    test_file_path = Path("/tmp") / TEST_FILE_NAME
    with open(test_file_path, "wb") as f:
        f.write(TEST_FILE_CONTENT)
    logger.info(f"Created dummy file: {test_file_path}")

    try:
        # 2. Retrieve Data Sink details from DB
        data_sink = DataSink.get_matching_data_sink(
            config_file=config_file,
            data_sink_name=TEST_DATA_SINK_NAME,
            site_id=TEST_SITE_ID,
            project_id=TEST_PROJECT_ID,
        )
        data_sink_i: DataSinkI = MinioSink(data_sink=data_sink)  # type: ignore

        if data_sink is None:
            logger.error(f" Data sink '{TEST_DATA_SINK_NAME}' not found in database.")
            return

        file_obj = File(
            file_path=test_file_path,
        )

        data_push = data_sink_i.push(
            file_to_push=test_file_path,
            push_metadata={
                "testing": "true",
            },
            config_file=config_file,
        )

        db.execute_queries(
            config_file=config_file,
            queries=[
                file_obj.to_sql_query(),
                data_push.to_sql_query(),
            ],
            show_commands=False,
        )

    except Exception as e:  # pylint: disable=broad-except
        logger.error(f"An unexpected error occurred: {e}")
        traceback.print_exc()
    finally:
        # Clean up dummy file
        if test_file_path.exists():
            os.remove(test_file_path)
            logger.info(f"Cleaned up dummy file: {test_file_path}")


if __name__ == "__main__":
    main()
