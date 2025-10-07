"""
Implementation of a data sink for Azure Blob Storage.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional
from datetime import datetime

from lochness.sinks.azure_blob_storage import api as azure_api
from lochness.helpers.timer import Timer
from lochness.models.data_push import DataPush
from lochness.models.keystore import KeyStore
from lochness.sinks.data_sink_i import DataSinkI
from lochness.models.logs import Logs
from lochness.helpers import hash as hash_helper

logger = logging.getLogger(__name__)


class AzureBlobSink(DataSinkI):
    """
    A concrete implementation of a data sink for Azure Blob Storage.

    This class implements the abstract methods defined in DataSinkI
    to push data to an Azure Blob Storage account.
    """

    def push(
        self,
        file_to_push: Path,
        push_metadata: Dict[str, str],
        config_file: Path,
    ) -> DataPush:
        """
        Pushes data to the Azure Blob Storage sink.

        Args:
            file_to_push (Path): The path to the file to be pushed.
            push_metadata (Dict[str, str]): Metadata associated with the push.
            data_sink (DataSink): The DataSink object containing Azure Blob Storage configuration.
            config_file (str): Path to the configuration file.

        Returns:
            DataPush: An instance of DataPush if successful, None otherwise.
        """

        azure_metadata = self.data_sink.data_sink_metadata
        container_name: Optional[str] = azure_metadata.get("container_name")
        keystore_name: Optional[str] = azure_metadata.get("keystore_name")

        if not all([container_name, keystore_name]):
            logger.error(
                f"Missing Azure Blob Storage configuration (container_name, or "
                f"keystore_name) for sink {self.data_sink.data_sink_name}"
            )
            logger.debug(f"Data sink metadata: {azure_metadata}")
            raise ValueError("Incomplete Azure Blob Storage configuration.")

        # Retrieve Azure Blob Storage credentials from the keystore
        keystore_data = KeyStore.retrieve_keystore(
            key_name=keystore_name,  # type: ignore
            project_id=self.data_sink.project_id,
            config_file=config_file,
        )

        if not keystore_data:
            logger.error(
                f"Failed to retrieve keystore data for {keystore_name} "
                f"in project {self.data_sink.project_id}"
            )
            raise ValueError("Keystore data not found for the specified keystore name.")

        logger.debug(f"Retrieved keystore data for {keystore_name} ")
        logger.debug(f"Keystore data: {keystore_data}")

        keystore_value: Dict[str, Any] = json.loads(keystore_data.key_value)
        connection_string: Optional[str] = keystore_value.get("connection_string", None)

        if not all([connection_string]):
            logger.error(
                f"Missing Azure Blob Storage credentials (connection_string) "
                f"for keystore {keystore_name} in project {self.data_sink.project_id}"
            )
            logger.debug(f"Keystore data: {keystore_value}")
            raise ValueError("Incomplete Azure Blob Storage credentials.")

        # Check if Container exists
        container_exists = azure_api.check_if_container_exists(
            connection_string=connection_string,  # type: ignore
            container_name=container_name,  # type: ignore
        )

        if not container_exists:
            logger.error(
                f"Azure Blob Storage container '{container_name}' does not exist. "
                f"Cannot push data to sink '{self.data_sink.data_sink_name}'."
            )
            raise ValueError("Azure Blob Storage container does not exist.")

        project_name_cap = (
            self.data_sink.project_id[:1].upper()
            + self.data_sink.project_id[1:].lower()
        )

        object_name = (
            f"{project_name_cap}/PHOENIX/PROTECTED/"
            f"{project_name_cap}{self.data_sink.site_id}/raw/"
            f"{push_metadata.get('subject_id', 'unknown')}/"
            f"{push_metadata.get('modality', 'unknown')}/"
            f"{file_to_push.name}"
        )
        file_md5 = hash_helper.compute_fingerprint(file_to_push)

        try:
            with Timer() as timer:
                azure_metadata = push_metadata.copy()
                azure_metadata["upload_timestamp"] = datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                azure_metadata["file_md5"] = file_md5

                azure_api.upload_to_blob(
                    connection_string=connection_string,  # type: ignore
                    container_name=container_name,  # type: ignore
                    blob_name=object_name,
                    source_file_path=file_to_push,
                    tags=azure_metadata,
                )
        except Exception as e:  # pylint: disable=broad-except
            log_message = (
                f"Failed to push file {file_to_push} to Azure Blob Storage: {e}"
            )
            logger.error(log_message)

            Logs(
                log_level="ERROR",
                log_message={
                    "event": "azure_blob_push_error",
                    "message": log_message,
                    "data_sink_name": self.data_sink.data_sink_name,
                    "project_id": self.data_sink.project_id,
                    "site_id": self.data_sink.site_id,
                    "file_path": file_to_push,
                },
            ).insert(config_file)
            raise e

        data_push = DataPush(
            data_sink_id=self.data_sink.get_data_sink_id(config_file=config_file),  # type: ignore
            file_path=str(file_to_push),
            file_md5=file_md5,
            push_time_s=int(timer.duration) if timer.duration is not None else 0,
            push_metadata={
                "container_name": container_name,
                "object_name": object_name,
            },
            push_timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )

        return data_push
