import logging
from pathlib import Path
from typing import Any, Dict

from minio import Minio
from minio.error import S3Error

from lochness.models.data_sinks import DataSink
from lochness.models.keystore import KeyStore
from lochness.helpers import db
from lochness.models.logs import Logs # Added Logs model


logger = logging.getLogger(__name__)

def push_file(file_path: Path,
              data_sink: DataSink,
              config_file: Path,
              push_metadata: Dict[str, Any],
              encryption_passphrase: str) -> bool:
    """
    Pushes a file to a MinIO data sink.

    Args:
        file_path (Path): The path to the file to push.
        data_sink (DataSink): The DataSink object containing MinIO configuration.
        config_file (Path): Path to the Lochness configuration file.
        push_metadata (Dict[str, Any]): Metadata to be included in the push record.
        encryption_passphrase (str): The passphrase used to encrypt the data.

    Returns:
        bool: True if the push was successful, False otherwise.
    """
    minio_metadata = data_sink.data_sink_metadata
    endpoint = minio_metadata.get("endpoint")
    bucket_name = minio_metadata.get("bucket_name")
    secure = minio_metadata.get("secure", True) # Default to secure connection
    keystore_name = minio_metadata.get("keystore_name")

    if not all([endpoint, bucket_name, keystore_name]):
        msg = f"Missing MinIO configuration (endpoint, bucket_name, or " \
              f"keystore_name) for sink {data_sink.data_sink_name}"
        logger.error(msg)
        Logs(log_level="ERROR",
             log_message={
                 "event": "minio_push_config_error",
                 "message": msg,
                 "data_sink_name": data_sink.data_sink_name,
                 "project_id": data_sink.project_id,
                 "site_id": data_sink.site_id}).insert(config_file)
        return False

    # Retrieve access_key and secret_key from KeyStore
    query_access_key = KeyStore.retrieve_key_query(keystore_name,
                                                   data_sink.project_id,
                                                   encryption_passphrase)
    key_df = db.execute_sql(config_file, query_access_key)
    secret_key = key_df['key_value'][0] if not key_df.empty \
            else None

    query_metadata = KeyStore.retrieve_key_metadata(keystore_name,
                                                    data_sink.project_id)
    key_metadata = db.execute_sql(config_file, query_metadata)
    access_key = key_metadata.iloc[0]['key_metadata']['access_key'] if not {} \
            else None


    if not all([access_key, secret_key]):
        msg = "Missing MinIO credentials (access_key or secret_key) in " \
              f"KeyStore for sink {data_sink.data_sink_name}"
        logger.error(msg)
        Logs(log_level="ERROR",
             log_message={
                 "event": "minio_push_credential_error",
                 "message": msg,
                 "data_sink_name": data_sink.data_sink_name,
                 "project_id": data_sink.project_id,
                 "site_id": data_sink.site_id,
                 "keystore_name": keystore_name}).insert(config_file)
        return False

    try:
        endpoint = 'pnl-minio-1.partners.org:9000'
        client = Minio(
            endpoint.replace("http://", "").replace("https://", ""),
            access_key=access_key,
            secret_key=secret_key,
            secure=endpoint.startswith("https")
        )

        # Ensure the bucket exists
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            Logs(log_level="INFO",
                 log_message={
                     "event": "minio_bucket_created",
                     "message": f"Created MinIO bucket: {bucket_name}",
                     "bucket_name": bucket_name,
                     "data_sink_name": data_sink.data_sink_name}
                 ).insert(config_file)

        # Determine object name (path within the bucket)
        # For simplicity, let's use a path similar to the local storage structure
        # e.g., <project_id>/<site_id>/<data_source_name>/<subject_id>/<filename>
        object_name = f"{data_sink.project_id}/{data_sink.site_id}/" \
                      f"{push_metadata.get('data_source_name', 'unknown')}/" \
                      f"{push_metadata.get('subject_id', 'unknown')}/" \
                      f"{file_path.name}"

        client.fput_object(
            bucket_name,
            object_name,
            str(file_path),
            metadata=push_metadata # Pass additional metadata to MinIO
        )
        msg = f"Successfully pushed {file_path.name} to MinIO bucket " \
              f"{bucket_name} as {object_name}"
        Logs(log_level="INFO",
             log_message={
                 "event": "minio_push_success",
                 "message": msg,
                 "file_name": file_path.name,
                 "bucket_name": bucket_name,
                 "object_name": object_name,
                 "data_sink_name": data_sink.data_sink_name,
                 "project_id": data_sink.project_id,
                 "site_id": data_sink.site_id}).insert(config_file)
        return True

    except S3Error as e:
        msg = f"MinIO S3 Error pushing {file_path.name} to " \
              f"{data_sink.data_sink_name}: {e}"
        logger.error(msg)
        Logs(log_level="ERROR",
             log_message={
                 "event": "minio_s3_error",
                 "message": msg,
                 "file_name": file_path.name,
                 "data_sink_name": data_sink.data_sink_name,
                 "error": str(e)}).insert(config_file)
        return False

    except Exception as e:
        msg = f"Unexpected error pushing {file_path.name} to " \
              f"{data_sink.data_sink_name}: {e}"
        logger.error(msg)
        Logs(log_level="ERROR",
             log_message={
                 "event": "minio_push_unexpected_error",
                 "message": msg,
                 "file_name": file_path.name,
                 "data_sink_name": data_sink.data_sink_name,
                 "error": str(e)}).insert(config_file)
