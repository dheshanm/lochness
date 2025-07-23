#!/usr/bin/env python
"""
Pushes data from the local file system to configured data sinks.
"""

import sys
from pathlib import Path
import argparse
import logging
from typing import Any, Dict, List, Optional, cast
from datetime import datetime
import importlib

file = Path(__file__).resolve()
parent = file.parent
root_dir = None
for parent in file.parents:
    if parent.name == "lochness-v2":
        root_dir = parent

sys.path.append(str(root_dir))

try:
    sys.path.remove(str(parent))
except ValueError:
    pass

from lochness.helpers import logs, utils, db, config
from lochness.models.data_sinks import DataSink
from lochness.models.files import File
from lochness.models.data_pushes import DataPush
from lochness.models.logs import Logs

MODULE_NAME = "lochness.tasks.push_data"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    "format": "%(message)s",
    # Use default handler; RichHandler removed
}
logging.basicConfig(**logargs)


def push_file_to_sink(file_obj: File,
                      dataSink: DataSink,
                      config_file: Path,
                      encryption_passphrase: str) -> bool:
    """
    Dispatches the file push to the appropriate sink-specific handler.
    Only pushes if the file (by path and md5) has not already been
    pushed to this sink.
    """
    sink_type = dataSink.data_sink_metadata.get("type")
    if not sink_type:
        msg = f"Data sink {dataSink.data_sink_name} " \
              "has no 'type' defined in its metadata."
        logger.error(msg)
        Logs(log_level="ERROR",
             log_message={
                 "event": "data_push_missing_sink_type",
                 "message": msg,
                 "data_sink_name": dataSink.data_sink_name,
                 "project_id": dataSink.project_id,
                 "site_id": dataSink.site_id,
                 },
             ).insert(config_file)
        return False


    if dataSink.is_file_already_pushed(config_file,
                                       file_obj.file_path,
                                       file_obj.md5):
        msg = f"File {file_obj.file_name} (md5={file_obj.md5}) already " \
              f"pushed to {dataSink.data_sink_name}, skipping."
        logger.info(msg)
        Logs(
            log_level="INFO",
            log_message={
                "event": "data_push_already_exists",
                "message": msg,
                "file_path": file_obj.file_path,
                "data_sink_name": dataSink.data_sink_name,
                "project_id": dataSink.project_id,
                "site_id": dataSink.site_id,
            },
        ).insert(config_file)
        return True  # Not an error, just skip

    try:
        # Dynamically import the push module for the sink type
        # e.g., lochness.sinks.minio.push
        push_module = importlib.import_module(f"lochness.sinks.{sink_type}.push")
        
        start_time = datetime.now()
        success = push_module.push_file(
            file_path=file_obj.file_path,
            data_sink=dataSink,
            config_file=config_file,
            push_metadata={
                "data_source_name": file_obj.data_source_name,
                "subject_id": file_obj.subject_id,
                "site_id": file_obj.site_id,
                "project_id": file_obj.project_id,
                "file_name": file_obj.file_name,
                "file_size_mb": file_obj.file_size_mb,
            },
            encryption_passphrase=encryption_passphrase,
        )
        end_time = datetime.now()
        push_time_s = int((end_time - start_time).total_seconds())

        if success:
            # Record successful push in data_pushes table
            data_push = DataPush(
                file_path=str(file_obj.file_path),
                file_md5=file_obj.md5,
                data_sink_name=dataSink.data_sink_name,
                site_id=dataSink.site_id,
                project_id=dataSink.project_id,
                push_time_s=push_time_s,
                push_metadata={
                    "sink_type": sink_type,
                    "file_size_mb": file_obj.file_size_mb,
                    "destination_path": "TODO: Get actual destination path from sink handler",
                },
            )
            data_push.insert(config_file)

            Logs(
                log_level="INFO",
                log_message={
                    "event": "data_push_success",
                    "message": f"Successfully pushed {file_obj.file_name} to {dataSink.data_sink_name}.",
                    "file_path": str(file_obj.file_path),
                    "data_sink_name": dataSink.data_sink_name,
                    "project_id": dataSink.project_id,
                    "site_id": dataSink.site_id,
                    "push_time_s": push_time_s,
                },
            ).insert(config_file)
            return True
        else:
            Logs(
                log_level="ERROR",
                log_message={
                    "event": "data_push_failed",
                    "message": f"Failed to push {file_obj.file_name} to {dataSink.data_sink_name}.",
                    "file_path": str(file_obj.file_path),
                    "data_sink_name": dataSink.data_sink_name,
                    "project_id": dataSink.project_id,
                    "site_id": dataSink.site_id,
                },
            ).insert(config_file)
            return False

    except ModuleNotFoundError:
        logger.error(f"No push handler found for sink type: {sink_type}")
        Logs(
            log_level="ERROR",
            log_message={
                "event": "data_push_handler_not_found",
                "message": f"No push handler found for sink type: {sink_type}.",
                "sink_type": sink_type,
                "data_sink_name": dataSink.data_sink_name,
            },
        ).insert(config_file)
        return False

    except Exception as e:
        logger.error(f"Error pushing file {file_obj.file_name} to {dataSink.data_sink_name}: {e}")
        Logs(
            log_level="ERROR",
            log_message={
                "event": "data_push_exception",
                "message": f"Exception during push of {file_obj.file_name} to {dataSink.data_sink_name}.",
                "file_path": str(file_obj.file_path),
                "data_sink_name": dataSink.data_sink_name,
                "error": str(e),
            },
        ).insert(config_file)
        return False


def get_matching_dataSink_list(config_file: Path, project_id: str, site_id: str) -> List[DataSink]:
    active_data_sinks = DataSink.get_all_data_sinks(
        config_file=config_file,
        active_only=True # Assuming a method to get active sinks exists
    )

    if project_id:
        active_data_sinks = [ds for ds in active_data_sinks if ds.project_id == project_id]
    if site_id:
        active_data_sinks = [ds for ds in active_data_sinks if ds.site_id == site_id]

    if not active_data_sinks:
        logger.info("No active data sinks found.")
        Logs(
            log_level="INFO",
            log_message={
                "event": "data_push_no_active_sinks",
                "message": "No active data sinks found for push.",
                "project_id": project_id,
                "site_id": site_id,
            },
        ).insert(config_file)
        return []

    logger.info(f"Found {len(active_data_sinks)} active data sinks.")
    Logs(
        log_level="INFO",
        log_message={
            "event": "data_push_active_sinks_found",
            "message": f"Found {len(active_data_sinks)} active data sinks.",
            "count": len(active_data_sinks),
            "project_id": project_id,
            "site_id": site_id,
        },
    ).insert(config_file)

    return active_data_sinks


def push_all_data(config_file: Path,
                  project_id: str = None,
                  site_id: str = None) -> None:
    """
    Main function to push data to all active data sinks.
    """
    Logs(log_level="INFO",
         log_message={
             "event": "data_push_start",
             "message": "Starting data push process.",
             "project_id": project_id,
             "site_id": site_id}).insert(config_file)

    active_data_sinks = get_matching_dataSink_list(config_file, project_id, site_id)
    if active_data_sinks == []:
        return
    
    files_to_push = File.get_files_to_push(config_file)
    if files_to_push == []:
        logger.info("No files found to push.")
        Logs(log_level="INFO",
             log_message={
                 "event": "data_push_no_files_to_push",
                 "message": "No files found in the database to push.",
                 "project_id": project_id,
                 "site_id": site_id}).insert(config_file)
        return

    logger.info(f"Found {len(files_to_push)} files to push.")
    Logs(log_level="INFO",
         log_message={
             "event": "data_push_files_found",
             "message": f"Found {len(files_to_push)} files to push.",
             "count": len(files_to_push),
             "project_id": project_id,
             "site_id": site_id}).insert(config_file)

    encryption_passphrase = config.parse(
            config_file, 'general')['encryption_passphrase']

    for file_obj in files_to_push:
        for dataSink in active_data_sinks:
            logger.info(f"Attempting to push {file_obj.file_name} to "
                        f"{dataSink.data_sink_name}...")
            push_file_to_sink(
                    file_obj=file_obj,
                    dataSink=dataSink,
                    config_file=config_file,
                    encryption_passphrase=encryption_passphrase,
            )

    Logs(log_level="INFO",
         log_message={
             "event": "data_push_complete",
             "message": "Finished data push process.",
             "project_id": project_id,
             "site_id": site_id}
         ).insert(config_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Push data to all or specific data sinks.")
    parser.add_argument('--project_id', type=str, default=None, help='Project ID to push data for (optional)')
    parser.add_argument('--site_id', type=str, default=None, help='Site ID to push data for (optional)')
    args = parser.parse_args()

    config_file = utils.get_config_file_path()
    logs.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    logger.info("Starting data push...")

    if not config_file.exists():
        logger.error(f"Config file does not exist: {config_file}")
        Logs(
            log_level="FATAL",
            log_message={
                "event": "data_push_config_missing",
                "message": f"Config file does not exist: {config_file}",
                "config_file_path": str(config_file),
            },
        ).insert(config_file)
        sys.exit(1)

    push_all_data(config_file=config_file, project_id=args.project_id, site_id=args.site_id)

    logger.info("Finished data push.")
