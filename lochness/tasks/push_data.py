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
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def get_files_to_push(config_file: Path) -> List[File]:
    """
    Retrieves files from the database that need to be pushed.
    For simplicity, this currently returns all files in the 'files' table.
    In a real scenario, you might filter based on push status or other criteria.
    """
    # TODO: Implement more sophisticated logic to determine which files to push
    # For now, fetch all files
    query = "SELECT file_path, file_md5 FROM files;"
    files_df = db.execute_sql(config_file, query)

    files_to_push: List[File] = []
    for _, row in files_df.iterrows():
        try:
            file_path = Path(row["file_path"])
            file_md5 = row["file_md5"]
            # Re-instantiate File object, ensuring it doesn't recompute hash if already provided
            file_obj = File(file_path=file_path, with_hash=False) # Don't recompute hash
            file_obj.md5 = file_md5 # Assign the retrieved MD5
            files_to_push.append(file_obj)
        except FileNotFoundError:
            logger.warning(f"File not found on disk, skipping: {row["file_path"]}")
            Logs(
                log_level="WARN",
                log_message={
                    "event": "data_push_file_not_found",
                    "message": f"File not found on disk, skipping: {row["file_path"]}",
                    "file_path": row["file_path"],
                },
            ).insert(config_file)
    return files_to_push


def push_file_to_sink(
    file_obj: File,
    data_sink: DataSink,
    config_file: Path,
    encryption_passphrase: str,
) -> bool:
    """
    Dispatches the file push to the appropriate sink-specific handler.
    """
    sink_type = data_sink.data_sink_metadata.get("type")
    if not sink_type:
        logger.error(f"Data sink {data_sink.data_sink_name} has no 'type' defined in its metadata.")
        Logs(
            log_level="ERROR",
            log_message={
                "event": "data_push_missing_sink_type",
                "message": f"Data sink {data_sink.data_sink_name} has no 'type' defined in its metadata.",
                "data_sink_name": data_sink.data_sink_name,
                "project_id": data_sink.project_id,
                "site_id": data_sink.site_id,
            },
        ).insert(config_file)
        return False

    try:
        # Dynamically import the push module for the sink type
        # e.g., lochness.sinks.minio.push
        push_module = importlib.import_module(f"lochness.sinks.{sink_type}.push")
        
        start_time = datetime.now()
        success = push_module.push_file(
            file_path=file_obj.file_path,
            data_sink=data_sink,
            config_file=config_file,
            push_metadata={
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
                data_sink_name=data_sink.data_sink_name,
                site_id=data_sink.site_id,
                project_id=data_sink.project_id,
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
                    "message": f"Successfully pushed {file_obj.file_name} to {data_sink.data_sink_name}.",
                    "file_path": str(file_obj.file_path),
                    "data_sink_name": data_sink.data_sink_name,
                    "project_id": data_sink.project_id,
                    "site_id": data_sink.site_id,
                    "push_time_s": push_time_s,
                },
            ).insert(config_file)
            return True
        else:
            Logs(
                log_level="ERROR",
                log_message={
                    "event": "data_push_failed",
                    "message": f"Failed to push {file_obj.file_name} to {data_sink.data_sink_name}.",
                    "file_path": str(file_obj.file_path),
                    "data_sink_name": data_sink.data_sink_name,
                    "project_id": data_sink.project_id,
                    "site_id": data_sink.site_id,
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
                "data_sink_name": data_sink.data_sink_name,
            },
        ).insert(config_file)
        return False
    except Exception as e:
        logger.error(f"Error pushing file {file_obj.file_name} to {data_sink.data_sink_name}: {e}")
        Logs(
            log_level="ERROR",
            log_message={
                "event": "data_push_exception",
                "message": f"Exception during push of {file_obj.file_name} to {data_sink.data_sink_name}.",
                "file_path": str(file_obj.file_path),
                "data_sink_name": data_sink.data_sink_name,
                "error": str(e),
            },
        ).insert(config_file)
        return False


def push_all_data(config_file: Path, project_id: str = None, site_id: str = None):
    """
    Main function to push data to all active data sinks.
    """
    Logs(
        log_level="INFO",
        log_message={
            "event": "data_push_start",
            "message": "Starting data push process.",
            "project_id": project_id,
            "site_id": site_id,
        },
    ).insert(config_file)

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
        return

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

    files_to_push = get_files_to_push(config_file)
    if not files_to_push:
        logger.info("No files found to push.")
        Logs(
            log_level="INFO",
            log_message={
                "event": "data_push_no_files_to_push",
                "message": "No files found in the database to push.",
                "project_id": project_id,
                "site_id": site_id,
            },
        ).insert(config_file)
        return

    logger.info(f"Found {len(files_to_push)} files to push.")
    Logs(
        log_level="INFO",
        log_message={
            "event": "data_push_files_found",
            "message": f"Found {len(files_to_push)} files to push.",
            "count": len(files_to_push),
            "project_id": project_id,
            "site_id": site_id,
        },
    ).insert(config_file)

    encryption_passphrase = config.parse(config_file, 'general')['encryption_passphrase']

    for file_obj in files_to_push:
        for data_sink in active_data_sinks:
            logger.info(f"Attempting to push {file_obj.file_name} to {data_sink.data_sink_name}...")
            push_file_to_sink(
                file_obj=file_obj,
                data_sink=data_sink,
                config_file=config_file,
                encryption_passphrase=encryption_passphrase,
            )

    Logs(
        log_level="INFO",
        log_message={
            "event": "data_push_complete",
            "message": "Finished data push process.",
            "project_id": project_id,
            "site_id": site_id,
        },
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
