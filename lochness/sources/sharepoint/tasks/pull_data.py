#!/usr/bin/env python
"""
Pulls data from SharePoint for active data sources and subjects.

This script is intended to be run as a cron job.
It will pull data for all active SharePoint data sources and their associated subjects.
"""

import sys
import logging
import argparse
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime

from rich.logging import RichHandler

from lochness.helpers import logs, utils, db, config
from lochness.models.subjects import Subject
from lochness.models.keystore import KeyStore
from lochness.models.logs import Logs
from lochness.models.files import File
from lochness.models.data_pulls import DataPull
from lochness.sources.sharepoint.models.data_source import SharepointDataSource
from lochness.sources.sharepoint.tasks.pull_utils import (
        log_event,
        authenticate,
        get_site_id,
        get_drives,
        find_drive_by_name,
        list_drive_root,
        find_folder_in_drive,
        list_folder_items,
        find_subfolder,
        should_download_file,
        get_matching_subfolders,
        download_new_or_updated_files
    )


MODULE_NAME = "lochness.sources.sharepoint.tasks.pull_data"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

# Set up logger
logging.basicConfig(**logargs)
logger = logging.getLogger(MODULE_NAME)


config_file = utils.get_config_file_path()


def fetch_subject_data(sharepoint_data_source: SharepointDataSource,
                       subject_id: str) -> None:
    """
    Fetches data for a single subject from SharePoint.

    Args:
        sharepoint_data_source: SharepointDataSource object.
        subject_id (str): The subject ID to fetch data for.

    Returns:
        None
    """
    project_id = sharepoint_data_source.project_id
    site_id = sharepoint_data_source.site_id
    data_source_name = sharepoint_data_source.data_source_name

    metadata = sharepoint_data_source.data_source_metadata
    form_name = metadata.form_name
    modality = getattr(metadata, 'modality', 'unknown')

    identifier = f"{project_id}::{site_id}::{data_source_name}::{subject_id}"
    logger.debug("Fetching data for %s", identifier)

    # keystore
    keystore = KeyStore.retrieve_keystore(metadata.keystore_name,
                                          project_id,
                                          config_file=config_file)
    metadata_query = KeyStore.retrieve_key_metadata(metadata.keystore_name,
                                                    project_id)
    keystore_metadata = db.execute_sql(config_file,
                                       metadata_query).iloc[0]['key_metadata']

    # Authenticate the sharepoint application
    # For device flow use client_secret=None
    headers = authenticate(client_id=keystore_metadata['client_id'],
                           tenant_id=keystore_metadata['tenant_id'],
                           client_secret=keystore.key_value)

    # Get site ID
    sharepoint_site_id = get_site_id(headers,
                                     keystore_metadata['site_url'])

    drives = get_drives(sharepoint_site_id, headers)
    team_forms_drive = find_drive_by_name(drives, "Team Forms")
    if not team_forms_drive:
        raise RuntimeError("Team Forms drive not found.")
    drive_id = team_forms_drive["id"]

    responses_folder = find_folder_in_drive(drive_id, "Responses", headers)
    if not responses_folder:
        raise RuntimeError("Responses folder not found in Team Forms drive.")

    # Build output path
    project_name_cap = project_id[:1].upper() + project_id[1:].lower() \
        if project_id else project_id

    lochness_root = config.parse(config_file, 'general')['lochness_root']
    output_dir = (
        Path(lochness_root)
        / project_name_cap
        / "PHOENIX"
        / "PROTECTED"
        / f"{project_name_cap}{site_id}"
        / "raw"
        / subject_id
        / modality
    )
    subfolders = get_matching_subfolders(drive_id, responses_folder,
                                         form_name, headers)

    for subfolder in subfolders:
        download_new_or_updated_files(subfolder,
                                      drive_id,
                                      headers,
                                      form_name,
                                      subject_id,
                                      site_id,
                                      project_id,
                                      data_source_name,
                                      output_dir)


def save_subject_data(
    data: bytes,
    project_id: str,
    site_id: str,
    subject_id: str,
    data_source_name: str,
    config_file: Path,
) -> Optional[tuple[Path, str]]:
    """
    Saves the fetched subject data to the file system and records it in the database.

    Args:
        data (bytes): The raw data to save.
        project_id (str): The project ID.
        site_id (str): The site ID.
        subject_id (str): The subject ID.
        data_source_name (str): The name of the data source.
        config_file (Path): Path to the config file.

    Returns:
        Optional[Path]: The path to the saved file, or None if saving fails.
    """
    try:
        # Define the path where the data will be stored
        # Example: <lochness_root>/data/<project_id>/<site_id>/<data_source_name>/<subject_id>/<timestamp>.json
        lochness_root = config.parse(config_file, 'general')['lochness_root']
        output_dir = Path(lochness_root) / "data" / project_id / site_id / data_source_name / subject_id
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = utils.get_timestamp()
        file_name = f"{timestamp}.zip"  # ZIP format for SharePoint data
        file_path = output_dir / file_name

        with open(file_path, "wb") as f:
            f.write(data)

        # Record the file in the database
        file_model = File(
            file_path=file_path,
        )
        file_md5 = file_model.md5
        # Insert file_model into the database
        db.execute_queries(config_file, [file_model.to_sql_query()], show_commands=False)

        Logs(
            log_level="INFO",
            log_message={
                "event": "sharepoint_data_pull_save_success",
                "message": f"Successfully saved data for {subject_id} to {file_path}.",
                "project_id": project_id,
                "site_id": site_id,
                "data_source_name": data_source_name,
                "subject_id": subject_id,
                "file_path": str(file_path),
                "file_md5": file_md5 if file_md5 else None,
            },
        ).insert(config_file)
        return file_path, file_md5 if file_md5 else ""

    except Exception as e:
        logger.error(f"Failed to save data for {subject_id}: {e}")
        Logs(
            log_level="ERROR",
            log_message={
                "event": "sharepoint_data_pull_save_failed",
                "message": f"Failed to save data for {subject_id}.",
                "project_id": project_id,
                "site_id": site_id,
                "data_source_name": data_source_name,
                "subject_id": subject_id,
                "error": str(e),
            },
        ).insert(config_file)
        return None


def pull_all_data(
    config_file: Path,
    project_id: str = None,
    site_id: str = None,
    subject_id_list: Optional[List[str]] = None
    ):
    """
    Main function to pull data for all SharePoint data sources and subjects.
    """

    log_event(
        config_file=config_file,
        log_level="INFO",
        event="sharepoint_data_pull_start",
        message="Starting SharePoint data pull process.",
        project_id=project_id,
        site_id=site_id,
    )

    active_sharepoint_data_sources = \
        SharepointDataSource.get_all_sharepoint_data_sources(
                config_file=config_file,
                active_only=True)

    if project_id:
        active_sharepoint_data_sources = [
                ds for ds in active_sharepoint_data_sources
                if ds.project_id == project_id]
    if site_id:
        active_sharepoint_data_sources = [
                ds for ds in active_sharepoint_data_sources
                if ds.site_id == site_id]

    if not active_sharepoint_data_sources:
        msg = "No active SharePoint data sources found for data pull."
        logger.info(msg)
        log_event(
            config_file=config_file,
            log_level="INFO",
            event="sharepoint_data_pull_no_active_sources",
            message=msg,
            project_id=project_id,
            site_id=site_id,
        )
        return

    msg = ("Found "
           + str(len(active_sharepoint_data_sources))
           + " active REDCap data sources for data pull.")

    logger.info(msg)
    log_event(
        config_file=config_file,
        log_level="INFO",
        event="sharepoint_data_pull_active_sources_found",
        message=msg,        project_id=project_id,
        site_id=site_id,
        extra={"count": len(active_sharepoint_data_sources)},
    )

    for sharepoint_data_source in active_sharepoint_data_sources:
        # Get subjects for this data source
        subjects_in_db = Subject.get_subjects_for_project_site(
            project_id=sharepoint_data_source.project_id,
            site_id=sharepoint_data_source.site_id,
            config_file=config_file
        )

        if subject_id_list:
            subjects_in_db = [
                x for x in subjects_in_db if x.subject_id in subject_id_list
            ]

        if not subjects_in_db:
            logger.info(  # pylint: disable=logging-not-lazy
                (
                    "No subjects found for "
                    + f"{sharepoint_data_source.project_id}"
                    + "::"
                    + f"{sharepoint_data_source.site_id}."
                )
            )
            log_event(
                config_file=config_file,
                log_level="INFO",
                event="sharepoint_data_pull_no_subjects",
                message=(
                    f"No subjects found for "
                    f"{sharepoint_data_source.project_id}::"
                    f"{sharepoint_data_source.site_id}."
                ),
                project_id=sharepoint_data_source.project_id,
                site_id=sharepoint_data_source.site_id,
                data_source_name=sharepoint_data_source.data_source_name,
            )
            continue

        msg = f"Found {len(subjects_in_db)} subjects for " \
            f"{sharepoint_data_source.data_source_name}."
        logger.info(msg)
        log_event(
            config_file=config_file,
            log_level="INFO",
            event="sharepoint_data_pull_subjects_found",
            message=msg,
            project_id=sharepoint_data_source.project_id,
            site_id=sharepoint_data_source.site_id,
            data_source_name=sharepoint_data_source.data_source_name,
            extra={"count": len(subjects_in_db)},
        )

        for subject in subjects_in_db:
            start_time = datetime.now()
            raw_data = fetch_subject_data(
                sharepoint_data_source=sharepoint_data_source,
                subject_id=subject.subject_id,
            )

            if raw_data:
                result = save_subject_data(
                    data=raw_data,
                    project_id=subject.project_id,
                    site_id=subject.site_id,
                    subject_id=subject.subject_id,
                    data_source_name=sharepoint_data_source.data_source_name,
                    config_file=config_file,
                )
                if result:
                    file_path, file_md5 = result
                    end_time = datetime.now()
                    pull_time_s = int((end_time - start_time).total_seconds())

                    data_pull = DataPull(
                        subject_id=subject.subject_id,
                        data_source_name=sharepoint_data_source.data_source_name,
                        site_id=subject.site_id,
                        project_id=subject.project_id,
                        file_path=str(file_path),
                        file_md5=file_md5,
                        pull_time_s=pull_time_s,
                        pull_metadata={
                            "sharepoint_site_url": sharepoint_data_source.data_source_metadata.site_url,
                            "form_name": sharepoint_data_source.data_source_metadata.form_name,
                            "records_pulled_bytes": len(raw_data),
                        },
                    )
                    db.execute_queries(config_file, [data_pull.to_sql_query()], show_commands=False)


    log_event(
        config_file=config_file,
        log_level="INFO",
        event="sharepoint_data_pull_complete",
        message="Finished SharePoint data pull process.",
        project_id=sharepoint_data_source.project_id,
        site_id=sharepoint_data_source.site_id,
        data_source_name=sharepoint_data_source.data_source_name,
    )



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pull SharePoint data for all or specific project/site."
    )
    parser.add_argument(
        '--project_id',
        type=str,
        default=None,
        help='Project ID to pull data for (optional)'
    )
    parser.add_argument(
        '--site_id',
        type=str,
        default=None,
        help='Site ID to pull data for (optional)'
    )
    args = parser.parse_args()

    config_file = Path(__file__).resolve().parents[4] / "sample.config.ini"
    logger.info(f"Using config file: {config_file}")
    if not config_file.exists():
        logger.error(f"Config file does not exist: {config_file}")
        sys.exit(1)

    logs.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    logger.info("Starting SharePoint data pull...")

    if not config_file.exists():
        logger.error(f"Config file does not exist: {config_file}")
        Logs(
            log_level="FATAL",
            log_message={
                "event": "sharepoint_data_pull_config_missing",
                "message": f"Config file does not exist: {config_file}",
                "config_file_path": str(config_file),
            },
        ).insert(config_file)
        sys.exit(1)

    pull_all_data(
        config_file=config_file,
        project_id=args.project_id,
        site_id=args.site_id,
    )

    logger.info("Finished SharePoint data pull.") 
