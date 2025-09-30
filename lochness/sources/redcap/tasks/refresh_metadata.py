#!/usr/bin/env python
"""
Refreshes Study Metadata in the database.

This script is intended to be run as a cron job.
It will refresh the metadata for all active REDCap data sources in the database.
"""

import sys
from pathlib import Path
import argparse

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
from typing import Any, Dict, List, Optional, cast

import pandas as pd
import requests
from rich.logging import RichHandler

from lochness.helpers import logs, utils, db
from lochness.models.subjects import Subject
from lochness.models.keystore import KeyStore
from lochness.models.logs import Logs
from lochness.sources.redcap.models.data_source import RedcapDataSource

MODULE_NAME = "lochness.sources.redcap.tasks.refresh_metadata"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def log_event(
    config_file: Path,
    log_level: str,
    event: str,
    message: str,
    project_id: Optional[str] = None,
    site_id: Optional[str] = None,
    data_source_name: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Standardized logging for REDCap metadata refresh events.

    Args:
        config_file (Path): Path to the config file.
        log_level (str): Log level (e.g., "INFO", "ERROR").
        event (str): Event name.
        message (str): Log message.
        project_id (Optional[str]): Project ID.
        site_id (Optional[str]): Site ID.
        data_source_name (Optional[str]): Data source name.
        extra (Optional[Dict[str, Any]]): Additional key-value pairs
            to include in the log.

    Returns:
        None
    """
    data_source_identifier = (
        f"{project_id}::{site_id}::{data_source_name}"
        if project_id and site_id and data_source_name
        else None
    )

    log_message = {
        "event": event,
        "message": message,
        "project_id": project_id,
        "site_id": site_id,
        "data_source_type": "redcap",
        "module": MODULE_NAME,
    }
    if data_source_identifier:
        log_message["data_source_identifier"] = data_source_identifier
    if extra:
        log_message.update(extra)
    Logs(
        log_level=log_level,
        log_message=log_message,
    ).insert(config_file)


def fetch_metadata(
    redcap_data_source: RedcapDataSource,
    config_file: Path,
    timeout_s: int = 30,
) -> Optional[pd.DataFrame]:
    """
    Refreshes the metadata for a given REDCap data source.

    Args:
        redcap_data_source (RedcapDataSource): The REDCap data source to refresh.
        encryption_passphrase (str): The encryption passphrase for keystore access.
        timeout_s (int): Timeout for the API request.

    Returns:
        Optional[pd.DataFrame]: A DataFrame containing the metadata for the REDCap data source.
    """
    project_id = redcap_data_source.project_id
    site_id = redcap_data_source.site_id
    data_source_name = redcap_data_source.data_source_name

    subject_id_variable: Optional[str] = redcap_data_source.data_source_metadata.subject_id_variable
    redcap_endpoint_url: str = redcap_data_source.data_source_metadata.endpoint_url

    identifier = f"{project_id}::{site_id}::{data_source_name}"
    logger.info(f"Refreshing metadata for {identifier}...")

    # Get API token from keystore
    keystore = KeyStore.retrieve_keystore(
        redcap_data_source.data_source_metadata.keystore_name,
        project_id,
        config_file,
    )
    if keystore is None:
        logger.error(f"Keystore entry not found for {identifier}")

        log_event(
            config_file=config_file,
            log_level="ERROR",
            event="redcap_metadata_fetch_keystore_missing",
            message=f"Keystore entry not found for {identifier}.",
            project_id=project_id,
            site_id=site_id,
            data_source_name=data_source_name,
        )
        return None

    api_token = keystore.key_value

    optional_variables_dictionary = (
        redcap_data_source.data_source_metadata.optional_variables_dictionary
    )

    data = {
        "token": api_token,
        "content": "record",
        "action": "export",
        "format": "json",
        "type": "flat",
        "csvDelimiter": "",
        "returnFormat": "json",
    }

    required_variables = [subject_id_variable]

    rename_columns: Dict[str, str] = {
        subject_id_variable: "subject_id",  # type: ignore
    }
    for variable in optional_variables_dictionary:
        variable_name = variable["variable_name"]
        internal_name = variable.get("internal_name", variable_name)
        rename_columns[variable_name] = internal_name
        if variable_name not in required_variables:
            required_variables.append(variable_name)

    for i, variable in enumerate(required_variables):
        data[f"fields[{i}]"] = variable  # type: ignore

    r = requests.post(redcap_endpoint_url, data=data, timeout=timeout_s)
    if r.status_code != 200:
        logger.error(
            f"Failed to fetch metadata for {identifier}: {r.status_code} - {r.text}"
        )
        return None

    raw_data = r.json()

    results: List[Dict[str, str]] = []
    for record in raw_data:
        result: Dict[str, str] = {}
        for variable in required_variables:
            result[variable] = record[variable]  # type: ignore
        results.append(result)  # type: ignore

    df = pd.DataFrame(results)
    df = df.rename(columns=rename_columns)

    return df


def filter_metadata(
    df: pd.DataFrame,
    site_id: str,
) -> pd.DataFrame:
    """
    Filters the metadata DataFrame to include only records for the specified site ID.

    Args:
        df (pd.DataFrame): The metadata DataFrame to filter.
        site_id (str): The site ID to filter by.

    Returns:
        pd.DataFrame: The filtered metadata DataFrame.
    """

    # Get only rows with subject_id that starts with the site_id
    filtered_df = df[df["subject_id"].astype(str).str.startswith(site_id)]
    filtered_df = filtered_df.reset_index(drop=True)
    return filtered_df


def insert_metadata(
    df: pd.DataFrame,
    project_id: str,
    site_id: str,
    config_file: Path,
) -> None:
    """
    Inserts the metadata DataFrame into the database.

    Args:
        df (pd.DataFrame): The metadata DataFrame to insert.
        project_id (str): The project ID.
        site_id (str): The site ID.
        config_file (Path): Path to the config file.
    """
    subjects: List[Subject] = []

    for _, row in df.iterrows():  # type: ignore
        subject_id: str = cast(str, row["subject_id"])

        # Check if subject_id already exists in subjects list
        if any(sub.subject_id == subject_id for sub in subjects):
            continue

        # Use all other columns as metadata
        subject_metadata: Dict[str, Any] = cast(
            Dict[str, Any], dict(row.drop("subject_id").items())  # type: ignore
        )

        subject = Subject(
            subject_id=subject_id,
            site_id=site_id,
            project_id=project_id,
            subject_metadata=subject_metadata,
        )
        subjects.append(subject)

    insert_queries: List[str] = []
    for subject in subjects:
        sql_query = subject.to_sql_query()
        insert_queries.append(sql_query)

    db.execute_queries(  # type: ignore
        config_file=config_file,
        queries=insert_queries,
        show_commands=False,
    )

    duplicates_skipped = len(df) - len(subjects)
    if duplicates_skipped > 0:
        logger.info(f"Skipped {duplicates_skipped} duplicate subjects.")

    logger.info(
        "Inserted / Updated metadata for "
        f"{len(subjects)} subjects in {project_id}::{site_id}"
    )


def refresh_all_metadata(
    config_file: Path,
    project_id: Optional[str] = None,
    site_id: Optional[str] = None
) -> None:
    """
    Refreshes the metadata for all active REDCap data sources in the database.

    Args:
        config_file (Path): Path to the config file.
        project_id (Optional[str]): If provided, only refresh this project ID.
        site_id (Optional[str]): If provided, only refresh this site ID.

    Returns:
        None
    """
    log_event(
        config_file=config_file,
        log_level="INFO",
        event="redcap_metadata_refresh_start",
        message="Starting REDCap metadata refresh process.",
        project_id=project_id,
        site_id=site_id,
    )

    active_redcap_data_sources = RedcapDataSource.get_all_redcap_data_sources(
        config_file=config_file,
        active_only=True,
    )

    # make sure they have subject_id_variable in metadata
    active_redcap_data_sources = [
        ds
        for ds in active_redcap_data_sources
        if ds.data_source_metadata.main_redcap
    ]

    # Filter by project_id and/or site_id if provided
    if project_id:
        active_redcap_data_sources = [
            ds for ds in active_redcap_data_sources if ds.project_id == project_id
        ]
    if site_id:
        active_redcap_data_sources = [
            ds for ds in active_redcap_data_sources if ds.site_id == site_id
        ]

    if not active_redcap_data_sources:
        logger.info("No active REDCap data sources found.")
        log_event(
            config_file=config_file,
            log_level="INFO",
            event="redcap_metadata_refresh_no_active_sources",
            message="No active REDCap data sources found for refresh.",
            project_id=project_id,
            site_id=site_id,
        )
        return

    logger.info(f"Found {len(active_redcap_data_sources)} active REDCap data sources.")
    log_event(
        config_file=config_file,
        log_level="INFO",
        event="redcap_metadata_refresh_active_sources_found",
        message=f"Found {len(active_redcap_data_sources)} active REDCap data sources.",
        project_id=project_id,
        site_id=site_id,
        extra={"count": len(active_redcap_data_sources)}
    )

    for redcap_data_source in active_redcap_data_sources:
        log_event(
            config_file=config_file,
            log_level="INFO",
            event="redcap_metadata_fetch_start",
            message=f"Starting metadata fetch for {redcap_data_source.data_source_name}.",
            project_id=redcap_data_source.project_id,
            site_id=redcap_data_source.site_id,
            data_source_name=redcap_data_source.data_source_name,
        )

        source_metadata = fetch_metadata(
            redcap_data_source=redcap_data_source,
            config_file=config_file,
        )
        if source_metadata is None:
            log_event(
                config_file=config_file,
                log_level="ERROR",
                event="redcap_metadata_fetch_failed",
                message=(
                    "Failed to fetch metadata for "
                    f"{redcap_data_source.data_source_name}."
                ),
                project_id=redcap_data_source.project_id,
                site_id=redcap_data_source.site_id,
                data_source_name=redcap_data_source.data_source_name,
            )
            continue

        logger.info(
            f"Fetched metadata for {redcap_data_source.data_source_name} "
            f"({len(source_metadata)} records)"
        )
        log_event(
            config_file=config_file,
            log_level="INFO",
            event="redcap_metadata_fetch_success",
            message=f"Fetched metadata for {redcap_data_source.data_source_name}.",
            project_id=redcap_data_source.project_id,
            site_id=redcap_data_source.site_id,
            data_source_name=redcap_data_source.data_source_name,
            extra={"records_fetched": len(source_metadata)}
        )

        filtered_metadata = filter_metadata(
            df=source_metadata,
            site_id=redcap_data_source.site_id,
        )

        logger.info(
            f"Filtered metadata for {redcap_data_source.data_source_name} "
            f"({len(filtered_metadata)} records)"
        )
        log_event(
            config_file=config_file,
            log_level="INFO",
            event="redcap_metadata_filter_success",
            message=f"Filtered metadata for {redcap_data_source.data_source_name}.",
            project_id=redcap_data_source.project_id,
            site_id=redcap_data_source.site_id,
            data_source_name=redcap_data_source.data_source_name,
            extra={"records_filtered": len(filtered_metadata)}
        )

        insert_metadata(
            df=filtered_metadata,
            project_id=redcap_data_source.project_id,
            site_id=redcap_data_source.site_id,
            config_file=config_file,
        )
        log_event(
            config_file=config_file,
            log_level="INFO",
            event="redcap_metadata_insert_success",
            message=f"Inserted/updated metadata for {len(filtered_metadata)} subjects.",
            project_id=redcap_data_source.project_id,
            site_id=redcap_data_source.site_id,
            data_source_name=redcap_data_source.data_source_name,
            extra={"subjects_processed": len(filtered_metadata)}
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Refresh REDCap metadata for all or specific project/site."
    )
    parser.add_argument(
        "--project_id", type=str, default=None, help="Project ID to refresh (optional)"
    )
    parser.add_argument(
        "--site_id", type=str, default=None, help="Site ID to refresh (optional)"
    )
    args = parser.parse_args()

    config_file = utils.get_config_file_path()
    logger.info(f"Using config file: {config_file}")

    if not config_file.exists():
        logger.error(f"Config file does not exist: {config_file}")
        sys.exit(1)

    logs.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    logger.info("Refreshing REDCap metadata...")
    refresh_all_metadata(
        config_file=config_file, project_id=args.project_id, site_id=args.site_id
    )

    logger.info("Finished refreshing REDCap metadata.")
    log_event(
        config_file=config_file,
        log_level="INFO",
        event="redcap_metadata_refresh_complete",
        message="Finished REDCap metadata refresh process.",
        project_id=args.project_id,
        site_id=args.site_id,
    )
