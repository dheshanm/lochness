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
    if parent.name == "lochness-v2":
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

from lochness.helpers import logs, utils, db, config
from lochness.models.subjects import Subject
from lochness.models.keystore import KeyStore
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


def fetch_metadata(
    redcap_data_source: RedcapDataSource, 
    encryption_passphrase: str,
    timeout_s: int = 30
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

    subject_id_variable = redcap_data_source.data_source_metadata.subject_id_variable
    redcap_endpoint_url = redcap_data_source.data_source_metadata.endpoint_url

    identifier = f"{project_id}::{site_id}::{data_source_name}"
    logger.info(f"Refreshing metadata for {identifier}...")

    # Get API token from keystore
    config_file = utils.get_config_file_path()
    query = KeyStore.retrieve_key_query(
        redcap_data_source.data_source_metadata.keystore_name, 
        project_id, 
        encryption_passphrase
    )
    api_token_df = db.execute_sql(config_file, query)
    api_token = api_token_df['key_value'][0]

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

    for variable in optional_variables_dictionary:
        variable_name = variable["variable_name"]
        required_variables.append(variable_name)

    for i, variable in enumerate(required_variables):
        data[f"fields[{i}]"] = variable

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
            result[variable] = record[variable]
        results.append(result)  # type: ignore

    df = pd.DataFrame(results)
    df = df.rename(columns={subject_id_variable: "subject_id"})

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

    logger.info(
        "Inserted / Updated metadata for "
        f"{len(subjects)} subjects in {project_id}::{site_id}"
    )


def refresh_all_metadata(config_file: Path, project_id: str = None, site_id: str = None):
    # Get encryption passphrase from config
    encryption_passphrase = config.parse(config_file, 'general')['encryption_passphrase']
    
    active_redcap_data_sources = RedcapDataSource.get_all_redcap_data_sources(
        config_file=config_file, 
        encryption_passphrase=encryption_passphrase,
        active_only=True
    )

    # Filter by project_id and/or site_id if provided
    if project_id:
        active_redcap_data_sources = [ds for ds in active_redcap_data_sources if ds.project_id == project_id]
    if site_id:
        active_redcap_data_sources = [ds for ds in active_redcap_data_sources if ds.site_id == site_id]

    if not active_redcap_data_sources:
        logger.info("No active REDCap data sources found.")
        return

    logger.info(f"Found {len(active_redcap_data_sources)} active REDCap data sources.")

    for redcap_data_source in active_redcap_data_sources:
        source_metadata = fetch_metadata(
            redcap_data_source=redcap_data_source,
            encryption_passphrase=encryption_passphrase,
        )
        if source_metadata is None:
            continue

        logger.info(
            f"Fetched metadata for {redcap_data_source.data_source_name} "
            f"({len(source_metadata)} records)"
        )

        filtered_metadata = filter_metadata(
            df=source_metadata,
            site_id=redcap_data_source.site_id,
        )

        logger.info(
            f"Filtered metadata for {redcap_data_source.data_source_name} "
            f"({len(filtered_metadata)} records)"
        )

        insert_metadata(
            df=filtered_metadata,
            project_id=redcap_data_source.project_id,
            site_id=redcap_data_source.site_id,
            config_file=config_file,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Refresh REDCap metadata for all or specific project/site.")
    parser.add_argument('--project_id', type=str, default=None, help='Project ID to refresh (optional)')
    parser.add_argument('--site_id', type=str, default=None, help='Site ID to refresh (optional)')
    args = parser.parse_args()

    config_file = utils.get_config_file_path()
    logs.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    logger.info("Refreshing REDCap metadata...")

    if not config_file.exists():
        logger.error(f"Config file does not exist: {config_file}")
        sys.exit(1)

    refresh_all_metadata(config_file=config_file, project_id=args.project_id, site_id=args.site_id)

    logger.info("Finished refreshing REDCap metadata.")
