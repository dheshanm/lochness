#!/usr/bin/env python
"""
Refreshes Study Metadata in the database.

This script is intended to be run as a cron job.
It will refresh the metadata for all active REDCap data sources in the database.
"""

import sys
from pathlib import Path

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

from lochness.helpers import logs, utils, db
from lochness.models.subjects import Subject
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
    redcap_data_source: RedcapDataSource, timeout_s: int = 30
) -> Optional[pd.DataFrame]:
    """
    Refreshes the metadata for a given REDCap data source.

    Args:
        redcap_data_source (RedcapDataSource): The REDCap data source to refresh.
        config_file (Path): Path to the config file.

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

    optional_variables_dictionary = (
        redcap_data_source.data_source_metadata.optional_variables_dictionary
    )

    data = {
        "token": redcap_data_source.data_source_metadata.api_token,
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


def refresh_all_metadata(config_file: Path):
    active_redcap_data_sources = RedcapDataSource.get_all_redcap_data_sources(
        config_file=config_file, active_only=True
    )

    if not active_redcap_data_sources:
        logger.info("No active REDCap data sources found.")
        return

    logger.info(f"Found {len(active_redcap_data_sources)} active REDCap data sources.")

    for redcap_data_source in active_redcap_data_sources:
        source_metadata = fetch_metadata(
            redcap_data_source=redcap_data_source,
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
    config_file = utils.get_config_file_path()
    logs.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    logger.info("Refreshing REDCap metadata...")

    if not config_file.exists():
        logger.error(f"Config file does not exist: {config_file}")
        sys.exit(1)

    refresh_all_metadata(config_file=config_file)

    logger.info("Finished refreshing REDCap metadata.")
