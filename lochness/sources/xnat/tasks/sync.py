#!/usr/bin/env python
"""
XNAT module
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
import json
from typing import Any, Dict, List, Optional, cast

import pandas as pd
import xnat
import requests
from rich.logging import RichHandler

from lochness.helpers import logs, utils, db, config
from lochness.models.keystore import KeyStore
from lochness.models.subjects import Subject
from lochness.sources.xnat.models.data_source import XnatDataSource


MODULE_NAME = "lochness.sources.xnat.tasks.sync"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def insert_xnat_cred(key_name: str, key_value: str, project_id: str):
    config_file = utils.get_config_file_path()

    encryption_passphrase = config.parse(config_file, 'general')[
            'encryption_passphrase']

    my_key = KeyStore(
        key_name=key_name,
        key_value=key_value,
        key_type="xnat",
        project_id=project_id,
        key_metadata={
            "description": "Access token for XNAT",
            "created_by": f"{MODULE_NAME}"}
    )

    insert_query = my_key.to_sql_query(
            encryption_passphrase=encryption_passphrase)

    db.execute_queries(
        config_file=config_file,
        queries=[insert_query],
        show_commands=False,
    )

def get_xnat_cred(xnat_data_source: XnatDataSource) -> Dict[str, str]:
    config_file = utils.get_config_file_path()
    encryption_passphrase = config.parse(config_file, "general")[
        "encryption_passphrase"
    ]

    keystore = KeyStore.get_by_name_and_project(
        config_file,
        xnat_data_source.data_source_metadata.keystore_name,
        xnat_data_source.project_id,
        encryption_passphrase,
    )
    if keystore:
        return json.loads(keystore.key_value)
    else:
        raise ValueError("XNAT credentials not found in keystore")


def get_xnat_projects(xnat_data_source: XnatDataSource) -> List[str]:
    """Get all project IDs from a XNAT data source.

    Args:
        xnat_data_source (XnatDataSource): The XNAT data source to get the projects from.

    Returns:
        List[str]: A list of project IDs.
    """
    with xnat.connect(xnat_data_source.data_source_metadata.endpoint_url, 
                       user=xnat_data_source.data_source_metadata.api_token, 
                       password=xnat_data_source.data_source_metadata.api_token) as connection:
        projects = connection.projects.keys()
    return list(projects)


def get_xnat_subjects(xnat_data_source: XnatDataSource, project_id: str) -> List[str]:
    """Get all subject IDs from a XNAT project.

    Args:
        xnat_data_source (XnatDataSource): The XNAT data source.
        project_id (str): The project ID.

    Returns:
        List[str]: A list of subject IDs.
    """
    with xnat.connect(xnat_data_source.data_source_metadata.endpoint_url, 
                       user=xnat_data_source.data_source_metadata.api_token, 
                       password=xnat_data_source.data_source_metadata.api_token) as connection:
        subjects = connection.projects[project_id].subjects.keys()
    return list(subjects)


def get_xnat_experiments(xnat_data_source: XnatDataSource, project_id: str, subject_id: str) -> List[str]:
    """Get all experiment IDs from a XNAT subject.

    Args:
        xnat_data_source (XnatDataSource): The XNAT data source.
        project_id (str): The project ID.
        subject_id (str): The subject ID.

    Returns:
        List[str]: A list of experiment IDs.
    """
    with xnat.connect(xnat_data_source.data_source_metadata.endpoint_url, 
                       user=xnat_data_source.data_source_metadata.api_token, 
                       password=xnat_data_source.data_source_metadata.api_token) as connection:
        experiments = connection.projects[project_id].subjects[subject_id].experiments.keys()
    return list(experiments)


def download_xnat_experiment(
    xnat_data_source: XnatDataSource,
    project_id: str,
    subject_id: str,
    experiment_id: str,
    download_dir: Path,
) -> Path:
    """Download an experiment from XNAT.

    Args:
        xnat_data_source (XnatDataSource): The XNAT data source.
        project_id (str): The project ID.
        subject_id (str): The subject ID.
        experiment_id (str): The experiment ID.
        download_dir (Path): The directory to download the experiment to.

    Returns:
        Path: The path to the downloaded file.
    """
    with xnat.connect(xnat_data_source.data_source_metadata.endpoint_url, 
                       user=xnat_data_source.data_source_metadata.api_token, 
                       password=xnat_data_source.data_source_metadata.api_token) as connection:
        experiment = connection.projects[project_id].subjects[subject_id].experiments[experiment_id]
        # xnatpy's download method downloads to a specified directory
        # It usually returns the path to the downloaded file/directory
        downloaded_path = experiment.download(download_dir)
    return Path(downloaded_path)


def schedule_xnat_download(
    xnat_data_source: XnatDataSource,
    project_id: str,
    subject_id: str,
    experiment_id: str,
    download_dir: Path,
) -> None:
    """Schedule an XNAT experiment download as a job in the database.

    Args:
        xnat_data_source (XnatDataSource): The XNAT data source.
        project_id (str): The project ID.
        subject_id (str): The subject ID.
        experiment_id (str): The experiment ID.
        download_dir (Path): The directory to download the experiment to.
    """
    config_file = utils.get_config_file_path()
    job_payload = {
        "xnat_data_source": xnat_data_source.dict(),
        "project_id": project_id,
        "subject_id": subject_id,
        "experiment_id": experiment_id,
        "download_dir": str(download_dir),
    }
    job_payload_json = json.dumps(job_payload)

    # Escape single quotes in the JSON string for SQL insertion
    escaped_job_payload_json = job_payload_json.replace("'", "''")

    insert_query = f"""INSERT INTO jobs (job_type, job_payload) VALUES ('xnat_download', '{escaped_job_payload_json}');"""

    db.execute_queries(
        config_file=config_file,
        queries=[insert_query],
        show_commands=False,
    )
    logger.info(f"Scheduled XNAT download for experiment {experiment_id} in project {project_id} subject {subject_id}")

def check_xnat_connection(xnat_data_source: XnatDataSource) -> bool:
    """Check connectivity and authentication to the XNAT server.

    Args:
        xnat_data_source (XnatDataSource): The XNAT data source.

    Returns:
        bool: True if connection is successful, False otherwise.
    """
    try:
        with xnat.connect(xnat_data_source.data_source_metadata.endpoint_url, 
                           user=xnat_data_source.data_source_metadata.api_token, 
                           password=xnat_data_source.data_source_metadata.api_token) as connection:
            # Attempt to list projects to verify connection
            connection.projects.keys()
        logger.info("Successfully connected to XNAT server.")
        return True
    except Exception as e:
        logger.error(f"Failed to connect to XNAT server: {e}")
        return False

def get_xnat_project_metadata(xnat_data_source: XnatDataSource, project_id: str) -> Dict[str, Any]:
    """Get detailed metadata for a specific XNAT project.

    Args:
        xnat_data_source (XnatDataSource): The XNAT data source.
        project_id (str): The project ID.

    Returns:
        Dict[str, Any]: A dictionary containing the project metadata.
    """
    with xnat.connect(xnat_data_source.data_source_metadata.endpoint_url, 
                       user=xnat_data_source.data_source_metadata.api_token, 
                       password=xnat_data_source.data_source_metadata.api_token) as connection:
        project = connection.projects[project_id]
        # xnatpy project objects have attributes like name, description, etc.
        # Convert to a dictionary for consistency with previous implementation
        metadata = {
            "ID": project.id,
            "name": project.name,
            "description": project.description,
            # Add other relevant metadata fields as needed
        }
    return metadata

