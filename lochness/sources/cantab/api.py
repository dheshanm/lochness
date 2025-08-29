"""
Interact with the CANTAB API to retrieve subject information.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from requests.auth import HTTPBasicAuth

from lochness.models.keystore import KeyStore
from lochness.sources.cantab.models.data_source import CANTABDataSource

logger = logging.getLogger(__name__)


def get_cantab_cred(
    cantab_data_source: CANTABDataSource, config_file: Path
) -> Dict[str, str]:
    """
    Get CANTAB credentials from the keystore.

    Args:
        cantab_data_source (CANTABDataSource): The CANTAB data source.
        config_file (Path): Path to the configuration file.

    Returns:
        Dict[str, str]: Dictionary containing 'username' and 'password'.
    """
    project_id = cantab_data_source.project_id
    keystore_name = cantab_data_source.data_source_metadata.keystore_name

    keystore = KeyStore.retrieve_keystore(
        config_file=config_file, key_name=keystore_name, project_id=project_id
    )

    if keystore:
        return json.loads(keystore.key_value)
    else:
        raise ValueError("CANTAB credentials not found in keystore")


def get_cantab_auth(
    cantab_data_source: CANTABDataSource, config_file: Path
) -> HTTPBasicAuth:
    """
    Get authentication headers for CANTAB API.

    Args:
        cantab_data_source (CANTABDataSource): The CANTAB data source.
        config_file (Path): Path to the configuration file.

    Returns:
        HTTPBasicAuth: Auth object
    """
    credentials = get_cantab_cred(
        cantab_data_source=cantab_data_source, config_file=config_file
    )
    username = credentials["username"]
    password = credentials["password"]

    auth = HTTPBasicAuth(username, password)
    return auth


def fetch_cantab_id(
    cantab_data_source: CANTABDataSource, subject_id: str, config_file: Path
) -> Optional[str]:
    """
    Fetch the CANTAB ID for a given subject ID from the CANTAB API.

    Args:
        cantab_data_source (CANTABDataSource): The CANTAB data source.
        subject_id (str): The subject ID to fetch the CANTAB ID for.
        config_file (Path): Path to the configuration file.

    Returns:
        Optional[str]: The CANTAB ID if found, else None.
    """
    cantab_auth = get_cantab_auth(cantab_data_source, config_file)
    api_url = cantab_data_source.data_source_metadata.api_url
    url = f'{api_url}/subject?filter={{"subjectIds":"{subject_id}"}}&limit=100'

    response = requests.get(url, auth=cantab_auth, timeout=30)
    response.raise_for_status()

    response_obj: Dict[str, Any] = response.json()
    records: List[Dict[str, Any]] = response_obj.get("records", [])
    if records:
        cantab_id: Optional[str] = records[0].get("id")
    else:
        cantab_id = None

    return cantab_id


def get_cantab_data(
    cantab_data_source: CANTABDataSource,
    cantab_id: str,
    config_file: Path,
) -> Dict[str, Any]:
    """
    Fetch data for a given CANTAB ID from the CANTAB API.

    Args:
        cantab_data_source (CANTABDataSource): The CANTAB data source.
        cantab_id (str): The CANTAB ID to fetch data for.
        config_file (Path): Path to the configuration file.

    Returns:
        List[Dict[str, Any]]: List of data records.
    """
    cantab_auth = get_cantab_auth(cantab_data_source, config_file)
    api_url = cantab_data_source.data_source_metadata.api_url
    url = f'{api_url}/visit?filter={{"subject":"{cantab_id}"}}&limit=100'

    response = requests.get(url, auth=cantab_auth, timeout=30)
    response.raise_for_status()

    response_obj: Dict[str, Any] = response.json()
    return response_obj
