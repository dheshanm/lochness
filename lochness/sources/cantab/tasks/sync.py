import sys
from pathlib import Path
import base64
import json
import logging
from typing import Any, Dict, List, Optional

import requests
from rich.logging import RichHandler

# Add project root to Python path
file = Path(__file__).resolve()
parent = file.parent
root_dir = None
for p in file.parents:
    if p.name == "lochness_v2":
        root_dir = p

sys.path.append(str(root_dir))

from lochness.helpers import utils, db, config
from lochness.models.keystore import KeyStore
from lochness.sources.cantab.models.data_source import CANTABDataSource

MODULE_NAME = "lochness.sources.cantab.tasks.sync"

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

def get_cantab_cred(
    cantab_data_source: CANTABDataSource,
) -> Dict[str, str]:
    """Get CANTAB credentials from the keystore."""
    config_file = utils.get_config_file_path()
    encryption_passphrase = config.parse(config_file, "general")[
        "encryption_passphrase"
    ]

    keystore = KeyStore.get_by_name_and_project(
        config_file,
        cantab_data_source.data_source_metadata.keystore_name,
        cantab_data_source.project_id,
        encryption_passphrase,
    )
    if keystore:
        return json.loads(keystore.key_value)
    else:
        raise ValueError("CANTAB credentials not found in keystore")

def get_cantab_auth_headers(cantab_data_source: CANTABDataSource) -> Dict[str, str]:
    """Get authentication headers for CANTAB API."""
    credentials = get_cantab_cred(cantab_data_source)
    username = credentials["username"]
    password = credentials["password"]

    auth_string = f"{username}:{password}"
    encoded_auth_string = base64.b64encode(auth_string.encode("utf-8")).decode("utf-8")

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Basic {encoded_auth_string}"
    }
    return headers

def get_cantab_subjects(cantab_data_source: CANTABDataSource) -> List[Dict[str, Any]]:
    """Get all subjects from CANTAB API."""
    headers = get_cantab_auth_headers(cantab_data_source)
    api_endpoint = cantab_data_source.data_source_metadata.api_endpoint
    url = f"{api_endpoint}/subject?limit=100" # Use limit for pagination

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json().get("records", [])

def get_cantab_visits(cantab_data_source: CANTABDataSource, subject_id: str) -> List[Dict[str, Any]]:
    """Get all visits for a subject from CANTAB API."""
    headers = get_cantab_auth_headers(cantab_data_source)
    api_endpoint = cantab_data_source.data_source_metadata.api_endpoint
    filter_param = json.dumps({"subject": subject_id})
    url = f"{api_endpoint}/visit?filter={filter_param}&limit=100"

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json().get("records", [])

def sync_cantab_metadata(cantab_data_source: CANTABDataSource):
    """Syncs CANTAB metadata (subjects, visits) to Lochness database.

    Args:
        cantab_data_source (CANTABDataSource): The CANTAB data source to sync.
    """
    logger.info(f"Syncing CANTAB metadata for data source: {cantab_data_source.data_source_name}")

    # 1. Get subjects
    subjects = get_cantab_subjects(cantab_data_source)
    logger.info(f"Found {len(subjects)} subjects in CANTAB.")

    for subject_data in subjects:
        subject_id = subject_data.get("id")
        if not subject_id:
            continue

        # Here you would typically insert/update subject info into Lochness subjects table
        # For now, just log it
        logger.info(f"Processing subject: {subject_id}")

        # 2. Get visits for each subject
        visits = get_cantab_visits(cantab_data_source, subject_id)
        logger.info(f"  Found {len(visits)} visits for subject {subject_id}.")

        for visit_data in visits:
            visit_id = visit_data.get("id")
            if not visit_id:
                continue

            # Here you would typically insert/update visit info into Lochness data_pull table
            # or a dedicated CANTAB visit metadata table.
            # For now, just log it.
            logger.info(f"    Processing visit: {visit_id}")

            # Example: Extracting task variant info (as per your previous query)
            # This would involve further API calls to /studyDef and /testMode
            # based on the IDs found in visit_data.itemGroups

    logger.info("CANTAB metadata sync complete.")
