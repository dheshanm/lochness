"""
SharePoint module
"""

import sys
from pathlib import Path

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

import logging
import json
from typing import Any, Dict, List, Optional

import requests
import msal
from rich.logging import RichHandler

from lochness.helpers import utils, db, config
from lochness.models.keystore import KeyStore
from lochness.sources.sharepoint.models.data_source import SharepointDataSource


MODULE_NAME = "lochness.sources.sharepoint.tasks.sync"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def get_sharepoint_cred(
    sharepoint_data_source: SharepointDataSource,
) -> Dict[str, str]:
    """Get SharePoint credentials from the keystore."""
    config_file = utils.get_config_file_path()
    encryption_passphrase = config.parse(config_file, "general")[
        "encryption_passphrase"
    ]

    keystore = KeyStore.get_by_name_and_project(
        config_file,
        sharepoint_data_source.data_source_metadata.keystore_name,
        sharepoint_data_source.project_id,
        encryption_passphrase,
    )
    if keystore:
        return json.loads(keystore.key_value)
    else:
        raise ValueError("SharePoint credentials not found in keystore")

def get_access_token(sharepoint_data_source: SharepointDataSource) -> str:
    """Get access token for SharePoint."""
    credentials = get_sharepoint_cred(sharepoint_data_source)
    authority = f"https://login.microsoftonline.com/{credentials['tenant_id']}"
    app = msal.ConfidentialClientApplication(
        credentials["client_id"],
        authority=authority,
        client_credential=credentials["client_secret"],
    )

    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])

    if "access_token" in result:
        return result["access_token"]
    else:
        raise Exception(result.get("error_description"))

def get_form_responses(sharepoint_data_source: SharepointDataSource, access_token: str) -> List[Dict[str, Any]]:
    """Get all form responses from a SharePoint list."""
    site_url = sharepoint_data_source.data_source_metadata.site_url
    form_id = sharepoint_data_source.data_source_metadata.form_id
    url = f"https://graph.microsoft.com/v1.0/sites/{site_url}/lists/{form_id}/items?expand=fields"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()["value"]


def download_file(file_url: str, access_token: str, download_dir: Path) -> Path:
    """Download a file from SharePoint."""
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(file_url, headers=headers, stream=True)
    response.raise_for_status()

    download_dir.mkdir(parents=True, exist_ok=True)
    file_path = download_dir / Path(file_url).name

    with open(file_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    return file_path


def schedule_sharepoint_download(
    sharepoint_data_source: SharepointDataSource,
    file_url: str,
    download_dir: Path,
) -> None:
    """Schedule a SharePoint file download as a job in the database."""
    config_file = utils.get_config_file_path()
    job_payload = {
        "sharepoint_data_source": sharepoint_data_source.model_dump(),
        "file_url": file_url,
        "download_dir": str(download_dir),
    }
    job_payload_json = json.dumps(job_payload)

    escaped_job_payload_json = job_payload_json.replace("'", "''")

    insert_query = f"""INSERT INTO jobs (job_type, job_payload) VALUES ('sharepoint_download', '{escaped_job_payload_json}');"""

    db.execute_queries(
        config_file=config_file,
        queries=[insert_query],
        show_commands=False,
    )
    logger.info(f"Scheduled SharePoint download for file {file_url}")
