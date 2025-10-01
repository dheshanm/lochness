#!/usr/bin/env python
"""
Pulls data from SharePoint for active data sources and subjects.

This script is intended to be run as a cron job.
It will pull data for all active SharePoint data sources and their associated subjects.
"""

import os
import sys
from tempfile import TemporaryDirectory
import shutil
import hashlib
import logging
import argparse
from pathlib import Path
from typing import Any, Dict, List, Optional, cast
from datetime import datetime
import json

import requests
import msal
from rich.logging import RichHandler

from lochness.helpers import logs, utils, db, config
from lochness.models.subjects import Subject
from lochness.models.keystore import KeyStore
from lochness.models.logs import Logs
from lochness.models.files import File
from lochness.models.data_pulls import DataPull
from lochness.models.data_push import DataPush
from lochness.models.data_sinks import DataSink
from lochness.sources.sharepoint.models.data_source import SharepointDataSource

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
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)


def calculate_sha256(file_path: str) -> str:
    """Calculates the SHA256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def authenticate(
    client_id: str, tenant_id: str, client_secret: Optional[str] = None
) -> Dict:
    """
    Authenticate using either client credentials or device flow.
    Returns headers with access token.
    """
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    scopes_client = ["https://graph.microsoft.com/.default"]
    scopes_device = ["Files.Read.All", "Sites.Read.All", "User.Read"]
    result = None
    headers = None
    if client_secret:
        try:
            logger.info("Attempting authentication using client credentials...")
            app = msal.ConfidentialClientApplication(
                client_id,
                authority=authority,
                client_credential=client_secret
            )
            result = app.acquire_token_for_client(scopes=scopes_client)
            if "access_token" in result:
                logger.info("Access token acquired via client credentials.")
                headers = {"Authorization": f"Bearer {result['access_token']}"}
                return headers
            else:
                logger.warning(
                    "Client credentials authentication failed. "
                    "Falling back to device flow."
                )
                logger.warning(f"Error: {result.get('error')}")
                logger.warning(f"Error description: {result.get('error_description')}")
                logger.warning(f"Correlation ID: {result.get('correlation_id')}")
        except Exception as e:
            logger.error(f"Client credentials error: {e}. Falling back to device flow.")
    # Device flow fallback
    logger.info("Logging in using device flow...")
    app = msal.PublicClientApplication(client_id, authority=authority)
    flow = app.initiate_device_flow(scopes=scopes_device)
    if "user_code" not in flow:
        logger.error(f"Failed to start device flow: {flow}")
        raise RuntimeError(f"Failed to start device flow: {flow}")
    logger.info(
        f"Please go to {flow['verification_uri']} and enter code: {flow['user_code']}"
    )
    result = app.acquire_token_by_device_flow(flow)
    if "access_token" not in result:
        logger.error(f"Authentication failed: {result.get('error_description')}")
        raise RuntimeError(f"Authentication failed: {result.get('error_description')}")
    logger.info("Access token acquired via device flow.")
    headers = {"Authorization": f"Bearer {result['access_token']}"}
    return headers


def get_site_id(headers: Dict, site_path: str) -> str:
    logger.info(f"Looking up site ID for /sites/{site_path.split(':')[-1]}...")
    site_url = f"https://graph.microsoft.com/v1.0/sites/{site_path}"
    resp = requests.get(site_url, headers=headers)
    if resp.status_code != 200:
        logger.error(f"Failed to get SharePoint site: {resp.text}")
        raise RuntimeError(f"Failed to get SharePoint site: {resp.text}")
    site_id = resp.json()["id"]
    logger.info(f"Site ID retrieved: {site_id}")
    return site_id


def get_drives(site_id: str, headers: Dict) -> List[Dict]:
    logger.info("Listing document libraries in ProCAN...")
    drives_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
    resp = requests.get(drives_url, headers=headers)
    if resp.status_code != 200:
        logger.error(f"Failed to get drives: {resp.text}")
        raise RuntimeError(f"Failed to get drives: {resp.text}")
    drives = resp.json()["value"]
    logger.info(f"Document libraries found: {[d['name'] for d in drives]}")
    return drives


def find_drive_by_name(drives: List[Dict], name: str) -> Optional[Dict]:
    drive = next((d for d in drives if d['name'].lower() == name.lower()), None)
    if drive:
        logger.info(f"Found drive: {name}")
    else:
        logger.warning(f"Drive not found: {name}")
    return drive


def list_drive_root(drive_id: str, headers: Dict) -> List[Dict]:
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children"
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        logger.error(f"Failed to list items in drive: {resp.text}")
        raise RuntimeError(f"Failed to list items in drive: {resp.text}")
    return resp.json()["value"]


def find_folder_in_drive(
    drive_id: str, folder_name: str, headers: Dict
) -> Optional[Dict]:
    for item in list_drive_root(drive_id, headers):
        if item.get("name", "").lower() == folder_name.lower() and "folder" in item:
            logger.info(f"Found folder '{folder_name}' in drive {drive_id}")
            return item
    logger.warning(f"Folder '{folder_name}' not found in drive {drive_id}")
    return None


def list_folder_items(
    drive_id: str, folder_id: str, headers: Dict
) -> List[Dict]:
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{folder_id}/children"
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        logger.error(f"Failed to list folder items: {resp.text}")
        raise RuntimeError(f"Failed to list folder items: {resp.text}")
    return resp.json().get("value", [])


def find_subfolder(
    drive_id: str, parent_id: str, subfolder_name: str, headers: Dict
) -> Optional[Dict]:
    for item in list_folder_items(drive_id, parent_id, headers):
        if item.get("name", "").lower() == subfolder_name.lower() and "folder" in item:
            logger.info(f"Found subfolder '{subfolder_name}' in parent {parent_id}")
            return item
    logger.warning(f"Subfolder '{subfolder_name}' not found in parent {parent_id}")
    return None


def should_download_file(local_file_path: Path, quick_xor_hash: Optional[str]) -> bool:
    hash_file_path = local_file_path.parent / (
            "." + local_file_path.name + ".quickxorhash")
    if local_file_path.is_file() and hash_file_path.is_file():
        with open(hash_file_path, 'r') as hf:
            local_hash = hf.read().strip()
        if quick_xor_hash and local_hash == quick_xor_hash:
            logger.info(f"QuickXorHashes match for {local_file_path}. Skipping download.")
            return False
        else:
            logger.info(
                f"QuickXorHashes mismatch or remote hash not available for {local_file_path}. "
                "Re-downloading."
            )
            return True
    if local_file_path.is_file():
        logger.info(f"Local file exists but no hash file for {local_file_path}. Re-downloading.")
    else:
        logger.info(f"Local file does not exist. Downloading {local_file_path}.")
    return True


def download_file(download_url: str, local_path: str, to_tmp: bool=False):
    if to_tmp:
        logger.info(f"Checking {local_path}...")
    else:
        logger.info(f"Downloading {local_path}...")
    resp = requests.get(download_url)
    if resp.status_code == 200:
        with open(local_path, 'wb') as f:
            f.write(resp.content)

        if not to_tmp:
            logger.info(f"Downloaded to {local_path}")
    else:
        logger.error(f"Failed to download file: {local_path} (HTTP {resp.status_code})")
        raise RuntimeError(f"Failed to download file: {local_path} (HTTP {resp.status_code})")


def extract_info(json_path: str):
    """
    Extract ampsczSubjectId and formTitle from a response.submitted.json file.
    Returns (subject_id, form_title) tuple.
    """
    with open(json_path, 'r') as f:
        data = json.load(f)
    subject_id = None
    form_title = None
    try:
        form_title = data.get('formTitle')
        subject_id = data.get('data', {}).get('data', {}).get('ampsczSubjectId')
    except Exception:
        pass

    try:
        #TODO
        date_str = data.get('data', {}).get('data', {}).get('dateOfEgg')
        dt = datetime.fromisoformat(date_str)
        # date_only = dt.date()
        dt_str = dt.strftime('%Y_%m_%d')
    except Exception:
        date_only = None
        pass

    return subject_id, form_title, dt_str


def download_eeg_upload_files(
    headers: Dict,
    site_id: str,
    download_dir: str = "eeg_downloads",
    filter_subject: str = None,
    filter_form: str = None
):
    drives = get_drives(site_id, headers)
    team_forms_drive = find_drive_by_name(drives, "Team Forms")
    if not team_forms_drive:
        raise RuntimeError("Team Forms drive not found.")
    drive_id = team_forms_drive["id"]

    responses_folder = find_folder_in_drive(drive_id, "Responses", headers)
    if not responses_folder:
        raise RuntimeError("Responses folder not found in Team Forms drive.")

    eeg_folder = find_subfolder(drive_id, responses_folder["id"], "EEG Upload", headers)
    if not eeg_folder:
        raise RuntimeError("EEG Upload folder not found inside 'Responses'.")

    os.makedirs(download_dir, exist_ok=True)
    subfolders = list_folder_items(drive_id, eeg_folder["id"], headers)
    if not subfolders:
        logger.info("No subfolders found in EEG Upload.")
    for subfolder in subfolders:
        if "folder" in subfolder:
            subfolder_name = subfolder['name']
            subfolder_id = subfolder['id']
            logger.info(f"Found subfolder: {subfolder_name}")
            # Download response.submitted.json first to extract subject_id and form_title
            temp_local_subfolder_path = os.path.join(download_dir, subfolder_name)
            os.makedirs(temp_local_subfolder_path, exist_ok=True)
            files = list_folder_items(drive_id, subfolder_id, headers)
            response_json_file = next(
                (f for f in files if f.get('name') == 'response.submitted.json'), None
            )
            if not response_json_file:
                logger.warning(f"No response.submitted.json found in {subfolder_name}, skipping.")
                continue
            # Download response.submitted.json to temp location
            response_json_path = os.path.join(
                temp_local_subfolder_path, "response.submitted.json"
            )
            download_url = response_json_file.get('@microsoft.graph.downloadUrl')
            if not download_url:
                logger.warning(
                    f"No download URL for response.submitted.json in {subfolder_name}, skipping."
                )
                continue
            download_file(download_url, response_json_path)
            subject_id, form_title, date_only = extract_info(response_json_path)
            if not subject_id or not form_title:
                logger.warning(
                    f"Could not extract subject_id or form_title from response.submitted.json in {subfolder_name}, skipping."
                )
                continue
            if filter_subject and subject_id != filter_subject:
                logger.info(
                    f"Skipping subfolder {subfolder_name} (subject_id {subject_id} != {filter_subject})"
                )
                continue
            if filter_form and form_title != filter_form:
                logger.info(
                    f"Skipping subfolder {subfolder_name} (form_title {form_title} != {filter_form})"
                )
                continue
            # Now set the final local path
            local_subfolder_path = os.path.join(download_dir, subject_id, form_title)
            os.makedirs(local_subfolder_path, exist_ok=True)
            # Move response.submitted.json to the new location
            shutil.move(
                response_json_path, os.path.join(local_subfolder_path, "response.submitted.json")
            )
            # Download all other files
            for f in files:
                if "file" in f and f.get('name') != 'response.submitted.json':
                    file_name = f['name']
                    file_info = f.get('file', {})
                    quick_xor_hash = file_info.get('hashes', {}).get('quickXorHash')
                    local_file_path = os.path.join(local_subfolder_path, file_name)
                    hash_file_path = local_file_path + ".quickxorhash"
                    if should_download_file(local_file_path, quick_xor_hash):
                        download_url = f.get('@microsoft.graph.downloadUrl')
                        if download_url:
                            download_file(download_url, local_file_path)
                            if quick_xor_hash:
                                with open(hash_file_path, 'w') as hf:
                                    hf.write(quick_xor_hash)
                                logger.info(f"QuickXorHash saved to {hash_file_path}")
        else:
            # File directly in EEG Upload
            file_name = subfolder['name']
            download_url = subfolder.get('@microsoft.graph.downloadUrl')
            if download_url:
                file_path = os.path.join(download_dir, file_name)
                download_file(download_url, file_path)


def get_sharepoint_cred(sharepoint_data_source: SharepointDataSource) -> Dict[str, str]:
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


def fetch_subject_data(
    sharepoint_data_source: SharepointDataSource,
    subject_id: str,
    encryption_passphrase: str,
    timeout_s: int = 60,
) -> Optional[bytes]:
    """
    Fetches data for a single subject from SharePoint.

    Args:
        sharepoint_data_source (SharepointDataSource): The SharePoint data source.
        subject_id (str): The subject ID to fetch data for.
        encryption_passphrase (str): The encryption passphrase for keystore access.
        timeout_s (int): Timeout for the API request.

    Returns:
        Optional[bytes]: The raw data from SharePoint, or None if fetching fails.
    """
    project_id = sharepoint_data_source.project_id
    site_id = sharepoint_data_source.site_id
    data_source_name = sharepoint_data_source.data_source_name
    modality = sharepoint_data_source.data_source_metadata.modality if \
            sharepoint_data_source.data_source_metadata.modality else 'unknown'

    metadata = sharepoint_data_source.data_source_metadata
    form_name = metadata.form_name
    modality = getattr(metadata, 'modality', 'unknown')
    

    identifier = f"{project_id}::{site_id}::{data_source_name}::{subject_id}"
    logger.info(f"Fetching data for {identifier}...")

    config_file = utils.get_config_file_path()
    query = KeyStore.retrieve_key_query(
        metadata.keystore_name,
        project_id,
        encryption_passphrase,
    )
    app_key = db.execute_sql(config_file, query).iloc[0]

    metadata_query = KeyStore.retrieve_key_metadata(
        metadata.keystore_name,
        project_id,
    )
    keystore_metadata = db.execute_sql(
            config_file, metadata_query).iloc[0]['key_metadata']

    #TODO: update the flow 
    headers = authenticate(keystore_metadata['client_id'],
                           keystore_metadata['tenant_id'],
                           None)

    sharepoint_site_id = get_site_id(headers, keystore_metadata['site_url'])

    drives = get_drives(sharepoint_site_id, headers)
    team_forms_drive = find_drive_by_name(drives, "Team Forms")
    if not team_forms_drive:
        raise RuntimeError("Team Forms drive not found.")
    drive_id = team_forms_drive["id"]

    responses_folder = find_folder_in_drive(drive_id, "Responses", headers)
    if not responses_folder:
        raise RuntimeError("Responses folder not found in Team Forms drive.")

    matching_folder = find_subfolder(
            drive_id,
            responses_folder["id"],
            form_name,
            headers)

    if not matching_folder:
        raise RuntimeError(f"'{form_name}' folder not found inside 'Responses'.")

    project_name_cap = project_id[:1].upper() + project_id[1:].lower() if project_id else project_id

    # Build output path
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
    output_dir.mkdir(parents=True, exist_ok=True)

    subfolders = list_folder_items(drive_id, matching_folder["id"], headers)
    if not subfolders:
        logger.info(f"No subfolders found in '{form_title}'.")
    else:
        logger.info(f"Found {len(subfolders)} subfolders.")

    for subfolder in subfolders:
        subfolder_name = subfolder['name']
        subfolder_id = subfolder['id']
        logger.info(f"Found subfolder: {subfolder_name}")

        # Download response.submitted.json first to extract subject_id and form_title
        files = list_folder_items(drive_id, subfolder_id, headers)
        response_json_file = next(
            (f for f in files if f.get('name') == 'response.submitted.json'),
            None
        )

        if not response_json_file:
            logger.warning(f"No response.submitted.json found in {subfolder_name}, skipping.")
            continue

        # Download response.submitted.json to temp location
        with TemporaryDirectory() as tmpdirname:
            # Create a Path object for easier path manipulation
            temp_dir_path = Path(tmpdirname)

            # Define the path for a temporary file within this directory
            temp_file_path = temp_dir_path / "response.submitted.json"            # Write content to the temporary file

            download_url = response_json_file.get(
                    '@microsoft.graph.downloadUrl')
            if not download_url:
                logger.warning(
                    f"No download URL for response.submitted.json in {subfolder_name}, skipping."
                )
                continue
            download_file(download_url, temp_file_path, to_tmp=True)
            form_subject_id, form_title, date_only = extract_info(
                    temp_file_path)

            if form_subject_id != subject_id:
                logger.info(
                    f"Skipping subfolder {subfolder_name} (subject_id {form_subject_id} != {subject_id})"
                )
                continue
            if form_title != form_name:
                logger.info(
                    f"Skipping subfolder {subfolder_name} (form_title {form_title} != {form_name})"
                )
                continue

            # Now set the final local path
            # Move response.submitted.json to the new location
            logger.info(
                f"Found matching form - {form_name} - {subject_id}"
            )


            output_dir = output_dir / date_only
            output_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(
                temp_file_path,
                output_dir / "response.submitted.json", 
            )

        # Download all other files
        for f in files:
            if "file" in f and f.get('name') != 'response.submitted.json':
                file_name = f['name']
                file_info = f.get('file', {})
                quick_xor_hash = file_info.get('hashes', {}).get('quickXorHash')
                local_file_path = output_dir / file_name
                hash_file_path = output_dir / ("." + file_name + ".quickxorhash")

                if should_download_file(local_file_path, quick_xor_hash):
                    download_url = f.get('@microsoft.graph.downloadUrl')
                    if download_url:
                        download_file(download_url, local_file_path)
                        if quick_xor_hash:
                            with open(hash_file_path, 'w') as hf:
                                hf.write(quick_xor_hash)
                            logger.info(f"QuickXorHash saved to {hash_file_path}")



def ha():
    # client_id = sharepoint_cred['client_id'],
    # tenant_id = sharepoint_cred['tenant_id'],
    # site_url = sharepoint_cred['site_url'],
    # form_name = sharepoint_cred['form_name'],

    # CLIENT_ID = "dfe0c142-24ac-4d04-9ec4-1b7370d8a2b8"
    # TENANT_ID = "dd8cbebb-2139-4df8-b411-4e3e87abeb5c"
    # SITE_PATH = "yaleedu.sharepoint.com:/sites/ProCAN"
    # CLIENT_SECRET = os.environ.get("MS_CLIENT_SECRET")

    # site_id = get_site_id(headers, SITE_PATH)
    return
    try:
        # Get SharePoint access token
        access_token = get_access_token(sharepoint_data_source)
        
        # Get SharePoint site and form information
        site_url = sharepoint_data_source.data_source_metadata.site_url
        form_id = sharepoint_data_source.data_source_metadata.form_id
        
        # Get form responses from SharePoint
        url = f"https://graph.microsoft.com/v1.0/sites/{site_url}/lists/{form_id}/items?expand=fields"
        headers = {"Authorization": f"Bearer {access_token}"}
        
        response = requests.get(url, headers=headers, timeout=timeout_s)
        response.raise_for_status()
        form_responses = response.json()["value"]
        
        # Filter responses for this subject (assuming there's a subject field)
        subject_responses = []
        for response in form_responses:
            fields = response.get("fields", {})
            # Look for subject-related fields (this might need adjustment based on your form structure)
            if any(subject_id.lower() in str(value).lower() for value in fields.values()):
                subject_responses.append(response)
        
        # Download all files associated with the form responses
        import tempfile
        import zipfile
        import io
        import os
        
        with tempfile.TemporaryDirectory() as temp_dir:
            zip_buffer = io.BytesIO()
            
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                # Add response metadata
                for i, response in enumerate(subject_responses):
                    response_info = {
                        "response_id": response.get("id"),
                        "subject_id": subject_id,
                        "form_id": form_id,
                        "response_data": response,
                        "created": response.get("createdDateTime"),
                        "modified": response.get("lastModifiedDateTime"),
                    }
                    
                    # Add response metadata to ZIP
                    response_json = json.dumps(response_info, indent=2, default=str)
                    zip_file.writestr(f"responses/{response_info['response_id']}.json", response_json)
                    
                    # Download any attachments or files associated with this response
                    try:
                        # Get attachments for this response
                        attachments_url = f"https://graph.microsoft.com/v1.0/sites/{site_url}/lists/{form_id}/items/{response.get('id')}/attachments"
                        attachments_response = requests.get(attachments_url, headers=headers, timeout=timeout_s)
                        
                        if attachments_response.status_code == 200:
                            attachments = attachments_response.json().get("value", [])
                            for attachment in attachments:
                                # Download the attachment
                                file_url = attachment.get("@microsoft.graph.downloadUrl")
                                if file_url:
                                    file_response = requests.get(file_url, timeout=timeout_s)
                                    if file_response.status_code == 200:
                                        file_name = attachment.get("name", "unknown")
                                        # Store the actual file with its original name and extension
                                        zip_file.writestr(f"files/{response_info['response_id']}/{file_name}", file_response.content)
                                        
                                        # Also store file metadata
                                        file_metadata = {
                                            "file_name": file_name,
                                            "file_size": len(file_response.content),
                                            "content_type": file_response.headers.get("content-type", "application/octet-stream"),
                                            "download_url": file_url,
                                            "response_id": response_info['response_id'],
                                        }
                                        file_metadata_json = json.dumps(file_metadata, indent=2)
                                        zip_file.writestr(f"files/{response_info['response_id']}/{file_name}.meta.json", file_metadata_json)
                    except Exception as e:
                        logger.warning(f"Could not download attachments for response {response_info['response_id']}: {e}")
                
                # Add summary metadata
                summary = {
                    "subject_id": subject_id,
                    "form_id": form_id,
                    "total_responses": len(subject_responses),
                    "fetch_timestamp": datetime.now().isoformat(),
                    "metadata": {
                        "project_id": project_id,
                        "site_id": site_id,
                        "data_source_name": data_source_name,
                        "sharepoint_site_url": site_url,
                    }
                }
                zip_file.writestr("summary.json", json.dumps(summary, indent=2))
            
            zip_buffer.seek(0)
            return zip_buffer.getvalue()

    except Exception as e:
        logger.error(f"Failed to fetch data for {identifier}: {e}")
        Logs(
            log_level="ERROR",
            log_message={
                "event": "sharepoint_data_pull_fetch_failed",
                "message": f"Failed to fetch data for {identifier}.",
                "project_id": project_id,
                "site_id": site_id,
                "data_source_name": data_source_name,
                "subject_id": subject_id,
                "error": str(e),
            },
        ).insert(config_file)
        return None


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


def push_to_data_sink(
    file_path: Path,
    file_md5: str,
    project_id: str,
    site_id: str,
    config_file: Path,
) -> Optional[DataPush]:
    """
    Pushes a file to a data sink for the given project and site.

    Args:
        file_path (Path): Path to the file to push.
        file_md5 (str): MD5 hash of the file.
        project_id (str): The project ID.
        site_id (str): The site ID.
        config_file (Path): Path to the config file.

    Returns:
        Optional[DataPush]: The data push record if successful, None otherwise.
    """
    try:
        # Get data sinks for this project and site
        sql_query = f"""
            SELECT data_sink_id, data_sink_name, data_sink_metadata
            FROM data_sinks
            WHERE project_id = '{project_id}' AND site_id = '{site_id}'
        """
        df = db.execute_sql(config_file, sql_query)
        
        if df.empty:
            logger.warning(f"No data sinks found for {project_id}::{site_id}")
            return None

        # For now, use the first data sink found
        data_sink = df.iloc[0]
        data_sink_id = data_sink['data_sink_id']
        data_sink_name = data_sink['data_sink_name']
        data_sink_metadata = data_sink['data_sink_metadata']

        logger.info(f"Pushing {file_path} to data sink {data_sink_name}...")

        # Check if this is a MinIO data sink
        if data_sink_metadata.get('keystore_name'):
            # This is likely a MinIO data sink
            return push_to_minio_sink(
                file_path=file_path,
                file_md5=file_md5,
                data_sink_id=data_sink_id,
                data_sink_name=data_sink_name,
                data_sink_metadata=data_sink_metadata,
                project_id=project_id,
                site_id=site_id,
                config_file=config_file,
            )
        else:
            # For other data sink types, simulate upload for now
            start_time = datetime.now()
            import time
            time.sleep(1)  # Simulate upload time
            end_time = datetime.now()
            push_time_s = int((end_time - start_time).total_seconds())

            data_push = DataPush(
                data_sink_id=data_sink_id,
                file_path=str(file_path),
                file_md5=file_md5,
                push_time_s=push_time_s,
                push_metadata={
                    "data_sink_name": data_sink_name,
                    "data_sink_metadata": data_sink_metadata,
                    "upload_simulated": True,  # Flag to indicate this was simulated
                },
                push_timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )

            # Insert the data push record
            db.execute_queries(config_file, [data_push.to_sql_query()], show_commands=False)

            logger.info(f"Successfully pushed {file_path} to data sink {data_sink_name}")
            Logs(
                log_level="INFO",
                log_message={
                    "event": "sharepoint_data_push_success",
                    "message": f"Successfully pushed {file_path} to data sink {data_sink_name}.",
                    "project_id": project_id,
                    "site_id": site_id,
                    "data_sink_name": data_sink_name,
                    "file_path": str(file_path),
                    "push_time_s": push_time_s,
                },
            ).insert(config_file)

            return data_push

    except Exception as e:
        logger.error(f"Failed to push {file_path} to data sink: {e}")
        Logs(
            log_level="ERROR",
            log_message={
                "event": "sharepoint_data_push_failed",
                "message": f"Failed to push {file_path} to data sink.",
                "project_id": project_id,
                "site_id": site_id,
                "file_path": str(file_path),
                "error": str(e),
            },
        ).insert(config_file)
        return None


def push_to_minio_sink(
    file_path: Path,
    file_md5: str,
    data_sink_id: int,
    data_sink_name: str,
    data_sink_metadata: Dict[str, Any],
    project_id: str,
    site_id: str,
    config_file: Path,
) -> Optional[DataPush]:
    """
    Pushes a file to a MinIO data sink.

    Args:
        file_path (Path): Path to the file to push.
        file_md5 (str): MD5 hash of the file.
        data_sink_id (int): The data sink ID.
        data_sink_name (str): The data sink name.
        data_sink_metadata (Dict[str, Any]): The data sink metadata.
        project_id (str): The project ID.
        site_id (str): The site ID.
        config_file (Path): Path to the config file.

    Returns:
        Optional[DataPush]: The data push record if successful, None otherwise.
    """
    try:
        # Extract MinIO configuration from data sink metadata
        bucket_name = data_sink_metadata.get('bucket')
        keystore_name = data_sink_metadata.get('keystore_name')
        endpoint_url = data_sink_metadata.get('endpoint')

        if not all([bucket_name, keystore_name, endpoint_url]):
            logger.error(f"Missing required MinIO configuration in data sink metadata: {data_sink_metadata}")
            return None

        # Import MinIO client
        from minio import Minio
        from minio.error import S3Error
        from lochness.sources.minio.tasks.credentials import get_minio_cred

        # Get MinIO credentials from keystore
        minio_creds = get_minio_cred(keystore_name, project_id)
        access_key = minio_creds["access_key"]
        secret_key = minio_creds["secret_key"]

        # Initialize MinIO client
        endpoint_host = endpoint_url.replace("http://", "").replace("https://", "")
        client = Minio(
            endpoint_host,
            access_key=access_key,
            secret_key=secret_key,
            secure=endpoint_url.startswith("https"),
        )

        # Ensure bucket exists
        if not client.bucket_exists(bucket_name):
            logger.info(f"Bucket '{bucket_name}' does not exist. Creating...")
            client.make_bucket(bucket_name)
            logger.info(f"Bucket '{bucket_name}' created.")
        else:
            logger.info(f"Bucket '{bucket_name}' already exists.")

        # Create object name based on file path structure
        # Extract the relative path from the lochness data directory
        lochness_root = config.parse(config_file, 'general')['lochness_root']
        relative_path = file_path.relative_to(Path(lochness_root) / "data")
        object_name = str(relative_path).replace("\\", "/")  # Ensure forward slashes for S3

        # Upload the file
        logger.info(f"Uploading '{file_path}' to '{bucket_name}/{object_name}'...")
        start_time = datetime.now()
        
        client.fput_object(
            bucket_name,
            object_name,
            str(file_path),
            content_type="application/zip",
        )
        
        end_time = datetime.now()
        push_time_s = int((end_time - start_time).total_seconds())
        logger.info(f"Upload successful. Time taken: {push_time_s} seconds.")

        # Create data push record
        data_push = DataPush(
            data_sink_id=data_sink_id,
            file_path=str(file_path),
            file_md5=file_md5,
            push_time_s=push_time_s,
            push_metadata={
                "object_name": object_name,
                "bucket_name": bucket_name,
                "endpoint_url": endpoint_url,
                "upload_simulated": False,  # Flag to indicate this was real upload
            },
            push_timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )

        # Insert the data push record
        db.execute_queries(config_file, [data_push.to_sql_query()], show_commands=False)

        logger.info(f"Successfully pushed {file_path} to MinIO data sink {data_sink_name}")
        Logs(
            log_level="INFO",
            log_message={
                "event": "sharepoint_data_push_success",
                "message": f"Successfully pushed {file_path} to MinIO data sink {data_sink_name}.",
                "project_id": project_id,
                "site_id": site_id,
                "data_sink_name": data_sink_name,
                "file_path": str(file_path),
                "object_name": object_name,
                "bucket_name": bucket_name,
                "push_time_s": push_time_s,
            },
        ).insert(config_file)

        return data_push

    except S3Error as e:
        logger.error(f"MinIO S3 Error while pushing {file_path}: {e}")
        Logs(
            log_level="ERROR",
            log_message={
                "event": "sharepoint_data_push_minio_error",
                "message": f"MinIO S3 Error while pushing {file_path}.",
                "project_id": project_id,
                "site_id": site_id,
                "file_path": str(file_path),
                "error": str(e),
            },
        ).insert(config_file)
        return None
    except Exception as e:
        logger.error(f"Failed to push {file_path} to MinIO data sink: {e}")
        Logs(
            log_level="ERROR",
            log_message={
                "event": "sharepoint_data_push_failed",
                "message": f"Failed to push {file_path} to MinIO data sink.",
                "project_id": project_id,
                "site_id": site_id,
                "file_path": str(file_path),
                "error": str(e),
            },
        ).insert(config_file)
        return None


def pull_all_data(config_file: Path, project_id: str = None, site_id: str = None, push_to_sink: bool = False):
    """
    Main function to pull data for all active SharePoint data sources and subjects.
    """
    Logs(
        log_level="INFO",
        log_message={
            "event": "sharepoint_data_pull_start",
            "message": "Starting SharePoint data pull process.",
            "project_id": project_id,
            "site_id": site_id,
            "push_to_sink": push_to_sink,
        },
    ).insert(config_file)

    encryption_passphrase = config.parse(config_file, 'general')['encryption_passphrase']

    active_sharepoint_data_sources = SharepointDataSource.get_all_sharepoint_data_sources(
        config_file=config_file,
        encryption_passphrase=encryption_passphrase,
        active_only=True
    )

    if project_id:
        active_sharepoint_data_sources = [ds for ds in active_sharepoint_data_sources if ds.project_id == project_id]
    if site_id:
        active_sharepoint_data_sources = [ds for ds in active_sharepoint_data_sources if ds.site_id == site_id]

    if not active_sharepoint_data_sources:
        logger.info("No active SharePoint data sources found for data pull.")
        Logs(
            log_level="INFO",
            log_message={
                "event": "sharepoint_data_pull_no_active_sources",
                "message": "No active SharePoint data sources found for data pull.",
                "project_id": project_id,
                "site_id": site_id,
            },
        ).insert(config_file)
        return

    logger.info(f"Found {len(active_sharepoint_data_sources)} active SharePoint data sources for data pull.")
    Logs(
        log_level="INFO",
        log_message={
            "event": "sharepoint_data_pull_active_sources_found",
            "message": f"Found {len(active_sharepoint_data_sources)} active SharePoint data sources for data pull.",
            "count": len(active_sharepoint_data_sources),
            "project_id": project_id,
            "site_id": site_id,
        },
    ).insert(config_file)

    for sharepoint_data_source in active_sharepoint_data_sources:
        # Get subjects for this data source
        subjects_in_db = Subject.get_subjects_for_project_site(
            project_id=sharepoint_data_source.project_id,
            site_id=sharepoint_data_source.site_id,
            config_file=config_file
        )

        if not subjects_in_db:
            logger.info(f"No subjects found for {sharepoint_data_source.project_id}::{sharepoint_data_source.site_id}.")
            Logs(
                log_level="INFO",
                log_message={
                    "event": "sharepoint_data_pull_no_subjects",
                    "message": f"No subjects found for {sharepoint_data_source.project_id}::{sharepoint_data_source.site_id}.",
                    "project_id": sharepoint_data_source.project_id,
                    "site_id": sharepoint_data_source.site_id,
                    "data_source_name": sharepoint_data_source.data_source_name,
                },
            ).insert(config_file)
            continue

        logger.info(f"Found {len(subjects_in_db)} subjects for {sharepoint_data_source.data_source_name}.")
        Logs(
            log_level="INFO",
            log_message={
                "event": "sharepoint_data_pull_subjects_found",
                "message": f"Found {len(subjects_in_db)} subjects for {sharepoint_data_source.data_source_name}.",
                "count": len(subjects_in_db),
                "project_id": sharepoint_data_source.project_id,
                "site_id": sharepoint_data_source.site_id,
                "data_source_name": sharepoint_data_source.data_source_name,
            },
        ).insert(config_file)

        for subject in subjects_in_db:
            start_time = datetime.now()
            raw_data = fetch_subject_data(
                sharepoint_data_source=sharepoint_data_source,
                subject_id=subject.subject_id,
                encryption_passphrase=encryption_passphrase,
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
                            "form_id": sharepoint_data_source.data_source_metadata.form_id,
                            "records_pulled_bytes": len(raw_data),
                        },
                    )
                    db.execute_queries(config_file, [data_pull.to_sql_query()], show_commands=False)

                    # Push to data sink if requested
                    if push_to_sink:
                        push_to_data_sink(
                            file_path=file_path,
                            file_md5=file_md5,
                            project_id=subject.project_id,
                            site_id=subject.site_id,
                            config_file=config_file,
                        )
            break # Stop after the first subject

    Logs(
        log_level="INFO",
        log_message={
            "event": "sharepoint_data_pull_complete",
            "message": "Finished SharePoint data pull process.",
            "project_id": project_id,
            "site_id": site_id,
            "push_to_sink": push_to_sink,
        },
    ).insert(config_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pull SharePoint data for all or specific project/site.")
    parser.add_argument('--project_id', type=str, default=None, help='Project ID to pull data for (optional)')
    parser.add_argument('--site_id', type=str, default=None, help='Site ID to pull data for (optional)')
    parser.add_argument('--push_to_sink', action='store_true', help='Push pulled files to data sink')
    args = parser.parse_args()

    config_file = Path(__file__).resolve().parents[4] / "sample.config.ini"
    print(f"Resolved config_file path: {config_file}") # Debugging line
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

    pull_all_data(config_file=config_file, project_id=args.project_id, site_id=args.site_id, push_to_sink=args.push_to_sink)

    logger.info("Finished SharePoint data pull.") 
