import os
import re
import json
import shutil
import requests
from pathlib import Path
from datetime import datetime
from tempfile import TemporaryDirectory
import msal
import logging
from typing import Any, Dict, List, Optional, Union, Tuple
from lochness.helpers import logs, utils, db, config
from lochness.models.logs import Logs
from lochness.models.files import File
from lochness.models.data_pulls import DataPull


MODULE_NAME = "lochness.sources.sharepoint.tasks.pull_data"

logger = logging.getLogger(__name__)


config_file = utils.get_config_file_path()


def log_event(
    config_file: Path,
    log_level: str,
    event: str,
    message: str,
    project_id: Optional[str] = None,
    site_id: Optional[str] = None,
    data_source_name: Optional[str] = None,
    subject_id: Optional[str] = None,
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
        "subject_id": subject_id,
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


def authenticate(
    client_id: str,
    tenant_id: str,
    client_secret: Optional[str] = None
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

    # application mode
    if client_secret:
        try:
            logger.info("Attempting authentication using client credentials..")
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
                logger.warning("Error: %s", result.get('error'))
                logger.warning("Error description: %s",
                               result.get('error_description'))
                logger.warning("Correlation ID: %s",
                               result.get('correlation_id'))
        except Exception as e:
            logger.error(f"Client credentials error: {e}. "
                         "Falling back to device flow.")

    # Device flow fallback
    logger.info("Logging in using device flow...")
    app = msal.PublicClientApplication(client_id, authority=authority)
    flow = app.initiate_device_flow(scopes=scopes_device)
    if "user_code" not in flow:
        logger.error(f"Failed to start device flow: {flow}")
        raise RuntimeError(f"Failed to start device flow: {flow}")
    logger.info(
        f"Please go to {flow['verification_uri']} "
        f"and enter code: {flow['user_code']}"
    )
    result = app.acquire_token_by_device_flow(flow)
    if "access_token" not in result:
        logger.error("Authentication failed: %s",
                     result.get('error_description'))
        raise RuntimeError("Authentication failed: %s",
                           result.get('error_description'))
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


def should_download_file(local_file_path: Path,
                         quick_xor_hash: Optional[str]) -> bool:
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


def get_matching_subfolders(drive_id: str,
                            responses_folder: dict,
                            form_name: str,
                            headers: dict) -> list:
    matching_folder = find_subfolder(
            drive_id,
            responses_folder["id"],
            form_name,
            headers)

    if not matching_folder:
        raise RuntimeError(f"'{form_name}' folder not found inside 'Responses'.")

    subfolders = list_folder_items(drive_id, matching_folder["id"], headers)
    if not subfolders:
        logger.info(f"No subfolders found in '{form_name}'.")
    else:
        logger.info(f"Found {len(subfolders)} subfolders.")

    return subfolders


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
        timestamp = data.get('timestamp')
    except Exception:
        pass

    try:
        date_str = data.get('data', {}).get('data', {}).get('dateOfEgg')
        dt = datetime.fromisoformat(date_str)
        # date_only = dt.date()
        dt_str = dt.strftime('%Y_%m_%d')
    except Exception:
        date_only = None
        pass

    return subject_id, form_title, dt_str, timestamp


def download_subdirectory(files: list,
                          subject_id: str,
                          site_id: str,
                          project_id: str,
                          data_source_name: str,
                          output_dir: Path):
    """Download updated files to the output_dir and clean up previous files"""

    # Label previously downloaded files that are now removed from the form
    filename_list = [x['name'] for x in files]
    removed_file_paths = [x for x in output_dir.glob('*')
                          if x.name not in filename_list]

    for removed_file_path in removed_file_paths:
        file_model = File(file_path=removed_file_path)
        file_model.md5 = "DELETED_FROM_TEAMS_FORM"
        db.execute_queries(config_file,
                           [file_model.to_sql_query()],
                           show_commands=False)

    # Download all other files
    for f in files:
        if "file" not in f:
            continue

        file_name = f['name']
        file_info = f.get('file', {})
        quick_xor_hash = file_info.get('hashes', {}).get('quickXorHash')
        local_file_path = output_dir / file_name
        hash_file_path = output_dir / ("." + file_name + ".quickxorhash")

        file_target_path = output_dir / file_name

        if should_download_file(local_file_path, quick_xor_hash):
            download_url = f.get('@microsoft.graph.downloadUrl')
            if download_url:
                start_time = datetime.now()
                download_file(download_url, file_target_path)
                file_model = File(file_path=file_target_path)
                file_md5 = file_model.md5
                db.execute_queries(config_file,
                                   [file_model.to_sql_query()],
                                   show_commands=False)

                with open(hash_file_path, 'w') as hf:
                    hf.write(quick_xor_hash)

                file_model = File(file_path=hash_file_path)
                db.execute_queries(config_file,
                                   [file_model.to_sql_query()],
                                   show_commands=False)

                msg = f"Successfully saved data for {subject_id} " \
                    f"to {file_target_path}."
                log_event(
                    config_file=config_file,
                    log_level="INFO",
                    event="sharepoint_data_pull_save_success",
                    message=msg,
                    project_id=project_id,
                    site_id=site_id,
                    data_source_name=data_source_name,
                    subject_id=subject_id,
                    extra={"file_path": str(file_target_path),
                           "file_md5": file_md5 if file_md5 else None,
                           "quickxorhash": quick_xor_hash},
                )
                end_time = datetime.now()
                pull_time_s = int((end_time - start_time).total_seconds())

                data_pull = DataPull(
                    subject_id=subject_id,
                    data_source_name=data_source_name,
                    site_id=site_id,
                    project_id=project_id,
                    file_path=str(file_target_path),
                    file_md5=file_md5,
                    pull_time_s=pull_time_s,
                    pull_metadata={'quickxorhash': quick_xor_hash},
                )
                db.execute_queries(
                    config_file, [data_pull.to_sql_query()],
                    show_commands=False
                )


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


def is_response_json_updated(response_json_file,
                             subfolder_name: str,
                             subject_id: str,
                             form_name: str,
                             output_dir: Path,
                             ) -> Tuple[bool, Union[bool, Path]]:
    """Investigates the response json to determine and execute the download"""
    # Download response.submitted.json to temp location
    with TemporaryDirectory() as tmpdirname:
        # Create a Path object for easier path manipulation
        temp_dir_path = Path(tmpdirname)

        # Define the path for a temporary file within this directory
        temp_file_path = temp_dir_path / "response.submitted.json"            # Write content to the temporary file

        download_url = response_json_file.get('@microsoft.graph.downloadUrl')
        if not download_url:
            logger.warning(
                f"No download URL for response.submitted.json in {subfolder_name}, skipping."
            )
            return (False, False)

        download_file(download_url, temp_file_path, to_tmp=True)
        form_subject_id, form_title, date_only, timestamp = extract_info(
                temp_file_path)

        if form_subject_id != subject_id:
            logger.info(
                f"Skipping subfolder {subfolder_name} (subject_id {form_subject_id} != {subject_id})"
            )
            return (False, False)

        if form_title != form_name:
            logger.info(
                f"Skipping subfolder {subfolder_name} (form_title {form_title} != {form_name})"
            )
            return (False, False)

        # Now set the final local path
        # Move response.submitted.json to the new location
        logger.info(
            f"Found matching form - {form_name} - {subject_id}"
        )

        output_dir = output_dir / date_only

        response_json_local = output_dir / "response.submitted.json"
        timestamp_pre = None
        if response_json_local.is_file():
            _, _, _, timestamp_pre = extract_info(
                    response_json_local)
            logger.debug("Response json exists: %s", timestamp_pre)
        else:
            logger.debug("A new response is found: %s %s %s",
                         form_subject_id,
                         form_title,
                         date_only)

        if timestamp == timestamp_pre:
            logger.debug("No update in the response forms: %s %s %s",
                         form_subject_id,
                         form_title,
                         date_only)
            return (False, False)
        else:
            # new or updated response
            return (True, output_dir)


def download_new_or_updated_files(subfolder: dict,
                                  drive_id: str,
                                  headers: str,
                                  form_name: str,
                                  subject_id: str,
                                  site_id: str,
                                  project_id: str,
                                  data_source_name: str,
                                  output_dir_root: Path):
    """Download all files under the subfolder from a submitted form"""
    subfolder_name = subfolder['name']
    subfolder_id = subfolder['id']
    logger.info("Found subfolder: %s", subfolder_name)

    files = list_folder_items(drive_id, subfolder_id, headers)
    response_json_file = next(
        (f for f in files if f.get('name') == 'response.submitted.json'),
        None
    )
    if not response_json_file:
        logger.debug("No response.submitted.json found in %s, skipping.",
                     subfolder_name)
        return

    # reponse needs to be downloaded each time as it does not have checksum
    new_or_updated_form, output_dir = is_response_json_updated(
            response_json_file,
            subfolder_name,
            subject_id,
            form_name,
            output_dir_root)

    if new_or_updated_form:
        output_dir.mkdir(parents=True, exist_ok=True)
        download_subdirectory(files,
                              subject_id,
                              site_id,
                              project_id,
                              data_source_name,
                              output_dir)
                           
