"""
MindLAMP API connection and data fetching utilities.
"""

from typing import Any, Dict, List, Optional
from pathlib import Path
import json
import logging
import LAMP
from lochness.models.keystore import KeyStore
from lochness.sources.mindlamp.models.data_source import MindLAMPDataSource

logger = logging.getLogger(__name__)
LIMIT = 1000000


def get_mindlamp_credentials(
    mindlamp_data_source: MindLAMPDataSource, config_file: Path
) -> Dict[str, str]:
    """
    Get MindLAMP credentials from the keystore.
    """
    project_id = mindlamp_data_source.project_id
    keystore_name = mindlamp_data_source.data_source_metadata.keystore_name
    keystore = KeyStore.retrieve_keystore(
        config_file=config_file, key_name=keystore_name, project_id=project_id
    )
    if keystore:
        return json.loads(keystore.key_value)
    else:
        raise ValueError("MindLAMP credentials not found in keystore")


def connect_to_mindlamp(
    mindlamp_data_source: MindLAMPDataSource, config_file: Path
) -> None:
    """
    Connect to MindLAMP API
    """
    credentials = get_mindlamp_credentials(
        mindlamp_data_source=mindlamp_data_source, config_file=config_file
    )
    api_url = mindlamp_data_source.data_source_metadata.api_url
    access_key = credentials.get("access_key")
    secret_key = credentials.get("secret_key")
    if not all([api_url, access_key, secret_key]):
        raise ValueError("Missing required MindLAMP credentials")
    try:
        LAMP.connect(access_key, secret_key, api_url)
    except Exception as e:
        logger.error(f"Failed to connect to MindLAMP API: {e}")
        raise e


def get_activity_events_lamp(
    mindlamp_id: str,
    from_ts: Optional[int] = None,
    to_ts: Optional[int] = None,
    limit: int = LIMIT,
) -> List[Dict[str, Any]]:
    """
    Get activity events for a subject from MindLAMP API.
    """
    try:
        activity_events = LAMP.ActivityEvent.all_by_participant(
            mindlamp_id, _from=from_ts, to=to_ts, _limit=limit
        )["data"]
        return activity_events
    except Exception as e:
        logger.error(f"Failed to get activity events for subject {mindlamp_id}: {e}")
        return []


def get_sensor_events_lamp(
    subject_id: str,
    from_ts: Optional[int] = None,
    to_ts: Optional[int] = None,
    limit: int = LIMIT,
) -> List[Dict[str, Any]]:
    """
    Get sensor events for a subject from MindLAMP API.
    """
    try:
        sensor_events = LAMP.SensorEvent.all_by_participant(
            subject_id, _from=from_ts, to=to_ts, _limit=limit
        )["data"]
        return sensor_events
    except Exception as e:
        logger.error(f"Failed to get sensor events for subject {subject_id}: {e}")
        return []
