"""
MindLAMP Data Pull Script

This script pulls data from MindLAMP data sources and saves it to the file system.
"""

import sys
from pathlib import Path
import json
import logging
import base64
import re
import tempfile
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import pytz

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
from lochness.models.subjects import Subject
from lochness.models.files import File
from lochness.models.data_pulls import DataPull
from lochness.models.data_push import DataPush
from lochness.models.logs import Logs
from lochness.sources.mindlamp.models.data_source import MindLAMPDataSource

MODULE_NAME = "lochness.sources.mindlamp.tasks.pull_data"

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

# Import LAMP SDK
try:
    import LAMP
except ImportError:
    logger.error("LAMP SDK not found. Please install it with: pip install lamp-python")
    sys.exit(1)

LIMIT = 1000000


def get_mindlamp_cred(
    mindlamp_data_source: MindLAMPDataSource,
) -> Dict[str, str]:
    """Get MindLAMP credentials from the keystore."""
    config_file = utils.get_config_file_path()
    encryption_passphrase = config.parse(config_file, "general")[
        "encryption_passphrase"
    ]

    keystore = KeyStore.get_by_name_and_project(
        config_file,
        mindlamp_data_source.data_source_metadata.keystore_name,
        mindlamp_data_source.project_id,
        encryption_passphrase,
    )
    if keystore:
        return json.loads(keystore.key_value)
    else:
        raise ValueError("MindLAMP credentials not found in keystore")


def connect_to_mindlamp(mindlamp_data_source: MindLAMPDataSource) -> LAMP:
    """Connect to MindLAMP API."""
    credentials = get_mindlamp_cred(mindlamp_data_source)
    api_url = mindlamp_data_source.data_source_metadata.api_url
    access_key = credentials.get("access_key")
    secret_key = credentials.get("secret_key")

    if not all([api_url, access_key, secret_key]):
        raise ValueError("Missing required MindLAMP credentials")

    # Connect to MindLAMP API
    LAMP.connect(access_key, secret_key, api_url)
    return LAMP


def get_mindlamp_subjects(lamp: LAMP) -> List[Dict[str, Any]]:
    """Get all subjects from MindLAMP API."""
    try:
        # Get study information
        study_objs = lamp.Study.all_by_researcher('me')['data']
        if not study_objs:
            logger.warning("No studies found in MindLAMP")
            return []
        
        study_obj = study_objs[0]  # Use first study
        study_id = study_obj['id']
        
        # Get participants for the study
        subject_objs = lamp.Participant.all_by_study(study_id)['data']
        return subject_objs
    except Exception as e:
        logger.error(f"Failed to get MindLAMP subjects: {e}")
        return []


def get_activity_events_lamp(
    lamp: LAMP, 
    subject_id: str,
    from_ts: int = None, 
    to_ts: int = None
) -> List[Dict[str, Any]]:
    """Get activity events for a subject from MindLAMP API."""
    try:
        activity_events = lamp.ActivityEvent.all_by_participant(
            subject_id, _from=from_ts, to=to_ts, _limit=LIMIT
        )['data']
        return activity_events
    except Exception as e:
        logger.error(f"Failed to get activity events for subject {subject_id}: {e}")
        return []


def get_sensor_events_lamp(
    lamp: LAMP, 
    subject_id: str,
    from_ts: int = None, 
    to_ts: int = None
) -> List[Dict[str, Any]]:
    """Get sensor events for a subject from MindLAMP API."""
    try:
        sensor_events = lamp.SensorEvent.all_by_participant(
            subject_id, _from=from_ts, to=to_ts, _limit=LIMIT
        )['data']
        return sensor_events
    except Exception as e:
        logger.error(f"Failed to get sensor events for subject {subject_id}: {e}")
        return []


def get_audio_out_from_content(activity_dicts: List[Dict[str, Any]], audio_file_name: str) -> List[Dict[str, Any]]:
    """Separate out audio data from the content pulled from MindLAMP API."""
    activity_dicts_wo_sound = []
    num = 0
    
    for activity_events_dicts in activity_dicts:
        if 'url' in activity_events_dicts.get('static_data', {}):
            audio = activity_events_dicts['static_data']['url']
            activity_events_dicts['static_data']['url'] = f'SOUND_{num}'

            try:
                decode_bytes = base64.b64decode(audio.split(',')[1])
                with open(re.sub(r'.mp3', f'_{num}.mp3', audio_file_name), 'wb') as f:
                    f.write(decode_bytes)
                num += 1
            except Exception as e:
                logger.warning(f"Failed to decode audio data: {e}")

        activity_dicts_wo_sound.append(activity_events_dicts)

    return activity_dicts_wo_sound


def fetch_subject_data(
    mindlamp_data_source: MindLAMPDataSource,
    subject_id: str,
    encryption_passphrase: str,
    days_to_pull: int = 7,
    timeout_s: int = 60,
) -> Optional[bytes]:
    """
    Fetches data for a single subject from MindLAMP.

    Args:
        mindlamp_data_source (MindLAMPDataSource): The MindLAMP data source.
        subject_id (str): The subject ID to fetch data for.
        encryption_passphrase (str): The encryption passphrase for keystore access.
        days_to_pull (int): Number of days of data to pull.
        timeout_s (int): Timeout for the API request.

    Returns:
        Optional[bytes]: The raw data from MindLAMP, or None if fetching fails.
    """
    project_id = mindlamp_data_source.project_id
    site_id = mindlamp_data_source.site_id
    data_source_name = mindlamp_data_source.data_source_name

    identifier = f"{project_id}::{site_id}::{data_source_name}::{subject_id}"
    logger.info(f"Fetching data for {identifier}...")

    try:
        # Connect to MindLAMP
        lamp = connect_to_mindlamp(mindlamp_data_source)
        
        # Calculate time range
        ct_utc = datetime.now(pytz.timezone('UTC'))
        ct_utc_00 = ct_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Collect data for the specified number of days
        all_data = {
            "subject_id": subject_id,
            "project_id": project_id,
            "site_id": site_id,
            "data_source_name": data_source_name,
            "fetch_timestamp": datetime.now().isoformat(),
            "days_data": {}
        }
        
        for days_from_ct in reversed(range(days_to_pull)):
            # Calculate time range for this day
            time_utc_00 = ct_utc_00 - timedelta(days=days_from_ct)
            time_utc_00_ts = int(time.mktime(time_utc_00.timetuple()) * 1000)
            time_utc_24 = time_utc_00 + timedelta(hours=24)
            time_utc_24_ts = int(time.mktime(time_utc_24.timetuple()) * 1000)
            
            date_str = time_utc_00.strftime("%Y_%m_%d")
            logger.info(f"Fetching MindLAMP data for {subject_id} on {date_str}")
            
            day_data = {
                "date": date_str,
                "timestamp_start": time_utc_00_ts,
                "timestamp_end": time_utc_24_ts,
                "activity_events": [],
                "sensor_events": []
            }
            
            # Get activity events
            activity_events = get_activity_events_lamp(
                lamp, subject_id, from_ts=time_utc_00_ts, to_ts=time_utc_24_ts
            )
            day_data["activity_events"] = activity_events
            
            # Get sensor events
            sensor_events = get_sensor_events_lamp(
                lamp, subject_id, from_ts=time_utc_00_ts, to_ts=time_utc_24_ts
            )
            day_data["sensor_events"] = sensor_events
            
            # Process audio data from activity events
            if activity_events:
                audio_file_name = f"mindlamp_{subject_id}_{date_str}_audio.mp3"
                day_data["activity_events"] = get_audio_out_from_content(
                    activity_events, audio_file_name
                )
            
            all_data["days_data"][date_str] = day_data
        
        return json.dumps(all_data, indent=2, default=str).encode('utf-8')

    except Exception as e:
        logger.error(f"Failed to fetch data for {identifier}: {e}")
        Logs(
            log_level="ERROR",
            log_message={
                "event": "mindlamp_data_pull_fetch_failed",
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
        Optional[tuple[Path, str]]: The path to the saved file and MD5 hash, or None if saving fails.
    """
    try:
        # Define the path where the data will be stored
        lochness_root = config.parse(config_file, 'general')['lochness_root']
        output_dir = Path(lochness_root) / "data" / project_id / site_id / data_source_name / subject_id
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = utils.get_timestamp()
        file_name = f"{timestamp}.json"  # JSON format for MindLAMP data
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
                "event": "mindlamp_data_pull_save_success",
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
                "event": "mindlamp_data_pull_save_failed",
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
                    "event": "mindlamp_data_push_success",
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
                "event": "mindlamp_data_push_failed",
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

        # Get MinIO credentials from keystore
        encryption_passphrase = config.parse(config_file, "general")["encryption_passphrase"]
        keystore = KeyStore.get_by_name_and_project(
            config_file, keystore_name, project_id, encryption_passphrase
        )
        if not keystore:
            raise ValueError(f"MinIO credentials not found in keystore: {keystore_name}")

        credentials = json.loads(keystore.key_value)
        access_key = credentials.get("access_key")
        secret_key = credentials.get("secret_key")

        if not all([access_key, secret_key]):
            raise ValueError("Missing MinIO access_key or secret_key in keystore")

        # Create MinIO client
        client = Minio(
            endpoint_url.replace("https://", "").replace("http://", ""),
            access_key=access_key,
            secret_key=secret_key,
            secure=endpoint_url.startswith("https://"),
        )

        # Create object name
        object_name = f"{project_id}/{site_id}/mindlamp/{file_path.name}"

        # Upload the file
        logger.info(f"Uploading '{file_path}' to '{bucket_name}/{object_name}'...")
        start_time = datetime.now()
        
        client.fput_object(
            bucket_name,
            object_name,
            str(file_path),
            content_type="application/json",
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
                "event": "mindlamp_data_push_success",
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
                "event": "mindlamp_data_push_minio_error",
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
                "event": "mindlamp_data_push_failed",
                "message": f"Failed to push {file_path} to MinIO data sink.",
                "project_id": project_id,
                "site_id": site_id,
                "file_path": str(file_path),
                "error": str(e),
            },
        ).insert(config_file)
        return None


def pull_all_data(config_file: Path, project_id: str = None, site_id: str = None, push_to_sink: bool = False, days_to_pull: int = 7):
    """
    Main function to pull data for all active MindLAMP data sources and subjects.
    """
    Logs(
        log_level="INFO",
        log_message={
            "event": "mindlamp_data_pull_start",
            "message": "Starting MindLAMP data pull process.",
            "project_id": project_id,
            "site_id": site_id,
            "push_to_sink": push_to_sink,
            "days_to_pull": days_to_pull,
        },
    ).insert(config_file)

    encryption_passphrase = config.parse(config_file, 'general')['encryption_passphrase']

    active_mindlamp_data_sources = MindLAMPDataSource.get_all_mindlamp_data_sources(
        config_file=config_file,
        active_only=True
    )

    if project_id:
        active_mindlamp_data_sources = [ds for ds in active_mindlamp_data_sources if ds.project_id == project_id]
    if site_id:
        active_mindlamp_data_sources = [ds for ds in active_mindlamp_data_sources if ds.site_id == site_id]

    if not active_mindlamp_data_sources:
        logger.info("No active MindLAMP data sources found for data pull.")
        Logs(
            log_level="INFO",
            log_message={
                "event": "mindlamp_data_pull_no_active_sources",
                "message": "No active MindLAMP data sources found for data pull.",
                "project_id": project_id,
                "site_id": site_id,
            },
        ).insert(config_file)
        return

    logger.info(f"Found {len(active_mindlamp_data_sources)} active MindLAMP data sources for data pull.")
    Logs(
        log_level="INFO",
        log_message={
            "event": "mindlamp_data_pull_active_sources_found",
            "message": f"Found {len(active_mindlamp_data_sources)} active MindLAMP data sources for data pull.",
            "count": len(active_mindlamp_data_sources),
            "project_id": project_id,
            "site_id": site_id,
        },
    ).insert(config_file)

    for mindlamp_data_source in active_mindlamp_data_sources:
        # Get subjects for this data source
        subjects_in_db = Subject.get_subjects_for_project_site(
            project_id=mindlamp_data_source.project_id,
            site_id=mindlamp_data_source.site_id,
            config_file=config_file
        )

        if not subjects_in_db:
            logger.info(f"No subjects found for {mindlamp_data_source.project_id}::{mindlamp_data_source.site_id}.")
            Logs(
                log_level="INFO",
                log_message={
                    "event": "mindlamp_data_pull_no_subjects",
                    "message": f"No subjects found for {mindlamp_data_source.project_id}::{mindlamp_data_source.site_id}.",
                    "project_id": mindlamp_data_source.project_id,
                    "site_id": mindlamp_data_source.site_id,
                    "data_source_name": mindlamp_data_source.data_source_name,
                },
            ).insert(config_file)
            continue

        logger.info(f"Found {len(subjects_in_db)} subjects for {mindlamp_data_source.data_source_name}.")
        Logs(
            log_level="INFO",
            log_message={
                "event": "mindlamp_data_pull_subjects_found",
                "message": f"Found {len(subjects_in_db)} subjects for {mindlamp_data_source.data_source_name}.",
                "count": len(subjects_in_db),
                "project_id": mindlamp_data_source.project_id,
                "site_id": mindlamp_data_source.site_id,
                "data_source_name": mindlamp_data_source.data_source_name,
            },
        ).insert(config_file)

        for subject in subjects_in_db:
            start_time = datetime.now()
            raw_data = fetch_subject_data(
                mindlamp_data_source=mindlamp_data_source,
                subject_id=subject.subject_id,
                encryption_passphrase=encryption_passphrase,
                days_to_pull=days_to_pull,
            )

            if raw_data:
                result = save_subject_data(
                    data=raw_data,
                    project_id=subject.project_id,
                    site_id=subject.site_id,
                    subject_id=subject.subject_id,
                    data_source_name=mindlamp_data_source.data_source_name,
                    config_file=config_file,
                )
                if result:
                    file_path, file_md5 = result
                    end_time = datetime.now()
                    pull_time_s = int((end_time - start_time).total_seconds())

                    data_pull = DataPull(
                        subject_id=subject.subject_id,
                        data_source_name=mindlamp_data_source.data_source_name,
                        site_id=subject.site_id,
                        project_id=subject.project_id,
                        file_path=str(file_path),
                        file_md5=file_md5,
                        pull_time_s=pull_time_s,
                        pull_metadata={
                            "data_source_type": "mindlamp",
                            "days_to_pull": days_to_pull,
                            "pull_timestamp": datetime.now().isoformat(),
                        },
                        pull_timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    )

                    # Insert the data pull record
                    db.execute_queries(config_file, [data_pull.to_sql_query()], show_commands=False)

                    logger.info(f"Successfully pulled data for {subject.subject_id} from {mindlamp_data_source.data_source_name}")
                    Logs(
                        log_level="INFO",
                        log_message={
                            "event": "mindlamp_data_pull_success",
                            "message": f"Successfully pulled data for {subject.subject_id} from {mindlamp_data_source.data_source_name}.",
                            "project_id": subject.project_id,
                            "site_id": subject.site_id,
                            "data_source_name": mindlamp_data_source.data_source_name,
                            "subject_id": subject.subject_id,
                            "file_path": str(file_path),
                            "pull_time_s": pull_time_s,
                        },
                    ).insert(config_file)

                    # Push to data sink if requested
                    if push_to_sink:
                        push_result = push_to_data_sink(
                            file_path=file_path,
                            file_md5=file_md5,
                            project_id=subject.project_id,
                            site_id=subject.site_id,
                            config_file=config_file,
                        )
                        if push_result:
                            logger.info(f"Successfully pushed {file_path} to data sink")
                        else:
                            logger.warning(f"Failed to push {file_path} to data sink")

    logger.info("MindLAMP data pull process completed.")
    Logs(
        log_level="INFO",
        log_message={
            "event": "mindlamp_data_pull_complete",
            "message": "MindLAMP data pull process completed.",
            "project_id": project_id,
            "site_id": site_id,
        },
    ).insert(config_file)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pull data from MindLAMP data sources")
    parser.add_argument("--config", type=str, default="config.ini", help="Path to config file")
    parser.add_argument("--project-id", type=str, help="Project ID to filter by")
    parser.add_argument("--site-id", type=str, help="Site ID to filter by")
    parser.add_argument("--push-to-sink", action="store_true", help="Push data to data sink after pulling")
    parser.add_argument("--days-to-pull", type=int, default=7, help="Number of days of data to pull")

    args = parser.parse_args()

    config_file = Path(args.config)
    if not config_file.exists():
        logger.error(f"Config file not found: {config_file}")
        sys.exit(1)

    pull_all_data(
        config_file=config_file,
        project_id=args.project_id,
        site_id=args.site_id,
        push_to_sink=args.push_to_sink,
        days_to_pull=args.days_to_pull,
    ) 