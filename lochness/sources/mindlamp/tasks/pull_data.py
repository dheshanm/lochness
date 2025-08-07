"""
MindLAMP Data Pull Script

This script pulls data from MindLAMP data sources and saves it to the file system.
"""

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
root_dir = None  # pylint: disable=invalid-name
for parent in file.parents:
    if parent.name == "lochness_v2":
        root_dir = parent

sys.path.append(str(root_dir))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import argparse
import base64
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

import LAMP
import pandas as pd
import pytz
from rich.logging import RichHandler

from lochness.helpers import config, db
from lochness.helpers.timer import Timer
from lochness.models.data_pulls import DataPull
from lochness.models.files import File
from lochness.models.keystore import KeyStore
from lochness.models.logs import Logs
from lochness.models.subjects import Subject
from lochness.sources.mindlamp.models.data_source import MindLAMPDataSource

MODULE_NAME = "lochness.sources.mindlamp.tasks.pull_data"

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

LIMIT = 1000000


def get_mindlamp_credentials(
    mindlamp_data_source: MindLAMPDataSource, config_file: Path
) -> Dict[str, str]:
    """
    Get MindLAMP credentials from the keystore.

    Args:
        mindlamp_data_source (MindLAMPDataSource): The MindLAMP data source object.
        config_file (Path): Path to the configuration file.

    Returns:
        Dict[str, str]: A dictionary containing the MindLAMP credentials.
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

    Args:
        mindlamp_data_source (MindLAMPDataSource): The MindLAMP data source object.
        config_file (Path): Path to the configuration file.

    Raises:
        ValueError: If required credentials are missing.
    """
    credentials = get_mindlamp_credentials(
        mindlamp_data_source=mindlamp_data_source, config_file=config_file
    )
    api_url = mindlamp_data_source.data_source_metadata.api_url
    access_key = credentials.get("access_key")
    secret_key = credentials.get("secret_key")

    if not all([api_url, access_key, secret_key]):
        raise ValueError("Missing required MindLAMP credentials")

    # Connect to MindLAMP API
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

    Note: Might contain base64 encoded audio data.

    Args:
        mindlamp_id (str): The MindLAMP ID of the subject.
        from_ts (Optional[int]): Start timestamp in milliseconds. Defaults to None.
        to_ts (Optional[int]): End timestamp in milliseconds. Defaults to None.
        limit (int): Maximum number of events to retrieve. Defaults to LIMIT.

    Returns:
        List[Dict[str, Any]]: A list of activity events.
    """
    try:
        activity_events = LAMP.ActivityEvent.all_by_participant(
            mindlamp_id, _from=from_ts, to=to_ts, _limit=limit
        )["data"]
        return activity_events
    except Exception as e:  # pylint: disable=broad-except
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

    Args:
        subject_id (str): The MindLAMP ID of the subject.
        from_ts (Optional[int]): Start timestamp in milliseconds. Defaults to None.
        to_ts (Optional[int]): End timestamp in milliseconds. Defaults to None.
        limit (int): Maximum number of events to retrieve. Defaults to LIMIT.

    Returns:
        List[Dict[str, Any]]: A list of sensor events.
    """
    try:
        sensor_events = LAMP.SensorEvent.all_by_participant(
            subject_id, _from=from_ts, to=to_ts, _limit=limit
        )["data"]
        return sensor_events
    except Exception as e:  # pylint: disable=broad-except
        logger.error(f"Failed to get sensor events for subject {subject_id}: {e}")
        return []


def get_audio_out_from_content(
    activity_dicts: List[Dict[str, Any]],
    audio_data_root: Path,
    audio_file_name_template: str,
) -> Tuple[List[Dict[str, Any]], List[Path]]:
    """
    Separate out audio data from the content pulled from MindLAMP API.

    Returns a tuple containing:
    - List of activity dictionaries with audio URLs replaced by placeholders
    - List of Paths to the saved audio files
    """
    activity_dicts_wo_sound = []
    num = 0
    audio_file_paths = []

    for activity_events_dicts in activity_dicts:
        if "url" in activity_events_dicts.get("static_data", {}):
            audio: str = activity_events_dicts["static_data"]["url"]
            activity_events_dicts["static_data"]["url"] = f"SOUND_{num}"
            audio_timestamp: Optional[int] = activity_events_dicts.get(
                "timestamp", None
            )  # 1684976609734
            audio_dt = (
                datetime.fromtimestamp(audio_timestamp / 1000, tz=pytz.UTC)
                if audio_timestamp
                else None
            )
            suffix = (
                f"_{audio_dt.strftime('%H_%M_%S')}_{num}_audio.mp3"
                if audio_dt
                else f"TIME_{num}_audio.mp3"
            )
            try:
                decode_bytes = base64.b64decode(audio.split(",")[1])
                # Generate a unique audio file name for each audio event
                audio_file_name = f"{audio_file_name_template}{suffix}"
                audio_file_path = audio_data_root / audio_file_name
                with open(audio_file_path, "wb", encoding="utf-8") as f:
                    f.write(decode_bytes)
                audio_file_paths.append(Path(audio_file_path))
                logger.debug(f"Saved audio file: {audio_file_path}")
                num += 1
            except Exception as e:  # pylint: disable=broad-except
                logger.warning(f"Failed to decode audio data: {e}")

        activity_dicts_wo_sound.append(activity_events_dicts)

    return activity_dicts_wo_sound, audio_file_paths


def fetch_subject_data_for_date(
    mindlamp_data_source: MindLAMPDataSource,
    subject_id: str,
    mindlamp_id: str,
    datetime_dt: datetime,
    config_file: Path,
    subject_mindlamp_data_root: Path,
) -> List[DataPull]:
    """
    Fetch data for a subject from MindLAMP for a specific date.

    This includes fetching activity and sensor events, daily audio journals.

    Args:
        mindlamp_data_source (MindLAMPDataSource): The MindLAMP data source object.
        subject_id (str): The subject ID.
        mindlamp_id (str): The MindLAMP ID of the subject.
        datetime_dt (datetime): The date for which to fetch data.
        config_file (Path): Path to the configuration file.
        subject_mindlamp_data_root (Path): Path to the directory where subject data will be saved.

    Returns:
        List[DataPull]: A list of data pulls for the subject.
    """
    data_pulls: List[DataPull] = []
    associated_files: List[File] = []

    project_id = mindlamp_data_source.project_id
    site_id = mindlamp_data_source.site_id
    data_source_name = mindlamp_data_source.data_source_name

    identifier = f"{project_id}::{site_id}::{data_source_name}::{subject_id}"

    connect_to_mindlamp(mindlamp_data_source, config_file)

    dt_in_utc = datetime_dt.astimezone(pytz.timezone("UTC"))
    date_utc = dt_in_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    date_str = date_utc.strftime("%Y_%m_%d")

    # unix timestamps in milliseconds
    start_timestamp = int(date_utc.timestamp() * 1000)
    end_timestamp = start_timestamp + 24 * 60 * 60 * 1000

    logger.debug(f"Fetching data for {identifier} for date {date_str}...")
    logger.debug(f"Start timestamp: {start_timestamp}, End timestamp: {end_timestamp}")

    logger.debug(f"Fetching activity and sensor events for {identifier}...")
    with Timer() as a_timer:
        activity_events = get_activity_events_lamp(
            mindlamp_id, from_ts=start_timestamp, to_ts=end_timestamp
        )
    if len(activity_events) == 0:
        logger.debug(f"No activity events found for {identifier} on {date_str}")

    logger.debug(f"Fetching sensor events for {identifier}...")
    with Timer() as timer:
        sensor_events = get_sensor_events_lamp(
            mindlamp_id, from_ts=start_timestamp, to_ts=end_timestamp
        )
    if sensor_events:
        sensor_file_name = f"{mindlamp_id}_{subject_id}_sensor_{date_str}.json"
        sensor_file_path = subject_mindlamp_data_root / sensor_file_name
        with open(sensor_file_path, "w", encoding="utf-8") as f:
            json.dump(sensor_events, f, indent=4)
            logger.debug(f"Saved sensor events to {sensor_file_path}")

        sensors_file = File(
            file_path=sensor_file_path,
        )
        associated_files.append(sensors_file)

        sensor_data_pull = DataPull(
            subject_id=subject_id,
            data_source_name=mindlamp_data_source.data_source_name,
            site_id=mindlamp_data_source.site_id,
            project_id=mindlamp_data_source.project_id,
            file_path=str(sensor_file_path),
            file_md5=sensors_file.md5,  # type: ignore
            pull_time_s=int(timer.duration),  # type: ignore
            pull_metadata={
                "mindlamp_id": mindlamp_id,
                "mindlamp_data_type": "sensor",
                "data_date_utc": date_utc,
            },
        )
        data_pulls.append(sensor_data_pull)
    else:
        logger.debug(f"No sensor events found for {identifier} on {date_str}.")

    if activity_events:
        # get audio data from activity events
        logger.debug(f"Processing audio data for {identifier}...")
        audio_file_name_template = f"{mindlamp_id}_{subject_id}_activity_{date_str}"
        activity_dicts_wo_sound, audio_file_paths = get_audio_out_from_content(
            activity_dicts=activity_events,
            audio_data_root=subject_mindlamp_data_root,
            audio_file_name_template=audio_file_name_template,
        )
        activity_events = activity_dicts_wo_sound

        activity_file_name = f"{mindlamp_id}_{subject_id}_activity_{date_str}.json"
        activity_file_path = subject_mindlamp_data_root / activity_file_name
        with open(activity_file_path, "w", encoding="utf-8") as f:
            json.dump(activity_events, f, indent=4)
            logger.debug(f"Saved activity events to {activity_file_path}")

        activities_file = File(
            file_path=activity_file_path,
        )
        associated_files.append(activities_file)

        activity_data_pull = DataPull(
            subject_id=subject_id,
            data_source_name=mindlamp_data_source.data_source_name,
            site_id=mindlamp_data_source.site_id,
            project_id=mindlamp_data_source.project_id,
            file_path=str(activity_file_path),
            file_md5=activities_file.md5,  # type: ignore
            pull_time_s=int(a_timer.duration),  # type: ignore
            pull_metadata={
                "mindlamp_id": mindlamp_id,
                "mindlamp_data_type": "activity",
                "data_date_utc": date_utc,
            },
        )
        data_pulls.append(activity_data_pull)

        for audio_file in audio_file_paths:
            audio_file_o = File(
                file_path=audio_file,
            )
            associated_files.append(audio_file_o)

            audio_data_pull = DataPull(
                subject_id=subject_id,
                data_source_name=mindlamp_data_source.data_source_name,
                site_id=mindlamp_data_source.site_id,
                project_id=mindlamp_data_source.project_id,
                file_path=str(audio_file),
                file_md5=audio_file_o.md5,  # type: ignore
                pull_time_s=int(a_timer.duration),  # type: ignore
                pull_metadata={
                    "mindlamp_id": mindlamp_id,
                    "mindlamp_data_type": "audio_journals",
                    "data_date_utc": date_utc,
                },
            )
            data_pulls.append(audio_data_pull)

        logger.debug(f"Fetched {len(audio_file_paths)} audio files for {identifier}.")

    queries: List[str] = []
    for file in associated_files:
        file_query = file.to_sql_query()
        queries.append(file_query)

    db.execute_queries(
        config_file=config_file,
        queries=queries,
    )

    return data_pulls


def get_subject_mindlamp_id(
    mindlamp_data_source: MindLAMPDataSource,
    subject_id: str,
    config_file: Path,
) -> Optional[str]:
    """
    Get the MindLAMP ID for a subject.

    Args:
        mindlamp_data_source (MindLAMPDataSource): The MindLAMP data source object.
        subject_id (str): The subject ID.
        config_file (Path): Path to the configuration file.

    Returns:
        Optional[str]: The MindLAMP ID for the subject, or None if not found.
    """
    subject = Subject.get(
        project_id=mindlamp_data_source.project_id,
        site_id=mindlamp_data_source.site_id,
        subject_id=subject_id,
        config_file=config_file,
    )

    if subject:
        return subject.subject_metadata.get("mindlamp_id", None)

    return None


def get_subject_mindlamp_data_root(
    subject_id: str, mindlamp_data_source: MindLAMPDataSource, config_file: Path
) -> Path:
    """
    Get the root directory for storing MindLAMP data for a subject.

    Args:
        subject_id (str): The subject ID.
        mindlamp_data_source (MindLAMPDataSource): The MindLAMP data source object.
        config_file (Path): Path to the configuration file.

    Returns:
        Path: The root directory for storing MindLAMP data for the subject.
    """
    lochness_root: str = config.parse(config_file, "general")["lochness_root"]  # type: ignore

    project_id = mindlamp_data_source.project_id
    site_id = mindlamp_data_source.site_id

    project_name_cap = (
        project_id[:1].upper() + project_id[1:].lower() if project_id else project_id
    )

    output_dir = (
        Path(lochness_root)
        / project_name_cap
        / "PHOENIX"
        / "PROTECTED"
        / f"{project_name_cap}{site_id}"
        / "raw"
        / subject_id
        / "phone"
    )

    return output_dir


def get_dates_without_data(
    subject_id: str,
    mindlamp_ds: MindLAMPDataSource,
    start_date: datetime,
    config_file: Path,
    end_date: Optional[datetime] = None,
) -> Set[datetime]:
    """
    Returns a set of dates for which there is no data pull for the given subject.

    Args:
        subject_id (str): The subject ID.
        mindlamp_ds (MindLAMPDataSource): The MindLAMP data source object.
        start_date (datetime): The start date.
        config_file (Path): Path to the configuration file.
        end_date (Optional[datetime]): The end date. Defaults to None, which sets it to yesterday.

    Returns:
        Set[datetime]: A set of dates without data pulls.
    """

    if end_date is None:
        end_date = datetime.now(tz=pytz.UTC) - timedelta(days=1)
        end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)

    data_pulls_df = DataPull.get_data_pulls_for_subject(
        subject_id=subject_id,
        site_id=mindlamp_ds.site_id,
        project_id=mindlamp_ds.project_id,
        config_file=config_file,
        data_source_name=mindlamp_ds.data_source_name,
    )
    data_pulls_df["data_date_utc"] = pd.to_datetime(
        data_pulls_df["data_date_utc"], utc=True
    )

    all_dates = pd.date_range(start=start_date, end=end_date, freq="D", tz=pytz.UTC)
    pulled_dates = data_pulls_df["data_date_utc"].dt.normalize().unique()

    missing_dates = sorted(set(all_dates) - set(pulled_dates))

    missing_dates_dt = [date.to_pydatetime() for date in missing_dates]
    return set(missing_dates_dt)


def fetch_subject_data(
    mindlamp_data_source: MindLAMPDataSource,
    subject_id: str,
    days_to_fetch: int,
    days_to_redownload: int,
    config_file: Path,
) -> List[DataPull]:
    """
    Fetch data for a subject from MindLAMP.

    Args:
        mindlamp_data_source (MindLAMPDataSource): The MindLAMP data source object.
        subject_id (str): The subject ID.
        days_to_fetch (int): Number of days to fetch data for.
        days_to_redownload (int): Number of days to redownload data for.
        config_file (Path): Path to the configuration file.

    Returns:
        List[DataPull]: A list of data pulls for the subject.
    """
    data_pulls: List[DataPull] = []

    subject_mindlamp_id = get_subject_mindlamp_id(
        mindlamp_data_source=mindlamp_data_source,
        subject_id=subject_id,
        config_file=config_file,
    )

    if not subject_mindlamp_id:
        logger.warning(
            (
                f"MindLAMP ID not found for subject {subject_id} "
                f"in project {mindlamp_data_source.project_id} "
                f"and site {mindlamp_data_source.site_id}."
            )
        )
        return data_pulls

    current_dt = datetime.now(pytz.timezone("UTC"))
    # Drop time info
    current_dt = current_dt.replace(hour=0, minute=0, second=0, microsecond=0)

    start_date = current_dt - timedelta(days=days_to_fetch)
    end_date = current_dt

    dates_to_fetch = get_dates_without_data(
        subject_id=subject_id,
        mindlamp_ds=mindlamp_data_source,
        start_date=start_date,
        end_date=end_date,
        config_file=config_file,
    )

    dates_to_redownload = pd.date_range(
        start=current_dt - timedelta(days=days_to_redownload),
        end=current_dt - timedelta(days=1),
        freq="D",
    )
    dates_to_redownload_set = set(date.to_pydatetime() for date in dates_to_redownload)

    dates_to_download: Set[datetime] = dates_to_fetch.union(dates_to_redownload_set)

    subject_mindlamp_data_root = get_subject_mindlamp_data_root(
        subject_id=subject_id,
        mindlamp_data_source=mindlamp_data_source,
        config_file=config_file,
    )
    subject_mindlamp_data_root.mkdir(parents=True, exist_ok=True)

    logger.debug(
        f"Found {len(dates_to_download)} dates to fetch for subject {subject_id}."
    )
    for date_dt in sorted(dates_to_download):
        pulls = fetch_subject_data_for_date(
            mindlamp_data_source=mindlamp_data_source,
            subject_id=subject_id,
            mindlamp_id=subject_mindlamp_id,
            datetime_dt=date_dt,
            config_file=config_file,
            subject_mindlamp_data_root=subject_mindlamp_data_root,
        )
        data_pulls.extend(pulls)

    return data_pulls


def pull_all_data(
    config_file: Path,
    project_id: Optional[str] = None,
    site_id: Optional[str] = None,
    days_to_pull: int = 14,
    days_to_redownload: int = 7,
):
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
            "days_to_pull": days_to_pull,
        },
    ).insert(config_file)

    active_mindlamp_data_sources = MindLAMPDataSource.get_all_mindlamp_data_sources(
        config_file=config_file, active_only=True
    )

    if project_id:
        active_mindlamp_data_sources = [
            ds for ds in active_mindlamp_data_sources if ds.project_id == project_id
        ]
    if site_id:
        active_mindlamp_data_sources = [
            ds for ds in active_mindlamp_data_sources if ds.site_id == site_id
        ]

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

    logger.info(
        "Identified "
        f"{len(active_mindlamp_data_sources)} "
        "active MindLAMP data sources "
        "for data pull."
    )
    Logs(
        log_level="INFO",
        log_message={
            "event": "mindlamp_data_pull_active_sources_found",
            "message": (
                f"Identified {len(active_mindlamp_data_sources)} "
                "active MindLAMP data sources for data pull."
            ),
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
            config_file=config_file,
        )

        if not subjects_in_db:
            logger.info(
                (
                    "No subjects found for "
                    f"{mindlamp_data_source.project_id}::"
                    f"{mindlamp_data_source.site_id}."
                )
            )
            Logs(
                log_level="INFO",
                log_message={
                    "event": "mindlamp_data_pull_no_subjects",
                    "message": (
                        f"No subjects found for {mindlamp_data_source.project_id}::"
                        f"{mindlamp_data_source.site_id}."
                    ),
                    "project_id": mindlamp_data_source.project_id,
                    "site_id": mindlamp_data_source.site_id,
                    "data_source_name": mindlamp_data_source.data_source_name,
                },
            ).insert(config_file)
            continue

        logger.info(
            f"Found {len(subjects_in_db)} subjects for {mindlamp_data_source.data_source_name}."
        )
        Logs(
            log_level="INFO",
            log_message={
                "event": "mindlamp_data_pull_subjects_found",
                "message": (
                    f"Found {len(subjects_in_db)} subjects for "
                    f"{mindlamp_data_source.data_source_name}."
                ),
                "count": len(subjects_in_db),
                "project_id": mindlamp_data_source.project_id,
                "site_id": mindlamp_data_source.site_id,
                "data_source_name": mindlamp_data_source.data_source_name,
            },
        ).insert(config_file)

        for subject in subjects_in_db:
            data_pulls = fetch_subject_data(
                mindlamp_data_source=mindlamp_data_source,
                subject_id=subject.subject_id,
                days_to_fetch=days_to_pull,
                days_to_redownload=days_to_redownload,
                config_file=config_file,
            )

            if data_pulls:
                logger.info(
                    f"Fetched {len(data_pulls)} data pulls for subject {subject.subject_id} "
                    f"in project {mindlamp_data_source.project_id} and site "
                    f"{mindlamp_data_source.site_id}."
                )
                Logs(
                    log_level="INFO",
                    log_message={
                        "event": "mindlamp_data_pull_subject_complete",
                        "message": (
                            f"Fetched {len(data_pulls)} data pulls for subject "
                            f"{subject.subject_id} in project {mindlamp_data_source.project_id} "
                            f"and site {mindlamp_data_source.site_id}."
                        ),
                        "subject_id": subject.subject_id,
                        "data_source_name": mindlamp_data_source.data_source_name,
                        "project_id": mindlamp_data_source.project_id,
                        "site_id": mindlamp_data_source.site_id,
                    },
                ).insert(config_file)

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
    parser = argparse.ArgumentParser(description="Pull data from MindLAMP data sources")
    parser.add_argument(
        "--config", type=str, default="config.ini", help="Path to config file"
    )
    parser.add_argument("--project-id", type=str, help="Project ID to filter by")
    parser.add_argument("--site-id", type=str, help="Site ID to filter by")
    parser.add_argument(
        "--days-to-pull", type=int, default=7, help="Number of days of data to pull"
    )

    args = parser.parse_args()

    config_file = Path(args.config)
    if not config_file.exists():
        logger.error(f"Config file not found: {config_file}")
        sys.exit(1)

    pull_all_data(
        config_file=config_file,
        project_id=args.project_id,
        site_id=args.site_id,
        days_to_pull=args.days_to_pull,
    )
