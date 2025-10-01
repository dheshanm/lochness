#!/usr/bin/env python
"""
Pulls data from CANTAB for active data sources and subjects.

This script is intended to be run as a cron job.
It will pull data for all active CANTAB data sources and their associated subjects.
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
import logging
import json
from typing import Any, Dict, List, Optional

from rich.logging import RichHandler

from lochness.helpers import logs, utils, db, config
from lochness.helpers.timer import Timer
from lochness.models.logs import Logs
from lochness.models.files import File
from lochness.sources.cantab import api as cantab_api
from lochness.models.subjects import Subject
from lochness.models.data_pulls import DataPull
from lochness.sources.cantab.models.data_source import CANTABDataSource

MODULE_NAME = "lochness.sources.cantab.tasks.pull_data"

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)
logs.silence_logs(["urllib3.connectionpool"])


def get_subject_cantab_data_root(
    subject_id: str, cantab_data_source: CANTABDataSource, config_file: Path
) -> Path:
    """
    Get the root directory for storing CANTAB data for a subject.

    Args:
        subject_id (str): The subject ID.
        cantab_data_source (CANTABDataSource): The CANTAB data source object.
        config_file (Path): Path to the configuration file.

    Returns:
        Path: The root directory for storing CANTAB data for the subject.
    """
    lochness_root: str = config.parse(config_file, "general")["lochness_root"]  # type: ignore

    project_id = cantab_data_source.project_id
    site_id = cantab_data_source.site_id

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
        / "cantab"
    )

    return output_dir


def get_subject_cantab_id(
    subject: Subject, data_source: CANTABDataSource
) -> Optional[str]:
    """
    Retrieve the CANTAB ID for a given subject from their metadata.

    Args:
        subject (Subject): The subject to retrieve the CANTAB ID for.
        data_source (CANTABDataSource): The CANTAB data source to use.

    Returns:
        Optional[str]: The CANTAB ID if found, otherwise None.
    """
    cantab_metadata = subject.subject_metadata.get("cantab", {})
    data_source_metadata = cantab_metadata.get(data_source.data_source_name, {})
    cantab_id = data_source_metadata.get("cantab_id", None)
    return cantab_id


def pull_data_for_subject(
    config_file: Path,
    data_source: CANTABDataSource,
    subject: Subject,
) -> List[DataPull]:
    """
    Pull data for a specific subject from a CANTAB data source.

    Args:
        config_file (Path): Path to the configuration file.
        data_source (CANTABDataSource): The CANTAB data source to pull data from.
        subject (Subject): The subject to pull data for.

    Returns:
        List[DataPull]: A list of DataPull records created for the subject.
    """
    associated_files: List[File] = []
    data_pulls: List[DataPull] = []
    subject_cantab_id = get_subject_cantab_id(subject=subject, data_source=data_source)

    if not subject_cantab_id:
        logger.warning(
            f"No CANTAB ID found for subject: "
            f"{subject.subject_id} "
            f"from data source: "
            f"{data_source.data_source_name}"
        )
        return data_pulls

    with Timer() as timer:
        cantab_data = cantab_api.get_cantab_data(
            cantab_data_source=data_source,
            cantab_id=subject_cantab_id,
            config_file=config_file,
        )

    cantab_data_root = get_subject_cantab_data_root(
        subject_id=subject.subject_id,
        cantab_data_source=data_source,
        config_file=config_file,
    )
    data_file_name = f"{subject.subject_id}.{data_source.data_source_name}.json"
    cantab_data_root.mkdir(parents=True, exist_ok=True)
    data_file_path = cantab_data_root / data_file_name

    with open(data_file_path, "w", encoding="utf-8") as f:
        json.dump(cantab_data, f, indent=4)

    data_file = File(file_path=data_file_path)
    associated_files.append(data_file)

    data_pull = DataPull(
        subject_id=subject.subject_id,
        project_id=data_source.project_id,
        site_id=data_source.site_id,
        data_source_name=data_source.data_source_name,
        file_path=str(data_file_path),
        file_md5=data_file.md5,  # type: ignore
        pull_time_s=int(timer.duration),  # type: ignore
        pull_metadata={"cantab_id": subject_cantab_id},
    )
    data_pulls.append(data_pull)

    logger.info(
        (
            f"Pulled data for subject: {subject.subject_id} "
            f"from data source: {data_source.data_source_name} "
            f"in {timer.duration:.2f} seconds. "
            f"Data saved to: {data_file_path}"
        )
    )

    queries: List[str] = []
    for file in associated_files:
        file_query = file.to_sql_query()
        queries.append(file_query)

    db.execute_queries(
        config_file=config_file,
        queries=queries,
        show_commands=False,
    )

    return data_pulls


def pull_data_for_data_source(
    config_file: Path,
    data_source: CANTABDataSource,
) -> None:
    """
    Pull data for a specific CANTAB data source.

    Args:
        config_file (Path): Path to the configuration file.
        data_source (CANTABDataSource): The CANTAB data source to pull data for.

    Returns:
        None
    """
    data_source_filter: Dict[str, Any] = {
        f"cantab.{data_source.data_source_name}": None
    }
    subjects_to_pull = Subject.get_by_filter(
        project_id=data_source.project_id,
        site_id=data_source.site_id,
        filters=data_source_filter,
        config_file=config_file,
    )
    if not subjects_to_pull:
        logger.warning(
            f"No subjects found for data source: {data_source.data_source_name}"
        )
        Logs(
            log_level="INFO",
            log_message={
                "event": "cantab_data_pull_no_subjects",
                "message": f"No subjects found for data source: {data_source.data_source_name}",
                "data_source": data_source.data_source_name,
                "project_id": data_source.project_id,
                "site_id": data_source.site_id,
            },
        ).insert(config_file)
        return

    logger.info(
        f"Found {len(subjects_to_pull)} subjects for data source: {data_source.data_source_name}"
    )

    for subject in subjects_to_pull:
        logger.info(
            "Pulling data for subject "
            f"{subject.subject_id} "
            "from data source "
            f"{data_source.data_source_name}"
        )
        try:
            data_pulls = pull_data_for_subject(
                config_file=config_file,
                data_source=data_source,
                subject=subject,
            )

            if data_pulls:
                queries: List[str] = [
                    data_pull.to_sql_query() for data_pull in data_pulls
                ]
                db.execute_queries(
                    config_file=config_file,
                    queries=queries,
                    show_commands=False,
                )
                Logs(
                    log_level="INFO",
                    log_message={
                        "event": "cantab_data_pull_subject_complete",
                        "message": (
                            f"Fetched {len(data_pulls)} data pulls for subject "
                            f"{subject.subject_id} in project {data_source.project_id} "
                            f"and site {data_source.site_id}."
                        ),
                        "subject_id": subject.subject_id,
                        "data_source_name": data_source.data_source_name,
                        "project_id": data_source.project_id,
                        "site_id": data_source.site_id,
                    },
                ).insert(config_file)
        except Exception as e:  # pylint: disable=broad-except
            logger.error(
                (
                    f"Error pulling data for subject {subject.subject_id} "
                    f"from data source {data_source.data_source_name}: "
                    f"{e}"
                )
            )

    logger.info(f"Completed data pull for data source: {data_source.data_source_name}")


def pull_all_data(
    config_file: Path,
    project_id: Optional[str] = None,
    site_id: Optional[str] = None,
) -> None:
    """
    Pull data from all active CANTAB data sources.

    Args:
        config_file (Path): Path to the configuration file.
        project_id (Optional[str]): Project ID to filter by.
        site_id (Optional[str]): Site ID to filter by.

    Returns:
        None
    """

    active_cantab_data_sources: List[CANTABDataSource] = (
        CANTABDataSource.get_all_cantab_data_sources(
            config_file=config_file,
            active_only=True,
        )
    )

    if project_id:
        active_cantab_data_sources = [
            ds for ds in active_cantab_data_sources if ds.project_id == project_id
        ]

    if site_id:
        active_cantab_data_sources = [
            ds for ds in active_cantab_data_sources if ds.site_id == site_id
        ]

    if not active_cantab_data_sources:
        logger.warning("No active CANTAB data sources found.")
        Logs(
            log_level="INFO",
            log_message={
                "event": "cantab_data_pull_no_active_sources",
                "message": "No active CANTAB data sources found for data pull.",
                "project_id": project_id,
                "site_id": site_id,
            },
        ).insert(config_file)
        return

    for data_source in active_cantab_data_sources:
        logger.info(f"Pulling data for data source: {data_source.data_source_name}")
        pull_data_for_data_source(
            config_file=config_file,
            data_source=data_source,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pull data from CANTAB data sources")
    parser.add_argument("-p", "--project-id", type=str, help="Project ID to filter by")
    parser.add_argument("-s", "--site-id", type=str, help="Site ID to filter by")

    args = parser.parse_args()

    config_file = utils.get_config_file_path()
    if not config_file.exists():
        logger.error(f"Config file not found: {config_file}")
        sys.exit(1)

    logs.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    project_id = args.project_id
    site_id = args.site_id

    pull_all_data(
        config_file=config_file,
        project_id=project_id,
        site_id=site_id,
    )
