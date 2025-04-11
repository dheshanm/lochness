"""
Initializes the Database for Lochness.
"""

from typing import no_type_check, List, Union
from pathlib import Path

from lochness.helpers import db

from lochness.models.logs import Logs
from lochness.models.files import File
from lochness.models.projects import Project
from lochness.models.sites import Site
from lochness.models.subjects import Subject
from lochness.models.data_source import DataSource
from lochness.models.supported_data_source_types import (
    SupportedDataSourceTypes,
    populate_supported_data_source_types,
)
from lochness.models.keystore import KeyStore
from lochness.models.data_pulls import DataPull
from lochness.models.data_sinks import DataSink
from lochness.models.data_push import DataPush
from lochness.models.metrics import Metrics


@no_type_check
def flatten_list(coll: list) -> list:
    """
    Flattens a list of lists into a single list.

    Args:
        coll (list): List of lists.

    Returns:
        list: Flattened list.
    """
    flat_list = []
    for i in coll:
        if isinstance(i, list):
            flat_list += flatten_list(i)
        else:
            flat_list.append(i)
    return flat_list


def init_db(config_file: Path):
    """
    Initializes the database.

    WARNING: This will drop all tables and recreate them.
    DO NOT RUN THIS IN PRODUCTION.

    Args:
        config_file (Path): Path to the config file.
    """
    drop_queries_l: List[Union[str, List[str]]] = [
        Logs.drop_db_table_query(),
        Metrics.drop_db_table_query(),
        DataPush.drop_db_table_query(),
        DataSink.drop_db_table_query(),
        DataPull.drop_db_table_query(),
        File.drop_db_table_query(),
        DataSource.drop_db_table_query(),
        SupportedDataSourceTypes.drop_db_table_query(),
        Subject.drop_db_table_query(),
        Site.drop_db_table_query(),
        Project.drop_db_table_query(),
        KeyStore.drop_db_table_query(),
    ]

    create_queries_l: List[Union[str, List[str]]] = [
        Project.init_db_table_query(),
        Site.init_db_table_query(),
        Subject.init_db_table_query(),
        SupportedDataSourceTypes.init_db_table_query(),
        populate_supported_data_source_types(),
        DataSource.init_db_table_query(),
        KeyStore.init_db_table_query(),
        Logs.init_db_table_query(),
        File.init_db_table_query(),
        DataPull.init_db_table_query(),
        DataSink.init_db_table_query(),
        DataPush.init_db_table_query(),
        Metrics.init_db_table_query(),
    ]

    drop_queries: List[str] = flatten_list(drop_queries_l)
    create_queries: List[str] = flatten_list(create_queries_l)

    sql_queries: List[str] = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries)  # type: ignore
