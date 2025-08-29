"""
Data Source Model for CANTAB
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from lochness.helpers import db


class CANTABDataSourceMetadata(BaseModel):
    """
    Metadata for a CANTAB data source.
    """

    keystore_name: str
    api_url: str
    modality: str = 'cantab'


class CANTABDataSource(BaseModel):
    """
    A CANTAB data source is a specific type of data source that Lochness can connect to
    and pull metadata from.
    """

    data_source_name: str
    is_active: bool
    site_id: str
    project_id: str
    data_source_type: str
    data_source_metadata: CANTABDataSourceMetadata

    @staticmethod
    def get_all_cantab_data_sources(
        config_file: Path,
        active_only: bool = True,
    ) -> List["CANTABDataSource"]:
        """
        Get all active CANTAB data sources.

        Returns:
            List[CANTABDataSource]: A list of active CANTAB data sources.
        """
        sql_query = """
            SELECT *
            FROM data_sources
            WHERE data_source_type = 'cantab'
        """

        if active_only:
            sql_query += " AND data_source_is_active = TRUE"

        df = db.execute_sql(
            config_file=config_file,
            query=sql_query,
        )

        def convert_to_cantab_data_source(row: Dict[str, Any]) -> "CANTABDataSource":
            """
            Convert a row from the data_sources table to a CANTABDataSource object.

            Args:
                row (Dict[str, Any]): A dictionary representing a row from the data_sources table.

            Returns:
                CANTABDataSource: A CANTABDataSource object.
            """
            cantab_data_source = CANTABDataSource(
                data_source_name=row["data_source_name"],
                is_active=row["data_source_is_active"],
                site_id=row["site_id"],
                project_id=row["project_id"],
                data_source_type=row["data_source_type"],
                data_source_metadata=CANTABDataSourceMetadata(
                    keystore_name=row["data_source_metadata"]["keystore_name"],
                    api_url=row["data_source_metadata"]["api_url"],
                    modality=row["data_source_metadata"]["modality"],
                ),
            )
            return cantab_data_source

        cantab_data_sources: List[CANTABDataSource] = []

        for _, row in df.iterrows():  # type: ignore
            cantab_data_source = convert_to_cantab_data_source(row.to_dict())  # type: ignore
            cantab_data_sources.append(cantab_data_source)

        return cantab_data_sources

    @staticmethod
    def get(
        data_source_name: str,
        site_id: str,
        project_id: str,
        config_file: Path
    ) -> Optional["CANTABDataSource"]:
        """
        Get a specific CANTAB data source by name, site ID, and project ID.

        Args:
            data_source_name (str): The name of the data source.
            site_id (str): The site ID.
            project_id (str): The project ID.
            config_file (Path): Path to the Lochness configuration file.

        Returns:
            Optional[CANTABDataSource]: The CANTAB data source if found, else None.
        """
        all_data_sources = CANTABDataSource.get_all_cantab_data_sources(
            config_file=config_file,
            active_only=False,
        )

        for data_source in all_data_sources:
            if (
                data_source.data_source_name == data_source_name
                and data_source.site_id == site_id
                and data_source.project_id == project_id
            ):
                return data_source

        return None
