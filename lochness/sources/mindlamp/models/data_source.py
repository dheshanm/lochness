"""
Data Source Model for MindLAMP
"""

from pathlib import Path
from typing import Any, Dict, List

from pydantic import BaseModel

from lochness.helpers import db


class MindLAMPDataSourceMetadata(BaseModel):
    """
    Metadata for a MindLAMP data source.
    """

    keystore_name: str
    api_url: str


class MindLAMPDataSource(BaseModel):
    """
    A MindLAMP data source is a specific type of data source that Lochness can connect to
    and pull metadata from.
    """

    data_source_name: str
    is_active: bool
    site_id: str
    project_id: str
    data_source_type: str
    data_source_metadata: MindLAMPDataSourceMetadata

    @staticmethod
    def get_all_mindlamp_data_sources(
        config_file: Path,
        active_only: bool = True,
    ) -> List["MindLAMPDataSource"]:
        """
        Get all active MindLAMP data sources.

        Returns:
            List[MindLAMPDataSource]: A list of active MindLAMP data sources.
        """
        sql_query = """
            SELECT *
            FROM data_sources
            WHERE data_source_type = 'mindlamp'
        """

        if active_only:
            sql_query += " AND data_source_is_active = TRUE"

        df = db.execute_sql(
            config_file=config_file,
            query=sql_query,
        )

        def convert_to_mindlamp_data_source(row: Dict[str, Any]) -> "MindLAMPDataSource":
            """
            Convert a row from the data_sources table to a MindLAMPDataSource object.

            Args:
                row (Dict[str, Any]): A dictionary representing a row from the data_sources table.

            Returns:
                MindLAMPDataSource: A MindLAMPDataSource object.
            """
            mindlamp_data_source = MindLAMPDataSource(
                data_source_name=row["data_source_name"],
                is_active=row["data_source_is_active"],
                site_id=row["site_id"],
                project_id=row["project_id"],
                data_source_type=row["data_source_type"],
                data_source_metadata=MindLAMPDataSourceMetadata(
                    keystore_name=row["data_source_metadata"]["keystore_name"],
                    api_url=row["data_source_metadata"]["api_url"],
                ),
            )
            return mindlamp_data_source

        mindlamp_data_sources: List[MindLAMPDataSource] = []

        for _, row in df.iterrows():  # type: ignore
            mindlamp_data_source = convert_to_mindlamp_data_source(row.to_dict())  # type: ignore
            mindlamp_data_sources.append(mindlamp_data_source)

        return mindlamp_data_sources 