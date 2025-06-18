"""
Data Source Model
"""

from pathlib import Path
from typing import Any, Dict, List

from pydantic import BaseModel

from lochness.helpers import db


class XnatDataSourceMetadata(BaseModel):
    """
    Metadata for a XNAT data source.
    """

    api_token: str
    endpoint_url: str
    subject_id_variable: str
    optional_variables_dictionary: List[Dict[str, str]]


class XnatDataSource(BaseModel):
    """
    A XNAT data source is a specific type of data source that Lochness can connect to
    and pull data from.
    """

    data_source_name: str
    is_active: bool
    site_id: str
    project_id: str
    data_source_type: str
    data_source_metadata: XnatDataSourceMetadata

    @staticmethod
    def get_all_xnat_data_sources(
        config_file: Path,
        active_only: bool = True,
    ) -> List["XnatDataSource"]:
        """
        Get all active XNAT data sources.

        Returns:
            List[XnatDataSource]: A list of active XNAT data sources.
        """
        sql_query = """
            SELECT *
            FROM data_sources
            WHERE data_source_type = 'xnat'
        """

        if active_only:
            sql_query += " AND data_source_is_active = TRUE"

        df = db.execute_sql(
            config_file=config_file,
            query=sql_query,
        )

        def convert_to_xnat_data_source(row: Dict[str, Any]) -> "XnatDataSource":
            """
            Convert a row from the data_sources table to a XnatDataSource object.

            Args:
                row (Dict[str, Any]): A dictionary representing a row from the data_sources table.

            Returns:
                XnatDataSource: A XnatDataSource object.
            """
            xnat_data_source = XnatDataSource(
                data_source_name=row["data_source_name"],
                is_active=row["data_source_is_active"],
                site_id=row["site_id"],
                project_id=row["project_id"],
                data_source_type=row["data_source_type"],
                data_source_metadata=XnatDataSourceMetadata(
                    api_token=row["data_source_metadata"]["api_token"],
                    endpoint_url=row["data_source_metadata"]["endpoint_url"],
                    subject_id_variable=row["data_source_metadata"][
                        "subject_id_variable"
                    ],
                    optional_variables_dictionary=row["data_source_metadata"][
                        "optional_variables_dictionary"
                    ],
                ),
            )
            return xnat_data_source

        xnat_data_sources: List[XnatDataSource] = []

        for _, row in df.iterrows():  # type: ignore
            xnat_data_source = convert_to_xnat_data_source(row.to_dict())  # type: ignore
            xnat_data_sources.append(xnat_data_source)

        return xnat_data_sources
