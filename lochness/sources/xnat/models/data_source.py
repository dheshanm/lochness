"""
Data Source Model
"""

import json
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
    def init_db_table_query() -> str:
        """
        Returns the SQL query to create the database table for data sources.
        """
        sql_query = """
            CREATE TABLE IF NOT EXISTS data_sources (
                data_source_name TEXT NOT NULL,
                data_source_is_active BOOLEAN DEFAULT TRUE NOT NULL,
                site_id TEXT NOT NULL,
                project_id TEXT NOT NULL,
                data_source_type TEXT NOT NULL,
                data_source_metadata JSONB NOT NULL,
                PRIMARY KEY (data_source_name),
                FOREIGN KEY (site_id, project_id) REFERENCES sites(site_id, project_id)
            );
        """
        return sql_query

    @staticmethod
    def drop_db_table_query() -> str:
        """
        Returns the SQL query to drop the database table for data sources.
        """
        sql_query = """
            DROP TABLE IF EXISTS data_sources;
        """
        return sql_query

    def to_sql_query(self) -> str:
        """
        Returns the SQL query to insert the data source into the database.
        """
        data_source_metadata = db.sanitize_json(json.loads(self.data_source_metadata.model_dump_json()))
        sql_query = f"""
            INSERT INTO data_sources (
                data_source_name,
                data_source_is_active,
                site_id,
                project_id,
                data_source_type,
                data_source_metadata
            ) VALUES (
                '{self.data_source_name}',
                {self.is_active},
                '{self.site_id}',
                '{self.project_id}',
                '{self.data_source_type}',
                '{data_source_metadata}'
            )
            ON CONFLICT (data_source_name) DO UPDATE SET
                data_source_is_active = EXCLUDED.data_source_is_active,
                site_id = EXCLUDED.site_id,
                project_id = EXCLUDED.project_id,
                data_source_type = EXCLUDED.data_source_type,
                data_source_metadata = EXCLUDED.data_source_metadata;
        """
        return sql_query



    @staticmethod
    def get_all_xnat_data_sources(
        config_file: Path,
        encryption_passphrase: str,
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
        print(df)

        def convert_to_xnat_data_source(row: Dict[str, Any]) -> "XnatDataSource":
            """
            Convert a row from the data_sources table to a XnatDataSource object.

            Args:
                row (Dict[str, Any]): A dictionary representing a row from the data_sources table.

            Returns:
                XnatDataSource: A XnatDataSource object.
            """
            from lochness.models.keystore import KeyStore
            query = KeyStore.retrieve_key_query(row["data_source_name"], row["project_id"], encryption_passphrase)
            api_token_df = db.execute_sql(config_file, query)
            api_token = api_token_df['key_value'][0]

            xnat_data_source = XnatDataSource(
                data_source_name=row["data_source_name"],
                is_active=row["data_source_is_active"],
                site_id=row["site_id"],
                project_id=row["project_id"],
                data_source_type=row["data_source_type"],
                data_source_metadata=XnatDataSourceMetadata(
                    api_token=api_token,
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
