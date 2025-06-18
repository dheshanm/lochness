"""
Data Source Model
"""

from typing import Dict, Any
from pydantic import BaseModel

from lochness.helpers import db


class DataSource(BaseModel):
    """
    A data source is any external source of data, that Lochnes
    can connect to and pull data from.
    """

    data_source_name: str
    is_active: bool
    site_id: str
    project_id: str
    data_source_type: str
    data_source_metadata: Dict[str, Any]

    @staticmethod
    def init_db_table_query() -> str:
        """
        Returns the SQL query to create the database table for data sources.
        """
        sql_query = """
            CREATE TABLE data_sources (
                data_source_name TEXT NOT NULL,
                data_source_is_active BOOLEAN DEFAULT TRUE,
                site_id TEXT NOT NULL,
                project_id TEXT NOT NULL,
                data_source_type TEXT REFERENCES supported_data_source_types(data_source_type),
                data_source_metadata JSONB NOT NULL,
                PRIMARY KEY (data_source_name, site_id, project_id),
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

    def __str__(self) -> str:
        return (
            f"[Data Source: {self.data_source_name} {self.data_source_type} | "
            f"site ID: {self.site_id} | Project ID: {self.project_id}]"
        )

    def __repr__(self) -> str:
        return self.__str__()

    def to_sql_query(self) -> str:
        """
        Returns the SQL query to insert the data source into the database.
        """
        data_source_metadata = db.sanitize_json(self.data_source_metadata)
        sql_query = f"""
            INSERT INTO data_sources (
                data_source_name, is_active, site_id,
                project_id, data_source_type, data_source_metadata
            ) VALUES (
                '{self.data_source_name}', {self.is_active}, '{self.site_id}',
                '{self.project_id}', '{self.data_source_type}', '{data_source_metadata}'
            ) ON CONFLICT (data_source_name, site_id, project_id)
                DO UPDATE SET
                data_source_type = EXCLUDED.data_source_type,
                data_source_metadata = EXCLUDED.data_source_metadata;
        """

        return sql_query
