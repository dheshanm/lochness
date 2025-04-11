"""
Data Sinks refer to the destination systems or services where data is
pushed or stored after aquisition. This can include Object Stores,
File Systems, or any other storage solutions.
"""

from typing import Dict, Any
from pydantic import BaseModel

from lochness.helpers import db


class DataSink(BaseModel):
    """
    A DataSink referes to a place where data is to be aggregated.

    Attributes:
        data_sink_name (str): Name of the data sink.
        data_sink_metadata (Dict[str, Any]): Metadata associated with the data sink.
    """

    data_sink_name: str
    site_id: str
    project_id: str
    data_sink_metadata: Dict[str, Any]

    @staticmethod
    def init_db_table_query() -> str:
        """
        Returns the SQL query to create the database table for data sinks.
        """
        sql_query = """
            CREATE TABLE data_sinks (
            data_sink_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            site_id TEXT NOT NULL,
            project_id TEXT NOT NULL,
            data_sink_name TEXT,
            data_sink_metadata JSONB NOT NULL,
            FOREIGN KEY (site_id, project_id)
                REFERENCES sites (site_id, project_id)
            );
        """

        return sql_query

    @staticmethod
    def drop_db_table_query() -> str:
        """
        Returns the SQL query to drop the database table for data sinks.
        """
        sql_query = """
            DROP TABLE IF EXISTS data_sinks;
        """

        return sql_query

    def __str__(self) -> str:
        return (
            f"[Data Sink: {self.data_sink_name} | site ID: {self.site_id} | "
            f"Project ID: {self.project_id}]"
        )

    def __repr__(self) -> str:
        return self.__str__()

    def to_sql_query(self) -> str:
        """
        Returns the SQL query to insert the data sink into the database.
        """

        metadata_str = db.sanitize_json(self.data_sink_metadata)
        data_sink_name = db.sanitize_string(self.data_sink_name)

        sql_query = f"""
            INSERT INTO data_sinks (data_sink_name, site_id, project_id, data_sink_metadata)
            VALUES ('{data_sink_name}', '{self.site_id}', '{self.project_id}', '{metadata_str}')
            ON CONFLICT (site_id, project_id) DO NOTHING;
        """
        return sql_query
