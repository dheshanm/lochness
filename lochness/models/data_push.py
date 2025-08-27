"""
Data Push represents a data push from a local file system to a
configured data sink. (Typically a Object Store)
"""

from pathlib import Path
from typing import Any, Dict

from pydantic import BaseModel

from lochness.helpers import db


class DataPush(BaseModel):
    """
    A Data Push of a single file to a configured data sink.

    Attributes:
        data_sink_name (str): Name of the data sink.
        file_path (str): Path to the file.
        file_md5 (str): MD5 hash of the file.
        push_time_s (int): Time taken for the data push in seconds.
        push_metadata (Dict[str, Any]): Metadata associated with the data push.
    """

    data_sink_id: int
    file_path: str | Path
    file_md5: str
    push_time_s: int
    push_metadata: Dict[str, Any]
    push_timestamp: str

    @staticmethod
    def init_db_table_query() -> str:
        """
        Returns the SQL query to create the database table for data pushes.
        """
        sql_query = """
            CREATE TABLE IF NOT EXISTS data_push (
                data_push_id SERIAL PRIMARY KEY,
                data_sink_id INTEGER REFERENCES data_sinks(data_sink_id),
                file_path TEXT NOT NULL,
                file_md5 TEXT NOT NULL,
                push_time_s INTEGER NOT NULL,
                push_timestamp TIMESTAMPTZ DEFAULT NOW(),
                push_metadata JSONB NOT NULL,
                FOREIGN KEY (file_path, file_md5)
                    REFERENCES files (file_path, file_md5)
            );
        """
        return sql_query

    @staticmethod
    def drop_db_table_query() -> str:
        """
        Returns the SQL query to drop the database table for data pushes.
        """
        sql_query = """
            DROP TABLE IF EXISTS data_push;
        """

        return sql_query

    def __str__(self) -> str:
        """
        Returns a string representation of the data push.
        """
        return f"[Data Push: {self.file_path} | Data Sink ID: {self.data_sink_id}]"

    def __repr__(self) -> str:
        """ "
        Returns a string representation of the data push.
        """
        return self.__str__()

    def to_sql_query(self) -> str:
        """
        Returns the SQL query to insert the data push into the database.
        """
        file_path = db.sanitize_string(str(self.file_path))
        file_md5 = db.sanitize_string(self.file_md5)
        push_time_s = self.push_time_s
        push_metadata = db.sanitize_json(self.push_metadata)

        sql_query = f"""
            INSERT INTO data_push (data_sink_id, file_path, file_md5, push_time_s, push_metadata)
            VALUES ({self.data_sink_id}, '{file_path}', '{file_md5}', {push_time_s}, '{push_metadata}');
        """

        return sql_query

    def delete_record_query(self, data_sink_id) -> str:
        """Generate a query to delete a record from the table"""
        query = f"""DELETE FROM data_push
        WHERE data_sink_id = '{data_sink_id}'
          AND file_path = '{self.file_path}'
          AND file_md5 = '{self.file_md5}';"""
        return query
