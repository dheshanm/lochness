"""
DataPush Model
"""

from typing import Dict, Any
from pydantic import BaseModel

from lochness.helpers import db


class DataPush(BaseModel):
    """
    A Data Push represents an upload / data push from
    a local file system to a configured data sink.

    Attributes:
        file_path (str): Path to the file that was pushed.
        file_md5 (str): MD5 hash of the file that was pushed.
        data_sink_name (str): Name of the data sink the file was pushed to.
        site_id (str): Site associated with the data push.
        project_id (str): Project associated with the data push.
        push_time_s (int): Time taken for the data push in seconds.
        push_metadata (Dict[str, Any]): Metadata associated with the data push.
    """

    file_path: str
    file_md5: str
    data_sink_name: str
    site_id: str
    project_id: str
    push_time_s: int
    push_metadata: Dict[str, Any]

    @staticmethod
    def init_db_table_query() -> str:
        """
        Returns the SQL query to create the database table for data pushes.
        """
        sql_query = """
            CREATE TABLE IF NOT EXISTS data_pushes (
                data_push_id SERIAL PRIMARY KEY,
                file_path TEXT NOT NULL,
                file_md5 TEXT NOT NULL,
                data_sink_name TEXT NOT NULL,
                site_id TEXT NOT NULL,
                project_id TEXT NOT NULL,
                push_time_s INTEGER NOT NULL,
                push_timestamp TIMESTAMPTZ DEFAULT NOW(),
                push_metadata JSONB NOT NULL,
                FOREIGN KEY (file_path, file_md5)
                    REFERENCES files (file_path, file_md5),
                FOREIGN KEY (data_sink_name, site_id, project_id)
                    REFERENCES data_sinks (data_sink_name, site_id, project_id)
            );
        """
        return sql_query

    @staticmethod
    def drop_db_table_query() -> str:
        """
        Returns the SQL query to drop the database table for data pushes.
        """
        sql_query = """
            DROP TABLE IF EXISTS data_pushes CASCADE;
        """
        return sql_query

    def to_sql_query(self) -> str:
        """
        Returns the SQL query to insert the data push into the database.
        """
        file_path = db.sanitize_string(self.file_path)
        file_md5 = db.sanitize_string(self.file_md5)
        data_sink_name = db.sanitize_string(self.data_sink_name)
        site_id = db.sanitize_string(self.site_id)
        project_id = db.sanitize_string(self.project_id)
        push_time_s = self.push_time_s
        push_metadata = db.sanitize_json(self.push_metadata)

        sql_query = f"""
            INSERT INTO data_pushes (file_path, file_md5, data_sink_name, site_id, project_id,
                push_time_s, push_metadata)
            VALUES ('{file_path}', '{file_md5}', '{data_sink_name}', '{site_id}', '{project_id}',
                {push_time_s}, '{push_metadata}');
        """
        return sql_query

    def insert(self, config_file: Path) -> None:
        """
        Inserts the data push entry into the database.
        Args:
            config_file (Path): Path to the configuration file.
        """
        insert_query = self.to_sql_query()
        db.execute_queries(
            config_file=config_file,
            queries=[insert_query],
            show_commands=False,
            silent=True,
        )
