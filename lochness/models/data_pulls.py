"""
DataPull Model
"""

from typing import Dict, Any
from pydantic import BaseModel

from lochness.helpers import db


class DataPull(BaseModel):
    """
    A Data Pull represents a download / data pull from
    a configured data source to a local file system.

    Attributes:
        subject_id (str): Subject associated with the data pull.
        data_source_name (str): Name of the data source.
        site_id (str): site associated with the data pull.
        project_id (str): Project associated with the data pull.
        file_path (str): Path to the file.
        pull_time_s (int): Time taken for the data pull in seconds.
        pull_metadata (Dict[str, Any]): Metadata associated with the data pull.
    """

    subject_id: str
    data_source_name: str
    site_id: str
    project_id: str
    file_path: str
    file_md5: str
    pull_time_s: int
    pull_metadata: Dict[str, Any]

    @staticmethod
    def init_db_table_query() -> str:
        """
        Returns the SQL query to create the database table for data pulls.
        """
        sql_query = """
            CREATE TABLE data_pull (
                data_pull_id SERIAL PRIMARY KEY,
                subject_id TEXT NOT NULL,
                data_source_name TEXT NOT NULL,
                site_id TEXT NOT NULL,
                project_id TEXT NOT NULL,
                file_path TEXT NOT NULL,
                file_md5 TEXT NOT NULL,
                pull_time_s INTEGER NOT NULL,
                pull_timestamp TIMESTAMPTZ DEFAULT NOW(),
                pull_metadata JSONB NOT NULL,
                FOREIGN KEY (subject_id, site_id, project_id)
                    REFERENCES subjects (subject_id, site_id, project_id),
                FOREIGN KEY (data_source_name, site_id, project_id)
                    REFERENCES data_sources (data_source_name, site_id, project_id),
                FOREIGN KEY (file_path, file_md5)
                    REFERENCES files (file_path, file_md5)
            );
        """
        return sql_query

    @staticmethod
    def drop_db_table_query() -> str:
        """
        Returns the SQL query to drop the database table for data pulls.
        """
        sql_query = """
            DROP TABLE IF EXISTS data_pull CASCADE;
        """
        return sql_query

    def to_sql_query(self) -> str:
        """
        Returns the SQL query to insert the data pull into the database.
        """
        subject_id = db.sanitize_string(self.subject_id)
        data_source_name = db.sanitize_string(self.data_source_name)
        site_id = db.sanitize_string(self.site_id)
        project_id = db.sanitize_string(self.project_id)
        file_path = db.sanitize_string(self.file_path)
        file_md5 = db.sanitize_string(self.file_md5)
        pull_time_s = self.pull_time_s
        pull_metadata = db.sanitize_json(self.pull_metadata)

        sql_query = f"""
            INSERT INTO data_pull (subject_id, data_source_name, site_id, project_id,
                file_path, file_md5, pull_time_s, pull_metadata)
            VALUES ('{subject_id}', '{data_source_name}', '{site_id}', '{project_id}',
                '{file_path}', '{file_md5}', {pull_time_s}, '{pull_metadata}');
        """
        return sql_query
