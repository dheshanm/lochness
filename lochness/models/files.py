#!/usr/bin/env python
"""
File Model
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

from lochness.helpers import db
from lochness.helpers.hash import compute_hash
from lochness.models.logs import Logs


class File:
    """
    Represents a file.

    Attributes:
        file_path (Path): The path to the file.
    """

    def __init__(
        self,
        file_path: Path,
        with_hash: bool = True
    ):
        """
        Initialize a File object.

        Args:
            file_path (Path): The path to the file.
        """
        self.file_path = file_path

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        self.file_name = file_path.name
        self.file_type = file_path.suffix
        if self.file_type == ".lock":
            # Use previous suffix for lock files
            self.file_type = file_path.suffixes[-2]

        self.file_size_mb = file_path.stat().st_size / 1024 / 1024
        self.m_time = datetime.fromtimestamp(file_path.stat().st_mtime)
        if with_hash:
            self.md5 = compute_hash(file_path=file_path, hash_type="md5")
        else:
            self.md5 = None

    def __str__(self):
        """
        Return a string representation of the File object.
        """
        return f"File({self.file_name}, {self.file_type}, {self.file_size_mb}, \
            {self.file_path}, {self.m_time}, {self.md5})"

    def __repr__(self):
        """
        Return a string representation of the File object.
        """
        return self.__str__()

    @staticmethod
    def init_db_table_query() -> str:
        """
        Return the SQL query to create the 'files' table.
        """
        sql_query = """
        CREATE TABLE IF NOT EXISTS files (
            file_name TEXT NOT NULL,
            file_type TEXT NOT NULL,
            file_size_mb FLOAT NOT NULL,
            file_path TEXT NOT NULL,
            file_m_time TIMESTAMP NOT NULL,
            file_md5 TEXT NOT NULL,
            PRIMARY KEY (file_path, file_md5)
        );
        """

        return sql_query

    @staticmethod
    def drop_db_table_query() -> str:
        """
        Return the SQL query to drop the 'files' table if it exists.
        """
        sql_query = """
        DROP TABLE IF EXISTS files CASCADE;
        """

        return sql_query

    def to_sql_query(self):
        """
        Return the SQL query to insert the File object into the 'files' table.
        """
        f_name = db.sanitize_string(self.file_name)
        f_path = db.sanitize_string(str(self.file_path))

        if self.md5 is None:
            hash_val = "NULL"
        else:
            hash_val = self.md5

        sql_query = f"""
        INSERT INTO files (file_name, file_type, file_size_mb,
            file_path, file_m_time, file_md5)
        VALUES ('{f_name}', '{self.file_type}', '{self.file_size_mb}',
            '{f_path}', '{self.m_time}', '{hash_val}')
        ON CONFLICT (file_path, file_md5) DO UPDATE SET
            file_name = excluded.file_name,
            file_type = excluded.file_type,
            file_size_mb = excluded.file_size_mb,
            file_m_time = excluded.file_m_time;
        """

        sql_query = db.handle_null(sql_query)

        return sql_query

    @staticmethod
    def get_all_files_in_df(config_file: Path) -> pd.DataFrame:
        """
        Return the files to push for a given project and site.
        """
        sql_query = "SELECT * FROM files"""
        sql_query = db.handle_null(sql_query)
        files_df = db.execute_sql(config_file, sql_query)

        return files_df

    @staticmethod
    def get_all_files_in_df_detailed(config_file: Path) -> pd.DataFrame:
        """
        Return the files to push for a given project and site.
        """
        sql_query = """
        SELECT * FROM files
        LEFT JOIN data_pull on (data_pull.file_path = files.file_path AND
        data_pull.file_md5 = files.file_md5)
        """
        sql_query = db.handle_null(sql_query)
        files_df = db.execute_sql(config_file, sql_query)

        return files_df
    
    @staticmethod
    def get_files_to_push(config_file: Path) -> list['File']:
        """
        Return the files to push for a given project and site.
        """
        files_df = File.get_all_files_in_df(config_file)
        files_to_push = []
        for _, row in files_df.iterrows():
            try:
                file_path = Path(row["file_path"])
                file_md5 = row["file_md5"]
                file_obj = File(file_path=file_path, with_hash=False)
                file_obj.md5 = file_md5

                # extra
                file_obj.data_source_name = row["data_source_name"]
                file_obj.subject_id = row["subject_id"]
                file_obj.site_id = row["site_id"]
                file_obj.project_id = row["project_id"]
                files_to_push.append(file_obj)
            except FileNotFoundError:
                Logs(
                    log_level="WARN",
                    log_message={
                        "event": "data_push_file_not_found",
                        "message": f"File not found on disk, skipping: {row['file_path']}",
                        "file_path": row["file_path"],
                    },
                ).insert(config_file)
        return files_to_push

    def delete_record_query(self) -> str:
        """Generate a query to delete a record from the table"""
        query = f"""DELETE FROM files
        WHERE file_path = '{self.file_path}'
          AND file_md5 = '{self.md5}';"""
        return query
