"""
Logs Model
"""

from pathlib import Path
from typing import Optional, List
from pydantic import BaseModel

from lochness.helpers import db


class Logs(BaseModel):
    """
    Logs Model
    """

    subject_id: Optional[str] = None
    site_id: Optional[str] = None
    project_id: Optional[str] = None
    data_source_type: Optional[str] = None
    data_source_name: Optional[str] = None
    log_message: str
    log_level: str

    @staticmethod
    def init_db_table_query() -> List[str]:
        """
        Returns the SQL query to create the database table for logs.
        """
        log_level_type = """
            CREATE TYPE log_level
            AS ENUM ('DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL');
        """
        sql_query = """
            CREATE TABLE logs (
                log_id SERIAL PRIMARY KEY,
                subject_id TEXT,
                site_id TEXT,
                project_id TEXT,
                data_source_type TEXT,
                data_source_name TEXT,
                log_message TEXT STORAGE EXTENDED NOT NULL,
                log_level log_level NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                FOREIGN KEY (site_id, project_id)
                    REFERENCES sites(site_id, project_id)
                    ON DELETE SET NULL,
                FOREIGN KEY (subject_id, site_id, project_id)
                    REFERENCES subjects(subject_id, site_id, project_id)
                    ON DELETE SET NULL,
                FOREIGN KEY (data_source_name, site_id, project_id)
                    REFERENCES data_sources(data_source_name, site_id, project_id)
                    ON DELETE SET NULL
            );
        """
        index_queries = [
            "CREATE INDEX idx_logs_subject_id ON logs (subject_id);",
            "CREATE INDEX idx_logs_site_id ON logs (site_id);",
            "CREATE INDEX idx_logs_data_source_type ON logs (data_source_type);",
        ]

        init_query = [log_level_type, sql_query] + index_queries
        return init_query

    @staticmethod
    def drop_db_table_query() -> str:
        """
        Returns the SQL query to drop the database table for logs.
        """
        sql_query = """
            DROP TABLE IF EXISTS logs;
            DROP TYPE IF EXISTS log_level;
        """

        return sql_query

    def to_sql_query(self) -> str:
        """
        Converts the Logs instance to a SQL insert statement.
        """

        if self.subject_id is None:
            subject_id = "NULL"
        else:
            subject_id = f"'{self.subject_id}'"

        if self.site_id is None:
            site_id = "NULL"
        else:
            site_id = f"'{self.site_id}'"

        if self.project_id is None:
            project_id = "NULL"
        else:
            project_id = f"'{self.project_id}'"

        if self.data_source_type is None:
            data_source_type = "NULL"
        else:
            data_source_type = f"'{self.data_source_type}'"

        if self.data_source_name is None:
            data_source_name = "NULL"
        else:
            data_source_name = f"'{self.data_source_name}'"

        sql_query = f"""
            INSERT INTO logs (
                subject_id, site_id, project_id, data_source_type, data_source_name,
                log_message, log_level
            ) VALUES (
                '{subject_id}', '{site_id}', '{project_id}',
                '{data_source_type}', '{data_source_name}',
                '{db.sanitize_string(self.log_message)}', '{self.log_level}'
            );
        """
        sql_query = db.handle_null(sql_query)
        return sql_query

    def insert(self, config_file: Path) -> None:
        """
        Inserts the log entry into the database.
        Args:
            config_file (Path): Path to the configuration file.
        """

        insert_query = self.to_sql_query()
        db.execute_queries(  # type: ignore
            config_file=config_file,
            queries=[insert_query],
        )
