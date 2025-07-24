"""
Subject Model
"""

from typing import Dict, Any, List
from pydantic import BaseModel
from pathlib import Path

from lochness.helpers import db


class Subject(BaseModel):
    """
    A subject is a unique entity in a site, such as a patient or a sample.
    It is identified by a unique subject ID within the context of a site.

    Attributes:
        subject_id (str): Unique identifier for the subject.
        site_id (str): Unique identifier for the site.
        project_id (str): Unique identifier for the project.
        subject_metadata (Dict[str, Any]): Metadata associated with the subject.
    """

    subject_id: str
    site_id: str
    project_id: str
    subject_metadata: Dict[str, Any]

    @staticmethod
    def init_db_table_query() -> str:
        """
        Returns the SQL query to create the database table for subjects.
        """
        sql_query = """
            CREATE TABLE subjects (
                subject_id TEXT NOT NULL,
                site_id TEXT NOT NULL,
                project_id TEXT NOT NULL,
                subject_metadata JSONB NOT NULL,
                PRIMARY KEY (subject_id, site_id, project_id),
                FOREIGN KEY (site_id, project_id) REFERENCES sites(site_id, project_id)
            );
        """

        return sql_query

    @staticmethod
    def drop_db_table_query() -> str:
        """
        Returns the SQL query to drop the database table for subjects.
        """
        sql_query = """
            DROP TABLE IF EXISTS subjects;
        """

        return sql_query

    def __str__(self) -> str:
        """
        Returns a string representation of the subject.
        """
        return (
            f"[Subject: {self.subject_id} | site ID: {self.site_id} | "
            f"Project ID: {self.project_id}]"
        )

    def __repr__(self) -> str:
        """
        Returns a string representation of the subject.
        """
        return self.__str__()

    def to_sql_query(self) -> str:
        """
        Returns the SQL query to insert the subject into the database.
        """
        subject_metadata = db.sanitize_json(self.subject_metadata)
        sql_query = f"""
            INSERT INTO subjects (
                subject_id, site_id, project_id,
                subject_metadata
            ) VALUES (
                '{self.subject_id}', '{self.site_id}', '{self.project_id}',
                '{subject_metadata}'
            ) ON CONFLICT (subject_id, site_id, project_id) DO UPDATE
            SET subject_metadata = EXCLUDED.subject_metadata
            WHERE subjects.subject_metadata IS DISTINCT FROM EXCLUDED.subject_metadata;
        """

        return sql_query

    @staticmethod
    def get_subjects_for_project_site(project_id: str, site_id: str, config_file: Path) -> List["Subject"]:
        """
        Retrieves subjects for a given project and site from the database.

        Args:
            project_id (str): The project ID.
            site_id (str): The site ID.
            config_file (Path): Path to the configuration file.

        Returns:
            List[Subject]: A list of Subject objects.
        """
        query = f"SELECT subject_id, site_id, project_id, subject_metadata FROM subjects WHERE project_id = '{project_id}' AND site_id = '{site_id}';"
        subjects_df = db.execute_sql(config_file, query)

        subjects: List[Subject] = []
        for _, row in subjects_df.iterrows():
            subject = Subject(
                subject_id=row["subject_id"],
                site_id=row["site_id"],
                project_id=row["project_id"],
                subject_metadata=row["subject_metadata"],
            )
            subjects.append(subject)
        return subjects

    def delete_record_query(self) -> str:
        """Generate a query to delete a record from the table"""
        query = f"""DELETE FROM subjects
        WHERE subject_id = '{self.subject_id}';"""
        return query
