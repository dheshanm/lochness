import subprocess
import sys
import os
from datetime import datetime
from pathlib import Path


# Get the project root directory
project_root = Path(__file__).resolve().parent.parent

# Set PYTHONPATH to include the project root
sys.path.append(str(project_root))

from lochness.models.projects import Project
from lochness.models.sites import Site
from lochness.models.subjects import Subject
from lochness.models.data_source import DataSource
from lochness.models.data_pulls import DataPull
from lochness.models.files import File
from lochness.helpers import logs, utils, db, config

config_file = utils.get_config_file_path()


PROJECT_ID = 'fake_project_id'
PROJECT_NAME = 'fake_project'
SITE_ID = 'CP'
SITE_NAME = 'test_CP'
SUBJECT_ID = 'fake_subject'

# create fake project
project = Project(
        project_id=PROJECT_ID,
        project_name=PROJECT_NAME,
        project_is_active=True,
        project_metadata={'testing': True}
        )
db.execute_queries(config_file, [project.to_sql_query()])


# create fake site
site = Site(
        site_id=SITE_ID,
        site_name=SITE_NAME,
        project_id=PROJECT_ID,
        site_is_active=True,
        site_metadata={'testing': True}
        )
db.execute_queries(config_file, [site.to_sql_query()])


# create fake subject
subject = Subject(
    subject_id=SUBJECT_ID,
    site_id=SITE_ID,
    project_id=PROJECT_ID,
    subject_metadata={'testing': True})
db.execute_queries(config_file, [subject.to_sql_query()])


# create fake data source
dataSource = DataSource(
        data_source_name='main_redcap',
        is_active=True,
        site_id=SITE_ID,
        project_id=PROJECT_ID,
        data_source_type='redcap',
        data_source_metadata={'testing': True}
        )
db.execute_queries(config_file, [dataSource.to_sql_query()])


test_file = Path('test_file.zip')
test_file.touch()

fileObj = File(
        file_path=test_file,
        with_hash=True)
db.execute_queries(config_file, [fileObj.to_sql_query()])


dataPull = DataPull(
    subject_id=SUBJECT_ID,
    data_source_name='main_redcap',
    site_id=SITE_ID,
    project_id=PROJECT_ID,
    file_path=str(test_file),
    file_md5=fileObj.md5,
    pull_time_s=1,
    pull_metadata={'test': True})
db.execute_queries(config_file, [dataPull.to_sql_query()])

