import sys
import os
import pytest
import argparse
import subprocess
from datetime import datetime
from pathlib import Path


# Get the project root directory
project_root = Path(__file__).resolve().parent.parent

# Set PYTHONPATH to include the project root
sys.path.append(str(project_root))

from lochness.models.projects import Project
from lochness.models.keystore import KeyStore
from lochness.models.sites import Site
from lochness.models.subjects import Subject
from lochness.models.data_source import DataSource
from lochness.models.data_pulls import DataPull
from lochness.models.files import File
from lochness.helpers import logs, utils, db, config

config_file = utils.get_config_file_path()


def create_fake_records(project_id, project_name, site_id, site_name, subject_id):
    # create fake project
    project = Project(
            project_id=project_id,
            project_name=project_name,
            project_is_active=True,
            project_metadata={'testing': True}
            )
    db.execute_queries(config_file, [project.to_sql_query()])

    encryption_passphrase = config.parse(config_file, 'general')[
            'encryption_passphrase']

    redcap_cred = config.parse(config_file, 'redcap-test')
    # create key store
    keystore = KeyStore(
            key_name=redcap_cred['key_name'],
            key_value=redcap_cred['key_value'],
            key_type=redcap_cred['key_type'],
            project_id=project_id,
            key_metadata={})
    db.execute_queries(config_file,
                       [keystore.to_sql_query(encryption_passphrase)])

    minio_cred = config.parse(config_file, 'datasink-test')
    # create key store
    keystore = KeyStore(
            key_name=minio_cred['key_name'],
            key_value=minio_cred['key_value'],
            key_type=minio_cred['key_type'],
            project_id=project_id,
            key_metadata={})
    db.execute_queries(config_file,
                       [keystore.to_sql_query(encryption_passphrase)])


    # create fake site
    site = Site(
            site_id=site_id,
            site_name=site_name,
            project_id=project_id,
            site_is_active=True,
            site_metadata={'testing': True}
            )
    db.execute_queries(config_file, [site.to_sql_query()])


    # create fake subject
    subject = Subject(
        subject_id=subject_id,
        site_id=site_id,
        project_id=project_id,
        subject_metadata={'testing': True})
    db.execute_queries(config_file, [subject.to_sql_query()])


    # create fake data source
    dataSource = DataSource(
            data_source_name='main_redcap',
            is_active=True,
            site_id=site_id,
            project_id=project_id,
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
        subject_id=subject_id,
        data_source_name='main_redcap',
        site_id=site_id,
        project_id=project_id,
        file_path=str(test_file),
        file_md5=fileObj.md5,
        pull_time_s=1,
        pull_metadata={'test': True})
    db.execute_queries(config_file, [dataPull.to_sql_query()])



@pytest.fixture
def fake_data_fixture():
    """A pytest fixture to create and tear down fake data for tests."""
    PROJECT_ID = 'fake_project_id'
    PROJECT_NAME = 'fake_project'
    SITE_ID = 'CP'
    SITE_NAME = 'test_CP'
    SUBJECT_ID = 'fake_subject'

    # Setup: Create fake records
    create_fake_records(PROJECT_ID, PROJECT_NAME, SITE_ID, SITE_NAME, SUBJECT_ID)

    yield PROJECT_ID, PROJECT_NAME, SITE_ID, SITE_NAME, SUBJECT_ID

    # Teardown: Delete fake records
    delete_fake_records(PROJECT_ID, PROJECT_NAME, SITE_ID, SITE_NAME, SUBJECT_ID)


def test_pipeline_with_fake_data(fake_data_fixture):
    """
    This test function uses the fake_data_fixture.
    Pytest will automatically run the setup code in the fixture before this test,
    and the teardown code after the test is finished.
    """
    # You can access the data yielded by the fixture if needed
    project_id, project_name, site_id, site_name, subject_id = fake_data_fixture

    # Here you would run the tests for your pipelines that depend on this fake data
    print(f"Testing pipeline with project: {project_id}, site: {site_id}, subject: {subject_id}")

    # For demonstration, we'll just assert that the test runs
    assert True


def delete_fake_records(project_id, project_name, site_id, site_name, subject_id):
    test_file = Path('test_file.zip')

    fileObj = File(
            file_path=test_file,
            with_hash=True)

    dataPull = DataPull(
        subject_id=subject_id,
        data_source_name='main_redcap',
        site_id=site_id,
        project_id=project_id,
        file_path=str(test_file),
        file_md5=fileObj.md5,
        pull_time_s=1,
        pull_metadata={'test': True})
    db.execute_queries(config_file, [dataPull.delete_record_query()])


    db.execute_queries(config_file, [fileObj.delete_record_query()])
    os.remove(test_file)



    # create fake data source
    dataSource = DataSource(
            data_source_name='main_redcap',
            is_active=True,
            site_id=site_id,
            project_id=project_id,
            data_source_type='redcap',
            data_source_metadata={'testing': True}
            )
    db.execute_queries(config_file, [dataSource.delete_record_query()])

    # create fake subject
    subject = Subject(
        subject_id=subject_id,
        site_id=site_id,
        project_id=project_id,
        subject_metadata={'testing': True})
    db.execute_queries(config_file, [subject.delete_record_query()])


    # create fake site
    site = Site(
            site_id=site_id,
            site_name=site_name,
            project_id=project_id,
            site_is_active=True,
            site_metadata={'testing': True}
            )
    db.execute_queries(config_file, [site.delete_record_query()])

    redcap_cred = config.parse(config_file, 'redcap-test')
    # create key store
    keystore = KeyStore(
            key_name=redcap_cred['key_name'],
            key_value=redcap_cred['key_value'],
            key_type=redcap_cred['key_type'],
            project_id=project_id,
            key_metadata={})
    db.execute_queries(config_file,
                       [keystore.delete_record_query()])

    minio_cred = config.parse(config_file, 'datasink-test')
    # create key store
    keystore = KeyStore(
            key_name=minio_cred['key_name'],
            key_value=minio_cred['key_value'],
            key_type=minio_cred['key_type'],
            project_id=project_id,
            key_metadata={})
    db.execute_queries(config_file,
                       [keystore.delete_record_query()])

    # create fake project
    project = Project(
            project_id=project_id,
            project_name=project_name,
            project_is_active=True,
            project_metadata={'testing': True}
            )
    db.execute_queries(config_file, [project.delete_record_query()])




if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create fake records for testing.')
    parser.add_argument('--project_id', type=str, default='fake_project_id',
                        help='The project ID.')
    parser.add_argument('--project_name', type=str, default='fake_project',
                        help='The project name.')
    parser.add_argument('--site_id', type=str, default='CP',
                        help='The site ID.')
    parser.add_argument('--site_name', type=str, default='test_CP',
                        help='The site name.')
    parser.add_argument('--subject_id', type=str, default='fake_subject',
                        help='The subject ID.')
    args = parser.parse_args()

    create_fake_records(args.project_id, args.project_name, args.site_id,
                        args.site_name, args.subject_id)
