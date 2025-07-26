import sys
import os
import pytest
import argparse
import subprocess
from datetime import datetime
from pathlib import Path

for x in sys.path:
    if 'lochness' in x:
        sys.path.remove(x)

file = Path(__file__).resolve()
parent = file.parent
root_dir = None
for parent in file.parents:
    if parent.name == "lochness_v2":
        root_dir = parent

sys.path.append(str(root_dir))
sys.path.append(str(root_dir / 'test'))

# Get the project root directory
project_root = Path(__file__).resolve().parent.parent

# Set PYTHONPATH to include the project root
sys.path.append(str(project_root))

from lochness.models.projects import Project
from lochness.models.keystore import KeyStore
from lochness.models.sites import Site
from lochness.models.data_sinks import DataSink
from lochness.models.subjects import Subject
from lochness.models.data_source import DataSource
from lochness.models.data_pulls import DataPull
from lochness.models.data_push import DataPush
from lochness.models.files import File
from lochness.helpers import logs, utils, db, config

config_file = utils.get_config_file_path()


def create_fake_records(project_id, project_name, site_id,
                        site_name, subject_id, datasink_name):
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
            key_metadata={
                'access_key': 'lochness-dev'
                })
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


    # create fake datasink
    dataSink = DataSink(
            data_sink_name=datasink_name,
            site_id=site_id,
            project_id=project_id,
            data_sink_metadata={
                'type': 'minio',
                'endpoint': minio_cred['endpoint'],
                'bucket_name': minio_cred['bucket_name'],
                'secure': True,
                'keystore_name': minio_cred['key_name'],
                'active': True,
                })

    db.execute_queries(config_file, [dataSink.to_sql_query()])


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
            data_source_metadata={
                'testing': True,
                'modality': 'surveys',
                'keystore_name': redcap_cred['key_name'],
                'endpoint_url': redcap_cred['endpoint_url'],
                'subject_id_variable': redcap_cred['subject_id_variable'],
                }
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
    DATASINK_NAME = 'fake_datasink_name'

    # Setup: Create fake records
    create_fake_records(PROJECT_ID, PROJECT_NAME, SITE_ID,
                        SITE_NAME, SUBJECT_ID, DATASINK_NAME)

    yield PROJECT_ID, PROJECT_NAME, SITE_ID, \
            SITE_NAME, SUBJECT_ID, DATASINK_NAME



    # Teardown: Delete fake records
    delete_fake_records(PROJECT_ID, PROJECT_NAME, SITE_ID,
                        SITE_NAME, SUBJECT_ID, DATASINK_NAME)


@pytest.fixture
def prod_data_fixture():
    """A pytest fixture to create and tear down fake data for tests."""

    # load prod test information from config
    prod_test_info = config.parse(config_file, 'prod-test-info')
    PROJECT_ID = prod_test_info['project_id']
    PROJECT_NAME = prod_test_info['project_name']
    SITE_ID = prod_test_info['site_id']
    SITE_NAME = prod_test_info['site_name']
    SUBJECT_ID = prod_test_info['subject_id']
    DATASINK_NAME = prod_test_info['datasink_name']

    # Setup: Create fake records
    create_fake_records(PROJECT_ID, PROJECT_NAME, SITE_ID,
                        SITE_NAME, SUBJECT_ID, DATASINK_NAME)

    yield PROJECT_ID, PROJECT_NAME, SITE_ID, \
            SITE_NAME, SUBJECT_ID, DATASINK_NAME

    delete_data_push = f"""DELETE FROM data_push
    WHERE file_path IN (
        SELECT files.file_path FROM files
        JOIN data_pull ON files.file_path = data_pull.file_path
        WHERE data_pull.project_id = '{PROJECT_ID}'
          AND data_pull.site_id = '{SITE_ID}'
    );"""
    db.execute_queries(config_file, [delete_data_push])

    delete_data_pull = f"""DELETE FROM data_pull 
    WHERE project_id = '{PROJECT_ID}' AND site_id = '{SITE_ID}'
    """
    db.execute_queries(config_file, [delete_data_pull])
    

    # Teardown: Delete fake records
    delete_fake_records(PROJECT_ID, PROJECT_NAME, SITE_ID,
                        SITE_NAME, SUBJECT_ID, DATASINK_NAME)


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


def delete_fake_records(project_id, project_name, site_id,
                        site_name, subject_id, datasink_name):
    test_file = Path('test_file.zip')

    fileObj = File(
            file_path=test_file,
            with_hash=True)

    # delete fake datasink
    dataSink = DataSink(
            data_sink_name=datasink_name,
            site_id=site_id,
            project_id=project_id,
            data_sink_metadata={'type': 'minio'})
    data_sink_id = dataSink.get_data_sink_id(config_file)

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


    dataPush = DataPush(
            data_sink_id=data_sink_id,
            file_path=str(test_file),
            file_md5=fileObj.md5,
            push_time_s=1,
            push_metadata={'test': True},
            push_timestamp=datetime.now().isoformat()
            )

    db.execute_queries(config_file, [
        dataPush.delete_record_query(data_sink_id)])

    os.remove(test_file)
    db.execute_queries(config_file, [
        fileObj.delete_record_query()])


    # delete fake data source
    dataSource = DataSource(
            data_source_name='main_redcap',
            is_active=True,
            site_id=site_id,
            project_id=project_id,
            data_source_type='redcap',
            data_source_metadata={}
            )
    db.execute_queries(config_file, [dataSource.delete_record_query()])

    # delete subjects for the test project_id and test site_id
    subject_obj_list = Subject.get_subjects_for_project_site(
            project_id, site_id, config_file)
    for subject in subject_obj_list:
        db.execute_queries(config_file,
                           [subject.delete_record_query()])

    # delete fake site
    site = Site(
            site_id=site_id,
            site_name=site_name,
            project_id=project_id,
            site_is_active=True,
            site_metadata={'testing': True}
            )
    db.execute_queries(config_file, [dataSink.delete_record_query()])
    db.execute_queries(config_file, [site.delete_record_query()])


    redcap_cred = config.parse(config_file, 'redcap-test')
    # delete key store
    keystore = KeyStore(
            key_name=redcap_cred['key_name'],
            key_value=redcap_cred['key_value'],
            key_type=redcap_cred['key_type'],
            project_id=project_id,
            key_metadata={})
    db.execute_queries(config_file,
                       [keystore.delete_record_query()])

    minio_cred = config.parse(config_file, 'datasink-test')
    # delete key store
    keystore = KeyStore(
            key_name=minio_cred['key_name'],
            key_value=minio_cred['key_value'],
            key_type=minio_cred['key_type'],
            project_id=project_id,
            key_metadata={})
    db.execute_queries(config_file,
                       [keystore.delete_record_query()])

    # delete fake project
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
    parser.add_argument('--datasink_name', type=str, default='fake_datasink_name',
                        help='The datasink name.')
    args = parser.parse_args()

    create_fake_records(args.project_id, args.project_name, args.site_id,
                        args.site_name, args.subject_id, args.datasink_name)
