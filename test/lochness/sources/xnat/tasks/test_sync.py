import sys
from pathlib import Path

# Add the project root to sys.path for module discovery
sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

import logging
import json
import pandas as pd


from lochness.models.keystore import KeyStore
from lochness.models.projects import Project
from lochness.helpers import logs, utils, db, config
from lochness.sources.xnat.models.data_source import (
        XnatDataSource, XnatDataSourceMetadata
        )
import configparser
from lochness.sources.xnat.tasks.sync import (
        get_xnat_projects,
        get_xnat_subjects,
        get_xnat_experiments,
        download_xnat_experiment,
        get_xnat_project_metadata,
        get_xnat_cred,
        insert_xnat_cred,
        schedule_xnat_download,
        check_xnat_connection)

logger = logging.getLogger(__name__)
logargs = {
    "level": logging.DEBUG,
    "format": "%(message)s",
}
logging.basicConfig(**logargs)


import xnat
from unittest.mock import patch, MagicMock
import pytest

# def test_create_project():
    # config_file = utils.get_config_file_path()

    # project = Project(
            # project_id='ProCAN',
            # project_name='ProCAN',
            # project_metadata={})
    # query = project.delete_record_query()

    # db.execute_queries(  # type: ignore
        # config_file=config_file,
        # queries=[query],
        # show_commands=False,
    # )

    # query = project.to_sql_query()
    # db.execute_queries(  # type: ignore
        # config_file=config_file,
        # queries=[query],
        # show_commands=False,
    # )

    # pass

def test_insert_xnat_cred():
    config_file = utils.get_config_file_path()

    # how should we handle encryption passphrase?
    encryption_passphrase = config.parse(config_file, 'general')[
            'encryption_passphrase']

    # 2. Create a KeyStore instance
    my_key = KeyStore(
        key_name="xnat",
        key_value="secure_token_string_here",
        key_type="xnat",
        project_id="ProCAN",
        key_metadata={
            "description": "Access token for XNAT",
            "created_by": "kevin"}
    )

    insert_query = my_key.drop_db_table_query()
    db.execute_queries(  # type: ignore
        config_file=config_file,
        queries=[insert_query],
        show_commands=False,
    )
    insert_query = my_key.init_db_table_query()
    db.execute_queries(  # type: ignore
        config_file=config_file,
        queries=insert_query,
        show_commands=False,
    )

    insert_query = my_key.to_sql_query(
            encryption_passphrase=encryption_passphrase)

    db.execute_queries(  # type: ignore
        config_file=config_file,
        queries=[insert_query],
        show_commands=False,
    )

# def test_delete_project():
    # project = Project(
            # project_id='ProCAN',
            # project_name='ProCAN',
            # project_metadata={})
    # query = project.delete_record_query()
    # print(query)

    # config_file = utils.get_config_file_path()

    # db.execute_queries(  # type: ignore
        # config_file=config_file,
        # queries=[query],
        # show_commands=False,
    # )



# This is a new test function you would add to test_sync.py
def test_get_xnat_projects_with_mock():
    logger.info("Running test_get_xnat_projects_with_mock...")
    # Create a mock object to simulate the XNAT connection
    mock_xnat_data_source = XnatDataSource(
        data_source_name="test_xnat",
        is_active=True,
        site_id="test_site",
        project_id="test_project",
        data_source_type="xnat",
        data_source_metadata=XnatDataSourceMetadata(
            api_token="fake_token",
            endpoint_url="https://fake-xnat.com",
            subject_id_variable="xnat_subject_id",
            optional_variables_dictionary=[]
        )
    )

    with patch('lochness.sources.xnat.tasks.sync.xnat.connect') as mock_xnat_connect:
        mock_connection = MagicMock()
        mock_xnat_connect.return_value.__enter__.return_value = mock_connection

        mock_connection.projects.keys.return_value = ['project1', 'project2']

        # Call the function you want to test
        projects = get_xnat_projects(mock_xnat_data_source)
        logger.info(f"Retrieved projects: {projects}")

        # Assert that your function correctly processes the mock data
        assert len(projects) == 2
        assert 'project1' in projects
        assert 'project2' in projects

        mock_xnat_connect.assert_called_once_with(
            mock_xnat_data_source.data_source_metadata.endpoint_url,
            user=mock_xnat_data_source.data_source_metadata.api_token,
            password=mock_xnat_data_source.data_source_metadata.api_token
        )
    logger.info("test_get_xnat_projects_with_mock finished.")

def test_get_xnat_subjects_with_mock():
    logger.info("Running test_get_xnat_subjects_with_mock...")
    mock_xnat_data_source = XnatDataSource(
        data_source_name="test_xnat",
        is_active=True,
        site_id="test_site",
        project_id="test_project",
        data_source_type="xnat",
        data_source_metadata=XnatDataSourceMetadata(
            api_token="fake_token",
            endpoint_url="https://fake-xnat.com",
            subject_id_variable="xnat_subject_id",
            optional_variables_dictionary=[]
        )
    )

    with patch('lochness.sources.xnat.tasks.sync.xnat.connect') as mock_xnat_connect:
        mock_connection = MagicMock()
        mock_xnat_connect.return_value.__enter__.return_value = mock_connection

        mock_connection.projects.__getitem__.return_value.subjects.keys.return_value = ['subject1', 'subject2']

        subjects = get_xnat_subjects(mock_xnat_data_source, "project1")
        logger.info(f"Retrieved subjects: {subjects}")

        assert len(subjects) == 2
        assert 'subject1' in subjects
        assert 'subject2' in subjects

        mock_xnat_connect.assert_called_once_with(
            mock_xnat_data_source.data_source_metadata.endpoint_url,
            user=mock_xnat_data_source.data_source_metadata.api_token,
            password=mock_xnat_data_source.data_source_metadata.api_token
        )
    logger.info("test_get_xnat_subjects_with_mock finished.")

def test_get_xnat_experiments_with_mock():
    logger.info("Running test_get_xnat_experiments_with_mock...")
    mock_xnat_data_source = XnatDataSource(
        data_source_name="test_xnat",
        is_active=True,
        site_id="test_site",
        project_id="test_project",
        data_source_type="xnat",
        data_source_metadata=XnatDataSourceMetadata(
            api_token="fake_token",
            endpoint_url="https://fake-xnat.com",
            subject_id_variable="xnat_subject_id",
            optional_variables_dictionary=[]
        )
    )

    with patch('lochness.sources.xnat.tasks.sync.xnat.connect') as mock_xnat_connect:
        mock_connection = MagicMock()
        mock_xnat_connect.return_value.__enter__.return_value = mock_connection

        mock_connection.projects.__getitem__.return_value.subjects.__getitem__.return_value.experiments.keys.return_value = ['experiment1', 'experiment2']

        experiments = get_xnat_experiments(mock_xnat_data_source, "project1", "subject1")
        logger.info(f"Retrieved experiments: {experiments}")

        assert len(experiments) == 2
        assert 'experiment1' in experiments
        assert 'experiment2' in experiments

        mock_xnat_connect.assert_called_once_with(
            mock_xnat_data_source.data_source_metadata.endpoint_url,
            user=mock_xnat_data_source.data_source_metadata.api_token,
            password=mock_xnat_data_source.data_source_metadata.api_token
        )
    logger.info("test_get_xnat_experiments_with_mock finished.")


def test_download_xnat_experiment_with_mock():
    logger.info("Running test_download_xnat_experiment_with_mock...")
    mock_xnat_data_source = XnatDataSource(
        data_source_name="test_xnat",
        is_active=True,
        site_id="test_site",
        project_id="test_project",
        data_source_type="xnat",
        data_source_metadata=XnatDataSourceMetadata(
            api_token="fake_token",
            endpoint_url="https://fake-xnat.com",
            subject_id_variable="xnat_subject_id",
            optional_variables_dictionary=[]
        )
    )

    download_dir = Path("/tmp/test_download")

    with patch('lochness.sources.xnat.tasks.sync.xnat.connect') as mock_xnat_connect:
        mock_connection = MagicMock()
        mock_xnat_connect.return_value.__enter__.return_value = mock_connection

        mock_experiment = MagicMock()
        mock_connection.projects.__getitem__.return_value.subjects.__getitem__.return_value.experiments.__getitem__.return_value = mock_experiment
        mock_experiment.download.return_value = str(download_dir / "experiment1.zip")

        file_path = download_xnat_experiment(
            mock_xnat_data_source, "project1", "subject1", "experiment1", download_dir
        )
        logger.info(f"Downloaded file to: {file_path}")

        assert file_path == download_dir / "experiment1.zip"

        mock_experiment.download.assert_called_once_with(download_dir)
        mock_xnat_connect.assert_called_once_with(
            mock_xnat_data_source.data_source_metadata.endpoint_url,
            user=mock_xnat_data_source.data_source_metadata.api_token,
            password=mock_xnat_data_source.data_source_metadata.api_token
        )
    logger.info("test_download_xnat_experiment_with_mock finished.")

def test_check_xnat_connection_success_with_mock():
    logger.info("Running test_check_xnat_connection_success_with_mock...")
    mock_xnat_data_source = XnatDataSource(
        data_source_name="test_xnat",
        is_active=True,
        site_id="test_site",
        project_id="test_project",
        data_source_type="xnat",
        data_source_metadata=XnatDataSourceMetadata(
            api_token="fake_token",
            endpoint_url="https://fake-xnat.com",
            subject_id_variable="xnat_subject_id",
            optional_variables_dictionary=[]
        )
    )

    with patch('lochness.sources.xnat.tasks.sync.xnat.connect') as mock_xnat_connect:
        mock_connection = MagicMock()
        mock_xnat_connect.return_value.__enter__.return_value = mock_connection

        mock_connection.projects.keys.return_value = ['project1'] # Simulate successful connection

        is_connected = check_xnat_connection(mock_xnat_data_source)
        logger.info(f"XNAT connection successful: {is_connected}")

        assert is_connected is True
        mock_xnat_connect.assert_called_once_with(
            mock_xnat_data_source.data_source_metadata.endpoint_url,
            user=mock_xnat_data_source.data_source_metadata.api_token,
            password=mock_xnat_data_source.data_source_metadata.api_token
        )
    logger.info("test_check_xnat_connection_success_with_mock finished.")

def test_check_xnat_connection_failure_with_mock():
    logger.info("Running test_check_xnat_connection_failure_with_mock...")
    mock_xnat_data_source = XnatDataSource(
        data_source_name="test_xnat",
        is_active=True,
        site_id="test_site",
        project_id="test_project",
        data_source_type="xnat",
        data_source_metadata=XnatDataSourceMetadata(
            api_token="fake_token",
            endpoint_url="https://fake-xnat.com",
            subject_id_variable="xnat_subject_id",
            optional_variables_dictionary=[]
        )
    )

    with patch('lochness.sources.xnat.tasks.sync.xnat.connect') as mock_xnat_connect:
        mock_xnat_connect.side_effect = Exception("Connection error")

        is_connected = check_xnat_connection(mock_xnat_data_source)
        logger.info(f"XNAT connection failed: {is_connected}")

        assert is_connected is False
        mock_xnat_connect.assert_called_once_with(
            mock_xnat_data_source.data_source_metadata.endpoint_url,
            user=mock_xnat_data_source.data_source_metadata.api_token,
            password=mock_xnat_data_source.data_source_metadata.api_token
        )
    logger.info("test_check_xnat_connection_failure_with_mock finished.")

def test_get_xnat_project_metadata_with_mock():
    logger.info("Running test_get_xnat_project_metadata_with_mock...")
    mock_xnat_data_source = XnatDataSource(
        data_source_name="test_xnat",
        is_active=True,
        site_id="test_site",
        project_id="test_project",
        data_source_type="xnat",
        data_source_metadata=XnatDataSourceMetadata(
            api_token="fake_token",
            endpoint_url="https://fake-xnat.com",
            subject_id_variable="xnat_subject_id",
            optional_variables_dictionary=[]
        )
    )

    with patch('lochness.sources.xnat.tasks.sync.xnat.connect') as mock_xnat_connect:
        mock_connection = MagicMock()
        mock_xnat_connect.return_value.__enter__.return_value = mock_connection

        mock_project = MagicMock()
        mock_project.id = "project1"
        mock_project.name = "Project One"
        mock_project.description = "A test project"
        mock_connection.projects.__getitem__.return_value = mock_project

        metadata = get_xnat_project_metadata(mock_xnat_data_source, "project1")
        logger.info(f"Retrieved project metadata: {metadata}")

        assert metadata["ID"] == "project1"
        assert metadata["name"] == "Project One"
        assert metadata["description"] == "A test project"

        mock_xnat_connect.assert_called_once_with(
            mock_xnat_data_source.data_source_metadata.endpoint_url,
            user=mock_xnat_data_source.data_source_metadata.api_token,
            password=mock_xnat_data_source.data_source_metadata.api_token
        )
    logger.info("test_get_xnat_project_metadata_with_mock finished.")

def test_schedule_xnat_download_with_mock():
    logger.info("Running test_schedule_xnat_download_with_mock...")
    mock_xnat_data_source = XnatDataSource(
        data_source_name="test_xnat",
        is_active=True,
        site_id="test_site",
        project_id="test_project",
        data_source_type="xnat",
        data_source_metadata=XnatDataSourceMetadata(
            api_token="fake_token",
            endpoint_url="https://fake-xnat.com",
            subject_id_variable="xnat_subject_id",
            optional_variables_dictionary=[]
        )
    )

    download_dir = Path("/tmp/test_download")
    project_id = "project1"
    subject_id = "subject1"
    experiment_id = "experiment1"

    with (
        patch('lochness.helpers.utils.get_config_file_path') as mock_get_config_file_path,
        patch('lochness.helpers.db.execute_queries') as mock_execute_queries,
    ):

        mock_get_config_file_path.return_value = Path("/fake/config.ini")

        schedule_xnat_download(
            mock_xnat_data_source, project_id, subject_id, experiment_id, download_dir
        )

        mock_execute_queries.assert_called_once()
        args, kwargs = mock_execute_queries.call_args
        queries = kwargs['queries']
        assert len(queries) == 1
        
        # The query format is: INSERT INTO jobs (job_type, job_payload) VALUES ('xnat_download', '{json_payload}');
        # We need to extract the json_payload part.
        # Find the start of the JSON payload after ", '"
        start_json_payload = queries[0].find("', '") + 4
        # Find the end of the JSON payload before ");"
        end_json_payload = queries[0].rfind("');")
        
        json_str_in_query = queries[0][start_json_payload:end_json_payload]
        
        # Unescape single quotes that were escaped for SQL insertion
        unescaped_json_str = json_str_in_query.replace("''", "'")
        
        # Parse the JSON string
        parsed_payload = json.loads(unescaped_json_str)

        # Assert the content of the parsed payload
        assert parsed_payload["project_id"] == project_id
        assert parsed_payload["subject_id"] == subject_id
        assert parsed_payload["experiment_id"] == experiment_id
        assert parsed_payload["download_dir"] == str(download_dir)
        assert parsed_payload["xnat_data_source"] == mock_xnat_data_source.dict()

        logger.info(f"Scheduled download query: {queries[0]}")
    logger.info("test_schedule_xnat_download_with_mock finished.")


def test_xnat_sync_workflow_with_mock():
    logger.info("Running test_xnat_sync_workflow_with_mock...")
    mock_xnat_data_source = XnatDataSource(
        data_source_name="test_xnat",
        is_active=True,
        site_id="test_site",
        project_id="test_project",
        data_source_type="xnat",
        data_source_metadata=XnatDataSourceMetadata(
            api_token="fake_token",
            endpoint_url="https://fake-xnat.com",
            subject_id_variable="xnat_subject_id",
            optional_variables_dictionary=[]
        )
    )

    download_dir = Path("/tmp/test_download")
    project_id = "project1"
    subject_id = "subject1"
    experiment_id = "experiment1"

    with patch('lochness.sources.xnat.tasks.sync.xnat.connect') as mock_xnat_connect, \
         patch('lochness.helpers.utils.get_config_file_path') as mock_get_config_file_path, \
         patch('lochness.helpers.db.execute_queries') as mock_execute_queries:

        # Setup mock connection
        mock_connection = MagicMock()
        mock_xnat_connect.return_value.__enter__.return_value = mock_connection
        mock_get_config_file_path.return_value = Path("/fake/config.ini")

        # Mock for check_xnat_connection and get_xnat_projects
        mock_connection.projects.keys.return_value = ['project1', 'project2']

        # 1. Check XNAT connection
        logger.info("Checking XNAT connection...")
        is_connected = check_xnat_connection(mock_xnat_data_source)
        assert is_connected is True
        logger.info(f"XNAT connection successful: {is_connected}")

        # 2. Get XNAT projects
        logger.info("Getting XNAT projects...")
        projects = get_xnat_projects(mock_xnat_data_source)
        assert len(projects) == 2
        assert 'project1' in projects
        logger.info(f"Retrieved projects: {projects}")

        # Mocks for subjects and experiments
        mock_project_level = MagicMock()
        mock_subject_level = MagicMock()
        mock_connection.projects.__getitem__.return_value = mock_project_level
        mock_project_level.subjects.keys.return_value = ['subject1', 'subject2']
        mock_project_level.subjects.__getitem__.return_value = mock_subject_level
        mock_subject_level.experiments.keys.return_value = ['experiment1', 'experiment2']

        # 3. Get XNAT subjects for a project
        logger.info(f"Getting subjects for project {project_id}...")
        subjects = get_xnat_subjects(mock_xnat_data_source, project_id)
        assert len(subjects) == 2
        assert 'subject1' in subjects
        logger.info(f"Retrieved subjects: {subjects}")

        # 4. Get XNAT experiments for a subject
        logger.info(f"Getting experiments for subject {subject_id} in project {project_id}...\n")
        experiments = get_xnat_experiments(mock_xnat_data_source, project_id, subject_id)
        assert len(experiments) == 2
        assert 'experiment1' in experiments
        logger.info(f"Retrieved experiments: {experiments}")

        # Mock for get_xnat_project_metadata
        mock_project_for_metadata = MagicMock()
        mock_project_for_metadata.id = "project1"
        mock_project_for_metadata.name = "Project One"
        mock_project_for_metadata.description = "A test project"
        mock_connection.projects.__getitem__.return_value = mock_project_for_metadata

        # 5. Get XNAT project metadata
        logger.info(f"Getting metadata for project {project_id}...\n")
        metadata = get_xnat_project_metadata(mock_xnat_data_source, project_id)
        assert metadata["ID"] == "project1"
        assert metadata["name"] == "Project One"
        assert metadata["description"] == "A test project"

        mock_xnat_connect.assert_called_once_with(
            mock_xnat_data_source.data_source_metadata.endpoint_url,
            user=mock_xnat_data_source.data_source_metadata.api_token,
            password=mock_xnat_data_source.data_source_metadata.api_token
        )
    logger.info("test_xnat_sync_workflow_with_mock finished.\n")


def test_get_all_xnat_data_sources_with_mock():
    logger.info("Running test_get_all_xnat_data_sources_with_mock...\n")
    with patch('lochness.helpers.db.execute_sql') as mock_execute_sql:
        # Mock the return value of the first call to db.execute_sql (for data_sources)
        mock_execute_sql.side_effect = [
            pd.DataFrame([
                {
                    "data_source_name": "test_xnat",
                    "data_source_is_active": True,
                    "site_id": "test_site",
                    "project_id": "test_project",
                    "data_source_type": "xnat",
                    "data_source_metadata": {
                        "endpoint_url": "https://fake-xnat.com",
                        "subject_id_variable": "xnat_subject_id",
                        "optional_variables_dictionary": []
                    }
                }
            ]),
            pd.DataFrame([{"key_value": "fake_token"}])
        ]

        xnat_data_sources = XnatDataSource.get_all_xnat_data_sources(
            config_file=Path("/fake/config.ini"),
            encryption_passphrase="fake_passphrase"
        )

        assert len(xnat_data_sources) == 1
        assert xnat_data_sources[0].data_source_name == "test_xnat"
        assert xnat_data_sources[0].data_source_metadata.api_token == "fake_token"

    logger.info("test_get_all_xnat_data_sources_with_mock finished.\n")



@pytest.mark.integration
def test_check_xnat_connection_real():
    logger.info("Running test_check_xnat_connection_real...\n")
    config_file = utils.get_config_file_path()
    if not config_file.exists():
        pytest.skip("Skipping integration test: lochness.config.ini not found")

    try:
        encryption_passphrase = config.parse(config_file, 'general')['encryption_passphrase']
    except (KeyError, configparser.NoSectionError):
        pytest.skip("Skipping integration test: encryption_passphrase not in lochness.config.ini")

    try:
        xnat_data_sources = XnatDataSource.get_all_xnat_data_sources(config_file, encryption_passphrase)
    except Exception as e:
        pytest.skip(f"Skipping integration test: could not retrieve xnat_data_sources. Error: {e}")

    for xnat_data_source in xnat_data_sources:
        is_connected = check_xnat_connection(xnat_data_source)
        assert is_connected is True

    logger.info("test_check_xnat_connection_real finished.\n")


from lochness.models.sites import Site
from lochness.models.subjects import Subject

def test_setup_xnat_test_data():
    logger.info("Setting up XNAT test data...\n")

    config_file = utils.get_config_file_path()
    encryption_passphrase = config.parse(config_file, 'general')['encryption_passphrase']

    # Mock db.execute_queries and db.execute_sql to prevent actual database interaction
    # when running this script directly for setup.
    with patch('lochness.helpers.db.execute_queries') as mock_execute_queries, \
         patch('lochness.helpers.db.execute_sql') as mock_execute_sql:

        # Drop and re-initialize tables
        mock_execute_queries.side_effect = lambda config_file, queries, show_commands: None
        mock_execute_sql.side_effect = lambda config_file, query: pd.DataFrame([{"key_value": "mock_token"}]) # Mock for KeyStore.retrieve_key_query

        # Drop and re-initialize tables
        mock_execute_queries(config_file, queries=[
            XnatDataSource.drop_db_table_query(),
            KeyStore.drop_db_table_query(),
            Subject.drop_db_table_query(),
            Site.drop_db_table_query(),
            Project.drop_db_table_query(),
        ], show_commands=False)

        mock_execute_queries(config_file, queries=[
            Project.init_db_table_query(),
            Site.init_db_table_query(),
            Subject.init_db_table_query(),
            KeyStore.init_db_table_query(),
            XnatDataSource.init_db_table_query(),
        ], show_commands=False)

        # Create and insert test Project
        test_project = Project(
            project_id="test_project",
            project_name="Test Project",
            project_metadata={"description": "A project for XNAT testing"}
        )
        mock_execute_queries(config_file, queries=[test_project.to_sql_query()], show_commands=False)

        # Create and insert test Site
        test_site = Site(
            site_id="test_site",
            site_name="Test Site",
            project_id="test_project",
            site_metadata={"location": "Test Location"}
        )
        mock_execute_queries(config_file, queries=[test_site.to_sql_query()], show_commands=False)

        # Create and insert test Subject
        test_subject = Subject(
            subject_id="test_subject",
            site_id="test_site",
            project_id="test_project",
            subject_metadata={"age": 30}
        )
        mock_execute_queries(config_file, queries=[test_subject.to_sql_query()], show_commands=False)

        # Create and insert test XNAT API token into KeyStore
        xnat_api_token_key = KeyStore(
            key_name="test_xnat_data_source", # This should match data_source_name
            key_value="fake_test_api_token",
            key_type="xnat",
            project_id="test_project",
            key_metadata={"description": "Test XNAT API Token"}
        )
        mock_execute_queries(config_file, queries=[xnat_api_token_key.to_sql_query(encryption_passphrase)], show_commands=False)

        # Create and insert test XnatDataSource
        test_xnat_data_source = XnatDataSource(
            data_source_name="test_xnat_data_source",
            is_active=True,
            site_id="test_site",
            project_id="test_project",
            data_source_type="xnat",
            data_source_metadata=XnatDataSourceMetadata(
                api_token="placeholder", # This will be overwritten by the KeyStore lookup
                endpoint_url="https://test-xnat.com",
                subject_id_variable="xnat_subject_id",
                optional_variables_dictionary=[]
            )
        )
        mock_execute_queries(config_file, queries=[test_xnat_data_source.to_sql_query()], show_commands=False)

    logger.info("XNAT test data setup complete.\n")


if __name__ == "__main__":
    test_setup_xnat_test_data()
