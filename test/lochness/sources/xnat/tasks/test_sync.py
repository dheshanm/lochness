import logging
import json
from pathlib import Path
from rich.logging import RichHandler

from lochness.models.keystore import KeyStore
from lochness.models.projects import Project
from lochness.helpers import logs, utils, db, config
from lochness.sources.xnat.tasks.sync import (
        get_xnat_cred,
        insert_xnat_cred,
        schedule_xnat_download,
        )

logger = logging.getLogger(__name__)
logargs = {
    "level": logging.DEBUG,
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


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


from unittest.mock import patch, MagicMock
from lochness.sources.xnat.tasks.sync import get_xnat_projects, get_xnat_subjects, get_xnat_experiments, download_xnat_experiment
from lochness.sources.xnat.models.data_source import XnatDataSource, XnatDataSourceMetadata

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

    # Use patch to replace the actual requests.get with our mock
    with patch('lochness.sources.xnat.tasks.sync.requests.get') as mock_get:
        # Configure the mock to return a fake response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "ResultSet": {
                "Result": [
                    {"ID": "project1"},
                    {"ID": "project2"}
                ]
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Call the function you want to test
        projects = get_xnat_projects(mock_xnat_data_source)
        logger.info(f"Retrieved projects: {projects}")

        # Assert that your function correctly processes the mock data
        assert len(projects) == 2
        assert 'project1' in projects
        assert 'project2' in projects

        # Assert that requests.get was called with the correct url and headers
        expected_url = "https://fake-xnat.com/data/projects"
        expected_headers = {"Authorization": "Bearer fake_token"}
        mock_get.assert_called_once_with(expected_url, headers=expected_headers)
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

    with patch('lochness.sources.xnat.tasks.sync.requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "ResultSet": {
                "Result": [
                    {"label": "subject1"},
                    {"label": "subject2"}
                ]
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        subjects = get_xnat_subjects(mock_xnat_data_source, "project1")
        logger.info(f"Retrieved subjects: {subjects}")

        assert len(subjects) == 2
        assert 'subject1' in subjects
        assert 'subject2' in subjects

        expected_url = "https://fake-xnat.com/data/projects/project1/subjects"
        expected_headers = {"Authorization": "Bearer fake_token"}
        mock_get.assert_called_once_with(expected_url, headers=expected_headers)
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

    with patch('lochness.sources.xnat.tasks.sync.requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "ResultSet": {
                "Result": [
                    {"label": "experiment1"},
                    {"label": "experiment2"}
                ]
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        experiments = get_xnat_experiments(mock_xnat_data_source, "project1", "subject1")
        logger.info(f"Retrieved experiments: {experiments}")

        assert len(experiments) == 2
        assert 'experiment1' in experiments
        assert 'experiment2' in experiments

        expected_url = "https://fake-xnat.com/data/projects/project1/subjects/subject1/experiments"
        expected_headers = {"Authorization": "Bearer fake_token"}
        mock_get.assert_called_once_with(expected_url, headers=expected_headers)
    logger.info("test_get_xnat_experiments_with_mock finished.")


@patch('builtins.open', new_callable=MagicMock)
@patch('lochness.sources.xnat.tasks.sync.requests.get')
def test_download_xnat_experiment_with_mock(mock_get, mock_open):
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

    mock_response = MagicMock()
    mock_response.iter_content.return_value = [b'test_content']
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    download_dir = Path("/tmp/test_download")
    file_path = download_xnat_experiment(
        mock_xnat_data_source, "project1", "subject1", "experiment1", download_dir
    )
    logger.info(f"Downloaded file to: {file_path}")

    assert file_path == download_dir / "experiment1.zip"

    expected_url = "https://fake-xnat.com/data/projects/project1/subjects/subject1/experiments/experiment1/resources/files?format=zip"
    expected_headers = {"Authorization": "Bearer fake_token"}
    mock_get.assert_called_once_with(expected_url, headers=expected_headers, stream=True)

    mock_open.assert_called_once_with(file_path, "wb")
    mock_open().__enter__().write.assert_called_once_with(b'test_content')
    logger.info("test_download_xnat_experiment_with_mock finished.")

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
        # Find the end of the JSON payload before "');"
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
