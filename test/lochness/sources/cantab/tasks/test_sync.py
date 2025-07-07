import logging
import json
from pathlib import Path
from unittest.mock import patch, MagicMock

from rich.logging import RichHandler

from lochness.models.keystore import KeyStore
from lochness.helpers import utils, db, config
from lochness.sources.cantab.models.data_source import CANTABDataSource, CANTABDataSourceMetadata
from lochness.sources.cantab.tasks.sync import (
    get_cantab_cred,
    get_cantab_auth_headers,
    get_cantab_subjects,
    get_cantab_visits,
    sync_cantab_metadata,
)

logger = logging.getLogger(__name__)
logargs = {
    "level": logging.DEBUG,
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


@patch("lochness.sources.cantab.tasks.sync.KeyStore.get_by_name_and_project")
def test_get_cantab_cred(mock_get_by_name_and_project):
    mock_keystore_instance = MagicMock()
    mock_keystore_instance.key_value = json.dumps({"username": "test_user", "password": "test_pass"})
    mock_get_by_name_and_project.return_value = mock_keystore_instance

    data_source = CANTABDataSource(
        data_source_name="test_cantab",
        is_active=True,
        site_id="test_site",
        project_id="test_project",
        data_source_type="cantab",
        data_source_metadata=CANTABDataSourceMetadata(
            keystore_name="cantab_test_key",
            api_endpoint="https://fake-cantab.com/api",
        ),
    )

    creds = get_cantab_cred(data_source)
    assert creds == {"username": "test_user", "password": "test_pass"}


def test_get_cantab_auth_headers():
    data_source = CANTABDataSource(
        data_source_name="test_cantab",
        is_active=True,
        site_id="test_site",
        project_id="test_project",
        data_source_type="cantab",
        data_source_metadata=CANTABDataSourceMetadata(
            keystore_name="cantab_test_key",
            api_endpoint="https://fake-cantab.com/api",
        ),
    )

    with patch("lochness.sources.cantab.tasks.sync.get_cantab_cred") as mock_get_cantab_cred:
        mock_get_cantab_cred.return_value = {"username": "test_user", "password": "test_pass"}
        headers = get_cantab_auth_headers(data_source)
        expected_auth_string = "Basic dGVzdF91c2VyOnRlc3RfcGFzcw=="
        assert headers["Authorization"] == expected_auth_string
        assert headers["Content-Type"] == "application/json"
        assert headers["Accept"] == "application/json"


@patch("lochness.sources.cantab.tasks.sync.requests.get")
@patch("lochness.sources.cantab.tasks.sync.get_cantab_auth_headers")
def test_get_cantab_subjects(mock_get_cantab_auth_headers, mock_requests_get):
    mock_get_cantab_auth_headers.return_value = {"Authorization": "Basic fake_token"}
    mock_response = MagicMock()
    mock_response.json.return_value = {"records": [{"id": "sub1"}, {"id": "sub2"}], "success": True}
    mock_requests_get.return_value = mock_response

    data_source = CANTABDataSource(
        data_source_name="test_cantab",
        is_active=True,
        site_id="test_site",
        project_id="test_project",
        data_source_type="cantab",
        data_source_metadata=CANTABDataSourceMetadata(
            keystore_name="cantab_test_key",
            api_endpoint="https://fake-cantab.com/api",
        ),
    )

    subjects = get_cantab_subjects(data_source)
    assert len(subjects) == 2
    assert subjects[0]["id"] == "sub1"
    mock_requests_get.assert_called_once_with("https://fake-cantab.com/api/subject?limit=100", headers=mock_get_cantab_auth_headers.return_value)


@patch("lochness.sources.cantab.tasks.sync.requests.get")
@patch("lochness.sources.cantab.tasks.sync.get_cantab_auth_headers")
def test_get_cantab_visits(mock_get_cantab_auth_headers, mock_requests_get):
    mock_get_cantab_auth_headers.return_value = {"Authorization": "Basic fake_token"}
    mock_response = MagicMock()
    mock_response.json.return_value = {"records": [{"id": "visit1"}, {"id": "visit2"}], "success": True}
    mock_requests_get.return_value = mock_response

    data_source = CANTABDataSource(
        data_source_name="test_cantab",
        is_active=True,
        site_id="test_site",
        project_id="test_project",
        data_source_type="cantab",
        data_source_metadata=CANTABDataSourceMetadata(
            keystore_name="cantab_test_key",
            api_endpoint="https://fake-cantab.com/api",
        ),
    )

    visits = get_cantab_visits(data_source, "sub1")
    assert len(visits) == 2
    assert visits[0]["id"] == "visit1"
    expected_filter = json.dumps({"subject": "sub1"})
    mock_requests_get.assert_called_once_with(f"https://fake-cantab.com/api/visit?filter={expected_filter}&limit=100", headers=mock_get_cantab_auth_headers.return_value)


@patch("lochness.sources.cantab.tasks.sync.get_cantab_subjects")
@patch("lochness.sources.cantab.tasks.sync.get_cantab_visits")
def test_sync_cantab_metadata(mock_get_cantab_visits, mock_get_cantab_subjects):
    mock_get_cantab_subjects.return_value = [{"id": "sub1"}, {"id": "sub2"}]
    mock_get_cantab_visits.side_effect = [
        [{"id": "visit1_sub1"}],
        [{"id": "visit1_sub2"}, {"id": "visit2_sub2"}],
    ]

    data_source = CANTABDataSource(
        data_source_name="test_cantab",
        is_active=True,
        site_id="test_site",
        project_id="test_project",
        data_source_type="cantab",
        data_source_metadata=CANTABDataSourceMetadata(
            keystore_name="cantab_test_key",
            api_endpoint="https://fake-cantab.com/api",
        ),
    )

    sync_cantab_metadata(data_source)

    mock_get_cantab_subjects.assert_called_once_with(data_source)
    assert mock_get_cantab_visits.call_count == 2
    mock_get_cantab_visits.assert_any_call(data_source, "sub1")
    mock_get_cantab_visits.assert_any_call(data_source, "sub2")


# Integration test placeholder
# To run this, you would need real CANTAB credentials and a configured CANTABDataSource in your DB.
# You would also need to ensure the 'cantab' data_source_type is added to your supported_data_source_types table.
#
# from lochness.helpers import utils
#
# def test_real_cantab_sync():
#     config_file = utils.get_config_file_path()
#     # Assuming you have a CANTAB data source named 'cantab_prod' in your DB
#     cantab_data_sources = CANTABDataSource.get_all_cantab_data_sources(config_file, active_only=True)
#     assert len(cantab_data_sources) > 0
#
#     for ds in cantab_data_sources:
#         sync_cantab_metadata(ds)
#         print(f"Successfully synced metadata for CANTAB data source: {ds.data_source_name}")
