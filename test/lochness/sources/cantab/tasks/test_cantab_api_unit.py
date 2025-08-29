"""
Unit tests for lochness.sources.cantab.api
"""

from pathlib import Path
from unittest.mock import MagicMock, patch
from unittest.mock import Mock

import pytest
from requests.auth import HTTPBasicAuth

from lochness.sources.cantab import api as cantab_api
from lochness.sources.cantab.models.data_source import (
    CANTABDataSource,
    CANTABDataSourceMetadata,
)


@pytest.fixture
def dummy_cantab_ds():
    """Provides a dummy CANTABDataSource instance for tests."""
    return CANTABDataSource(
        data_source_name="dummy_ds",
        site_id="dummy_site",
        project_id="dummy_project",
        data_source_type="cantab",
        is_active=True,
        data_source_metadata=CANTABDataSourceMetadata(
            keystore_name="dummy_keystore", api_url="https://dummy.api"
        ),
    )


@patch("lochness.models.keystore.KeyStore.retrieve_keystore")
@pytest.mark.cantab
def test_get_cantab_cred_success(
    mock_retrieve: Mock, dummy_cantab_ds: CANTABDataSource
):
    """Test that get_cantab_cred returns correct credentials from keystore."""
    mock_retrieve.return_value = MagicMock(
        key_value='{"username": "user", "password": "pass"}'
    )
    ds = dummy_cantab_ds
    config_file = Path("/tmp/config.ini")
    creds = cantab_api.get_cantab_cred(ds, config_file)
    assert creds["username"] == "user"
    assert creds["password"] == "pass"


@patch("lochness.models.keystore.KeyStore.retrieve_keystore")
@pytest.mark.cantab
def test_get_cantab_cred_failure(
    mock_retrieve: Mock, dummy_cantab_ds: CANTABDataSource
):
    """Test that get_cantab_cred raises ValueError if keystore entry is missing."""
    mock_retrieve.return_value = None
    ds = dummy_cantab_ds
    config_file = Path("/tmp/config.ini")
    with pytest.raises(ValueError):
        cantab_api.get_cantab_cred(ds, config_file)


@patch("lochness.sources.cantab.api.get_cantab_cred")
@pytest.mark.cantab
def test_get_cantab_auth(mock_cred: Mock, dummy_cantab_ds: CANTABDataSource):
    """Test that get_cantab_auth returns a valid HTTPBasicAuth object."""
    mock_cred.return_value = {"username": "user", "password": "pass"}
    ds = dummy_cantab_ds
    config_file = Path("/tmp/config.ini")
    auth = cantab_api.get_cantab_auth(ds, config_file)
    assert isinstance(auth, HTTPBasicAuth)
    assert auth.username == "user"
    assert auth.password == "pass"


@patch("lochness.sources.cantab.api.get_cantab_auth")
@pytest.mark.cantab
@patch("requests.get")
def test_fetch_cantab_id_found(
    mock_get: Mock, mock_auth: Mock, dummy_cantab_ds: CANTABDataSource
):
    """Test fetch_cantab_id returns the correct CANTAB ID when found."""
    mock_auth.return_value = HTTPBasicAuth("user", "pass")
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"records": [{"id": "cantab_id_123"}]}
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp
    ds = dummy_cantab_ds
    config_file = Path("/tmp/config.ini")
    subject_id = "subj1"
    result = cantab_api.fetch_cantab_id(ds, subject_id, config_file)
    assert result == "cantab_id_123"


@patch("lochness.sources.cantab.api.get_cantab_auth")
@pytest.mark.cantab
@patch("requests.get")
def test_fetch_cantab_id_not_found(
    mock_get: Mock, mock_auth: Mock, dummy_cantab_ds: CANTABDataSource
):
    """Test fetch_cantab_id returns None when no records are found."""
    mock_auth.return_value = HTTPBasicAuth("user", "pass")
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"records": []}
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp
    ds = dummy_cantab_ds
    config_file = Path("/tmp/config.ini")
    subject_id = "subj1"
    result = cantab_api.fetch_cantab_id(ds, subject_id, config_file)
    assert result is None


@patch("lochness.sources.cantab.api.get_cantab_auth")
@patch("requests.get")
@pytest.mark.cantab
def test_get_cantab_data_success(
    mock_get: Mock, mock_auth: Mock, dummy_cantab_ds: CANTABDataSource
):
    """Test get_cantab_data returns expected data structure when successful."""
    mock_auth.return_value = HTTPBasicAuth("user", "pass")
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"records": [{"visit": 1}]}
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp
    ds = dummy_cantab_ds
    config_file = Path("/tmp/config.ini")
    cantab_id = "cantab_id_123"
    result = cantab_api.get_cantab_data(ds, cantab_id, config_file)
    assert "records" in result
    assert result["records"][0]["visit"] == 1


@patch("lochness.sources.cantab.api.get_cantab_auth")
@patch("requests.get")
@pytest.mark.cantab
def test_get_cantab_data_http_error(
    mock_get: Mock, mock_auth: Mock, dummy_cantab_ds: CANTABDataSource
):
    """Test get_cantab_data raises an Exception on HTTP error."""
    mock_auth.return_value = HTTPBasicAuth("user", "pass")
    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = Exception("HTTP error")
    mock_get.return_value = mock_resp
    ds = dummy_cantab_ds
    config_file = Path("/tmp/config.ini")
    cantab_id = "cantab_id_123"
    with pytest.raises(Exception):
        cantab_api.get_cantab_data(ds, cantab_id, config_file)
