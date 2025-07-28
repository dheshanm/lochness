import os
import sys
import shutil
from pathlib import Path
import pytest

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

from test_module import fake_data_fixture, config_file, prod_data_fixture

import json
from pathlib import Path
from unittest.mock import patch, MagicMock

from rich.logging import RichHandler

from lochness.helpers import utils, db, config
from lochness.models.keystore import KeyStore
from lochness.sources.sharepoint.models.data_source import (
        SharepointDataSource,
        SharepointDataSourceMetadata
        )
from lochness.sources.sharepoint.tasks.pull_data import (
    fetch_subject_data
)


def test_init():
    pass


def add_sharepoint_keystore(sharepoint_cred: dict, PROJECT_ID: str) -> None:
    encryption_passphrase = config.parse(config_file, 'general')[
            'encryption_passphrase']
    keystore = KeyStore(
            key_name=sharepoint_cred['key_name'],
            key_value=sharepoint_cred['key_value'],
            key_type=sharepoint_cred['key_type'],
            project_id=PROJECT_ID,
            key_metadata={
                'client_id':sharepoint_cred['client_id'],
                'tenant_id':sharepoint_cred['tenant_id'],
                'site_url':sharepoint_cred['site_url'],
                'form_name':sharepoint_cred['form_name'],
                })
    db.execute_queries(config_file,
                       [keystore.to_sql_query(encryption_passphrase)])


def delete_sharepoint_keystore(sharepoint_cred: dict, PROJECT_ID: str) -> None:
    keystore = KeyStore(
            key_name=sharepoint_cred['key_name'],
            key_value=sharepoint_cred['key_value'],
            key_type=sharepoint_cred['key_type'],
            project_id=PROJECT_ID,
            key_metadata={})
    db.execute_queries(config_file,
                       [keystore.delete_record_query()])


def test_adding_and_deleting_cred(prod_data_fixture):
    PROJECT_ID, PROJECT_NAME, SITE_ID, SITE_NAME, SUBJECT_ID, DATASINK_NAME = prod_data_fixture
    sharepoint_cred = config.parse(config_file, 'sharepoint-test')
    add_sharepoint_keystore(sharepoint_cred, PROJECT_ID)
    delete_sharepoint_keystore(sharepoint_cred, PROJECT_ID)


def test_make_sharepoint_datasource_obj(prod_data_fixture):
    PROJECT_ID, PROJECT_NAME, SITE_ID, SITE_NAME, SUBJECT_ID, DATASINK_NAME = prod_data_fixture
    sharepoint_cred = config.parse(config_file, 'sharepoint-test')
    add_sharepoint_keystore(sharepoint_cred, PROJECT_ID)

    sharepointDataSourceMetadata = SharepointDataSourceMetadata(
            keystore_name=sharepoint_cred['key_name'],
            site_url=sharepoint_cred['site_url'],
            form_name=sharepoint_cred['form_name'],
            )

    sharepointDataSource = SharepointDataSource(
            data_source_name=sharepoint_cred['data_source_name'],
            is_active=True,
            site_id=SITE_ID,
            project_id=PROJECT_ID,
            data_source_type='sharepoint',
            data_source_metadata=sharepointDataSourceMetadata)


    delete_sharepoint_keystore(sharepoint_cred, PROJECT_ID)


def test_get_sharepoint_datasource_obj_from_db(prod_data_fixture):
    PROJECT_ID, PROJECT_NAME, SITE_ID, SITE_NAME, SUBJECT_ID, DATASINK_NAME = prod_data_fixture
    sharepoint_cred = config.parse(config_file, 'sharepoint-test')
    add_sharepoint_keystore(sharepoint_cred, PROJECT_ID)

    sharepointDataSourceMetadata = SharepointDataSourceMetadata(
            keystore_name=sharepoint_cred['key_name'],
            site_url=sharepoint_cred['site_url'],
            form_name=sharepoint_cred['form_name'],
            )

    sharepointDataSource = SharepointDataSource(
            data_source_name=sharepoint_cred['data_source_name'],
            is_active=True,
            site_id=SITE_ID,
            project_id=PROJECT_ID,
            data_source_type='sharepoint',
            data_source_metadata=sharepointDataSourceMetadata)

    delete_sharepoint_keystore(sharepoint_cred, PROJECT_ID)


def test_get_sharepoint_datasource_obj_from_db(prod_data_fixture):
    PROJECT_ID, PROJECT_NAME, SITE_ID, SITE_NAME, SUBJECT_ID, DATASINK_NAME = prod_data_fixture
    sharepoint_cred = config.parse(config_file, 'sharepoint-test')
    add_sharepoint_keystore(sharepoint_cred, PROJECT_ID)

    sharepointDataSources_from_db = SharepointDataSource.\
            get_all_sharepoint_data_sources(config_file)

    assert len(sharepointDataSources_from_db) > 0

    delete_sharepoint_keystore(sharepoint_cred, PROJECT_ID)


def test_fetch_subject_data(prod_data_fixture):
    encryption_passphrase = config.parse(config_file, 'general')[
            'encryption_passphrase']
    PROJECT_ID, PROJECT_NAME, SITE_ID, SITE_NAME, SUBJECT_ID, DATASINK_NAME = prod_data_fixture
    sharepoint_cred = config.parse(config_file, 'sharepoint-test')
    add_sharepoint_keystore(sharepoint_cred, PROJECT_ID)

    sharepointDataSourceMetadata = SharepointDataSourceMetadata(
            keystore_name=sharepoint_cred['key_name'],
            site_url=sharepoint_cred['site_url'],
            form_name=sharepoint_cred['form_name'],
            )

    sharepointDataSource = SharepointDataSource(
            data_source_name=sharepoint_cred['data_source_name'],
            is_active=True,
            site_id=SITE_ID,
            project_id=PROJECT_ID,
            data_source_type='sharepoint',
            data_source_metadata=sharepointDataSourceMetadata)

    fetch_subject_data(
        sharepoint_data_source=sharepointDataSource,
        subject_id=SUBJECT_ID,
        encryption_passphrase=encryption_passphrase)

    delete_sharepoint_keystore(sharepoint_cred, PROJECT_ID)
# @patch("lochness.sources.sharepoint.tasks.sync.get_sharepoint_cred")
# @patch("lochness.sources.sharepoint.tasks.sync.msal.ConfidentialClientApplication")
# def test_get_access_token(mock_msal_app, mock_get_sharepoint_cred):
    # """Test getting an access token."""
    # mock_app_instance = MagicMock()
    # mock_app_instance.acquire_token_for_client.return_value = {"access_token": "test_token"}
    # mock_msal_app.return_value = mock_app_instance
    # mock_get_sharepoint_cred.return_value = {
        # "client_id": "test_client_id",
        # "client_secret": "test_client_secret",
        # "tenant_id": "test_tenant_id",
    # }

    # data_source = SharepointDataSource(
        # data_source_name="test_sharepoint",
        # is_active=True,
        # site_id="test_site",
        # project_id="test_project",
        # data_source_type="sharepoint",
        # data_source_metadata=SharepointDataSourceMetadata(
            # keystore_name="test_keystore",
            # site_url="test_site_url",
            # form_id="test_form_id",
        # ),
    # )

    # token = get_access_token(data_source)
    # assert token == "test_token"


# @patch("lochness.sources.sharepoint.tasks.sync.requests.get")
# def test_get_form_responses(mock_get):
    # """Test getting form responses."""
    # mock_response = MagicMock()
    # mock_response.json.return_value = {"value": [{"id": 1, "fields": {"Title": "Test Response"}}]}
    # mock_response.raise_for_status.return_value = None
    # mock_get.return_value = mock_response

    # data_source = SharepointDataSource(
        # data_source_name="test_sharepoint",
        # is_active=True,
        # site_id="test_site",
        # project_id="test_project",
        # data_source_type="sharepoint",
        # data_source_metadata=SharepointDataSourceMetadata(
            # keystore_name="test_keystore",
            # site_url="test_site_url",
            # form_id="test_form_id",
        # ),
    # )

    # responses = get_form_responses(data_source, "test_token")
    # assert len(responses) == 1
    # assert responses[0]["id"] == 1


# @patch("builtins.open", new_callable=MagicMock)
# @patch("lochness.sources.sharepoint.tasks.sync.requests.get")
# def test_download_file(mock_get, mock_open):
    # """Test downloading a file."""
    # mock_response = MagicMock()
    # mock_response.iter_content.return_value = [b"test_content"]
    # mock_response.raise_for_status.return_value = None
    # mock_get.return_value = mock_response

    # download_dir = Path("/tmp/test_download")
    # file_path = download_file("https://test.com/test.txt", "test_token", download_dir)

    # assert file_path == download_dir / "test.txt"
    # mock_open.assert_called_once_with(file_path, "wb")
    # mock_open().__enter__().write.assert_called_once_with(b"test_content")


# @patch("lochness.helpers.utils.get_config_file_path")
# @patch("lochness.helpers.db.execute_queries")
# def test_schedule_sharepoint_download(mock_execute_queries, mock_get_config_file_path):
    # """Test scheduling a SharePoint download."""
    # mock_get_config_file_path.return_value = Path("/fake/config.ini")

    # data_source = SharepointDataSource(
        # data_source_name="test_sharepoint",
        # is_active=True,
        # site_id="test_site",
        # project_id="test_project",
        # data_source_type="sharepoint",
        # data_source_metadata=SharepointDataSourceMetadata(
            # keystore_name="test_keystore",
            # site_url="test_site_url",
            # form_id="test_form_id",
        # ),
    # )

    # schedule_sharepoint_download(data_source, "https://test.com/test.txt", Path("/tmp/test_download"))

    # mock_execute_queries.assert_called_once()
    # args, kwargs = mock_execute_queries.call_args
    # queries = kwargs["queries"]
    # assert len(queries) == 1
    # assert "sharepoint_download" in queries[0]
    # assert "https://test.com/test.txt" in queries[0]


# @patch('psycopg2.connect')
# def test_insert_sharepoint_cred(mock_connect):
    # config_file = utils.get_config_file_path()

    # # how should we handle encryption passphrase?
    # encryption_passphrase = config.parse(config_file, 'general')[
            # 'encryption_passphrase']

    # # 2. Create a KeyStore instance
    # my_key = KeyStore(
        # key_name="sharepoint",
        # key_value=json.dumps({"client_id": "test_client_id", "client_secret": "test_client_secret", "tenant_id": "test_tenant_id"}),
        # key_type="sharepoint",
        # project_id="ProCAN",
        # key_metadata={
            # "description": "Access token for SharePoint",
            # "created_by": "kevin"}
    # )

    # insert_query = my_key.drop_db_table_query()
    # db.execute_queries(  # type: ignore
        # config_file=config_file,
        # queries=[insert_query],
        # show_commands=False,
    # )
    # insert_query = my_key.init_db_table_query()
    # db.execute_queries(  # type: ignore
        # config_file=config_file,
        # queries=insert_query,
        # show_commands=False,
    # )

    # insert_query = my_key.to_sql_query(
            # encryption_passphrase=encryption_passphrase)

    # db.execute_queries(  # type: ignore
        # config_file=config_file,
        # queries=[insert_query],
        # show_commands=False,
    # )

# # To run this test, you need to have a real SharePoint account and set up the credentials in a config file.
# # You also need to create a form and add some responses to it.
# # Then, you can run this test by setting the following environment variables:
# # export LOCHNESS_CONFIG=/path/to/your/config.ini
# # pytest -s -k test_real_sharepoint_sync
# #
# # def test_real_sharepoint_sync():
# #     """Test SharePoint sync with real credentials."""
# #     config_file = utils.get_config_file_path()
# #     data_sources = SharepointDataSource.get_all_sharepoint_data_sources(config_file)
# #     assert len(data_sources) > 0
# #
# #     data_source = data_sources[0]
# #     access_token = get_access_token(data_source)
# #     responses = get_form_responses(data_source, access_token)
# #
# #     for response in responses:
# #         for key, value in response["fields"].items():
# #             if isinstance(value, str) and value.startswith("https://"):
# #                 download_file(value, access_token, Path("/tmp/test_download"))
