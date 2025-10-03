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
    authenticate,
    get_site_id,
    get_drives,
    find_drive_by_name,
    find_folder_in_drive,
    find_subfolder,
    fetch_subject_data
)


def test_init():
    pass


def test_authenticate():
    sharepoint_cred = config.parse(config_file, 'sharepoint-test')
    headers = authenticate(
            client_id=sharepoint_cred['client_id'],
            tenant_id=sharepoint_cred['tenant_id'],
            client_secret=sharepoint_cred['client_secret'])

    assert len(headers['Authorization']) > 100


def test_get_matching_folder():
    sharepoint_cred = config.parse(config_file, 'sharepoint-test')

    form_name = sharepoint_cred['form_name']
    headers = authenticate(
            client_id=sharepoint_cred['client_id'],
            tenant_id=sharepoint_cred['tenant_id'],
            client_secret=sharepoint_cred['client_secret'])

    sharepoint_site_id = get_site_id(
            headers,
            sharepoint_cred['site_url'])

    drives = get_drives(sharepoint_site_id, headers)

    team_forms_drive = find_drive_by_name(drives, "Team Forms")
    if not team_forms_drive:
        raise RuntimeError("Team Forms drive not found.")
    drive_id = team_forms_drive["id"]

    responses_folder = find_folder_in_drive(drive_id,
                                            "Responses",
                                            headers)
    if not responses_folder:
        raise RuntimeError("Responses folder not found in Team Forms drive.")

    matching_folder = find_subfolder(
            drive_id,
            responses_folder["id"],
            form_name,
            headers)

    print(matching_folder)


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
            modality=sharepoint_cred['modality'],
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
            modality=sharepoint_cred['modality'],
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
    # SUBJECT_ID = 'TE12345'
    SUBJECT_ID = 'TS00001'
    sharepoint_cred = config.parse(config_file, 'sharepoint-test')
    add_sharepoint_keystore(sharepoint_cred, PROJECT_ID)

    sharepointDataSourceMetadata = SharepointDataSourceMetadata(
            keystore_name=sharepoint_cred['key_name'],
            site_url=sharepoint_cred['site_url'],
            form_name=sharepoint_cred['form_name'],
            modality=sharepoint_cred['modality']
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
        subject_id=SUBJECT_ID)

    # delete_sharepoint_keystore(sharepoint_cred, PROJECT_ID)

