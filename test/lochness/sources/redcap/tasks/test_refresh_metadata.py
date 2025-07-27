import sys
import os
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
from test_module import fake_data_fixture, config_file
from lochness.sources.redcap.models.data_source import (
        RedcapDataSourceMetadata,
        RedcapDataSource
        )
from lochness.sources.redcap.tasks.refresh_metadata import (
        fetch_metadata,
        refresh_all_metadata
        )
from lochness.models.subjects import Subject
from lochness.helpers import logs, utils, db, config


def test_init():
    pass


def test_fetch_metadata(fake_data_fixture):
    PROJECT_ID, PROJECT_NAME, SITE_ID, \
            SITE_NAME, SUBJECT_ID, DATASINK_NAME = fake_data_fixture

    config_file = utils.get_config_file_path()
    encryption_passphrase = config.parse(config_file, 'general')[
            'encryption_passphrase']
    redcap_cred = config.parse(config_file, 'redcap-test')
    data_source_name = redcap_cred['data_source_name']

    redcapDataSourceMetadata = RedcapDataSourceMetadata(
            keystore_name=redcap_cred['key_name'],
            endpoint_url=redcap_cred['endpoint_url'],
            subject_id_variable=redcap_cred['subject_id_variable'],
            optional_variables_dictionary=[],
            main_redcap=redcap_cred['main_redcap'])

    redcapDataSource = RedcapDataSource(
        data_source_name=data_source_name,
        is_active=True,
        site_id=SITE_ID,
        project_id=PROJECT_ID,
        data_source_type='redcap',
        data_source_metadata=redcapDataSourceMetadata)

    df = fetch_metadata(redcapDataSource, encryption_passphrase)
    assert len(df) > 0


def test_refresh_all_metadata(fake_data_fixture):
    #TODO: what happens when there are two redcap data sources, one for
    # main demo, and another for PENNCNB?
    PROJECT_ID, PROJECT_NAME, SITE_ID, \
            SITE_NAME, SUBJECT_ID, DATASINK_NAME = fake_data_fixture

    config_file = utils.get_config_file_path()
    refresh_all_metadata(config_file, PROJECT_ID, SITE_ID)

    subject_obj_list = Subject.get_subjects_for_project_site(
            PROJECT_ID, SITE_ID, config_file)
    assert len(subject_obj_list) > 0


def test_penncnb_redcap_fetch_metadata(fake_data_fixture):
    PROJECT_ID, PROJECT_NAME, SITE_ID, \
            SITE_NAME, SUBJECT_ID, DATASINK_NAME = fake_data_fixture

    config_file = utils.get_config_file_path()
    encryption_passphrase = config.parse(config_file, 'general')[
            'encryption_passphrase']
    redcap_penncnb_cred = config.parse(config_file, 'redcap-penncnb-test')
    data_source_name = redcap_penncnb_cred['data_source_name']

    redcapDataSourceMetadata = RedcapDataSourceMetadata(
            keystore_name=redcap_penncnb_cred['key_name'],
            endpoint_url=redcap_penncnb_cred['endpoint_url'],
            optional_variables_dictionary=[],
            subject_id_variable=redcap_penncnb_cred['subject_id_variable'],
            main_redcap=redcap_penncnb_cred['main_redcap'])

    redcapDataSource = RedcapDataSource(
        data_source_name=data_source_name,
        is_active=True,
        site_id=SITE_ID,
        project_id=PROJECT_ID,
        data_source_type='redcap',
        data_source_metadata=redcapDataSourceMetadata)

    df = fetch_metadata(redcapDataSource, encryption_passphrase)
    assert len(df) > 0
