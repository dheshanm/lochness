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
from lochness.sources.redcap.models.data_source import (
    RedcapDataSourceMetadata,
    RedcapDataSource
)
from lochness.sources.redcap.tasks.refresh_metadata import refresh_all_metadata
from lochness.sources.redcap.tasks.pull_data import (
    fetch_subject_data,
    save_subject_data,
    pull_all_data
)
from lochness.models.subjects import Subject
from lochness.models.data_pulls import DataPull
from lochness.helpers import logs, utils, db, config


def make_redcap_data_source(config_file, config_section, site_id, project_id):
    cred = config.parse(config_file, config_section)
    metadata_kwargs = dict(
        keystore_name=cred['key_name'],
        endpoint_url=cred['endpoint_url'],
        subject_id_variable=cred['subject_id_variable'],
        optional_variables_dictionary=[]
    )
    # Add optional keys if present
    for key in ['subject_id_variable_as_the_pk', 'messy_subject_id']:
        if key in cred:
            metadata_kwargs[key] = cred[key]
    metadata = RedcapDataSourceMetadata(**metadata_kwargs)
    return RedcapDataSource(
        data_source_name=cred['data_source_name'],
        is_active=True,
        site_id=site_id,
        project_id=project_id,
        data_source_type='redcap',
        data_source_metadata=metadata
    )


def get_encryption_passphrase(config_file):
    return config.parse(config_file, 'general')['encryption_passphrase']


def get_output_file_path(config_file, project_id, site_id, subject_id, data_source_name):
    lochness_root = config.parse(config_file, 'general')['lochness_root']
    project_name_cap = project_id[:1].upper() + project_id[1:].lower()
    output_dir = (
        Path(lochness_root)
        / project_name_cap
        / "PHOENIX"
        / "PROTECTED"
        / f"{project_name_cap}{site_id}"
        / "raw"
        / subject_id
        / "surveys"
    )
    file_name = f"{subject_id}.{project_name_cap}.{data_source_name}.json"
    return output_dir / file_name


def assert_file_and_db(config_file, file_path, project_id, site_id, subject_id):
    assert file_path.is_file()
    # DB checks
    data_pull_query = f"""SELECT subject_id FROM data_pull
    WHERE subject_id = '{subject_id}'
      AND project_id = '{project_id}'
      AND site_id = '{site_id}'
      AND file_path = '{file_path}'
      """
    df = db.execute_sql(config_file, data_pull_query)
    assert len(df) > 0
    files_query = f"""SELECT file_path FROM files
    WHERE file_path = '{file_path}'
      """
    df = db.execute_sql(config_file, files_query)
    assert len(df) > 0
    data_push_query = f"""SELECT file_path FROM data_push
    WHERE file_path = '{file_path}'
      """
    df = db.execute_sql(config_file, data_push_query)
    assert len(df) > 0


def cleanup_output(config_file, project_id):
    lochness_root = config.parse(config_file, 'general')['lochness_root']
    project_name_cap = project_id[:1].upper() + project_id[1:].lower()
    outdir_root = Path(lochness_root) / project_name_cap
    shutil.rmtree(outdir_root)


def test_init():
    pass


def test_fetch_subject_data_redcap(prod_data_fixture):
    PROJECT_ID, PROJECT_NAME, SITE_ID, SITE_NAME, SUBJECT_ID, DATASINK_NAME = prod_data_fixture
    config_file = utils.get_config_file_path()
    encryption_passphrase = get_encryption_passphrase(config_file)
    data_source = make_redcap_data_source(config_file, "redcap-test", SITE_ID, PROJECT_ID)
    data = fetch_subject_data(data_source, SUBJECT_ID, encryption_passphrase)
    assert len(data) > 100


def test_fetch_subject_data_penncnb(prod_data_fixture):
    PROJECT_ID, PROJECT_NAME, SITE_ID, SITE_NAME, SUBJECT_ID, DATASINK_NAME = prod_data_fixture
    config_file = utils.get_config_file_path()
    SUBJECT_ID=config.parse(config_file, 'prod-test-info')['subject_id_penncnb']

    encryption_passphrase = get_encryption_passphrase(config_file)
    data_source = make_redcap_data_source(config_file, "redcap-penncnb-test", SITE_ID, PROJECT_ID)
    data = fetch_subject_data(data_source, SUBJECT_ID, encryption_passphrase)
    assert len(data) < 100


@pytest.mark.parametrize("config_section", ["redcap-test", "redcap-penncnb-test"])
def test_save_subject_data(prod_data_fixture, config_section):
    PROJECT_ID, PROJECT_NAME, SITE_ID, SITE_NAME, SUBJECT_ID, DATASINK_NAME = prod_data_fixture
    config_file = utils.get_config_file_path()
    encryption_passphrase = get_encryption_passphrase(config_file)
    data_source = make_redcap_data_source(config_file, config_section, SITE_ID, PROJECT_ID)
    data = fetch_subject_data(data_source, SUBJECT_ID, encryption_passphrase)
    assert save_subject_data(data, PROJECT_ID, SITE_ID, SUBJECT_ID, data_source.data_source_name, config_file)
    file_path = get_output_file_path(config_file, PROJECT_ID, SITE_ID, SUBJECT_ID, data_source.data_source_name)
    assert file_path.is_file()


@pytest.mark.parametrize("config_section", ["redcap-test", "redcap-penncnb-test"])
def test_pull_and_push_single_data(prod_data_fixture, config_section):
    PROJECT_ID, PROJECT_NAME, SITE_ID, SITE_NAME, SUBJECT_ID, DATASINK_NAME = prod_data_fixture

    config_file = utils.get_config_file_path()
    SUBJECT_ID=config.parse(config_file, 'prod-test-info')['subject_id_penncnb']
    refresh_all_metadata(config_file, PROJECT_ID, SITE_ID)
    pull_all_data(config_file=config_file,
                  project_id=PROJECT_ID,
                  site_id=SITE_ID,
                  subject_id_list=[SUBJECT_ID],
                  push_to_sink=True)
    data_source = make_redcap_data_source(config_file, config_section, SITE_ID, PROJECT_ID)
    file_path = get_output_file_path(config_file, PROJECT_ID, SITE_ID, SUBJECT_ID, data_source.data_source_name)
    assert_file_and_db(config_file, file_path, PROJECT_ID, SITE_ID, SUBJECT_ID)
    cleanup_output(config_file, PROJECT_ID)


def test_pull_all_data(prod_data_fixture):
    PROJECT_ID, PROJECT_NAME, SITE_ID, SITE_NAME, SUBJECT_ID, DATASINK_NAME = prod_data_fixture

    config_file = utils.get_config_file_path()
    refresh_all_metadata(config_file, PROJECT_ID, SITE_ID)
    pull_all_data(config_file=config_file,
                  project_id=PROJECT_ID,
                  site_id=SITE_ID,
                  push_to_sink=True)

    for config_section in ['redcap-test', 'redcap-penncnb-test']:
        data_source = make_redcap_data_source(config_file, config_section, SITE_ID, PROJECT_ID)
        file_path = get_output_file_path(config_file, PROJECT_ID, SITE_ID, SUBJECT_ID, data_source.data_source_name)
    assert_file_and_db(config_file, file_path, PROJECT_ID, SITE_ID, SUBJECT_ID)
    cleanup_output(config_file, PROJECT_ID)
