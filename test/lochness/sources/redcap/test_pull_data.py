import os
import sys
import shutil
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


def test_init():
    pass


def test_fetch_subject_data(prod_data_fixture):
    PROJECT_ID, PROJECT_NAME, SITE_ID, \
            SITE_NAME, SUBJECT_ID, DATASINK_NAME = prod_data_fixture

    config_file = utils.get_config_file_path()
    encryption_passphrase = config.parse(config_file, 'general')[
            'encryption_passphrase']
    redcap_cred = config.parse(config_file, 'redcap-test')
    data_source_name = 'main_redcap'

    redcapDataSourceMetadata = RedcapDataSourceMetadata(
            keystore_name=redcap_cred['key_name'],
            endpoint_url=redcap_cred['endpoint_url'],
            subject_id_variable=redcap_cred['subject_id_variable'],
            optional_variables_dictionary=[])

    redcapDataSource = RedcapDataSource(
        data_source_name=data_source_name,
        is_active=True,
        site_id=SITE_ID,
        project_id=PROJECT_ID,
        data_source_type='redcap',
        data_source_metadata=redcapDataSourceMetadata)

    data = fetch_subject_data(
            redcapDataSource,
            SUBJECT_ID,
            encryption_passphrase)

    assert len(data) > 100

def test_save_subject_data(prod_data_fixture):
    PROJECT_ID, PROJECT_NAME, SITE_ID, \
            SITE_NAME, SUBJECT_ID, DATASINK_NAME = prod_data_fixture

    config_file = utils.get_config_file_path()
    encryption_passphrase = config.parse(config_file, 'general')[
            'encryption_passphrase']
    redcap_cred = config.parse(config_file, 'redcap-test')
    data_source_name = 'main_redcap'

    redcapDataSourceMetadata = RedcapDataSourceMetadata(
            keystore_name=redcap_cred['key_name'],
            endpoint_url=redcap_cred['endpoint_url'],
            subject_id_variable=redcap_cred['subject_id_variable'],
            optional_variables_dictionary=[])

    redcapDataSource = RedcapDataSource(
        data_source_name=data_source_name,
        is_active=True,
        site_id=SITE_ID,
        project_id=PROJECT_ID,
        data_source_type='redcap',
        data_source_metadata=redcapDataSourceMetadata)

    data = fetch_subject_data(
            redcapDataSource,
            SUBJECT_ID,
            encryption_passphrase)

    assert save_subject_data(data, PROJECT_ID, SITE_ID, SUBJECT_ID, 
                             data_source_name, config_file)

    lochness_root = config.parse(config_file, 'general')['lochness_root']
    project_name_cap = PROJECT_ID[:1].upper() + \
            PROJECT_ID[1:].lower()

    output_dir = (
        Path(lochness_root)
        / project_name_cap
        / "PHOENIX"
        / "PROTECTED"
        / f"{project_name_cap}{SITE_ID}"
        / "raw"
        / SUBJECT_ID
        / "surveys"
    )
    file_name = f"{SUBJECT_ID}.{project_name_cap}.{data_source_name}.json"
    file_path = output_dir / file_name
    assert file_path.is_file()


def test_pull_and_push_single_data(prod_data_fixture):
    PROJECT_ID, PROJECT_NAME, SITE_ID, \
            SITE_NAME, SUBJECT_ID, DATASINK_NAME = prod_data_fixture
    config_file = utils.get_config_file_path()

    refresh_all_metadata(config_file, PROJECT_ID, SITE_ID)

    pull_all_data(config_file=config_file,
                  project_id=PROJECT_ID,
                  site_id=SITE_ID,
                  subject_id_list=[SUBJECT_ID],
                  push_to_sink=True)

    lochness_root = config.parse(config_file, 'general')['lochness_root']
    project_name_cap = PROJECT_ID[:1].upper() + \
            PROJECT_ID[1:].lower()
    outdir_root = Path(lochness_root) / project_name_cap

    data_source_name = 'main_redcap'
    output_dir = (outdir_root
        / "PHOENIX"
        / "PROTECTED"
        / f"{project_name_cap}{SITE_ID}"
        / "raw"
        / SUBJECT_ID
        / "surveys"
    )
    file_name = f"{SUBJECT_ID}.{project_name_cap}.{data_source_name}.json"
    file_path = output_dir / file_name

    assert file_path.is_file()
    
    # make sure the function has added records to DB
    data_pull_qeury = f"""SELECT subject_id FROM data_pull
    WHERE subject_id = '{SUBJECT_ID}'
      AND project_id = '{PROJECT_ID}'
      AND site_id = '{SITE_ID}'
      AND file_path = '{file_path}'
      """
    df = db.execute_sql(config_file, data_pull_qeury)
    assert len(df) > 0

    files_qeury = f"""SELECT file_path FROM files
    WHERE file_path = '{file_path}'
      """
    df = db.execute_sql(config_file, files_qeury)
    assert len(df) > 0

    data_push_qeury = f"""SELECT file_path FROM data_push
    WHERE file_path = '{file_path}'
      """
    df = db.execute_sql(config_file, data_push_qeury)
    assert len(df) > 0

    shutil.rmtree(outdir_root)
