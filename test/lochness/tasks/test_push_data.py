import sys
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

from lochness.models.files import File
from lochness.models.data_sinks import DataSink
from lochness.models.data_pulls import DataPull
from lochness.tasks.push_data import (
        push_file_to_sink,
        push_all_data,
        get_matching_dataSink_list,
        simple_push_file_to_sink
        )
from lochness.helpers import logs, utils, db, config
from test_module import fake_data_fixture, config_file



def test_dataSink_methods(fake_data_fixture):
    PROJECT_ID, PROJECT_NAME, SITE_ID, \
            SITE_NAME, SUBJECT_ID, DATASINK_NAME = fake_data_fixture
    dataSink = DataSink(
            data_sink_name=DATASINK_NAME,
            site_id=SITE_ID,
            project_id=PROJECT_ID,
            data_sink_metadata={'type': 'minio'})
    config_file = utils.get_config_file_path()
    assert type(int(dataSink.get_data_sink_id(config_file))) == int


def test_push_file_to_sink(fake_data_fixture):
    PROJECT_ID, PROJECT_NAME, SITE_ID, \
            SITE_NAME, SUBJECT_ID, DATASINK_NAME = fake_data_fixture
    test_file = Path('test_file.zip')
    file_obj = File(file_path=test_file, with_hash=True)
    file_obj.modality = 'surveys'

    config_file = utils.get_config_file_path()
    redcap_cred = config.parse(config_file, 'redcap-test')
    minio_cred = config.parse(config_file, 'datasink-test')
    dataSink = DataSink.get_matching_data_sink(
            config_file=config_file,
            data_sink_name=DATASINK_NAME,
            site_id=SITE_ID,
            project_id=PROJECT_ID,
            active_only=True)
    encryption_passphrase = config.parse(config_file, 'general')[
            'encryption_passphrase']
    result = push_file_to_sink(file_obj=file_obj,
                               modality=file_obj.modality,
                               dataSink=dataSink,
                               data_source_name=redcap_cred['data_source_name'],
                               project_id=PROJECT_ID,
                               site_id=SITE_ID,
                               subject_id=SUBJECT_ID,
                               config_file=config_file,
                               encryption_passphrase=encryption_passphrase)
    assert result


def test_simple_push_file_to_sink(fake_data_fixture):
    PROJECT_ID, PROJECT_NAME, SITE_ID, \
            SITE_NAME, SUBJECT_ID, DATASINK_NAME = fake_data_fixture
    test_file = Path('test_file.zip')

    simple_push_file_to_sink(test_file)


def test_push_all_data(fake_data_fixture):
    PROJECT_ID, PROJECT_NAME, SITE_ID, \
            SITE_NAME, SUBJECT_ID, DATASINK_NAME = fake_data_fixture
    push_all_data(config_file, PROJECT_ID, SITE_ID)
