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
from lochness.tasks.push_data import (
        push_file_to_sink,
        push_all_data,
        get_matching_dataSink_list
        )
from lochness.helpers import logs, utils, db, config
from lochness.sinks.minio.push import push_file
from test_module import fake_data_fixture, config_file


def test_init():
    pass


def test_push_file(fake_data_fixture):
    PROJECT_ID, PROJECT_NAME, SITE_ID, \
            SITE_NAME, SUBJECT_ID, DATASINK_NAME = fake_data_fixture
    test_file = Path('test_file.zip')
    config_file = utils.get_config_file_path()
    dataSink = DataSink.get_matching_data_sink(
            config_file=config_file,
            data_sink_name=DATASINK_NAME,
            site_id=SITE_ID,
            project_id=PROJECT_ID,
            active_only=True)
    encryption_passphrase = config.parse(config_file, 'general')[
            'encryption_passphrase']
    result = push_file(
            test_file,
            dataSink,
            config_file,
            {
                'data_source_name': 'redcap_test',
                'subject_id': SUBJECT_ID,
             },
            encryption_passphrase)
    assert result

