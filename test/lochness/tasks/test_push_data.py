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

print(root_dir)
sys.path.append(str(root_dir))
sys.path.append(str(root_dir / 'test'))

from lochness.tasks.push_data import push_all_data
from test_module import fake_data_fixture, config_file


def test_example(fake_data_fixture):
    PROJECT_ID, PROJECT_NAME, SITE_ID, SITE_NAME, SUBJECT_ID = fake_data_fixture
    push_all_data(config_file, PROJECT_ID, SITE_ID)
