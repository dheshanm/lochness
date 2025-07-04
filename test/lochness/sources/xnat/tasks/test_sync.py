import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
root_dir = None  # pylint: disable=invalid-name
for parent in file.parents:
    if parent.name == "lochness-v2":
        root_dir = parent

sys.path.append(str(root_dir))


from lochness.sources.xnat.tasks.sync import (
        get_xnat_cred,
        insert_xnat_cred,
        )


def test_insert_xnat_cred():
    print('h')
    insert_xnat_cred()


def test_get_xnat_cred():
    print('h')
    get_xnat_cred()

