from lochness.tasks import push_data
from test_module import fake_data_fixture

def test_example(fake_data_fixture):
    PROJECT_ID, PROJECT_NAME, SITE_ID, SITE_NAME, SUBJECT_ID = fake_data_fixture
