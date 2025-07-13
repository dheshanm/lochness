import sys
from pathlib import Path

# Add project root to Python path
file = Path(__file__).resolve()
parent = file.parent
root_dir = None
for p in file.parents:
    if p.name == 'lochness_v2':
        root_dir = p
if root_dir:
    sys.path.append(str(root_dir))
else:
    # If running from project root, add current dir
    sys.path.append(str(parent))

from lochness.helpers import utils, db
from lochness.models.data_source import DataSource
from lochness.sources.mindlamp.models.data_source import MindLAMPDataSourceMetadata

# Data Source Details
DATA_SOURCE_NAME = "MindLAMP_Test_DS"
SITE_ID = "TestSite" # Should match the site_id used in setup_test_dataflow.py or your actual site
PROJECT_ID = "ProCAN" # Should match the project_id used in setup_test_dataflow.py or your actual project

# This must match the key_name used when inserting MindLAMP credentials
KEYSTORE_NAME = "mindlamp_prod"

# MindLAMP API URL
MINDLAMP_API_URL = "https://mindlamp.example.com/api"

def insert_mindlamp_data_source():
    """Insert a test MindLAMP data source into the database."""
    
    # Create the data source metadata
    data_source_metadata = MindLAMPDataSourceMetadata(
        keystore_name=KEYSTORE_NAME,
        api_url=MINDLAMP_API_URL,
    )
    
    # Create the data source
    data_source = DataSource(
        data_source_name=DATA_SOURCE_NAME,
        data_source_is_active=True,
        site_id=SITE_ID,
        project_id=PROJECT_ID,
        data_source_type="mindlamp",
        data_source_metadata=data_source_metadata.model_dump(),
    )
    
    # Get config file path
    config_file = utils.get_config_file_path()
    
    # Insert the data source into the database
    db.execute_queries(config_file, [data_source.to_sql_query()], show_commands=True)
    
    print(f"Successfully inserted MindLAMP data source: {DATA_SOURCE_NAME}")
    print(f"  Site: {SITE_ID}")
    print(f"  Project: {PROJECT_ID}")
    print(f"  Keystore Name: {KEYSTORE_NAME}")
    print(f"  API URL: {MINDLAMP_API_URL}")

if __name__ == "__main__":
    insert_mindlamp_data_source() 