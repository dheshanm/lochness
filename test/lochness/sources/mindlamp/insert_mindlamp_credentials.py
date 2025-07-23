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
from lochness.models.keystore import KeyStore

# Keystore Details
KEY_NAME = "mindlamp_prod"
PROJECT_ID = "ProCAN" # Should match the project_id used in setup_test_dataflow.py or your actual project

# MindLAMP Credentials (replace with actual values)
ACCESS_KEY = "your_mindlamp_access_key"
SECRET_KEY = "your_mindlamp_secret_key"

def insert_mindlamp_credentials():
    """Insert MindLAMP credentials into the keystore."""
    
    # Create the keystore entry
    keystore_entry = KeyStore(
        key_name=KEY_NAME,
        project_id=PROJECT_ID,
        key_value={
            "access_key": ACCESS_KEY,
            "secret_key": SECRET_KEY,
        },
    )
    
    # Get config file path
    config_file = utils.get_config_file_path()
    
    # Insert the keystore entry into the database
    db.execute_queries(config_file, [keystore_entry.to_sql_query()], show_commands=True)
    
    print(f"Successfully inserted MindLAMP credentials: {KEY_NAME}")
    print(f"  Project: {PROJECT_ID}")
    print(f"  Access Key: {ACCESS_KEY[:8]}...")
    print(f"  Secret Key: {SECRET_KEY[:8]}...")

if __name__ == "__main__":
    insert_mindlamp_credentials() 