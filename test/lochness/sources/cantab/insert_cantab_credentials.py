import json
import sys
import os
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

from lochness.helpers import utils, db, config
from lochness.models.keystore import KeyStore

# The name we will use to refer to these credentials in the keystore
KEY_NAME = "cantab_prod"
PROJECT_ID = "ProCAN" # Or your specific project ID

def main():
    print(f"Attempting to insert CANTAB credentials with key_name '{KEY_NAME}' for project '{PROJECT_ID}'...")

    try:
        config_file = utils.get_config_file_path()
        if not config_file.exists():
            print(f"ERROR: Configuration file not found at {config_file}")
            print("Please ensure 'config.ini' exists in the project root.")
            return

        print(f"Using configuration file: {config_file}")

        # Try to read credentials from config.ini first
        cantab_config = config.parse(config_file, 'cantab')
        username = cantab_config.get("username")
        password = cantab_config.get("password")

        if not all([username, password]):
            print("CANTAB credentials not found in config.ini under [cantab] section. Checking environment variables...")
            username = os.environ.get("CANTAB_USERNAME")
            password = os.environ.get("CANTAB_PASSWORD")
            if not all([username, password]):
                print("ERROR: CANTAB credentials (username, password) not found in config.ini or environment variables.")
                print("Please ensure they are set in config.ini under [cantab] or as environment variables (CANTAB_USERNAME, CANTAB_PASSWORD).")
                return
            else:
                print("CANTAB credentials found in environment variables.")
        else:
            print("CANTAB credentials found in config.ini.")

        CANTAB_CREDENTIALS = {
            "username": username,
            "password": password,
        }

        encryption_passphrase = config.parse(config_file, 'general')['encryption_passphrase']

        # Create a KeyStore instance
        cantab_key = KeyStore(
            key_name=KEY_NAME,
            key_value=json.dumps(CANTAB_CREDENTIALS),
            key_type="cantab",
            project_id=PROJECT_ID,
            key_metadata={
                "description": "Credentials for CANTAB API",
                "created_by": "script"
            }
        )

        # Initialize keystore table if it doesn't exist
        init_query = cantab_key.init_db_table_query()
        db.execute_queries(
            config_file=config_file,
            queries=init_query,
            show_commands=False,
        )
        print("Keystore table initialized (if not already present).")

        # Insert the new key
        insert_query = cantab_key.to_sql_query(
            encryption_passphrase=encryption_passphrase)
        db.execute_queries(
            config_file=config_file,
            queries=[insert_query],
            show_commands=False,
        )
        print(f"Successfully inserted credentials with key_name '{KEY_NAME}' into the keystore.")

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
