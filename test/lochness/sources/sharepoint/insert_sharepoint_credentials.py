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
KEY_NAME = "sharepoint_prod"
PROJECT_ID = "ProCAN" # Or your specific project ID

def main():
    """Inserts SharePoint credentials into the Lochness keystore."""
    client_id = None
    client_secret = None
    tenant_id = None

    try:
        config_file = utils.get_config_file_path()
        if not config_file.exists():
            print(f"ERROR: Configuration file not found at {config_file}")
            print("Please ensure 'config.ini' exists in the project root.")
            return

        print(f"Using configuration file: {config_file}")
        
        # Try to read credentials from config.ini first
        try:
            sharepoint_config = config.parse(config_file, 'sharepoint')
            client_id = sharepoint_config.get("application_id")
            client_secret = sharepoint_config.get("client_secret")
            tenant_id = sharepoint_config.get("tenant_id")
            if all([client_id, client_secret, tenant_id]):
                print("SharePoint credentials found in config.ini.")
            else:
                print("SharePoint credentials not fully specified in config.ini. Checking environment variables...")
                client_id = None # Reset to ensure we don't use partial config
                client_secret = None
                tenant_id = None
        except Exception as e:
            print(f"[sharepoint] section not found or incomplete in config.ini. Checking environment variables... (Error: {e})")

        # If not found in config.ini, try environment variables
        if not all([client_id, client_secret, tenant_id]):
            client_id = os.environ.get("SHAREPOINT_CLIENT_ID")
            client_secret = os.environ.get("SHAREPOINT_CLIENT_SECRET")
            tenant_id = os.environ.get("SHAREPOINT_TENANT_ID")
            if all([client_id, client_secret, tenant_id]):
                print("SharePoint credentials found in environment variables.")

        if not all([client_id, client_secret, tenant_id]):
            print("ERROR: SharePoint credentials (client_id, client_secret, tenant_id) not found in config.ini or environment variables.")
            print("Please ensure they are set in config.ini under [sharepoint] or as environment variables (SHAREPOINT_CLIENT_ID, SHAREPOINT_CLIENT_SECRET, SHAREPOINT_TENANT_ID).")
            return

        SHAREPOINT_CREDENTIALS = {
            "client_id": client_id,
            "client_secret": client_secret,
            "tenant_id": tenant_id
        }

        print(f"Attempting to insert credentials with key_name '{KEY_NAME}'...")

        encryption_passphrase = config.parse(config_file, 'general')['encryption_passphrase']

        # Create a KeyStore instance
        sharepoint_key = KeyStore(
            key_name=KEY_NAME,
            key_value=json.dumps(SHAREPOINT_CREDENTIALS),
            key_type="sharepoint",
            project_id=PROJECT_ID,
            key_metadata={
                "description": "Credentials for production SharePoint",
                "created_by": "script"
            }
        )

        # Initialize keystore table if it doesn't exist
        init_query = sharepoint_key.init_db_table_query()
        db.execute_queries(
            config_file=config_file,
            queries=init_query,
            show_commands=False,
        )
        print("Keystore table initialized (if not already present).")

        # Insert the new key
        insert_query = sharepoint_key.to_sql_query(encryption_passphrase=encryption_passphrase)
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