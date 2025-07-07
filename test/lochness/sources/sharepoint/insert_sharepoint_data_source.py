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

from lochness.helpers import utils, db, config
from lochness.models.data_source import DataSource
from lochness.sources.sharepoint.models.data_source import SharepointDataSourceMetadata

# --- IMPORTANT ---
# REPLACE THESE PLACEHOLDER VALUES WITH YOUR ACTUAL SHAREPOINT SITE AND FORM IDs
SHAREPOINT_SITE_URL = "https://yaleedu.sharepoint.com/sites/ProCAN"
SHAREPOINT_FORM_ID = "b809223c-2986-48a4-821f-fe828fafd85d"

# This should match the key_name used when inserting credentials into the keystore
KEYSTORE_NAME = "sharepoint_prod"

# Data source details
DATA_SOURCE_NAME = "ProCAN_EEG_SharePoint"
SITE_ID = "Yale"
PROJECT_ID = "ProCAN"
DATA_SOURCE_TYPE = "sharepoint"

def main():
    """Inserts a SharePoint data source into the Lochness database."""
    print(f"Attempting to insert SharePoint data source '{DATA_SOURCE_NAME}'...")

    try:
        config_file = utils.get_config_file_path()
        if not config_file.exists():
            print(f"ERROR: Configuration file not found at {config_file}")
            print("Please ensure 'config.ini' exists in the project root.")
            return

        print(f"Using configuration file: {config_file}")

        # Create the metadata for the SharePoint data source
        sharepoint_metadata = SharepointDataSourceMetadata(
            keystore_name=KEYSTORE_NAME,
            site_url=SHAREPOINT_SITE_URL,
            form_id=SHAREPOINT_FORM_ID,
        )

        # Create the generic DataSource object
        data_source = DataSource(
            data_source_name=DATA_SOURCE_NAME,
            data_source_is_active=True,
            site_id=SITE_ID,
            project_id=PROJECT_ID,
            data_source_type=DATA_SOURCE_TYPE,
            data_source_metadata=sharepoint_metadata.model_dump()
        )

        # Initialize data_sources table if it doesn't exist
        init_query = data_source.init_db_table_query()
        db.execute_queries(
            config_file=config_file,
            queries=init_query,
            show_commands=False,
        )
        print("Data sources table initialized (if not already present).")

        # Insert the new data source
        insert_query = data_source.to_sql_query()
        db.execute_queries(
            config_file=config_file,
            queries=[insert_query],
            show_commands=False,
        )
        print(f"Successfully inserted data source '{DATA_SOURCE_NAME}'.")

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
