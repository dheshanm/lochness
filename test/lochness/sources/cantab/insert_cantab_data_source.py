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
from lochness.sources.cantab.models.data_source import CANTABDataSourceMetadata

# Data Source Details
DATA_SOURCE_NAME = "CANTAB_Test_DS"
SITE_ID = "TestSite" # Should match the site_id used in setup_test_dataflow.py or your actual site
PROJECT_ID = "ProCAN" # Should match the project_id used in setup_test_dataflow.py or your actual project

# This must match the key_name used when inserting CANTAB credentials
KEYSTORE_NAME = "cantab_prod"

# CANTAB API Endpoint (from documentation: https://app.cantab.com/api/)
CANTAB_API_ENDPOINT = "https://app.cantab.com/api"

def main():
    print(f"Attempting to insert CANTAB data source '{DATA_SOURCE_NAME}'...")

    try:
        config_file = utils.get_config_file_path()
        if not config_file.exists():
            print(f"ERROR: Configuration file not found at {config_file}")
            print("Please ensure 'config.ini' exists in the project root.")
            return

        print(f"Using configuration file: {config_file}")

        # Data source metadata (references the keystore entry)
        data_source_metadata = {
            "keystore_name": KEYSTORE_NAME,
            "api_endpoint": CANTAB_API_ENDPOINT,
        }

        # Create the DataSource object
        data_source = DataSource(
            data_source_name=DATA_SOURCE_NAME,
            is_active=True,
            site_id=SITE_ID,
            project_id=PROJECT_ID,
            data_source_type="cantab",
            data_source_metadata=data_source_metadata
        )

        # Initialize data_sources table if it doesn't exist
        init_query = data_source.init_db_table_query()
        db.execute_queries(
            config_file=config_file,
            queries=[init_query],
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
