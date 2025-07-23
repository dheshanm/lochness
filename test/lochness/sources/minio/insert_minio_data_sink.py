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
from lochness.models.data_sinks import DataSink

# Data Sink Details
DATA_SINK_NAME = "MinIO_Test_Sink"
SITE_ID = "TestSite" # Should match the site_id used in setup_test_dataflow.py or your actual site
PROJECT_ID = "TestProject" # Should match the project_id used in setup_test_dataflow.py or your actual project

# This must match the key_name used when inserting MinIO credentials
KEYSTORE_NAME = "minio_dev"

# MinIO specific metadata (non-sensitive, but defines the sink)
MINIO_BUCKET_NAME = "lochness-test-bucket"
MINIO_REGION = "us-east-1" # MinIO often doesn't use regions, but good to have a placeholder
MINIO_ENDPOINT_URL = "http://pnl-minio-1.partners.org:9000/" # Must match the endpoint used for credentials

def main():
    print(f"Attempting to insert MinIO data sink '{DATA_SINK_NAME}'...")

    try:
        config_file = utils.get_config_file_path()
        if not config_file.exists():
            print(f"ERROR: Configuration file not found at {config_file}")
            print("Please ensure 'config.ini' exists in the project root.")
            return

        print(f"Using configuration file: {config_file}")

        # Data sink metadata (references the keystore entry)
        data_sink_metadata = {
            "type": "minio",
            "bucket_name": MINIO_BUCKET_NAME,
            "region": MINIO_REGION,
            "endpoint_url": MINIO_ENDPOINT_URL,
            "keystore_name": KEYSTORE_NAME,
        }

        # Create the DataSink object
        data_sink = DataSink(
            data_sink_name=DATA_SINK_NAME,
            site_id=SITE_ID,
            project_id=PROJECT_ID,
            data_sink_metadata=data_sink_metadata
        )

        # Initialize data_sinks table if it doesn't exist
        init_query = data_sink.init_db_table_query()
        db.execute_queries(
            config_file=config_file,
            queries=[init_query],
            show_commands=False,
        )
        print("Data sinks table initialized (if not already present).")

        # Insert the new data sink
        insert_query = data_sink.to_sql_query()
        db.execute_queries(
            config_file=config_file,
            queries=[insert_query],
            show_commands=False,
        )
        print(f"Successfully inserted data sink '{DATA_SINK_NAME}'.")

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
