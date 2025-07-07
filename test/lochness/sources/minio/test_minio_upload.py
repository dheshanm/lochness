import sys
import os
import hashlib
import time
from pathlib import Path
from minio import Minio
from minio.error import S3Error
import json

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
from lochness.models.data_sinks import DataSink
from lochness.models.data_push import DataPush
from lochness.models.files import File
from lochness.sources.minio.tasks.credentials import get_minio_cred

TEST_DATA_SINK_NAME = "MinIO_Test_Sink"
TEST_SITE_ID = "TestSite"
TEST_PROJECT_ID = "ProCAN"

# This must match the key_name used when inserting MinIO credentials
KEYSTORE_NAME = "minio_dev"

# MinIO specific metadata (non-sensitive, but defines the sink)
MINIO_BUCKET_NAME = "lochness-test-bucket"
MINIO_REGION = "us-east-1" # MinIO often doesn't use regions, but good to have a placeholder
MINIO_ENDPOINT_URL = "http://pnl-minio-1.partners.org:9000/" # Must match the endpoint used for credentials

# Dummy file details
TEST_FILE_NAME = "test_upload_file.txt"
TEST_FILE_CONTENT = b"This is a test file for MinIO upload.\n"

def calculate_md5(file_path: Path) -> str:
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def main():
    print(f"Attempting to upload a test file to MinIO via data sink '{TEST_DATA_SINK_NAME}'...")

    config_file = utils.get_config_file_path()
    if not config_file.exists():
        print(f"ERROR: Configuration file not found at {config_file}")
        print("Please ensure 'config.ini' exists in the project root.")
        return

    # --- Insert MinIO Data Sink (ensures it exists for the test) ---
    print(f"Ensuring MinIO data sink '{TEST_DATA_SINK_NAME}' exists...")
    data_sink_metadata_for_insert = {
        "type": "minio",
        "bucket_name": MINIO_BUCKET_NAME,
        "region": MINIO_REGION,
        "endpoint_url": MINIO_ENDPOINT_URL,
        "keystore_name": KEYSTORE_NAME,
    }
    data_sink_obj = DataSink(
        data_sink_name=TEST_DATA_SINK_NAME,
        site_id=TEST_SITE_ID,
        project_id=TEST_PROJECT_ID,
        data_sink_metadata=data_sink_metadata_for_insert
    )
    db.execute_queries(config_file, [data_sink_obj.init_db_table_query()], show_commands=False)
    db.execute_queries(config_file, [data_sink_obj.to_sql_query()], show_commands=False)
    print(f"MinIO data sink '{TEST_DATA_SINK_NAME}' ensured.")

    # 1. Create a dummy file
    test_file_path = Path("/tmp") / TEST_FILE_NAME
    with open(test_file_path, "wb") as f:
        f.write(TEST_FILE_CONTENT)
    print(f"Created dummy file: {test_file_path}")
    file_md5 = calculate_md5(test_file_path)

    try:
        # 2. Retrieve Data Sink details from DB
        data_sink_query = f"SELECT data_sink_id, data_sink_metadata FROM data_sinks WHERE data_sink_name = '{TEST_DATA_SINK_NAME}' AND site_id = '{TEST_SITE_ID}' AND project_id = '{TEST_PROJECT_ID}';"
        data_sink_record = db.execute_sql(config_file, data_sink_query).iloc[0]
        data_sink_id = data_sink_record["data_sink_id"]
        data_sink_metadata = data_sink_record["data_sink_metadata"]
        print(f"DEBUG: data_sink_metadata fetched: {data_sink_metadata}")
        print(f"DEBUG: type of data_sink_metadata: {type(data_sink_metadata)}")

        if data_sink_metadata is None:
            # This should ideally not happen now that we insert it above
            print(f"ERROR: Data sink '{TEST_DATA_SINK_NAME}' not found in database after insertion attempt.")
            return
        bucket_name = data_sink_metadata["bucket_name"]
        keystore_name = data_sink_metadata["keystore_name"]
        endpoint_url = data_sink_metadata["endpoint_url"]

        # 3. Retrieve MinIO credentials from KeyStore
        minio_creds = get_minio_cred(keystore_name, TEST_PROJECT_ID)
        access_key = minio_creds["access_key"]
        secret_key = minio_creds["secret_key"]

        # 4. Initialize MinIO client
        client = Minio(
            endpoint_url.replace("http://", "").replace("https://", ""), # MinIO client expects host:port
            access_key=access_key,
            secret_key=secret_key,
            secure=endpoint_url.startswith("https"),
        )

        # 5. Ensure bucket exists
        if not client.bucket_exists(bucket_name):
            print(f"Bucket '{bucket_name}' does not exist. Creating...")
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")

        # 6. Upload the file
        object_name = f"test_uploads/{TEST_PROJECT_ID}/{TEST_SITE_ID}/{TEST_FILE_NAME}"
        print(f"Uploading '{test_file_path}' to '{bucket_name}/{object_name}'...")
        start_time = time.time()
        client.fput_object(
            bucket_name,
            object_name,
            str(test_file_path),
            content_type="text/plain",
        )
        end_time = time.time()
        push_time_s = int(end_time - start_time)
        print(f"Upload successful. Time taken: {push_time_s} seconds.")

        # 7. Record data push in DB
        # First, ensure the file entry exists in the 'files' table
        file_obj = File(
            file_path=test_file_path,
        )
        # This will insert or update the file record
        db.execute_queries(config_file, [file_obj.init_db_table_query()], show_commands=False)
        db.execute_queries(config_file, [file_obj.to_sql_query()], show_commands=False)
        print("File record inserted/updated in 'files' table.")

        data_push = DataPush(
            data_sink_id=data_sink_id,
            file_path=str(test_file_path),
            file_md5=file_obj.md5,
            push_time_s=push_time_s,
            push_metadata={
                "object_name": object_name,
                "bucket_name": bucket_name,
                "endpoint_url": endpoint_url,
            },
            push_timestamp=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        )
        db.execute_queries(config_file, [data_push.to_sql_query()], show_commands=False)
        print("Data push record inserted into 'data_push' table.")

    except S3Error as e:
        print(f"MinIO S3 Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Clean up dummy file
        if test_file_path.exists():
            os.remove(test_file_path)
            print(f"Cleaned up dummy file: {test_file_path}")

if __name__ == "__main__":
    main()
