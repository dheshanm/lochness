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

from lochness.sources.minio.tasks.credentials import insert_minio_cred

# MinIO Credentials
KEY_NAME = "minio_dev"
ACCESS_KEY = "lochness-dev"
SECRET_KEY = "MCedHMB0KbPOhubVK81UcYaDy2tgFB4pxjQtrHcX"
ENDPOINT_URL = "http://pnl-minio-1.partners.org:9000/"
PROJECT_ID = "ProCAN"

def main():
    print(f"Attempting to insert MinIO credentials with key_name '{KEY_NAME}' for project '{PROJECT_ID}'...")
    try:
        insert_minio_cred(
            key_name=KEY_NAME,
            access_key=ACCESS_KEY,
            secret_key=SECRET_KEY,
            endpoint_url=ENDPOINT_URL,
            project_id=PROJECT_ID
        )
        print("MinIO credentials inserted successfully.")
    except Exception as e:
        print(f"An error occurred while inserting MinIO credentials: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
