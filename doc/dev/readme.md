# Developer Setup and Testing Guide

This guide provides instructions for setting up your development environment and running the test suite for the Lochness project.

## 1. Prerequisites

Before you begin, ensure you have the following installed and configured:

*   **Python 3.10+**: We recommend using `miniforge` or `conda` for environment management.
*   **PostgreSQL Database**: Lochness uses PostgreSQL for its metadata and credential storage. Ensure you have a running PostgreSQL instance and database.
*   **MinIO Server (Optional, for MinIO tests)**: If you plan to run the MinIO object storage tests, you'll need a running MinIO instance.
*   **Microsoft Azure Active Directory Application Registration (for SharePoint tests)**: If you plan to run the SharePoint tests, you'll need an Azure AD application registered with appropriate permissions (`Sites.Read.All`, `Files.Read.All`) and its `client_id`, `client_secret`, and `tenant_id`.

## 2. Project Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd lochness_v2
    ```

2.  **Create and activate a Python environment:**
    ```bash
    conda create -n lochness_dev python=3.10
    conda activate lochness_dev
    ```

3.  **Install project dependencies:**
    ```bash
    pip install -r requirements.txt # Assuming a requirements.txt exists or will be created
    pip install pytest minio msal pandas psycopg2-binary sqlalchemy
    ```
    *(Note: `psycopg2-binary` is used for convenience; `psycopg2` might require build tools.)*

4.  **Configure `config.ini`:**
    Create a `config.ini` file in the project root (`/Users/kc244/lochness_v2/config.ini`) with your database connection details and an encryption passphrase.

    ```ini
    [general]
    repo_root=/Users/kc244/lochness_v2
    encryption_passphrase=your-strong-encryption-passphrase

    [postgresql]
    host=your_db_host
    port=5432
    database=your_lochness_db
    user=your_db_user
    password=your_db_password

    [logging]
    # Add logging configurations as needed
    lochness.scripts.init_db=data/logs/init_db.log
    lochness.sources.redcap.tasks.refresh_metadata=data/logs/redcap_refresh_metadata.log
    lochness.sources.xnat.tasks.sync=data/logs/xant_sync.log
    lochness.sources.sharepoint.tasks.sync=data/logs/sharepoint_sync.log

    # Optional: For SharePoint development/testing convenience, you can add credentials here.
    # In production, these should come from environment variables.
    [sharepoint]
    application_id=your_sharepoint_client_id
    client_secret=your_sharepoint_client_secret
    tenant_id=your_sharepoint_tenant_id
    ```
    **IMPORTANT:** Replace placeholder values with your actual credentials and paths.

## 3. Database Setup and Credential Insertion

The following scripts populate your database with necessary test data and securely store credentials. Run them in the specified order.

1.  **Set `PYTHONPATH`**: Always set `PYTHONPATH` to the project root before running Lochness scripts.
    ```bash
    export PYTHONPATH=/Users/kc244/lochness_v2
    ```
    *(Note: If you're using a different shell or IDE, adjust how you set environment variables.)*

2.  **Add Supported Data Source Types**:
    ```bash
    /Users/kc244/miniforge3/bin/python lochness/scripts/db_cli.py "INSERT INTO supported_data_source_types (data_source_type, data_source_metadata_dict) VALUES ('sharepoint', '{"keystore_name": "Name of the keystore entry for SharePoint credentials", "site_url": "URL of the SharePoint site", "form_id": "GUID of the SharePoint List (form) ID"}');"
    # Repeat for 'xnat' if not already present
    # /Users/kc244/miniforge3/bin/python lochness/scripts/db_cli.py "INSERT INTO supported_data_source_types (data_source_type, data_source_metadata_dict) VALUES ('xnat', '{"api_token": "XNAT API token", "endpoint_url": "XNAT endpoint URL", "subject_id_variable": "XNAT subject ID variable"}');"
    ```

3.  **Insert SharePoint Credentials**:
    *   Ensure your SharePoint credentials are in `config.ini` under `[sharepoint]` (as shown above) or set as environment variables (`SHAREPOINT_CLIENT_ID`, `SHAREPOINT_CLIENT_SECRET`, `SHAREPOINT_TENANT_ID`).
    ```bash
    /Users/kc244/miniforge3/bin/python test/lochness/sources/sharepoint/insert_sharepoint_credentials.py
    ```

4.  **Insert MinIO Credentials**:
    *   **Option A (Hardcoded for testing - edit script directly):** Edit `test/lochness/sources/minio/insert_minio_credentials.py` and replace the placeholder values with your MinIO `access_key`, `secret_key`, and `endpoint_url`.
    *   **Option B (Environment Variables - recommended):** Set the following environment variables:
        ```bash
        export MINIO_ACCESS_KEY="your_minio_access_key"
        export MINIO_SECRET_KEY="your_minio_secret_key"
        export MINIO_ENDPOINT_URL="your_minio_endpoint_url"
        ```
    ```bash
    /Users/kc244/miniforge3/bin/python test/lochness/sources/minio/insert_minio_credentials.py
    ```

5.  **Setup Test Dataflow (Projects, Sites, Subjects, SharePoint Data Source)**:
    ```bash
    /Users/kc244/miniforge3/bin/python test/lochness/scripts/setup_test_dataflow.py
    ```

6.  **Insert SharePoint Data Source (if not covered by setup_test_dataflow.py or if you need specific values)**:
    *   Edit `test/lochness/sources/sharepoint/insert_sharepoint_data_source.py` to set `SHAREPOINT_SITE_URL` and `SHAREPOINT_FORM_ID` to your specific SharePoint site and form GUID.
    ```bash
    /Users/kc244/miniforge3/bin/python test/lochness/sources/sharepoint/insert_sharepoint_data_source.py
    ```

7.  **Insert MinIO Data Sink**:
    *   Edit `test/lochness/sources/minio/insert_minio_data_sink.py` to set `MINIO_BUCKET_NAME`, `MINIO_REGION`, and `MINIO_ENDPOINT_URL` to your specific MinIO details.
    ```bash
    /Users/kc244/miniforge3/bin/python test/lochness/sources/minio/insert_minio_data_sink.py
    ```

## 4. Running All Tests

Once the database and external services are configured and populated, you can run all tests using the `run_all_tests.py` script:

```bash
/Users/kc244/miniforge3/bin/python run_all_tests.py
```

This script will execute all `pytest` tests found in the `test/` directory and provide a summary of the results.

---
