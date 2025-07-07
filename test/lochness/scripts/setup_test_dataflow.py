import sys
from pathlib import Path
from typing import Dict, Any

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
from lochness.models.projects import Project
from lochness.models.sites import Site
from lochness.models.subjects import Subject
from lochness.models.data_source import DataSource
from lochness.sources.sharepoint.models.data_source import SharepointDataSourceMetadata


def setup_test_dataflow_data():
    config_file = utils.get_config_file_path()
    if not config_file.exists():
        print(f"ERROR: Configuration file not found at {config_file}")
        print("Please ensure 'config.ini' exists in the project root.")
        return

    print(f"Using configuration file: {config_file}")

    # --- Test Data Definitions ---
    # Project
    test_project_id = "TestProject"
    test_project_name = "Test Project for Dataflow"
    test_project_metadata = {"description": "A project for testing Lochness dataflow"}

    # Site
    test_site_id = "TestSite"
    test_site_name = "Test Site for Dataflow"
    test_site_metadata = {"location": "Test Location"}

    # Subject
    test_subject_id = "SUB001"
    test_subject_name = "Test Subject 001"
    test_subject_metadata = {"age": 30, "gender": "Male"}

    # SharePoint Data Source
    test_sharepoint_ds_name = "TestSharePointDS"
    test_sharepoint_site_url = "https://yourcompany.sharepoint.com/sites/TestTeam"
    test_sharepoint_form_id = "00000000-0000-0000-0000-000000000000" # Placeholder GUID
    test_keystore_name = "sharepoint_prod" # Must match the name used in insert_sharepoint_credentials.py

    # --- Insert Project ---
    project_exists = db.execute_sql(config_file, f"SELECT COUNT(*) FROM projects WHERE project_id = '{test_project_id}';").iloc[0, 0]
    if project_exists == 0:
        print(f"Inserting project '{test_project_name}'...")
        project = Project(
            project_id=test_project_id,
            project_name=test_project_name,
            project_metadata=test_project_metadata
        )
        db.execute_queries(config_file, [project.to_sql_query()], show_commands=False)
        print("Project inserted.")
    else:
        print(f"Project '{test_project_id}' already exists, skipping project insertion.")

    # --- Insert Site ---
    site_exists = db.execute_sql(config_file, f"SELECT COUNT(*) FROM sites WHERE site_id = '{test_site_id}' AND project_id = '{test_project_id}';").iloc[0, 0]
    if site_exists == 0:
        print(f"Inserting site '{test_site_name}'...")
        site = Site(
            site_id=test_site_id,
            site_name=test_site_name,
            site_metadata=test_site_metadata,
            project_id=test_project_id # Link to the test project
        )
        db.execute_queries(config_file, [site.to_sql_query()], show_commands=False)
        print("Site inserted.")
    else:
        print(f"Site '{test_site_id}' for project '{test_project_id}' already exists, skipping site insertion.")

    # --- Insert Subject ---
    subject_exists = db.execute_sql(config_file, f"SELECT COUNT(*) FROM subjects WHERE subject_id = '{test_subject_id}' AND site_id = '{test_site_id}' AND project_id = '{test_project_id}';").iloc[0, 0]
    if subject_exists == 0:
        print(f"Inserting subject '{test_subject_name}'...")
        subject = Subject(
            subject_id=test_subject_id,
            subject_name=test_subject_name,
            subject_metadata=test_subject_metadata,
            site_id=test_site_id, # Link to the test site
            project_id=test_project_id # Link to the test project
        )
        db.execute_queries(config_file, [subject.to_sql_query()], show_commands=False)
        print("Subject inserted.")
    else:
        print(f"Subject '{test_subject_id}' for site '{test_site_id}' and project '{test_project_id}' already exists, skipping subject insertion.")

    # --- Insert SharePoint Data Source ---
    # Ensure the 'sharepoint' type is in supported_data_source_types table
    # (This is handled by a separate script or manual insertion)
    ds_count = db.execute_sql(config_file, "SELECT COUNT(*) FROM data_sources WHERE data_source_name = '%s';" % test_sharepoint_ds_name).iloc[0, 0]
    if ds_count == 0:
        print(f"Inserting SharePoint data source '{test_sharepoint_ds_name}'...")
        sharepoint_metadata = SharepointDataSourceMetadata(
            keystore_name=test_keystore_name,
            site_url=test_sharepoint_site_url,
            form_id=test_sharepoint_form_id,
        )
        data_source = DataSource(
            data_source_name=test_sharepoint_ds_name,
            is_active=True,
            site_id=test_site_id,
            project_id=test_project_id,
            data_source_type="sharepoint",
            data_source_metadata=sharepoint_metadata.model_dump()
        )
        db.execute_queries(config_file, [data_source.to_sql_query()], show_commands=False)
        print("SharePoint Data Source inserted.")
    else:
        print(f"Data source '{test_sharepoint_ds_name}' already exists, skipping insertion.")

    print("Test dataflow setup complete.")

if __name__ == "__main__":
    setup_test_dataflow_data()
