from pathlib import Path
from lochness.helpers import db, utils, config
from lochness.models.data_sinks import DataSink
from lochness.sinks.minio.push import push_file
import sys
import pandas as pd
import sqlalchemy

PROJECT_ID = "ProCAN_WEB"
SITE_ID = "CP"

config_file = utils.get_config_file_path()

general_config = config.parse(config_file, section="general")
encryption_passphrase = general_config.get("encryption_passphrase")
if not encryption_passphrase:
    print("Encryption passphrase not found in config.ini under [general].")
    sys.exit(1)

sink_df = db.execute_sql(config_file, f"SELECT * FROM data_sinks WHERE project_id = '{PROJECT_ID}' AND site_id = '{SITE_ID}';")
if sink_df.empty:
    print(f"No data sink found for project {PROJECT_ID}, site {SITE_ID}.")
    sys.exit(1)
data_sink_row = sink_df.iloc[0]
data_sink = DataSink(
    data_sink_name=data_sink_row['data_sink_name'],
    site_id=data_sink_row['site_id'],
    project_id=data_sink_row['project_id'],
    data_sink_metadata=data_sink_row['data_sink_metadata']
)

# Debug: Try a minimal query first
try:
    print("Testing minimal query: SELECT file_path FROM files LIMIT 1;")
    test_df = db.execute_sql(config_file, "SELECT file_path FROM files LIMIT 1;")
    print(test_df)
    if test_df.empty:
        print("No files found in the files table.")
        sys.exit(1)
except Exception as e:
    print(f"Minimal query failed: {e}")
    print(f"Config file: {config_file}")
    print(f"DB config: {config.parse(config_file, section='postgresql')}")
    sys.exit(1)

# Try a simple query with no LIKE clause to debug the error
simple_query = "SELECT file_path FROM files LIMIT 10;"
print(f"Running simple file query: {simple_query}")
try:
    engine = db.get_db_connection(config_file)
    simple_df = pd.read_sql(simple_query, engine)
    print(simple_df)
    engine.dispose()
except Exception as e:
    print(f"Simple file query failed: {e}")
    sys.exit(1)

# If the simple query works, try incrementally adding filtering
if not simple_df.empty:
    print("Trying with project filter...")
    project_filter_query = f"SELECT file_path FROM files WHERE file_path LIKE '%/{PROJECT_ID}/%';"
    try:
        engine = db.get_db_connection(config_file)
        project_df = pd.read_sql(project_filter_query, engine)
        print(project_df)
        engine.dispose()
    except Exception as e:
        print(f"Project filter query failed: {e}")
        sys.exit(1)

# Try a parameterized query for the LIKE pattern with double percent signs
like_pattern = f"%%/{PROJECT_ID}/{SITE_ID}/%%"
site_filter_query = "SELECT file_path FROM files WHERE file_path LIKE %s;"
print(f"Running parameterized site filter query: {site_filter_query} with pattern {like_pattern}")
try:
    engine = db.get_db_connection(config_file)
    site_df = pd.read_sql(site_filter_query, engine, params=[like_pattern])
    print(site_df)
    engine.dispose()
except Exception as e:
    print(f"Parameterized site filter query failed: {e}")
    sys.exit(1)

# Fetch all file paths from the files table
all_files_query = "SELECT file_path FROM files;"
print(f"Running all files query: {all_files_query}")
try:
    engine = db.get_db_connection(config_file)
    all_files_df = pd.read_sql(all_files_query, engine)
    engine.dispose()
except Exception as e:
    print(f"All files query failed: {e}")
    sys.exit(1)

# Filter in Python for project and site
pattern = f"/{PROJECT_ID}/{SITE_ID}/"
filtered_df = all_files_df[all_files_df['file_path'].str.contains(pattern)]
print(f"Filtered files for {pattern}:")
print(filtered_df)

if filtered_df.empty:
    print(f"No files found for project {PROJECT_ID}, site {SITE_ID}.")
    sys.exit(1)

# Proceed to push files
for _, row in filtered_df.iterrows():
    file_path = Path(row['file_path'])
    push_metadata = {}  # Add any metadata you want to include
    print(f"Pushing {file_path} to sink {data_sink.data_sink_name}...")
    success = push_file(
        file_path=file_path,
        data_sink=data_sink,
        config_file=config_file,
        push_metadata=push_metadata,
        encryption_passphrase=encryption_passphrase
    )
    print(f"  Success: {success}") 