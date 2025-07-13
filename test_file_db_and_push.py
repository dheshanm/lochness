from pathlib import Path
from lochness.models.files import File
from lochness.helpers import db, utils, config
import sys

# Path to the downloaded file (update if needed)
file_path = Path("/Users/kc244/lochness_new/lochness_v2/data/ProCAN_WEB/CP/redcap_CP_1752284212721/CP07188/20250712_140439.csv")

print(f"Testing File model for: {file_path}")

try:
    file_model = File(file_path=file_path)
    print("File name:", file_model.file_name)
    print("File type:", file_model.file_type)
    print("File size (MB):", file_model.file_size_mb)
    print("File m_time:", file_model.m_time)
    print("File md5:", file_model.md5)

    # Insert into DB
    config_file = utils.get_config_file_path()
    sql = file_model.to_sql_query()
    print("SQL to insert file:")
    print(sql)
    db.execute_queries(config_file=config_file, queries=[sql], show_commands=True)
    print("File inserted into DB.")

    # Optionally, push to data sink (pseudo-code, implement as needed)
    # from lochness.models.data_push import DataPush
    # data_push = DataPush(...)
    # data_push.insert(config_file)

except Exception as e:
    print(f"Error: {e}")
    sys.exit(1) 