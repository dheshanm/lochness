import sys
from pathlib import Path
import argparse

# Add project root to Python path
file = Path(__file__).resolve()
parent = file.parent
root_dir = None
for p in file.parents:
    if p.name == 'lochness-v2':
        root_dir = p
if root_dir:
    sys.path.append(str(root_dir))
else:
    # If running from project root, add current dir
    sys.path.append(str(parent))

from lochness.helpers import utils, db

def main():
    parser = argparse.ArgumentParser(description="Execute SQL queries against the Lochness database.")
    parser.add_argument("query", type=str, help="The SQL query to execute.")
    args = parser.parse_args()

    try:
        config_file = utils.get_config_file_path()
        if not config_file.exists():
            print(f"ERROR: Configuration file not found at {config_file}")
            print("Please ensure 'config.ini' exists in the project root.")
            sys.exit(1)

        print(f"Executing query: {args.query}")
        results = db.execute_queries(
            config_file=config_file,
            queries=[args.query],
            show_commands=True,
            silent=False,
            on_failure=None # Don't exit on failure, let the script handle it
        )
        print("Query executed successfully.")
        if results:
            for result_set in results:
                if result_set:
                    print("Results:")
                    for row in result_set:
                        print(row)

    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
