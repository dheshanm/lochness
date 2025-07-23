import subprocess
import sys
import os
from pathlib import Path

# Get the project root directory
project_root = Path(__file__).resolve().parent

# Set PYTHONPATH to include the project root
os.environ['PYTHONPATH'] = str(project_root)

# Define the pytest command
# We run pytest on the 'test/' directory, which will discover all tests.
pytest_command = [
    sys.executable, # Use the current Python interpreter
    "-m",
    "pytest",
    str(project_root / "test"),
    "-v", # Verbose output
    "-s", # Allow print statements to show
]

def run_tests():
    print("\n====================================================================")
    print("Running all Lochness tests...")
    print("--------------------------------------------------------------------")
    print("NOTE: Some tests require a PostgreSQL database with specific setup.")
    print("      Please ensure you have run the following setup scripts:")
    print("      - test/lochness/scripts/setup_test_dataflow.py")
    print("      - test/lochness/sources/sharepoint/insert_sharepoint_credentials.py")
    print("      - test/lochness/sources/sharepoint/insert_sharepoint_data_source.py")
    print("      - test/lochness/sources/minio/insert_minio_credentials.py")
    print("      - test/lochness/sources/minio/insert_minio_data_sink.py")
    print("      Also ensure external services (MinIO, SharePoint) are accessible.")
    print("--------------------------------------------------------------------")

    try:
        # Run pytest as a subprocess
        result = subprocess.run(pytest_command, check=False, capture_output=True, text=True)

        print("\n====================================================================")
        print("Pytest Output:")
        print("====================================================================")
        print(result.stdout)
        print(result.stderr)
        print("====================================================================")

        if result.returncode == 0:
            print("All tests passed successfully!")
            return 0
        else:
            print(f"Tests failed with exit code {result.returncode}.")
            return result.returncode

    except FileNotFoundError:
        print("ERROR: 'pytest' command not found. Please ensure pytest is installed (pip install pytest).")
        return 1
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return 1

if __name__ == "__main__":
    exit_code = run_tests()
    sys.exit(exit_code)
