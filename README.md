# Lochness v2

This repository contains the Lochness v2 project.

## Running Tests

To run the tests, ensure you have `pytest` installed and set the `PYTHONPATH` environment variable to the absolute path of the project root. This path will need to be adjusted for each testing environment.

From the project root directory, execute the following command:

```bash
PYTHONPATH='/Users/kc244/lochness_new/lochness_v2' /Users/kc244/miniforge3/bin/pytest
```

Note: A `pytest.ini` file is included in the project root to configure `pytest` and exclude certain directories from test collection.
