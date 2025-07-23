#!/usr/bin/env python
"""
Initializes the database.
"""
import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
root_dir = None  # pylint: disable=invalid-name
for parent in file.parents:
    if parent.name == "lochness-v2":
        root_dir = parent

sys.path.append(str(root_dir))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import logging
from typing import Dict, Any

from rich.logging import RichHandler

from lochness.helpers import utils, logs
from lochness.models import init_db

MODULE_NAME = "lochness.scripts.init_db"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}

logging.basicConfig(**logargs)


if __name__ == "__main__":
    config_file = utils.get_config_file_path()
    logs.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger, use_db=False
    )

    console.rule(f"[bold red]{MODULE_NAME}")
    logger.info(f"Using config file: {config_file}")
    if not config_file.exists():
        logger.error(f"Config file does not exist: {config_file}")
        sys.exit(1)

    init_db(config_file=config_file)
    logger.info("Done!")
