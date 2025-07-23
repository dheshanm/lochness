#!/usr/bin/env python
"""
Initializes the database.
"""
import sys
import logging
from typing import Dict, Any

from rich.logging import RichHandler

from lochness.helpers import utils
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
    if not config_file.exists():
        logger.error(f"Config file does not exist: {config_file}")
        sys.exit(1)

    init_db(config_file=config_file)
    logger.info("Done!")
