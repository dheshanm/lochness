"""
Logging configuration
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import List

from lochness.helpers import config

logger = logging.getLogger(__name__)


def configure_logging(config_file: Path, module_name: str, logger: logging.Logger):
    """
    Configures logging for a given module using the specified configuration file.

    Args:
        config_file (str): The path to the configuration file.
        module_name (str): The name of the module to configure logging for.
        logger (logging.Logger): The logger object to use for logging.

    Returns:
        None
    """
    log_params = config.parse(config_file, "logging")
    log_file_r = log_params[module_name]

    if log_file_r.startswith("/"):
        log_file = Path(log_file_r)
    else:
        general_params = config.parse(config_file, "general")
        repo_root = Path(general_params["repo_root"])

        log_file = repo_root / log_file_r

    if log_file.exists() and log_file.stat().st_size > 10000000:  # 10MB
        archive_file = (
            log_file.parent
            / "archive"
            / f"{log_file.stem}_{datetime.now().strftime('%Y%m%d%H%M%S')}.log"
        )
        logger.info(f"Rotating log file to {archive_file}")

        archive_file.parent.mkdir(parents=True, exist_ok=True)
        log_file.rename(archive_file)

    file_handler = logging.FileHandler(log_file, mode="a")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s  - %(process)d - %(name)s - %(levelname)s - %(message)s - [%(filename)s:%(lineno)d]"
        )
    )

    logging.getLogger().addHandler(file_handler)
    logger.info(f"Logging to {log_file}")


def silence_logs(
    noisy_modules: List[str], target_level: int = logging.INFO
) -> None:
    """
    Silences logs from specified modules.

    Args:
        noisy_modules (List[str]): A list of modules to silence.
        target_level (int): The target log level to set the modules to.

    Returns:
        None
    """
    for module in noisy_modules:
        logger.debug(f"Setting log level for {module} to {target_level}")
        logging.getLogger(module).setLevel(target_level)
