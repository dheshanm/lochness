"""
Helper functions for the pipeline
"""
from pathlib import Path

from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from lochness.helpers import cli

_console = Console(color_system="standard")


def get_progress_bar(transient: bool = False) -> Progress:
    """
    Returns a rich Progress object with standard columns.

    Returns:
        Progress: A rich Progress object with standard columns.
    """
    return Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        transient=transient,
    )


def get_console() -> Console:
    """
    Returns a Console object with standard color system.

    Returns:
        Console: A Console object with standard color system.
    """
    return _console


def get_config_file_path() -> Path:
    """
    Returns the path to the config file.
    Checks <repo_root>/config.ini and <repo_root>/lochness_v2/config.ini.
    Returns the first that exists, else raises FileNotFoundError.
    """
    repo_root = cli.get_repo_root()
    candidates = [
        Path(repo_root) / "config.ini",
        Path(repo_root) / "lochness_v2" / "config.ini",
        Path(repo_root) / "sample.config.ini",
        Path(repo_root) / "lochness_v2" / "sample.config.ini",
    ]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(f"Config file not found in any of: {[str(c) for c in candidates]}")


def get_timestamp() -> str:
    """
    Returns the current timestamp as a string in YYYYMMDD_HHMMSS format.
    """
    from datetime import datetime
    return datetime.now().strftime("%Y%m%d_%H%M%S")
