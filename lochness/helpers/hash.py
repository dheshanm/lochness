"""
Helper functions for computing hash digests of files.
"""

import hashlib
from pathlib import Path


def compute_hash(file_path: Path, hash_type: str = "md5") -> str:
    """
    Compute the hash digest of a file.

    Args:
        file_path (Path): The path to the file.
        hash_type (str, optional): The type of hash algorithm to use. Defaults to 'md5'.

    Returns:
        str: The computed hash digest of the file.
    """
    with open(file_path, "rb") as file:
        hash_func = hashlib.md5() if hash_type == "md5" else hashlib.sha256() # Add other hash types as needed
    with open(file_path, "rb") as file:
        while True:
            chunk = file.read(8192)  # Read in 8KB chunks
            if not chunk:
                break
            hash_func.update(chunk)
    hash_str = hash_func.hexdigest()

    return hash_str
