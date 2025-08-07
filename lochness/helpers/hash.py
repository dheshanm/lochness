"""
Helper functions for computing hash digests of files.
"""

import hashlib
from pathlib import Path

from blake3 import blake3


def compute_hash(
    file_path: Path, hash_type: str = "md5", chunk_size: int = 8192
) -> str:
    """
    Compute the hash digest of a file.

    Args:
        file_path (Path): The path to the file.
        hash_type (str, optional): The type of hash algorithm to use. Defaults to 'md5'.
        chunk_size (int, optional): Size of chunks to read. Defaults to 8192.

    Returns:
        str: The computed hash digest of the file.
    """
    if not file_path.is_file():
        raise FileNotFoundError(
            f"The file {file_path} does not exist or is not a file."
        )

    if hash_type not in hashlib.algorithms_available:
        raise ValueError(f"Hash type '{hash_type}' is not supported.")
    hash_func = hashlib.new(hash_type)

    with file_path.open("rb") as file:
        for chunk in iter(lambda: file.read(chunk_size), b""):
            hash_func.update(chunk)
    return hash_func.hexdigest()


def compute_fingerprint(
    file_path: Path | str,
    total_sample_bytes: int = 64 * 1024,
    chunks: int = 4,
    hash_type: str = "blake3",
) -> str:
    """
    Produce a fixed-cost fingerprint of `path` by either:
    - hashing the entire file if it's <= total_sample_bytes,
    - or hashing `chunks` equally spaced pieces of size
    total_sample_bytes // chunks otherwise.

    Returns the hex digest string.
    """
    path = Path(file_path)
    size = path.stat().st_size

    if chunks < 1:
        raise ValueError("`chunks` must be >= 1")
    if total_sample_bytes < chunks:
        raise ValueError("`total_sample_bytes` must be >= `chunks`")

    # open once, pick the strategy
    with path.open("rb") as f:
        if hash_type == "blake3":
            h = blake3()  # pylint: disable=E1102
        else:
            if hash_type not in hashlib.algorithms_available:
                raise ValueError(f"Hash type '{hash_type}' is not supported.")
            h = hashlib.new(hash_type)

        # small file: hash in a cheap, streaming fashion
        if size <= total_sample_bytes:
            for chunk in iter(lambda: f.read(4096), b""):
                h.update(chunk)
            return h.hexdigest()

        # large file: sample “chunks” slices of size piece
        piece = total_sample_bytes // chunks
        step = (size - piece) / (chunks - 1)

        for i in range(chunks):
            offset = int(i * step)
            f.seek(offset)
            h.update(f.read(piece))

        return h.hexdigest()
