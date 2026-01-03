# organizer_io/hash_stream.py
# Keep the same API but ensure it supports chunk_bytes param

from blake3 import blake3


def sha256_file(path: str, chunk_bytes: int = 4 * 1024 * 1024) -> str:
    """
    Backward-compatible name. Uses BLAKE3.
    """
    h = blake3()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_bytes), b""):
            h.update(chunk)
    return h.hexdigest()
