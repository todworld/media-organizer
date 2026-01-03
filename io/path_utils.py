import os

def norm_abs_path(p: str) -> str:
    return os.path.abspath(os.path.normpath(p))

def ensure_parent(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
