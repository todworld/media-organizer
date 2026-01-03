import shutil

def free_bytes(path: str) -> int:
    usage = shutil.disk_usage(path)
    return int(usage.free)
