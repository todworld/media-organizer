from datetime import datetime

def now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()

def mtime_iso(path: str) -> str:
    import os
    ts = os.path.getmtime(path)
    return datetime.fromtimestamp(ts).replace(microsecond=0).isoformat()
