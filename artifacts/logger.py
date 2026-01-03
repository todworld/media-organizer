import os
from utils.timeutil import now_iso

class RunLogger:
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def log(self, msg: str):
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(f"{now_iso()} {msg}\n")
