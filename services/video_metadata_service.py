#services/video_metadata_service.py

from typing import Optional
import subprocess
import json
import os

def extract_video_created_datetime(path: str) -> Optional[str]:
    """
    Returns ISO datetime string (YYYY-MM-DDTHH:MM:SS) or None.
    Uses ffprobe if available.
    """
    try:
        # ffprobe must be available on PATH
        cmd = [
            "ffprobe",
            "-v", "quiet",
            "-print_format", "json",
            "-show_entries", "format_tags=creation_time",
            path
        ]
        p = subprocess.run(cmd, capture_output=True, text=True)
        if p.returncode != 0:
            return None

        data = json.loads(p.stdout)
        tags = data.get("format", {}).get("tags", {})
        ct = tags.get("creation_time")
        if not ct:
            return None

        # Normalize: "2023-07-14T18:22:10.000000Z"
        ct = ct.replace("Z", "")
        if "." in ct:
            ct = ct.split(".")[0]

        return ct
    except Exception:
        return None
