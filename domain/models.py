from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)
class ScanConfig:
    min_file_size: int
    include_photos: bool
    include_videos: bool
    include_raw: bool
    include_other: bool  # NEW: non-media files grouped by extension

    # UI-choice flags (stored in runs table as snapshot strings)
    overwrite_policy: str          # "UI_CHOICE"
    error_policy: str              # "UI_CHOICE"
    live_photo_policy: str         # "UI_CHOICE"
    thumbs_policy: str             # "UI_CHOICE"

    # Limits set via UI
    cpu_limit_pct: Optional[int] = None
    io_limit_mbps: Optional[int] = None
