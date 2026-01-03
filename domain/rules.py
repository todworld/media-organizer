import os
from datetime import datetime


def classify_media(ext: str) -> str:
    """Classify a file extension into PHOTO/VIDEO/RAW/OTHER.

    Notes:
    - ext should include the leading dot (e.g., ".jpg").
    - Unknown extensions are classified as OTHER.
    """
    from domain.constants import (
        PHOTO_EXTS,
        VIDEO_EXTS,
        RAW_EXTS,
        MEDIA_TYPE_PHOTO,
        MEDIA_TYPE_VIDEO,
        MEDIA_TYPE_RAW,
        MEDIA_TYPE_OTHER,
    )

    e = (ext or "").lower()

    if e in RAW_EXTS:
        return MEDIA_TYPE_RAW
    if e in VIDEO_EXTS:
        return MEDIA_TYPE_VIDEO
    if e in PHOTO_EXTS:
        return MEDIA_TYPE_PHOTO

    return MEDIA_TYPE_OTHER


def choose_date(primary_dt: str | None, mtime_iso: str, primary_source: str = "PRIMARY"):
    """Returns (YYYY-MM-DD, date_source).

    primary_dt:
      - Photos: EXIF DateTimeOriginal (date taken)
      - Videos: container creation_time (created)
    Fallback:
      - filesystem modified time
    """

    if primary_dt:
        try:
            dt = datetime.fromisoformat(primary_dt)
            return dt.strftime("%Y-%m-%d"), primary_source
        except Exception:
            pass

    dt = datetime.fromisoformat(mtime_iso)
    return dt.strftime("%Y-%m-%d"), "MTIME"


def dest_rel_path(media_type: str, date_yyyy_mm_dd: str, filename: str) -> str:
    """Destination relative path for primary (non-duplicate) items."""

    yyyy = date_yyyy_mm_dd[:4]

    if media_type == "PHOTO":
        return os.path.join("Photos", yyyy, date_yyyy_mm_dd, filename)

    if media_type == "VIDEO":
        return os.path.join("Videos", yyyy, date_yyyy_mm_dd, filename)

    if media_type == "RAW":
        return os.path.join("RAW", yyyy, date_yyyy_mm_dd, filename)

    # OTHER (group by extension)
    ext = os.path.splitext(filename)[1].lstrip(".").upper()
    ext_tag = ext if ext else "NOEXT"
    return os.path.join("OtherByExt", ext_tag, filename)


def duplicates_rel_path(run_id: str, filename: str) -> str:
    return os.path.join("Duplicates", run_id, filename)


def resolve_collision(dest_folder: str, filename: str) -> tuple[str, int]:
    """Returns (new_filename, suffix_num). suffix_num 0 means no change.

    Collision policy: (1), (2)...
    """

    base, ext = os.path.splitext(filename)
    candidate = filename
    n = 0
    while os.path.exists(os.path.join(dest_folder, candidate)):
        n += 1
        candidate = f"{base} ({n}){ext}"

    return candidate, n
