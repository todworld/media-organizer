# services/metadata_service.py

from __future__ import annotations

from datetime import datetime
from typing import Optional

from PIL import Image, ExifTags


_EXIF_DT_FORMATS = (
    "%Y:%m:%d %H:%M:%S",
    "%Y:%m:%d %H:%M:%S.%f",
)


def _parse_exif_datetime(value) -> Optional[str]:
    if not value:
        return None

    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8", errors="ignore")
        except Exception:
            return None

    if not isinstance(value, str):
        value = str(value)

    value = value.strip()
    if not value:
        return None

    for fmt in _EXIF_DT_FORMATS:
        try:
            dt = datetime.strptime(value, fmt)
            return dt.isoformat()
        except Exception:
            continue

    return None


def _extract_with_pillow(path: str) -> Optional[str]:
    img = Image.open(path)
    exif = img.getexif()
    if not exif:
        return None

    tag_map = {k: ExifTags.TAGS.get(k, k) for k in exif.keys()}

    by_name = {}
    for tag_id, v in exif.items():
        name = tag_map.get(tag_id, tag_id)
        by_name[name] = v

    for key in (
        "DateTimeOriginal",
        "CreateDate",
        "DateTimeDigitized",
        "DateTime",
        "ModifyDate",
    ):
        if key in by_name:
            parsed = _parse_exif_datetime(by_name[key])
            if parsed:
                return parsed

    return None


def _extract_with_exifread(path: str) -> Optional[str]:
    try:
        import exifread
    except Exception:
        return None

    with open(path, "rb") as f:
        tags = exifread.process_file(f, details=False, strict=False)

    for key in (
        "EXIF DateTimeOriginal",
        "EXIF DateTimeDigitized",
        "Image DateTime",
    ):
        if key in tags:
            parsed = _parse_exif_datetime(str(tags[key]))
            if parsed:
                return parsed

    return None


def extract_exif_datetime(path: str) -> Optional[str]:
    """
    Returns ISO-8601 capture datetime (no timezone) or None.

    Order:
      1) Pillow EXIF
      2) exifread fallback (if installed)
    """
    try:
        dt = _extract_with_pillow(path)
        if dt:
            return dt
    except Exception:
        pass

    try:
        dt = _extract_with_exifread(path)
        if dt:
            return dt
    except Exception:
        pass

    return None
