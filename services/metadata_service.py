from typing import Optional

def extract_exif_datetime(path: str) -> Optional[str]:
    """
    Best-effort EXIF extraction without extra dependencies.
    If Pillow is available, use it for JPEG/TIFF. Otherwise return None.
    Returns ISO datetime string or None.
    """
    try:
        from PIL import Image, ExifTags  # type: ignore
    except Exception:
        return None

    try:
        with Image.open(path) as img:
            exif = img.getexif()
            if not exif:
                return None
            # Find DateTimeOriginal tag
            tag_map = {v: k for k, v in ExifTags.TAGS.items()}
            dto_tag = tag_map.get("DateTimeOriginal")
            if dto_tag is None:
                return None
            val = exif.get(dto_tag)
            if not val:
                return None
            # EXIF format: "YYYY:MM:DD HH:MM:SS"
            s = str(val)
            parts = s.split()
            if len(parts) != 2:
                return None
            date_part = parts[0].replace(":", "-")
            iso = f"{date_part}T{parts[1]}"
            return iso
    except Exception:
        return None
