PHOTO_EXTS = {
    ".jpg", ".jpeg", ".png", ".heic", ".heif", ".bmp", ".tif", ".tiff", ".gif", ".webp"
}

RAW_EXTS = {
    ".cr2", ".cr3", ".nef", ".nrw", ".arw", ".raf", ".rw2", ".orf", ".pef", ".rwl", ".x3f", ".dng"
}

VIDEO_EXTS = {
    ".mp4", ".mov", ".m4v", ".avi", ".wmv", ".webm", ".mkv", ".3gp", ".mts", ".m2ts",
    ".mpg", ".mpeg", ".vob", ".ts", ".flv"
}

EXCLUDED_EXTS = {
    ".xmp", ".aae", ".db", ".sqlite", ".xml", ".json", ".exe", ".msi", ".zip", ".rar", ".7z"
}

MEDIA_TYPE_PHOTO = "PHOTO"
MEDIA_TYPE_VIDEO = "VIDEO"
MEDIA_TYPE_RAW = "RAW"
MEDIA_TYPE_OTHER = "OTHER"


RUN_STATUS = {
    "CREATED", "SCANNED", "PLANNED", "RUNNING", "PAUSED", "COMPLETED", "FAILED", "ROLLED_BACK"
}

PLAN_STATUS = {"PENDING", "COPYING", "COPIED", "VERIFIED", "FAILED", "SKIPPED"}

ERROR_PHASE = {"SCAN", "HASH", "PLAN", "COPY", "VERIFY", "ROLLBACK"}
