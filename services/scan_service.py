# services/scan_service.py

import os

from organizer_io.fs_scanner import iter_files
from domain.rules import classify_media, choose_date
from services.metadata_service import extract_exif_datetime
from services.video_metadata_service import extract_video_created_datetime
from utils.timeutil import now_iso, mtime_iso


class ScanService:
    def __init__(self, file_repo, error_repo):
        self.file_repo = file_repo
        self.error_repo = error_repo

    def scan(
        self,
        run_id: str,
        source_root: str,
        min_file_size: int,
        include_photos: bool,
        include_videos: bool,
        include_raw: bool,
        include_other: bool = False,
        progress_cb=None,
        skip_cb=None,          # NEW: skip_cb(reason, path)
        stop_flag=None,
        progress_every: int = 200,
    ):
        """Scan files under source_root and insert file records.

        Notes:
        - Photos: prefers EXIF DateTimeOriginal (date taken) when available.
        - Videos: prefers container creation_time (created date) when available.
        - RAW: tries EXIF when available, otherwise falls back.
        - OTHER: includes non-media files when include_other=True.
        - Falls back to filesystem modified time.
        - progress_cb(count, path) called every ~progress_every accepted files.
        - skip_cb(reason, path) called for skipped or fallback conditions.
        - stop_flag() can abort the scan.
        """

        try:
            from domain.constants import EXCLUDED_EXTS
        except Exception:
            EXCLUDED_EXTS = set()

        def _skip(reason: str, path: str):
            if skip_cb:
                try:
                    skip_cb(reason, path)
                except Exception:
                    pass

        count = 0
        rows = []

        for path in iter_files(source_root):
            if stop_flag and stop_flag():
                _skip("stop_flag", path)
                break

            try:
                try:
                    st = os.stat(path)
                except FileNotFoundError:
                    _skip("missing_during_scan", path)
                    continue
                except PermissionError:
                    _skip("permission_denied", path)
                    continue
                except OSError:
                    _skip("os_stat_error", path)
                    continue

                size = int(st.st_size)
                if size < min_file_size:
                    _skip("below_min_size", path)
                    continue

                ext = os.path.splitext(path)[1].lower()

                if ext in EXCLUDED_EXTS:
                    _skip("excluded_extension", path)
                    continue

                media_type = classify_media(ext)

                if media_type == "PHOTO" and not include_photos:
                    _skip("filtered_photo", path)
                    continue
                if media_type == "VIDEO" and not include_videos:
                    _skip("filtered_video", path)
                    continue
                if media_type == "RAW" and not include_raw:
                    _skip("filtered_raw", path)
                    continue
                if media_type == "OTHER" and not include_other:
                    _skip("filtered_other", path)
                    continue

                mtime = mtime_iso(path)

                exif_dt = None
                video_dt = None

                if media_type == "PHOTO":
                    exif_dt = extract_exif_datetime(path)
                    if not exif_dt:
                        _skip("photo_missing_exif_datetime", path)
                    chosen_date, date_source = choose_date(
                        exif_dt,
                        mtime,
                        primary_source="TAKEN_EXIF",
                    )

                elif media_type == "VIDEO":
                    try:
                        video_dt = extract_video_created_datetime(path)
                    except Exception:
                        _skip("video_meta_extract_error", path)
                        video_dt = None
                    chosen_date, date_source = choose_date(
                        video_dt,
                        mtime,
                        primary_source="CREATED_META",
                    )

                elif media_type == "RAW":
                    exif_dt = extract_exif_datetime(path)
                    if not exif_dt:
                        _skip("raw_missing_exif_datetime", path)
                    chosen_date, date_source = choose_date(
                        exif_dt,
                        mtime,
                        primary_source="TAKEN_EXIF",
                    )

                else:
                    chosen_date, date_source = choose_date(
                        None,
                        mtime,
                        primary_source="PRIMARY",
                    )

                # Record which date source was actually used (very useful for diagnostics)
                _skip(f"date_source:{media_type}:{date_source}", path)

                rows.append(
                    {
                        "source_path": path,
                        "source_root": source_root,
                        "ext": ext,
                        "media_type": media_type,
                        "file_size": size,
                        "mtime": mtime,
                        "exif_datetime": exif_dt,
                        "chosen_date": chosen_date,
                        "date_source": date_source,
                        "sha256": None,
                        "is_hidden": 0,
                        "is_system": 0,
                        "is_link": 0,
                        "created_at": now_iso(),
                    }
                )

                count += 1
                if progress_cb and (count % progress_every == 0):
                    progress_cb(count, path)

                if len(rows) >= 500:
                    self.file_repo.upsert_files(run_id, rows)
                    rows.clear()

            except Exception as e:
                _skip("scan_exception", path)
                self.error_repo.add(
                    run_id,
                    "SCAN",
                    f"{type(e).__name__}: {e}",
                    source_path=path,
                )

        if rows:
            self.file_repo.upsert_files(run_id, rows)

        if progress_cb:
            progress_cb(count, "")
