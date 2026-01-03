from typing import Optional, Iterable
from persistence.db import Database
from utils.timeutil import now_iso

class RunRepo:
    def __init__(self, db: Database): self.db = db

    def create_run(self, run_id: str, run_name: str, source_root: str, dest_root: str, artifacts_root: str, cfg: dict) -> None:
        con = self.db.connect()
        try:
            t = now_iso()
            con.execute("""
              INSERT INTO runs(run_id, run_name, created_at, updated_at, source_root, dest_root, artifacts_root, status,
                               min_file_size, overwrite_policy, error_policy, live_photo_policy, thumbs_policy,
                               cpu_limit_pct, io_limit_mbps)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                run_id, run_name, t, t, source_root, dest_root, artifacts_root, "CREATED",
                cfg["min_file_size"], cfg["overwrite_policy"], cfg["error_policy"], cfg["live_photo_policy"], cfg["thumbs_policy"],
                cfg.get("cpu_limit_pct"), cfg.get("io_limit_mbps")
            ))
            con.commit()
        finally:
            con.close()

    def update_status(self, run_id: str, status: str) -> None:
        con = self.db.connect()
        try:
            con.execute("UPDATE runs SET status=?, updated_at=? WHERE run_id=?", (status, now_iso(), run_id))
            con.commit()
        finally:
            con.close()

    def latest_incomplete(self) -> Optional[dict]:
        con = self.db.connect()
        try:
            row = con.execute("""
              SELECT * FROM runs
              WHERE status IN ('CREATED','SCANNED','PLANNED','RUNNING','PAUSED','FAILED')
              ORDER BY created_at DESC LIMIT 1
            """).fetchone()
            return dict(row) if row else None
        finally:
            con.close()

    def get(self, run_id: str) -> Optional[dict]:
        con = self.db.connect()
        try:
            row = con.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            return dict(row) if row else None
        finally:
            con.close()

class FileRepo:
    def __init__(self, db: Database): self.db = db

    def upsert_files(self, run_id: str, rows: Iterable[dict]) -> None:
        con = self.db.connect()
        try:
            con.executemany("""
              INSERT INTO files(run_id, source_path, source_root, ext, media_type, file_size, mtime, exif_datetime, chosen_date,
                               date_source, sha256, is_hidden, is_system, is_link, created_at)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
              ON CONFLICT(run_id, source_path) DO UPDATE SET
                ext=excluded.ext,
                media_type=excluded.media_type,
                file_size=excluded.file_size,
                mtime=excluded.mtime,
                exif_datetime=excluded.exif_datetime,
                chosen_date=excluded.chosen_date,
                date_source=excluded.date_source,
                is_hidden=excluded.is_hidden,
                is_system=excluded.is_system,
                is_link=excluded.is_link
            """, [(
                run_id, r["source_path"], r["source_root"], r["ext"], r["media_type"], r["file_size"], r["mtime"],
                r.get("exif_datetime"), r["chosen_date"], r["date_source"], r.get("sha256"),
                r["is_hidden"], r["is_system"], r["is_link"], r["created_at"]
            ) for r in rows])
            con.commit()
        finally:
            con.close()

    def list_files_for_hashing(self, run_id: str) -> list[dict]:
        con = self.db.connect()
        try:
            rows = con.execute("SELECT * FROM files WHERE run_id=? AND sha256 IS NULL", (run_id,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            con.close()

    def set_sha256(self, file_id: int, sha256: str) -> None:
        con = self.db.connect()
        try:
            con.execute("UPDATE files SET sha256=? WHERE file_id=?", (sha256, file_id))
            con.commit()
        finally:
            con.close()

    def aggregate_by_date_type(self, run_id: str) -> dict:
        con = self.db.connect()
        try:
            rows = con.execute("""
              SELECT chosen_date, media_type, COUNT(*) AS n
              FROM files WHERE run_id=?
              GROUP BY chosen_date, media_type
            """, (run_id,)).fetchall()
            out: dict = {}
            for r in rows:
                out.setdefault(r["chosen_date"], {}).setdefault(r["media_type"], 0)
                out[r["chosen_date"]][r["media_type"]] += int(r["n"])
            return out
        finally:
            con.close()

    def counts(self, run_id: str) -> dict:
        con = self.db.connect()
        try:
            row = con.execute("""
              SELECT
                COUNT(*) AS total,
                SUM(file_size) AS bytes,
                SUM(CASE WHEN media_type='PHOTO' THEN 1 ELSE 0 END) AS photos,
                SUM(CASE WHEN media_type='VIDEO' THEN 1 ELSE 0 END) AS videos,
                SUM(CASE WHEN media_type='RAW' THEN 1 ELSE 0 END) AS raws
              FROM files WHERE run_id=?
            """, (run_id,)).fetchone()
            return dict(row)
        finally:
            con.close()

class HashGroupRepo:
    def __init__(self, db: Database): self.db = db

    def upsert_group(self, run_id: str, sha256: str) -> int:
        con = self.db.connect()
        try:
            row = con.execute("SELECT group_id FROM hash_groups WHERE run_id=? AND sha256=?", (run_id, sha256)).fetchone()
            if row:
                return int(row["group_id"])
            con.execute("INSERT INTO hash_groups(run_id, sha256, created_at) VALUES(?,?,?)", (run_id, sha256, now_iso()))
            con.commit()
            return int(con.execute("SELECT last_insert_rowid()").fetchone()[0])
        finally:
            con.close()

    def duplicate_counts(self, run_id: str) -> int:
        con = self.db.connect()
        try:
            row = con.execute("""
              SELECT SUM(cnt - 1) AS dupes
              FROM (SELECT sha256, COUNT(*) AS cnt FROM files WHERE run_id=? AND sha256 IS NOT NULL GROUP BY sha256)
              WHERE cnt > 1
            """, (run_id,)).fetchone()
            return int(row["dupes"] or 0)
        finally:
            con.close()

class PlanRepo:
    def __init__(self, db: Database): self.db = db

    def clear_plan(self, run_id: str) -> None:
        con = self.db.connect()
        try:
            con.execute("DELETE FROM plan_items WHERE run_id=?", (run_id,))
            con.commit()
        finally:
            con.close()

    def insert_plan_items(self, run_id: str, items: list[dict]) -> None:
        con = self.db.connect()
        try:
            con.executemany("""
              INSERT INTO plan_items(run_id, file_id, action, dest_path, dest_rel_path, collision_resolved, collision_suffix,
                                     duplicate_group_id, is_primary_in_group, status)
              VALUES(?,?,?,?,?,?,?,?,?,?)
            """, [(
                run_id, it["file_id"], it["action"], it["dest_path"], it["dest_rel_path"],
                1 if it.get("collision_suffix", 0) else 0,
                it.get("collision_suffix"),
                it.get("duplicate_group_id"),
                1 if it.get("is_primary_in_group") else 0,
                "PENDING"
            ) for it in items])
            con.commit()
        finally:
            con.close()

    def list_pending_for_execution(self, run_id: str) -> list[dict]:
        con = self.db.connect()
        try:
            rows = con.execute("""
              SELECT p.*, f.source_path, f.file_size, f.sha256 AS source_sha256
              FROM plan_items p
              JOIN files f ON f.file_id = p.file_id
              WHERE p.run_id=? AND p.status IN ('PENDING','COPYING','FAILED')
              ORDER BY p.plan_item_id
            """, (run_id,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            con.close()

    def mark_copying(self, plan_item_id: int) -> None:
        con = self.db.connect()
        try:
            con.execute("UPDATE plan_items SET status='COPYING', started_at=? WHERE plan_item_id=?", (now_iso(), plan_item_id))
            con.commit()
        finally:
            con.close()

    def mark_verified(self, plan_item_id: int, bytes_copied: int) -> None:
        con = self.db.connect()
        try:
            con.execute("""
              UPDATE plan_items SET status='VERIFIED', bytes_copied=?, finished_at=?, error_code=NULL, error_message=NULL
              WHERE plan_item_id=?
            """, (bytes_copied, now_iso(), plan_item_id))
            con.commit()
        finally:
            con.close()

    def mark_failed(self, plan_item_id: int, code: str, msg: str) -> None:
        con = self.db.connect()
        try:
            con.execute("""
              UPDATE plan_items SET status='FAILED', finished_at=?, error_code=?, error_message=?
              WHERE plan_item_id=?
            """, (now_iso(), code, msg, plan_item_id))
            con.commit()
        finally:
            con.close()

class ArtifactRepo:
    def __init__(self, db: Database): self.db = db
    def add(self, run_id: str, kind: str, path: str) -> None:
        con = self.db.connect()
        try:
            con.execute("INSERT INTO run_artifacts(run_id, kind, path, created_at) VALUES(?,?,?,?)",
                        (run_id, kind, path, now_iso()))
            con.commit()
        finally:
            con.close()

    def list(self, run_id: str) -> list[dict]:
        con = self.db.connect()
        try:
            rows = con.execute("SELECT * FROM run_artifacts WHERE run_id=? ORDER BY artifact_id", (run_id,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            con.close()

class ErrorRepo:
    def __init__(self, db: Database): self.db = db
    def add(self, run_id: str, phase: str, message: str, code: str | None = None,
            source_path: str | None = None, dest_path: str | None = None, plan_item_id: int | None = None) -> None:
        con = self.db.connect()
        try:
            con.execute("""
              INSERT INTO errors(run_id, plan_item_id, phase, code, message, source_path, dest_path, created_at)
              VALUES(?,?,?,?,?,?,?,?)
            """, (run_id, plan_item_id, phase, code, message, source_path, dest_path, now_iso()))
            con.commit()
        finally:
            con.close()

    def list_latest(self, run_id: str, limit: int = 200) -> list[dict]:
        con = self.db.connect()
        try:
            rows = con.execute("""
              SELECT * FROM errors WHERE run_id=? ORDER BY error_id DESC LIMIT ?
            """, (run_id, limit)).fetchall()
            return [dict(r) for r in rows]
        finally:
            con.close()
