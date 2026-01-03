import os
from domain.rules import dest_rel_path, duplicates_rel_path, resolve_collision
from persistence.db import Database

class PlannerService:
    def __init__(self, db: Database, plan_repo, hash_group_repo, error_repo):
        self.db = db
        self.plan_repo = plan_repo
        self.hash_group_repo = hash_group_repo
        self.error_repo = error_repo

    def build_plan(self, run_id: str, dest_root: str):
        self.plan_repo.clear_plan(run_id)

        con = self.db.connect()
        try:
            # For each hash in run, choose first file_id as primary by file_id order
            rows = con.execute("""
              SELECT sha256, MIN(file_id) AS primary_id
              FROM files
              WHERE run_id=? AND sha256 IS NOT NULL
              GROUP BY sha256
            """, (run_id,)).fetchall()
            for r in rows:
                group_id = con.execute("SELECT group_id FROM hash_groups WHERE run_id=? AND sha256=?",
                                       (run_id, r["sha256"])).fetchone()
                if group_id:
                    con.execute("UPDATE hash_groups SET primary_file_id=? WHERE group_id=?",
                                (r["primary_id"], group_id["group_id"]))
            con.commit()

            file_rows = con.execute("""
              SELECT f.*, hg.group_id, hg.primary_file_id
              FROM files f
              LEFT JOIN hash_groups hg ON hg.run_id=f.run_id AND hg.sha256=f.sha256
              WHERE f.run_id=?
              ORDER BY f.file_id
            """, (run_id,)).fetchall()

            items = []
            collisions = 0
            for f in file_rows:
                filename = os.path.basename(f["source_path"])
                date = f["chosen_date"]
                mt = f["media_type"]

                is_primary = (f["group_id"] is None) or (f["primary_file_id"] == f["file_id"])
                if f["group_id"] is not None and not is_primary:
                    rel = duplicates_rel_path(run_id, filename)
                    dst = os.path.join(dest_root, rel)
                    action = "COPY_TO_DUPLICATES"
                    group_id = int(f["group_id"])
                else:
                    rel = dest_rel_path(mt, date, filename)
                    dst = os.path.join(dest_root, rel)
                    action = "COPY"
                    group_id = int(f["group_id"]) if f["group_id"] is not None else None

                collision_suffix = 0
                if action == "COPY":
                    folder = os.path.dirname(dst)
                    os.makedirs(folder, exist_ok=True)
                    new_name, suffix = resolve_collision(folder, os.path.basename(dst))
                    if suffix:
                        collisions += 1
                        collision_suffix = suffix
                        dst = os.path.join(folder, new_name)
                        rel = os.path.relpath(dst, dest_root)

                items.append({
                    "file_id": int(f["file_id"]),
                    "action": action,
                    "dest_path": dst,
                    "dest_rel_path": rel,
                    "collision_suffix": collision_suffix,
                    "duplicate_group_id": group_id,
                    "is_primary_in_group": bool(is_primary),
                })

            self.plan_repo.insert_plan_items(run_id, items)
            return {"collisions": collisions}
        finally:
            con.close()
