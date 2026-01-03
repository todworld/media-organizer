import os

class RollbackService:
    def __init__(self, db, error_repo):
        self.db = db
        self.error_repo = error_repo

    def rollback(self, run_id: str):
        con = self.db.connect()
        try:
            items = con.execute("""
              SELECT dest_path, plan_item_id
              FROM plan_items
              WHERE run_id=? AND status='VERIFIED'
            """, (run_id,)).fetchall()

            for r in items:
                dst = r["dest_path"]
                try:
                    if os.path.exists(dst):
                        os.remove(dst)
                except Exception as e:
                    self.error_repo.add(run_id, "ROLLBACK", f"{type(e).__name__}: {e}", dest_path=dst, plan_item_id=r["plan_item_id"])

            con.execute("UPDATE runs SET status='ROLLED_BACK' WHERE run_id=?", (run_id,))
            con.commit()
        finally:
            con.close()
