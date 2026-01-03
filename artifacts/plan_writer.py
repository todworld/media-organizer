import json

def write_plan_json(db, run_id: str, out_path: str):
    con = db.connect()
    try:
        run = con.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        items = con.execute("""
          SELECT p.*, f.source_path, f.sha256
          FROM plan_items p
          JOIN files f ON f.file_id=p.file_id
          WHERE p.run_id=?
          ORDER BY p.plan_item_id
        """, (run_id,)).fetchall()

        payload = {
            "run": dict(run),
            "items": [dict(i) for i in items]
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
    finally:
        con.close()
