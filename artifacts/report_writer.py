import csv

def write_csv_and_summary(db, run_id: str, csv_path: str, summary_path: str):
    con = db.connect()
    try:
        run = con.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        counts = con.execute("""
          SELECT status, COUNT(*) AS n
          FROM plan_items WHERE run_id=?
          GROUP BY status
        """, (run_id,)).fetchall()

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["run_id", run_id])
            w.writerow(["run_name", run["run_name"]])
            w.writerow([])
            w.writerow(["status", "count"])
            for r in counts:
                w.writerow([r["status"], r["n"]])

        total_files = con.execute("SELECT COUNT(*) AS n FROM files WHERE run_id=?", (run_id,)).fetchone()["n"]
        total_bytes = con.execute("SELECT SUM(file_size) AS b FROM files WHERE run_id=?", (run_id,)).fetchone()["b"] or 0
        dupes = con.execute("""
          SELECT SUM(cnt - 1) AS dupes
          FROM (SELECT sha256, COUNT(*) AS cnt FROM files WHERE run_id=? GROUP BY sha256)
          WHERE cnt > 1
        """, (run_id,)).fetchone()["dupes"] or 0

        errors = con.execute("SELECT COUNT(*) AS n FROM errors WHERE run_id=?", (run_id,)).fetchone()["n"]

        lines = []
        lines.append(f"Run: {run['run_name']} ({run_id})")
        lines.append(f"Source: {run['source_root']}")
        lines.append(f"Destination: {run['dest_root']}")
        lines.append("")
        lines.append("Summary")
        lines.append(f"- Total files scanned: {total_files}")
        lines.append(f"- Total bytes scanned: {total_bytes}")
        lines.append(f"- Exact duplicates found: {dupes}")
        lines.append(f"- Errors: {errors}")
        lines.append("")
        lines.append("Plan status counts")
        for r in counts:
            lines.append(f"- {r['status']}: {r['n']}")

        with open(summary_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
    finally:
        con.close()
