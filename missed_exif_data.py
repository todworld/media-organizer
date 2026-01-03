import sqlite3
from pathlib import Path

DB_PATH = "media.db"
OUT_FILE = Path("mtime_groups_latest_run.txt")

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# -------------------------
# 1) Find latest run
# -------------------------
cur.execute("""
SELECT run_id, run_name, created_at
FROM runs
ORDER BY created_at DESC
LIMIT 1
""")

row = cur.fetchone()
if not row:
    raise SystemExit("No runs found")

run_id, run_name, created_at = row

# -------------------------
# 2) Fetch all MTIME-used files
# -------------------------
cur.execute("""
SELECT
    chosen_date,
    source_path
FROM files
WHERE run_id = ?
  AND date_source = 'MTIME'
ORDER BY chosen_date, source_path
""", (run_id,))

rows = cur.fetchall()
conn.close()

# -------------------------
# 3) Write output to file
# -------------------------
OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

with OUT_FILE.open("w", encoding="utf-8") as f:
    f.write("MTIME GROUP REPORT (LATEST RUN)\n")
    f.write("=" * 50 + "\n\n")
    f.write(f"run_id     : {run_id}\n")
    f.write(f"run_name   : {run_name}\n")
    f.write(f"created_at : {created_at}\n")
    f.write(f"total MTIME files: {len(rows)}\n\n")

    if not rows:
        f.write("No files used MTIME in this run.\n")
    else:
        current_date = None
        count_in_group = 0

        for chosen_date, path in rows:
            if chosen_date != current_date:
                if current_date is not None:
                    f.write(f"  → {count_in_group} file(s)\n\n")

                current_date = chosen_date
                count_in_group = 0
                f.write(f"MTIME = {chosen_date}\n")

            f.write(f"  {path}\n")
            count_in_group += 1

        # last group
        f.write(f"  → {count_in_group} file(s)\n")

print(f"MTIME report written to: {OUT_FILE.resolve()}")
