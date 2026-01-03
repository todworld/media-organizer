# inspect_db.py
# Generic SQLite inspector for media.db (or any SQLite DB)
#
# What it does:
# - Lists tables and row counts
# - Shows schema (columns) per table
# - Shows indexes per table
# - Prints "runs" summary if runs table exists
# - Detects latest run_id and prints per-run row counts for common run_id tables
#
# Usage:
#   python inspect_db.py --db media.db
#   python inspect_db.py --db media.db --table files
#   python inspect_db.py --db media.db --runs 10
#   python inspect_db.py --db media.db --schema-only
#
import argparse
import sqlite3
from typing import List, Tuple, Optional


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def list_tables(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    return [r["name"] for r in rows]


def table_rowcount(conn: sqlite3.Connection, table: str) -> Optional[int]:
    try:
        row = conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()
        return int(row["c"])
    except Exception:
        return None


def table_info(conn: sqlite3.Connection, table: str) -> List[sqlite3.Row]:
    return conn.execute(f"PRAGMA table_info({table})").fetchall()


def index_list(conn: sqlite3.Connection, table: str) -> List[sqlite3.Row]:
    try:
        return conn.execute(f"PRAGMA index_list({table})").fetchall()
    except Exception:
        return []


def index_info(conn: sqlite3.Connection, index_name: str) -> List[sqlite3.Row]:
    try:
        return conn.execute(f"PRAGMA index_info({index_name})").fetchall()
    except Exception:
        return []


def has_table(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def has_column(conn: sqlite3.Connection, table: str, col: str) -> bool:
    return any(r["name"] == col for r in table_info(conn, table))


def print_table_summary(conn: sqlite3.Connection, tables: List[str]) -> None:
    print("Tables:")
    for t in tables:
        c = table_rowcount(conn, t)
        if c is None:
            print(f"  - {t}: [count unavailable]")
        else:
            print(f"  - {t}: {c}")


def print_schema(conn: sqlite3.Connection, tables: List[str]) -> None:
    print("\nSchema:")
    for t in tables:
        print(f"\n[{t}]")
        cols = table_info(conn, t)
        for c in cols:
            # (cid, name, type, notnull, dflt_value, pk)
            nn = "NOT NULL" if c["notnull"] else ""
            pk = "PK" if c["pk"] else ""
            dv = f"default={c['dflt_value']}" if c["dflt_value"] is not None else ""
            bits = " ".join(x for x in [c["type"], nn, pk, dv] if x)
            print(f"  - {c['name']}: {bits}".rstrip())


def print_indexes(conn: sqlite3.Connection, tables: List[str]) -> None:
    print("\nIndexes:")
    for t in tables:
        idxs = index_list(conn, t)
        if not idxs:
            continue
        print(f"\n[{t}]")
        for i in idxs:
            # (seq, name, unique, origin, partial)
            name = i["name"]
            unique = "UNIQUE" if i["unique"] else ""
            print(f"  - {name} {unique}".rstrip())
            cols = index_info(conn, name)
            if cols:
                col_list = ", ".join(c["name"] for c in cols)
                print(f"      cols: {col_list}")


def print_runs(conn: sqlite3.Connection, limit: int) -> None:
    if not has_table(conn, "runs"):
        print("\nRuns: [no runs table]")
        return

    wanted_cols = ["run_id", "run_name", "created_at", "status"]
    existing = [c for c in wanted_cols if has_column(conn, "runs", c)]
    if not existing:
        print("\nRuns: [runs table exists but expected columns not found]")
        return

    sel = ", ".join(existing)
    rows = conn.execute(
        f"SELECT {sel} FROM runs ORDER BY created_at DESC LIMIT ?",
        (limit,),
    ).fetchall()

    print(f"\nRuns (latest {limit}):")
    for r in rows:
        parts = []
        for c in existing:
            parts.append(f"{c}={r[c]}")
        print("  - " + "  ".join(parts))


def print_latest_run_stats(conn: sqlite3.Connection) -> None:
    if not has_table(conn, "runs") or not has_column(conn, "runs", "run_id") or not has_column(conn, "runs", "created_at"):
        return

    row = conn.execute(
        "SELECT run_id, created_at FROM runs ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    if not row:
        return

    run_id = row["run_id"]
    created_at = row["created_at"]
    print(f"\nLatest run_id: {run_id} (created_at={created_at})")

    # Find tables that have run_id
    tables = list_tables(conn)
    run_tables = [t for t in tables if has_column(conn, t, "run_id")]
    if not run_tables:
        print("No tables with run_id column found.")
        return

    print("Row counts for latest run_id across run_id tables:")
    for t in run_tables:
        try:
            c = conn.execute(f"SELECT COUNT(*) AS c FROM {t} WHERE run_id = ?", (run_id,)).fetchone()["c"]
            print(f"  - {t}: {int(c)}")
        except Exception:
            print(f"  - {t}: [query failed]")


def print_table_preview(conn: sqlite3.Connection, table: str, limit: int = 5) -> None:
    print(f"\nPreview [{table}] (first {limit} rows):")
    try:
        rows = conn.execute(f"SELECT * FROM {table} LIMIT {limit}").fetchall()
        if not rows:
            print("  [no rows]")
            return
        cols = rows[0].keys()
        print("  columns:", ", ".join(cols))
        for r in rows:
            # Print a compact dict-like row
            d = {k: r[k] for k in cols}
            print(" ", d)
    except Exception as e:
        print(f"  [preview failed: {type(e).__name__}: {e}]")


def main():
    ap = argparse.ArgumentParser(description="Generic SQLite DB inspector")
    ap.add_argument("--db", default="media.db", help="Path to SQLite DB (default: media.db)")
    ap.add_argument("--table", help="Inspect only this table (schema + preview)")
    ap.add_argument("--runs", type=int, default=10, help="How many recent runs to show (default: 10)")
    ap.add_argument("--schema-only", action="store_true", help="Only print schema (no row counts/indexes)")
    ap.add_argument("--no-indexes", action="store_true", help="Skip index output")
    ap.add_argument("--preview-rows", type=int, default=5, help="Rows to preview for --table (default: 5)")
    args = ap.parse_args()

    conn = connect(args.db)
    try:
        tables = list_tables(conn)

        if args.table:
            if args.table not in tables:
                print(f"Table not found: {args.table}")
                print("Available tables:", ", ".join(tables))
                return
            print_table_summary(conn, [args.table])
            print_schema(conn, [args.table])
            if not args.no_indexes:
                print_indexes(conn, [args.table])
            print_table_preview(conn, args.table, limit=args.preview_rows)
            return

        # Whole DB
        if not args.schema_only:
            print_table_summary(conn, tables)

        print_schema(conn, tables)

        if not args.schema_only and not args.no_indexes:
            print_indexes(conn, tables)

        print_runs(conn, args.runs)
        print_latest_run_stats(conn)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
