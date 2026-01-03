#!/usr/bin/env python3
"""
purge_db.py

Purge old runs from the Media Organizer SQLite database.

Features
- Purge by:
  - explicit run_id(s)
  - keep last N runs
  - older than X days
  - status filter (optional)
- Deletes related rows in dependent tables (best-effort).
- Safe by default: supports --dry-run.
- Optional VACUUM after purge.

Usage examples
  python purge_db.py --db media.db --keep-last 3 --dry-run
  python purge_db.py --db media.db --older-than-days 30 --vacuum
  python purge_db.py --db media.db --run-id <RUN_ID_1> --run-id <RUN_ID_2> --vacuum
  python purge_db.py --db media.db --keep-last 5 --status COMPLETED --vacuum
"""

from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, List, Dict, Tuple, Optional


@dataclass
class RunRow:
    run_id: str
    run_name: Optional[str]
    created_at: Optional[str]
    status: Optional[str]


# ----------------------------
# SQLite helpers
# ----------------------------

def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    # Enforce FK if schema uses it
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def list_tables(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    return [r["name"] for r in rows]


def has_column(conn: sqlite3.Connection, table: str, col: str) -> bool:
    cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(c["name"] == col for c in cols)


def select_runs(conn: sqlite3.Connection) -> List[RunRow]:
    # runs schema provided: run_id, run_name, created_at, status...
    rows = conn.execute(
        """
        SELECT run_id, run_name, created_at, status
        FROM runs
        ORDER BY created_at DESC
        """
    ).fetchall()
    out: List[RunRow] = []
    for r in rows:
        out.append(RunRow(
            run_id=str(r["run_id"]),
            run_name=r["run_name"],
            created_at=r["created_at"],
            status=r["status"],
        ))
    return out


def parse_iso_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    # Handle common ISO-ish strings. Your DB stores TEXT; this is best-effort.
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    # If it includes timezone offset like 2024-06-29 16:54:48-04:00, strip offset
    try:
        if "+" in s:
            s2 = s.split("+", 1)[0]
        elif "-" in s[19:]:
            # timezone like -05:00 after seconds
            s2 = s[:19]
        else:
            s2 = s
        return datetime.fromisoformat(s2)
    except Exception:
        return None


# ----------------------------
# Purge planning
# ----------------------------

def choose_runs_to_purge(
    runs: List[RunRow],
    run_ids: List[str],
    keep_last: Optional[int],
    older_than_days: Optional[int],
    status: Optional[str],
) -> List[RunRow]:
    by_id = {r.run_id: r for r in runs}

    # Explicit run_ids wins if provided
    if run_ids:
        selected = []
        for rid in run_ids:
            if rid in by_id:
                selected.append(by_id[rid])
        if status:
            selected = [r for r in selected if (r.status or "") == status]
        return selected

    filtered = runs
    if status:
        filtered = [r for r in filtered if (r.status or "") == status]

    # Apply keep_last
    if keep_last is not None:
        # Keep newest keep_last; purge the rest (after status filter)
        if keep_last < 0:
            keep_last = 0
        filtered = filtered[keep_last:]

    # Apply older_than_days
    if older_than_days is not None:
        cutoff = datetime.now() - timedelta(days=older_than_days)
        tmp = []
        for r in filtered:
            dt = parse_iso_dt(r.created_at)
            if dt is None:
                # If we can't parse date, do not purge based on age
                continue
            if dt < cutoff:
                tmp.append(r)
        filtered = tmp

    return filtered


# ----------------------------
# Purge execution
# ----------------------------

def find_runid_tables(conn: sqlite3.Connection) -> List[str]:
    """
    Finds tables that have a run_id column.
    These are usually safe to delete by run_id.
    """
    tables = list_tables(conn)
    out = []
    for t in tables:
        if has_column(conn, t, "run_id"):
            out.append(t)
    return out


def delete_by_run_id(conn: sqlite3.Connection, run_id: str, dry_run: bool) -> Dict[str, int]:
    """
    Deletes rows for the given run_id from all tables that contain a run_id column,
    plus deletes the run row itself from runs.

    Returns per-table deleted row counts (best-effort).
    """
    counts: Dict[str, int] = {}
    tables = find_runid_tables(conn)

    # Delete dependent tables first (everything except runs), then runs.
    # Order can matter if foreign keys exist.
    dep_tables = [t for t in tables if t != "runs"]

    for t in dep_tables:
        if dry_run:
            n = conn.execute(f"SELECT COUNT(*) AS c FROM {t} WHERE run_id = ?", (run_id,)).fetchone()["c"]
            if n:
                counts[t] = int(n)
        else:
            cur = conn.execute(f"DELETE FROM {t} WHERE run_id = ?", (run_id,))
            counts[t] = cur.rowcount if cur.rowcount != -1 else 0

    # Finally delete run record
    if dry_run:
        n = conn.execute("SELECT COUNT(*) AS c FROM runs WHERE run_id = ?", (run_id,)).fetchone()["c"]
        if n:
            counts["runs"] = int(n)
    else:
        cur = conn.execute("DELETE FROM runs WHERE run_id = ?", (run_id,))
        counts["runs"] = cur.rowcount if cur.rowcount != -1 else 0

    return counts


def summarize_counts(all_counts: List[Tuple[str, Dict[str, int]]]) -> str:
    # all_counts: list of (run_id, {table: count})
    total_by_table: Dict[str, int] = {}
    for _, d in all_counts:
        for t, c in d.items():
            total_by_table[t] = total_by_table.get(t, 0) + int(c)

    lines = []
    lines.append("Purge summary (rows affected):")
    for t in sorted(total_by_table.keys()):
        lines.append(f"  - {t}: {total_by_table[t]}")
    return "\n".join(lines)


# ----------------------------
# Main
# ----------------------------

def main():
    ap = argparse.ArgumentParser(description="Purge runs from media.db safely.")
    ap.add_argument("--db", default="media.db", help="SQLite DB path (default: media.db)")

    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--run-id", action="append", dest="run_ids", help="Run ID to purge (repeatable)")
    g.add_argument("--keep-last", type=int, help="Keep newest N runs; purge the rest (optionally filtered by --status)")
    g.add_argument("--older-than-days", type=int, help="Purge runs older than N days (optionally filtered by --status)")

    ap.add_argument("--status", help="Only purge runs with this status (e.g., COMPLETED)")
    ap.add_argument("--dry-run", action="store_true", help="Show what would be deleted; do not delete")
    ap.add_argument("--vacuum", action="store_true", help="Run VACUUM after purge (not available in dry-run)")

    args = ap.parse_args()

    conn = connect(args.db)

    try:
        runs = select_runs(conn)
        if not runs:
            print("No runs found.")
            return

        to_purge = choose_runs_to_purge(
            runs=runs,
            run_ids=args.run_ids or [],
            keep_last=args.keep_last,
            older_than_days=args.older_than_days,
            status=args.status,
        )

        if not to_purge:
            print("No runs matched purge criteria.")
            return

        print("Runs selected for purge:")
        for r in to_purge:
            print(f"  - run_id={r.run_id}  status={r.status}  created_at={r.created_at}  run_name={r.run_name}")

        all_counts: List[Tuple[str, Dict[str, int]]] = []

        if not args.dry_run:
            conn.execute("BEGIN")

        for r in to_purge:
            counts = delete_by_run_id(conn, r.run_id, dry_run=args.dry_run)
            all_counts.append((r.run_id, counts))

        if args.dry_run:
            print()
            print(summarize_counts(all_counts))
            print("\nDry-run complete. No changes were made.")
            return

        conn.commit()

        print()
        print(summarize_counts(all_counts))

        if args.vacuum:
            # VACUUM cannot run inside a transaction.
            conn.execute("VACUUM")
            print("\nVACUUM completed.")

        print("\nPurge complete.")

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
