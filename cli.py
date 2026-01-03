import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Dict, Any

from persistence.db import Database
from persistence.repos import (
    RunRepo,
    FileRepo,
    HashGroupRepo,
    PlanRepo,
    ArtifactRepo,
    ErrorRepo,
)
from domain.models import ScanConfig
from services.run_service import RunService
from services.scan_service import ScanService
from services.hash_service import HashService
from services.planner_service import PlannerService
from services.executor_service import ExecutorService
from services.report_service import ReportService

# Optional YAML support
try:
    import yaml
except ImportError:
    yaml = None


# ---------------------------
# Helpers
# ---------------------------

def load_config(path: str | None) -> Dict[str, Any]:
    if not path:
        return {}

    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    if p.suffix.lower() in (".yaml", ".yml"):
        if not yaml:
            raise RuntimeError("PyYAML not installed but YAML config was provided")
        return yaml.safe_load(p.read_text()) or {}

    return json.loads(p.read_text())


def merge_config(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in override.items():
        if v is not None:
            out[k] = v
    return out


def log(msg: str, logfile: Path | None):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    if logfile:
        logfile.parent.mkdir(parents=True, exist_ok=True)
        with logfile.open("a", encoding="utf-8") as f:
            f.write(line + "\n")


# ---------------------------
# CLI main
# ---------------------------

def main():
    parser = argparse.ArgumentParser(
        prog="MediaOrganizerCLI",
        description="Media Organizer – CLI"
    )

    parser.add_argument("--config", help="Config file (json or yaml)")
    parser.add_argument("--source", help="Source root")
    parser.add_argument("--dest", help="Destination root")

    parser.add_argument("--min-size", type=int, help="Minimum file size (bytes)")
    parser.add_argument("--include-photos", action="store_true", default=None)
    parser.add_argument("--include-videos", action="store_true", default=None)
    parser.add_argument("--include-raw", action="store_true", default=None)
    parser.add_argument("--include-other", action="store_true", default=None)
    parser.add_argument("--dry-run", action="store_true", default=None, help="Plan only; no copy")

    parser.add_argument("--resume", action="store_true", help="Resume last incomplete run")

    parser.add_argument("--log-file", help="Write logs to file")
    parser.add_argument("--db-path", default="media.db", help="SQLite DB path")

    args = parser.parse_args()

    logfile = Path(args.log_file) if args.log_file else None

    # ---------------------------
    # Load config
    # ---------------------------

    cfg_file = load_config(args.config)

    cli_cfg = {
        "source": args.source,
        "dest": args.dest,
        "min_file_size": args.min_size,
        "include_photos": args.include_photos,
        "include_videos": args.include_videos,
        "include_raw": args.include_raw,
        "include_other": args.include_other,
        "dry_run": args.dry_run,
    }

    cfg = merge_config(cfg_file, cli_cfg)

    # ---------------------------
    # DB + repos
    # ---------------------------

    db = Database(args.db_path)
    db.init()  # <-- add this (creates tables from schema.sql)


    run_repo = RunRepo(db)
    file_repo = FileRepo(db)
    hash_repo = HashGroupRepo(db)
    plan_repo = PlanRepo(db)
    artifact_repo = ArtifactRepo(db)
    error_repo = ErrorRepo(db)

    scan_service = ScanService(file_repo, error_repo)
    hash_service = HashService(file_repo, hash_repo, error_repo)
    planner = PlannerService(db, plan_repo, hash_repo, error_repo)
    executor = ExecutorService(plan_repo, error_repo)
    reporter = ReportService(db, artifact_repo)

    # ---------------------------
    # Resume or create run
    # ---------------------------

    if args.resume:
        run = run_repo.latest_incomplete()
        if not run:
            log("No incomplete run found to resume.", logfile)
            sys.exit(1)

        run_id = run["run_id"]
        source = run["source_root"]
        dest = run["dest_root"]

        log(f"Resuming run {run_id} ({run['status']})", logfile)
    else:
        if not cfg.get("source") or not cfg.get("dest"):
            parser.error("source and dest are required unless --resume is used")

        source = cfg["source"]
        dest = cfg["dest"]
        artifacts_root = os.path.join(dest, "Artifacts")
        os.makedirs(artifacts_root, exist_ok=True)

        scan_cfg = ScanConfig(
            min_file_size=int(cfg.get("min_file_size", 10240)),
            include_photos=bool(cfg.get("include_photos", True)),
            include_videos=bool(cfg.get("include_videos", True)),
            include_raw=bool(cfg.get("include_raw", True)),
            include_other=bool(cfg.get("include_other", False)),
            overwrite_policy="CLI",
            error_policy="CLI",
            live_photo_policy="CLI",
            thumbs_policy="CLI",
        )

        run_service = RunService(run_repo)
        run_name = time.strftime("CLI_Run_%Y%m%d_%H%M%S")
        run_id = run_service.create(run_name, source, dest, artifacts_root, scan_cfg)

        log(f"Created run {run_id}", logfile)

    # ---------------------------
    # Scan
    # ---------------------------

    def scan_progress(count, path):
        if count % 500 == 0:
            log(f"Scanned {count} files", logfile)

    log("Scanning files…", logfile)
    scan_service.scan(
        run_id,
        source,
        scan_cfg.min_file_size,
        scan_cfg.include_photos,
        scan_cfg.include_videos,
        scan_cfg.include_raw,
        include_other=scan_cfg.include_other,
        progress_cb=scan_progress,
    )

    run_repo.update_status(run_id, "SCANNED")

    # ---------------------------
    # Hash
    # ---------------------------

    def hash_progress(i, total, path):
        if i % 100 == 0 or i == total:
            log(f"Hashing {i}/{total}", logfile)

    log("Hashing files…", logfile)
    hash_service.hash_all(run_id, progress_cb=hash_progress)

    # ---------------------------
    # Plan
    # ---------------------------

    log("Building plan…", logfile)
    plan_info = planner.build_plan(run_id, dest)
    run_repo.update_status(run_id, "PLANNED")

    counts = file_repo.counts(run_id)
    log(f"Plan ready. Files={counts.get('total')} Collisions={plan_info.get('collisions')}", logfile)

    if cfg.get("dry_run"):
        log("Dry-run enabled. No files copied.", logfile)
        return

    # ---------------------------
    # Execute
    # ---------------------------

    items = plan_repo.list_pending_for_execution(run_id)

    def exec_progress(done, total, src, dst):
        if done % 25 == 0 or done == total:
            log(f"Copied {done}/{total}", logfile)

    log("Executing copy…", logfile)
    executor.execute(run_id, items, progress_cb=exec_progress)

    run_repo.update_status(run_id, "COMPLETED")

    # ---------------------------
    # Report
    # ---------------------------

    run = run_repo.get(run_id)
    reporter.produce(run_id, run["artifacts_root"])

    log("Run completed successfully.", logfile)


if __name__ == "__main__":
    main()
