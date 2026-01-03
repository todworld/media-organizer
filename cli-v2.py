# cli.py (full file, updated to: pre-count -> scan compare -> top skip reasons on mismatch,
# and tqdm progress bars for phases)

import argparse
import json
import os
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Dict, Any

from tqdm import tqdm

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


def tqdm_enabled() -> bool:
    return sys.stderr.isatty()


def log(msg: str, logfile: Path | None):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    tqdm.write(line)
    if logfile:
        logfile.parent.mkdir(parents=True, exist_ok=True)
        with logfile.open("a", encoding="utf-8") as f:
            f.write(line + "\n")


def count_total_files(root: str) -> int:
    """
    Fast pre-count of all files under root.
    No filtering. No stat calls.
    This count is expected to be >= accepted scan count (because scan filters).
    """
    total = 0
    for _, _, files in os.walk(root):
        total += len(files)
    return total


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
    bars_on = tqdm_enabled()

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
    # Resume or create run
    # ---------------------------

    # DB + repos
    db = Database(args.db_path)
    db.init()

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

    if args.resume:
        run = run_repo.latest_incomplete()
        if not run:
            log("No incomplete run found to resume.", logfile)
            sys.exit(1)

        run_id = run["run_id"]
        source = run["source_root"]
        dest = run["dest_root"]

        # If you persist ScanConfig in DB, load it here. Otherwise keep prior behavior.
        scan_cfg = ScanConfig(
            min_file_size=int(run.get("min_file_size", 10240)) if isinstance(run, dict) else 10240,
            include_photos=bool(run.get("include_photos", True)) if isinstance(run, dict) else True,
            include_videos=bool(run.get("include_videos", True)) if isinstance(run, dict) else True,
            include_raw=bool(run.get("include_raw", True)) if isinstance(run, dict) else True,
            include_other=bool(run.get("include_other", False)) if isinstance(run, dict) else False,
            overwrite_policy="CLI",
            error_policy="CLI",
            live_photo_policy="CLI",
            thumbs_policy="CLI",
        )

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
    # Pre-check total files present
    # ---------------------------

    log("Counting total files present under source (pre-scan)…", logfile)
    total_present = count_total_files(source)
    log(f"Total files present (pre-scan): {total_present}", logfile)

    # ---------------------------
    # Scan (phase bar + skip reasons)
    # ---------------------------

    log("Scanning files…", logfile)

    scan_pbar = tqdm(
        desc="Scan (accepted)",
        unit="file",
        dynamic_ncols=True,
        disable=not bars_on,
    )

    scanned_count = 0
    _last_scanned = 0
    skip_reasons = Counter()

    def scan_progress(count, path):
        nonlocal scanned_count, _last_scanned
        scanned_count = count
        delta = count - _last_scanned
        if delta > 0:
            scan_pbar.update(delta)
            _last_scanned = count

    def scan_skip(reason, path):
        skip_reasons[reason] += 1

    scan_service.scan(
        run_id,
        source,
        scan_cfg.min_file_size,
        scan_cfg.include_photos,
        scan_cfg.include_videos,
        scan_cfg.include_raw,
        include_other=scan_cfg.include_other,
        progress_cb=scan_progress,
        skip_cb=scan_skip,  # NEW
    )

    scan_pbar.close()
    run_repo.update_status(run_id, "SCANNED")

    # ---------------------------
    # Compare total present vs accepted scanned
    # ---------------------------
    #
    # Note: This will almost always differ because the scan filters by:
    # - min size
    # - excluded extensions
    # - include_* flags
    #
    # This check is still useful to detect sudden drops/changes.
    #

    if scanned_count != total_present:
        diff = total_present - scanned_count
        log(
            f"WARNING: Pre-scan present count differs from accepted scanned count. "
            f"Present={total_present}, AcceptedScanned={scanned_count}, Difference={diff}",
            logfile,
        )

        if skip_reasons:
            log("Top skip reasons (Top 10):", logfile)
            for reason, cnt in skip_reasons.most_common(10):
                log(f"  - {reason}: {cnt}", logfile)
        else:
            log("No skip reasons captured.", logfile)
    else:
        log("Counts match: present == accepted scanned.", logfile)

    # ---------------------------
    # Hash (phase bar)
    # ---------------------------

    log("Hashing files…", logfile)

    hash_pbar = None
    _last_i = 0

    def hash_progress(i, total, path):
        nonlocal hash_pbar, _last_i
        if hash_pbar is None:
            hash_pbar = tqdm(
                total=total,
                desc="Hash",
                unit="file",
                dynamic_ncols=True,
                disable=not bars_on,
            )
        delta = i - _last_i
        if delta > 0:
            hash_pbar.update(delta)
            _last_i = i

    hash_service.hash_all(run_id, progress_cb=hash_progress)

    if hash_pbar is not None:
        hash_pbar.close()

    # ---------------------------
    # Plan (phase bar)
    # ---------------------------

    with tqdm(
        total=1,
        desc="Plan",
        unit="step",
        dynamic_ncols=True,
        disable=not bars_on,
    ) as plan_pbar:
        log("Building plan…", logfile)
        plan_info = planner.build_plan(run_id, dest)
        plan_pbar.update(1)

    run_repo.update_status(run_id, "PLANNED")

    counts = file_repo.counts(run_id)
    log(f"Plan ready. Files={counts.get('total')} Collisions={plan_info.get('collisions')}", logfile)

    if cfg.get("dry_run"):
        log("Dry-run enabled. No files copied.", logfile)
        return

    # ---------------------------
    # Execute (phase bar)
    # ---------------------------

    items = plan_repo.list_pending_for_execution(run_id)
    total_items = len(items)

    log("Executing copy…", logfile)

    exec_pbar = tqdm(
        total=total_items,
        desc="Copy",
        unit="file",
        dynamic_ncols=True,
        disable=not bars_on,
    )

    _last_done = 0

    def exec_progress(done, total, src, dst):
        nonlocal _last_done
        delta = done - _last_done
        if delta > 0:
            exec_pbar.update(delta)
            _last_done = done

    executor.execute(run_id, items, progress_cb=exec_progress)

    exec_pbar.close()
    run_repo.update_status(run_id, "COMPLETED")

    # ---------------------------
    # Report (phase bar)
    # ---------------------------

    with tqdm(
        total=1,
        desc="Report",
        unit="step",
        dynamic_ncols=True,
        disable=not bars_on,
    ) as report_pbar:
        run = run_repo.get(run_id)
        reporter.produce(run_id, run["artifacts_root"])
        report_pbar.update(1)

    log("Run completed successfully.", logfile)


if __name__ == "__main__":
    main()
