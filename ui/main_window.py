import os
import datetime
import time

from PySide6.QtCore import QObject, Signal, QThread, Qt
from PySide6.QtWidgets import (
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QLineEdit,
    QFileDialog,
    QSpinBox,
    QCheckBox,
    QTableWidget,
    QTableWidgetItem,
    QMessageBox,
    QListWidget,
    QStackedWidget,
    QGroupBox,
    QFormLayout,
    QProgressBar,
)

from persistence.db import Database
from persistence.repos import RunRepo, FileRepo, HashGroupRepo, PlanRepo, ArtifactRepo, ErrorRepo
from domain.models import ScanConfig
from organizer_io.free_space import free_bytes

from services.run_service import RunService
from services.scan_service import ScanService
from services.hash_service import HashService
from services.planner_service import PlannerService
from services.executor_service import ExecutorService
from services.report_service import ReportService
from services.rollback_service import RollbackService


class Worker(QObject):
    progress = Signal(str)
    stage_progress = Signal(int, int, str)  # (current, total, detail)
    plan_ready = Signal(dict)
    exec_progress = Signal(int, int, str, str)  # (done, total, src, dst)
    finished = Signal(bool, str)

    def __init__(self, db: Database, run_id: str, source_root: str, dest_root: str, cfg: ScanConfig):
        super().__init__()
        self.db = db
        self.run_id = run_id
        self.source_root = source_root
        self.dest_root = dest_root
        self.cfg = cfg
        self._stop = False

        # Throttle UI emits to avoid "Not responding" on large runs
        self._last_emit = 0.0

        self.run_repo = RunRepo(db)
        self.file_repo = FileRepo(db)
        self.hash_repo = HashGroupRepo(db)
        self.plan_repo = PlanRepo(db)
        self.artifact_repo = ArtifactRepo(db)
        self.error_repo = ErrorRepo(db)

        self.scan = ScanService(self.file_repo, self.error_repo)
        self.hash = HashService(self.file_repo, self.hash_repo, self.error_repo)
        self.planner = PlannerService(db, self.plan_repo, self.hash_repo, self.error_repo)
        self.exec = ExecutorService(self.plan_repo, self.error_repo)
        self.report = ReportService(db, self.artifact_repo)

    def stop_flag(self):
        return self._stop

    def request_stop(self):
        self._stop = True

    def run_pipeline(self):
        try:
            self.run_repo.update_status(self.run_id, "RUNNING")

            # --------------------
            # Scan (indeterminate bar, file count)
            # --------------------
            self.progress.emit("Scanning files…")

            def sp(count, path):
                # total=0 => UI uses indeterminate mode
                now = time.monotonic()
                if (count % 200 == 0) or (now - self._last_emit > 0.2):
                    self._last_emit = now
                    self.stage_progress.emit(count, 0, path)

            self.scan.scan(
                self.run_id,
                self.source_root,
                self.cfg.min_file_size,
                self.cfg.include_photos,
                self.cfg.include_videos,
                self.cfg.include_raw,
                progress_cb=sp,
                stop_flag=self.stop_flag,
            )
            self.run_repo.update_status(self.run_id, "SCANNED")

            # --------------------
            # Hash (determinate bar)
            # --------------------
            self.progress.emit("Hashing (SHA-256)…")

            def hp(i, total, path):
                now = time.monotonic()
                # Emit at most ~10x/second or every 50 files
                if (i % 50 == 0) or (now - self._last_emit > 0.1) or (i == total):
                    self._last_emit = now
                    self.stage_progress.emit(i, total, path)

            self.hash.hash_all(self.run_id, progress_cb=hp, stop_flag=self.stop_flag)

            # --------------------
            # Plan
            # --------------------
            self.progress.emit("Building plan…")
            plan_info = self.planner.build_plan(self.run_id, self.dest_root)
            self.run_repo.update_status(self.run_id, "PLANNED")

            counts = self.file_repo.counts(self.run_id)
            by_date = self.file_repo.aggregate_by_date_type(self.run_id)
            dupes = self.hash_repo.duplicate_counts(self.run_id)
            self.plan_ready.emit(
                {
                    "counts": counts,
                    "by_date": by_date,
                    "dupes": dupes,
                    "collisions": plan_info.get("collisions", 0),
                }
            )

            self.finished.emit(True, "PLANNED")
        except Exception as e:
            self.finished.emit(False, f"{type(e).__name__}: {e}")

    def execute_plan(self, error_policy: str = "RETRY_THEN_SKIP"):
        try:
            self.progress.emit("Executing copy + verify…")
            items = self.plan_repo.list_pending_for_execution(self.run_id)

            def ep(done, total, src, dst):
                now = time.monotonic()
                # Emit at most ~10x/second or every 10 files
                if (done % 10 == 0) or (now - self._last_emit > 0.1) or (done == total):
                    self._last_emit = now
                    self.exec_progress.emit(done, total, src, dst)

            self.exec.execute(
                self.run_id,
                items,
                progress_cb=ep,
                stop_flag=self.stop_flag,
                error_policy=error_policy,
            )
            self.run_repo.update_status(self.run_id, "COMPLETED")

            self.progress.emit("Writing artifacts…")
            run = self.run_repo.get(self.run_id)
            self.report.produce(self.run_id, run["artifacts_root"])

            self.finished.emit(True, "COMPLETED")
        except Exception as e:
            self.run_repo.update_status(self.run_id, "FAILED")
            self.finished.emit(False, f"{type(e).__name__}: {e}")


class MainWindow(QMainWindow):
    def __init__(self, db: Database):
        super().__init__()
        self.setWindowTitle("Media Organizer")

        self.db = db
        self.run_repo = RunRepo(db)
        self.file_repo = FileRepo(db)
        self.plan_repo = PlanRepo(db)
        self.artifact_repo = ArtifactRepo(db)
        self.error_repo = ErrorRepo(db)

        self.run_id = None
        self.worker = None
        self.thread = None
        self.plan_stats = None

        self.stack = QStackedWidget()
        self.setCentralWidget(self.stack)

        self.screen1 = self._build_select_screen()
        self.screen2 = self._build_config_screen()
        self.screen3 = self._build_preview_screen()
        self.screen4 = self._build_progress_screen()
        self.screen5 = self._build_completed_screen()

        self.stack.addWidget(self.screen1)
        self.stack.addWidget(self.screen2)
        self.stack.addWidget(self.screen3)
        self.stack.addWidget(self.screen4)
        self.stack.addWidget(self.screen5)

        self.stack.setCurrentIndex(0)
        self._maybe_restore_last_run()

    def _build_select_screen(self):
        w = QWidget()
        v = QVBoxLayout(w)
        v.setContentsMargins(24, 24, 24, 24)
        v.setSpacing(14)

        title = QLabel("Step 1: Select source and destination")
        title.setStyleSheet("font-size: 18px; font-weight: 600;")
        v.addWidget(title)

        subtitle = QLabel("Artifacts are stored under Destination\\Artifacts")
        subtitle.setStyleSheet("color: #444;")
        v.addWidget(subtitle)

        card = QGroupBox("Folders")
        form = QFormLayout(card)
        form.setLabelAlignment(Qt.AlignLeft)
        form.setFormAlignment(Qt.AlignTop)
        form.setHorizontalSpacing(12)
        form.setVerticalSpacing(10)

        self.source_edit = QLineEdit()
        self.source_edit.setPlaceholderText(r"Example: D:\\UnsortedMedia")
        self.source_edit.setMinimumHeight(34)

        self.dest_edit = QLineEdit()
        self.dest_edit.setPlaceholderText(r"Example: E:\\MediaLibrary")
        self.dest_edit.setMinimumHeight(34)

        def pick_into(edit: QLineEdit, title_txt: str):
            p = QFileDialog.getExistingDirectory(self, title_txt)
            if p:
                edit.setText(p)

        src_row = QHBoxLayout()
        src_row.addWidget(self.source_edit, 1)
        src_btn = QPushButton("Browse…")
        src_btn.setMinimumHeight(34)
        src_btn.clicked.connect(lambda: pick_into(self.source_edit, "Select Source Folder"))
        src_row.addWidget(src_btn)

        dst_row = QHBoxLayout()
        dst_row.addWidget(self.dest_edit, 1)
        dst_btn = QPushButton("Browse…")
        dst_btn.setMinimumHeight(34)
        dst_btn.clicked.connect(lambda: pick_into(self.dest_edit, "Select Destination Folder"))
        dst_row.addWidget(dst_btn)

        form.addRow("Source root", src_row)
        form.addRow("Destination root", dst_row)

        v.addWidget(card)

        btn_row = QHBoxLayout()
        btn_row.addStretch(1)
        next_btn = QPushButton("Next →")
        next_btn.setMinimumHeight(36)
        next_btn.clicked.connect(lambda: self.stack.setCurrentWidget(self.screen2))
        btn_row.addWidget(next_btn)

        v.addLayout(btn_row)
        v.addStretch(1)

        return w

    def _build_config_screen(self):
        w = QWidget()
        v = QVBoxLayout(w)
        v.setContentsMargins(24, 24, 24, 24)
        v.setSpacing(14)

        title = QLabel("Step 2: Configure scan")
        title.setStyleSheet("font-size: 18px; font-weight: 600;")
        v.addWidget(title)

        gb = QGroupBox("Scan options")
        form = QFormLayout(gb)
        form.setHorizontalSpacing(12)
        form.setVerticalSpacing(10)

        self.min_size = QSpinBox()
        self.min_size.setRange(0, 5000000)
        self.min_size.setValue(10240)
        self.min_size.setSuffix(" bytes")
        self.min_size.setMinimumHeight(32)
        form.addRow("Minimum file size", self.min_size)

        self.cb_photos = QCheckBox("Include photos")
        self.cb_photos.setChecked(True)

        self.cb_videos = QCheckBox("Include videos")
        self.cb_videos.setChecked(True)

        self.cb_raw = QCheckBox("Include RAW")
        self.cb_raw.setChecked(True)

        # NEW: non-media files (group by extension under OtherByExt\<EXT>)
        self.cb_other = QCheckBox("Include non-media files (group by extension)")
        self.cb_other.setChecked(False)

        form.addRow(self.cb_photos)
        form.addRow(self.cb_videos)
        form.addRow(self.cb_raw)
        form.addRow(self.cb_other)

        v.addWidget(gb)

        btns = QHBoxLayout()

        # Make Back accessible later so we can disable it during scanning
        self.back_btn = QPushButton("← Back")
        self.back_btn.setMinimumHeight(36)
        self.back_btn.clicked.connect(lambda: self.stack.setCurrentWidget(self.screen1))

        self.scan_btn = QPushButton("Scan + Build Plan")
        self.scan_btn.setMinimumHeight(36)
        self.scan_btn.clicked.connect(self.start_scan_plan)

        btns.addWidget(self.back_btn)
        btns.addStretch(1)
        btns.addWidget(self.scan_btn)

        v.addLayout(btns)
        v.addStretch(1)

        return w


    def _build_preview_screen(self):
        w = QWidget()
        v = QVBoxLayout(w)
        v.addWidget(QLabel("Step 3: Preview plan (required)"))

        self.preview_label = QLabel("")
        v.addWidget(self.preview_label)

        self.preview_table = QTableWidget(0, 4)
        self.preview_table.setHorizontalHeaderLabels(["Date", "Photos", "Videos", "RAW"])
        v.addWidget(self.preview_table)

        btns = QHBoxLayout()
        back = QPushButton("Back")
        back.clicked.connect(lambda: self.stack.setCurrentWidget(self.screen2))
        run = QPushButton("Confirm & Execute Copy")
        run.clicked.connect(self.start_execute)
        btns.addWidget(back)
        btns.addWidget(run)
        v.addLayout(btns)
        return w

    def _build_progress_screen(self):
        w = QWidget()
        v = QVBoxLayout(w)

        self.stage_label = QLabel("Idle")
        v.addWidget(self.stage_label)

        self.progress_bar = QProgressBar()
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setMinimum(0)
        self.progress_bar.setMaximum(0)  # indeterminate by default
        self.progress_bar.setFormat("Working…")
        v.addWidget(self.progress_bar)

        self.progress_detail = QLabel("")
        self.progress_detail.setTextInteractionFlags(Qt.TextSelectableByMouse)
        v.addWidget(self.progress_detail)

        v.addWidget(QLabel("Errors (latest)"))
        self.error_list = QListWidget()
        v.addWidget(self.error_list)

        btns = QHBoxLayout()

        self.pause_btn = QPushButton("Pause (Stop after current file)")
        self.pause_btn.clicked.connect(self.request_stop)
        btns.addWidget(self.pause_btn)

        # Disabled until PLANNED
        self.view_plan_btn = QPushButton("View plan")
        self.view_plan_btn.setEnabled(False)
        self.view_plan_btn.clicked.connect(lambda: self.stack.setCurrentWidget(self.screen3))
        btns.addWidget(self.view_plan_btn)

        v.addLayout(btns)
        return w

    def _show_plan_ready_button(self):
        if hasattr(self, "view_plan_btn"):
            self.view_plan_btn.setEnabled(True)

    def _build_completed_screen(self):
        w = QWidget()
        v = QVBoxLayout(w)

        self.completed_label = QLabel("")
        v.addWidget(self.completed_label)

        self.artifacts_list = QListWidget()
        v.addWidget(self.artifacts_list)

        btns = QHBoxLayout()

        open_artifacts = QPushButton("Open artifacts folder")
        open_artifacts.clicked.connect(self.open_artifacts_folder)

        rollback = QPushButton("Rollback this run")
        rollback.clicked.connect(self.rollback_run)

        new_run = QPushButton("Start new run")
        new_run.clicked.connect(self.start_new_run)

        btns.addWidget(open_artifacts)
        btns.addWidget(rollback)
        btns.addStretch(1)
        btns.addWidget(new_run)

        v.addLayout(btns)
        return w

    def start_new_run(self):
        # Ensure worker/thread are cleaned up
        if self.worker:
            self.worker.request_stop()
            self.worker = None

        if self.thread:
            self.thread.quit()
            self.thread.wait()
            self.thread = None

        # Clear run-specific state
        self.run_id = None
        self.plan_stats = None

        # Reset UI fields
        self.stage_label.setText("Idle")
        self.progress_detail.setText("")
        self.progress_bar.setMinimum(0)
        self.progress_bar.setMaximum(0)
        self.progress_bar.setValue(0)

        self.error_list.clear()
        self.artifacts_list.clear()
        self.preview_table.setRowCount(0)
        self.preview_label.setText("")
        self.completed_label.setText("")

        if hasattr(self, "view_plan_btn"):
            self.view_plan_btn.setEnabled(False)

        if hasattr(self, "back_btn"):
            self.back_btn.setEnabled(True)

        if hasattr(self, "scan_btn"):
            self.scan_btn.setEnabled(True)

        # Return to first screen
        self.stack.setCurrentWidget(self.screen1)

    def _maybe_restore_last_run(self):
        last = self.run_repo.latest_incomplete()
        if not last:
            return
        r = QMessageBox.question(
            self,
            "Restore session?",
            f"Found an incomplete run:\n\n{last['run_name']}\nStatus: {last['status']}\n\nRestore it?",
            QMessageBox.Yes | QMessageBox.No,
        )
        if r == QMessageBox.Yes:
            self.run_id = last["run_id"]
            self.source_edit.setText(last["source_root"])
            self.dest_edit.setText(last["dest_root"])
            self.min_size.setValue(int(last["min_file_size"]))
            if last["status"] in ("PLANNED", "RUNNING", "PAUSED", "FAILED"):
                counts = self.file_repo.counts(self.run_id)
                by_date = self.file_repo.aggregate_by_date_type(self.run_id)
                self._render_preview({"counts": counts, "by_date": by_date, "dupes": 0, "collisions": 0})
                self.stack.setCurrentWidget(self.screen3)
            else:
                self.stack.setCurrentWidget(self.screen2)

    def request_stop(self):
        if self.worker:
            self.worker.request_stop()
            self.stage_label.setText("Stop requested. Will pause after current operation.")

    def _validate_paths(self):
        s = self.source_edit.text().strip()
        d = self.dest_edit.text().strip()

        if not (s and d):
            QMessageBox.warning(self, "Missing info", "Source and destination folders are required.")
            return None

        if not os.path.isdir(s):
            QMessageBox.warning(self, "Invalid source", "Source folder does not exist.")
            return None

        os.makedirs(d, exist_ok=True)
        artifacts_root = os.path.join(d, "Artifacts")
        os.makedirs(artifacts_root, exist_ok=True)

        return s, d, artifacts_root

    def start_scan_plan(self):
        paths = self._validate_paths()
        if not paths:
            return

        source_root, dest_root, artifacts_root = paths
        run_name = datetime.datetime.now().strftime("Run_%Y%m%d_%H%M%S")

        free = free_bytes(dest_root)
        if free < 1024 * 1024 * 1024:
            QMessageBox.warning(self, "Low disk space", "Destination free space is under 1GB. Choose another drive.")
            return

        cfg = ScanConfig(
            min_file_size=int(self.min_size.value()),
            include_photos=self.cb_photos.isChecked(),
            include_videos=self.cb_videos.isChecked(),
            include_raw=self.cb_raw.isChecked(),
            include_other=self.cb_other.isChecked(),  # NEW
            overwrite_policy="UI_CHOICE",
            error_policy="UI_CHOICE",
            live_photo_policy="UI_CHOICE",
            thumbs_policy="UI_CHOICE",
            cpu_limit_pct=None,
            io_limit_mbps=None,
        )


        # Disable navigation while running
        if hasattr(self, "view_plan_btn"):
            self.view_plan_btn.setEnabled(False)

        if hasattr(self, "back_btn"):
            self.back_btn.setEnabled(False)

        if hasattr(self, "scan_btn"):
            self.scan_btn.setEnabled(False)

        run_service = RunService(RunRepo(self.db))
        self.run_id = run_service.create(run_name, source_root, dest_root, artifacts_root, cfg)

        self.thread = QThread()
        self.worker = Worker(self.db, self.run_id, source_root, dest_root, cfg)
        self.worker.moveToThread(self.thread)

        self.worker.progress.connect(self.stage_label.setText)
        self.worker.stage_progress.connect(self.on_stage_progress)
        self.worker.plan_ready.connect(self.on_plan_ready)
        self.worker.finished.connect(self.on_worker_finished)

        self.thread.started.connect(self.worker.run_pipeline)
        self.thread.start()

        self.stack.setCurrentWidget(self.screen4)
        self.stage_label.setText("Starting…")

    def on_stage_progress(self, i, total, path):
        if total == 0:
            # Scanning: indeterminate
            self.progress_bar.setMinimum(0)
            self.progress_bar.setMaximum(0)
            self.progress_bar.setFormat(f"Scanning… {i} files")
            self.progress_detail.setText(path)
        else:
            # Hashing: determinate
            self.progress_bar.setMinimum(0)
            self.progress_bar.setMaximum(total)
            self.progress_bar.setValue(i)
            self.progress_bar.setFormat(f"Hashing… {i}/{total}")
            self.progress_detail.setText(path)

        # Refresh error list periodically so errors don't "flash"
        if self.run_id and (i % 200 == 0 or (total > 0 and i % 50 == 0) or i == total):
            self._refresh_errors()

    def _refresh_errors(self):
        self.error_list.clear()
        for e in reversed(self.error_repo.list_latest(self.run_id, 100)):
            self.error_list.addItem(f"[{e['phase']}] {e.get('code') or ''} {e['message']}")

    def on_plan_ready(self, stats: dict):
        self.plan_stats = stats
        self._render_preview(stats)

    def _render_preview(self, stats: dict):
        counts = stats["counts"]
        dupes = stats.get("dupes", 0)
        collisions = stats.get("collisions", 0)
        self.preview_label.setText(
            f"Files: {counts.get('total', 0)} | Photos: {counts.get('photos', 0)} | Videos: {counts.get('videos', 0)} | RAW: {counts.get('raws', 0)}\n"
            f"Bytes: {counts.get('bytes', 0) or 0} | Exact duplicates: {dupes} | Name collisions: {collisions}"
        )

        by_date = stats.get("by_date", {})
        dates = sorted(by_date.keys())
        self.preview_table.setRowCount(len(dates))
        for row, d in enumerate(dates):
            p = by_date[d].get("PHOTO", 0)
            v = by_date[d].get("VIDEO", 0)
            r = by_date[d].get("RAW", 0)
            self.preview_table.setItem(row, 0, QTableWidgetItem(d))
            self.preview_table.setItem(row, 1, QTableWidgetItem(str(p)))
            self.preview_table.setItem(row, 2, QTableWidgetItem(str(v)))
            self.preview_table.setItem(row, 3, QTableWidgetItem(str(r)))

    def on_worker_finished(self, ok: bool, msg: str):
        if self.thread:
            self.thread.quit()
            self.thread.wait()

        # Re-enable navigation after pipeline ends
        if hasattr(self, "back_btn"):
            self.back_btn.setEnabled(True)

        if hasattr(self, "scan_btn"):
            self.scan_btn.setEnabled(True)

        self._refresh_errors()

        if not ok:
            QMessageBox.critical(self, "Failed", msg)
            return

        if msg == "PLANNED":
            self.stage_label.setText("Plan ready. Review errors, then click View plan.")
            self._show_plan_ready_button()
            return

        if msg == "COMPLETED":
            self.show_completed()

    def start_execute(self):
        if not self.run_id:
            return
        r = QMessageBox.question(self, "Confirm", "Execute copy + verify now?", QMessageBox.Yes | QMessageBox.No)
        if r != QMessageBox.Yes:
            return

        self.thread = QThread()
        run = self.run_repo.get(self.run_id)
        cfg = ScanConfig(
            min_file_size=int(run["min_file_size"]),
            include_photos=True,
            include_videos=True,
            include_raw=True,
            include_other=bool(run.get("include_other", 0)),  # ← REQUIRED
            overwrite_policy=run["overwrite_policy"],
            error_policy=run["error_policy"],
            live_photo_policy=run["live_photo_policy"],
            thumbs_policy=run["thumbs_policy"],
        )

        self.worker = Worker(self.db, self.run_id, run["source_root"], run["dest_root"], cfg)
        self.worker.moveToThread(self.thread)

        self.worker.progress.connect(self.stage_label.setText)
        self.worker.exec_progress.connect(self.on_exec_progress)
        self.worker.finished.connect(self.on_worker_finished)

        self.thread.started.connect(lambda: self.worker.execute_plan(error_policy="RETRY_THEN_SKIP"))
        self.thread.start()

        self.stack.setCurrentWidget(self.screen4)
        self.stage_label.setText("Executing…")

    def on_exec_progress(self, done, total, src, dst):
        self.progress_bar.setMinimum(0)
        self.progress_bar.setMaximum(total)
        self.progress_bar.setValue(done)
        self.progress_bar.setFormat(f"Copying… {done}/{total}")
        self.progress_detail.setText(f"{src}\n→ {dst}")

        if self.run_id and done % 25 == 0:
            self._refresh_errors()

    def show_completed(self):
        self.completed_label.setText(f"Run completed: {self.run_id}")
        self.artifacts_list.clear()
        for a in self.artifact_repo.list(self.run_id):
            self.artifacts_list.addItem(f"{a['kind']}: {a['path']}")
        self.stack.setCurrentWidget(self.screen5)

    def open_artifacts_folder(self):
        if not self.run_id:
            return
        run = self.run_repo.get(self.run_id)
        if not run:
            return
        os.startfile(run["artifacts_root"])

    def rollback_run(self):
        if not self.run_id:
            return
        r = QMessageBox.question(
            self,
            "Rollback",
            "Rollback will delete destination copies created by this run. Continue?",
            QMessageBox.Yes | QMessageBox.No,
        )
        if r != QMessageBox.Yes:
            return
        rb = RollbackService(self.db, self.error_repo)
        rb.rollback(self.run_id)
        QMessageBox.information(self, "Rollback", "Rollback complete (check errors list for any failures).")
