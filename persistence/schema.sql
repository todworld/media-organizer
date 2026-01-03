PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS runs (
  run_id            TEXT PRIMARY KEY,
  run_name          TEXT NOT NULL,
  created_at        TEXT NOT NULL,
  updated_at        TEXT NOT NULL,

  source_root       TEXT NOT NULL,
  dest_root         TEXT NOT NULL,
  artifacts_root    TEXT NOT NULL,

  status            TEXT NOT NULL,
  last_checkpoint   TEXT,

  min_file_size     INTEGER NOT NULL,
  overwrite_policy  TEXT NOT NULL,
  error_policy      TEXT NOT NULL,
  live_photo_policy TEXT NOT NULL,
  thumbs_policy     TEXT NOT NULL,

  cpu_limit_pct     INTEGER,
  io_limit_mbps     INTEGER
);

CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);

CREATE TABLE IF NOT EXISTS files (
  file_id           INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id            TEXT NOT NULL,

  source_path       TEXT NOT NULL,
  source_root       TEXT NOT NULL,

  ext               TEXT NOT NULL,
  media_type        TEXT NOT NULL,
  file_size         INTEGER NOT NULL,

  mtime             TEXT NOT NULL,
  exif_datetime     TEXT,
  chosen_date       TEXT NOT NULL,
  date_source       TEXT NOT NULL,

  sha256            TEXT,

  is_hidden         INTEGER NOT NULL DEFAULT 0,
  is_system         INTEGER NOT NULL DEFAULT 0,
  is_link           INTEGER NOT NULL DEFAULT 0,

  created_at        TEXT NOT NULL,

  FOREIGN KEY(run_id) REFERENCES runs(run_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_files_run_source_path ON files(run_id, source_path);
CREATE INDEX IF NOT EXISTS idx_files_run_chosen_date ON files(run_id, chosen_date);
CREATE INDEX IF NOT EXISTS idx_files_sha256 ON files(sha256);

CREATE TABLE IF NOT EXISTS hash_groups (
  group_id        INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id          TEXT NOT NULL,
  sha256          TEXT NOT NULL,
  primary_file_id INTEGER,
  created_at      TEXT NOT NULL,
  FOREIGN KEY(run_id) REFERENCES runs(run_id),
  FOREIGN KEY(primary_file_id) REFERENCES files(file_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_hash_groups_run_sha ON hash_groups(run_id, sha256);

CREATE TABLE IF NOT EXISTS plan_items (
  plan_item_id        INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id              TEXT NOT NULL,
  file_id             INTEGER NOT NULL,

  action              TEXT NOT NULL,
  dest_path           TEXT NOT NULL,
  dest_rel_path       TEXT NOT NULL,

  collision_resolved  INTEGER NOT NULL DEFAULT 0,
  collision_suffix    INTEGER,

  duplicate_group_id  INTEGER,
  is_primary_in_group INTEGER NOT NULL DEFAULT 0,

  status              TEXT NOT NULL,
  bytes_copied        INTEGER NOT NULL DEFAULT 0,
  started_at          TEXT,
  finished_at         TEXT,

  error_code          TEXT,
  error_message       TEXT,

  FOREIGN KEY(run_id) REFERENCES runs(run_id),
  FOREIGN KEY(file_id) REFERENCES files(file_id),
  FOREIGN KEY(duplicate_group_id) REFERENCES hash_groups(group_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_plan_items_run_file ON plan_items(run_id, file_id);
CREATE INDEX IF NOT EXISTS idx_plan_items_run_status ON plan_items(run_id, status);

CREATE TABLE IF NOT EXISTS run_artifacts (
  artifact_id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id      TEXT NOT NULL,
  kind        TEXT NOT NULL,
  path        TEXT NOT NULL,
  created_at  TEXT NOT NULL,
  FOREIGN KEY(run_id) REFERENCES runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_run_artifacts_run ON run_artifacts(run_id);

CREATE TABLE IF NOT EXISTS errors (
  error_id     INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id       TEXT NOT NULL,
  plan_item_id INTEGER,
  phase        TEXT NOT NULL,
  code         TEXT,
  message      TEXT NOT NULL,
  source_path  TEXT,
  dest_path    TEXT,
  created_at   TEXT NOT NULL,
  FOREIGN KEY(run_id) REFERENCES runs(run_id),
  FOREIGN KEY(plan_item_id) REFERENCES plan_items(plan_item_id)
);

CREATE INDEX IF NOT EXISTS idx_errors_run_phase ON errors(run_id, phase);

CREATE TABLE IF NOT EXISTS rollback_items (
  rollback_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id           TEXT NOT NULL,
  plan_item_id     INTEGER NOT NULL,
  created_path     TEXT NOT NULL,
  status           TEXT NOT NULL,
  error_message    TEXT,
  created_at       TEXT NOT NULL,
  updated_at       TEXT NOT NULL,
  FOREIGN KEY(run_id) REFERENCES runs(run_id),
  FOREIGN KEY(plan_item_id) REFERENCES plan_items(plan_item_id)
);

CREATE INDEX IF NOT EXISTS idx_rollback_run_status ON rollback_items(run_id, status);
