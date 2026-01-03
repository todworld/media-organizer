# services/hash_service.py
# Parallel hashing (I/O bound) + single-threaded DB/group updates

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from organizer_io.hash_stream import sha256_file  # now BLAKE3 internally


class HashService:
    def __init__(self, file_repo, hash_group_repo, error_repo):
        self.file_repo = file_repo
        self.hash_group_repo = hash_group_repo
        self.error_repo = error_repo

    def _workers_default(self) -> int:
        # I/O bound: more than CPU count is often fine, but cap to avoid thrash.
        try:
            cpu = os.cpu_count() or 4
        except Exception:
            cpu = 4
        return max(4, min(32, cpu * 4))

    def hash_all(
        self,
        run_id: str,
        progress_cb=None,
        stop_flag=None,
        max_workers: int | None = None,
        chunk_bytes: int = 4 * 1024 * 1024,
    ):
        rows = self.file_repo.list_files_for_hashing(run_id)
        total = len(rows)
        if total == 0:
            return

        workers = max_workers or self._workers_default()

        def _task(file_id: str, path: str):
            # Return tuple so main thread can apply DB updates in order of completion
            h = sha256_file(path, chunk_bytes=chunk_bytes)
            return file_id, path, h

        # Submit hash work in parallel; apply DB/group updates in main thread only.
        completed = 0
        futures = {}

        with ThreadPoolExecutor(max_workers=workers) as pool:
            for r in rows:
                if stop_flag and stop_flag():
                    break
                fut = pool.submit(_task, r["file_id"], r["source_path"])
                futures[fut] = r

            for fut in as_completed(futures):
                r = futures[fut]
                completed += 1

                if stop_flag and stop_flag():
                    # Stop reporting/committing further results (threads may still be running)
                    break

                try:
                    file_id, path, h = fut.result()

                    # Single-threaded DB writes + grouping
                    self.file_repo.set_sha256(file_id, h)
                    self.hash_group_repo.upsert_group(run_id, h)

                except Exception as e:
                    self.error_repo.add(
                        run_id,
                        "HASH",
                        f"{type(e).__name__}: {e}",
                        source_path=r["source_path"],
                    )

                if progress_cb:
                    progress_cb(completed, total, r["source_path"])
