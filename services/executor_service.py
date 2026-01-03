from organizer_io.file_copy import copy_stream
from organizer_io.hash_stream import sha256_file

class ExecutorService:
    def __init__(self, plan_repo, error_repo, rollback_repo=None):
        self.plan_repo = plan_repo
        self.error_repo = error_repo
        self.rollback_repo = rollback_repo

    def execute(self, run_id: str, items: list[dict], progress_cb=None, stop_flag=None, error_policy="RETRY_THEN_SKIP", retries=2):
        total = len(items)
        done = 0
        for it in items:
            if stop_flag and stop_flag():
                break

            plan_item_id = int(it["plan_item_id"])
            src = it["source_path"]
            dst = it["dest_path"]

            self.plan_repo.mark_copying(plan_item_id)

            attempt = 0
            while True:
                attempt += 1
                try:
                    bytes_copied = copy_stream(src, dst)
                    expected = it.get("source_sha256")
                    actual = sha256_file(dst)
                    if expected and actual != expected:
                        raise RuntimeError("SHA256_MISMATCH")
                    self.plan_repo.mark_verified(plan_item_id, bytes_copied)
                    break
                except Exception as e:
                    code = "COPY_FAIL"
                    msg = f"{type(e).__name__}: {e}"
                    if str(e) == "SHA256_MISMATCH":
                        code = "VERIFY_FAIL"
                        msg = "SHA256 mismatch after copy"

                    if error_policy == "RETRY_THEN_SKIP" and attempt <= retries:
                        continue

                    self.plan_repo.mark_failed(plan_item_id, code, msg)
                    self.error_repo.add(run_id, "COPY" if code == "COPY_FAIL" else "VERIFY",
                                        msg, code=code, source_path=src, dest_path=dst, plan_item_id=plan_item_id)
                    break

            done += 1
            if progress_cb:
                progress_cb(done, total, src, dst)
