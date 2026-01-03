import uuid
from persistence.repos import RunRepo
from domain.models import ScanConfig

class RunService:
    def __init__(self, run_repo: RunRepo):
        self.run_repo = run_repo

    def create(self, run_name: str, source_root: str, dest_root: str, artifacts_root: str, cfg: ScanConfig) -> str:
        run_id = str(uuid.uuid4())
        cfg_dict = {
            "min_file_size": cfg.min_file_size,
            "overwrite_policy": cfg.overwrite_policy,
            "error_policy": cfg.error_policy,
            "live_photo_policy": cfg.live_photo_policy,
            "thumbs_policy": cfg.thumbs_policy,
            "cpu_limit_pct": cfg.cpu_limit_pct,
            "io_limit_mbps": cfg.io_limit_mbps,
        }
        self.run_repo.create_run(run_id, run_name, source_root, dest_root, artifacts_root, cfg_dict)
        return run_id
