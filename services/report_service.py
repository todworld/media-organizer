import os
from artifacts.plan_writer import write_plan_json
from artifacts.report_writer import write_csv_and_summary

class ReportService:
    def __init__(self, db, artifact_repo):
        self.db = db
        self.artifact_repo = artifact_repo

    def produce(self, run_id: str, artifacts_root: str):
        os.makedirs(artifacts_root, exist_ok=True)
        log_path = os.path.join(artifacts_root, f"{run_id}_run.log")
        csv_path = os.path.join(artifacts_root, f"{run_id}_summary.csv")
        plan_path = os.path.join(artifacts_root, f"{run_id}_plan.json")
        summary_path = os.path.join(artifacts_root, f"{run_id}_summary.txt")

        write_plan_json(self.db, run_id, plan_path)
        write_csv_and_summary(self.db, run_id, csv_path, summary_path)

        self.artifact_repo.add(run_id, "LOG", log_path)
        self.artifact_repo.add(run_id, "CSV", csv_path)
        self.artifact_repo.add(run_id, "PLAN_JSON", plan_path)
        self.artifact_repo.add(run_id, "SUMMARY_TXT", summary_path)

        # Create an empty log file placeholder for now (UI uses db errors list)
        if not os.path.exists(log_path):
            with open(log_path, "w", encoding="utf-8") as f:
                f.write("")
        return {"log": log_path, "csv": csv_path, "plan": plan_path, "summary": summary_path}
