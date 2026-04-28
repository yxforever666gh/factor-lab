from pathlib import Path
import sys
import json

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.storage import ExperimentStore
from factor_lab.research_queue import enqueue_baseline_tasks


if __name__ == "__main__":
    store = ExperimentStore(Path("artifacts") / "factor_lab.db")
    task_ids = enqueue_baseline_tasks(store)
    print(json.dumps({"seeded_task_ids": task_ids}, ensure_ascii=False, indent=2))
