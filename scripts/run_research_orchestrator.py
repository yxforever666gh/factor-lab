from pathlib import Path
import sys
import json

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.research_queue import run_orchestrator


if __name__ == "__main__":
    result = run_orchestrator(max_tasks=1)
    print(json.dumps(result, ensure_ascii=False, indent=2))
