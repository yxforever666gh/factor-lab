from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.research_planner_inject import inject_research_planner_tasks


if __name__ == "__main__":
    inject_research_planner_tasks(
        validated_path="artifacts/research_planner_validated.json",
        output_path="artifacts/research_planner_injected.json",
    )
    print("research planner tasks injected")
