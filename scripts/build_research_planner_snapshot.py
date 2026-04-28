from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.research_planner_snapshot import build_research_planner_snapshot


if __name__ == "__main__":
    build_research_planner_snapshot(
        db_path="artifacts/factor_lab.db",
        output_path="artifacts/research_planner_snapshot.json",
    )
    print("research planner snapshot built")
