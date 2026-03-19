from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.research_planner import build_research_plan


if __name__ == "__main__":
    build_research_plan(
        snapshot_path="artifacts/research_planner_snapshot.json",
        candidate_pool_path="artifacts/research_candidate_pool.json",
        output_path="artifacts/research_planner_proposal.json",
    )
    print("research planner proposal built")
