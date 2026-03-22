from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.research_branch_planner import build_branch_planner_output


if __name__ == "__main__":
    build_branch_planner_output(
        space_map_path="artifacts/research_space_map.json",
        snapshot_path="artifacts/research_planner_snapshot.json",
        candidate_pool_path="artifacts/research_candidate_pool.json",
        output_path="artifacts/research_branch_plan.json",
    )
    print("research branch plan built")
