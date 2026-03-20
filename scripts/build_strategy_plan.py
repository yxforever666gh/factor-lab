from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.research_strategy import build_strategy_plan


if __name__ == "__main__":
    build_strategy_plan(
        state_snapshot_path="artifacts/research_state_snapshot.json",
        proposal_path="artifacts/research_planner_proposal.json",
        output_path="artifacts/strategy_plan.json",
        branch_plan_path="artifacts/research_branch_plan.json",
    )
    print("strategy plan built")
