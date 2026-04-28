from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.research_planner_validate import validate_research_planner_proposal


if __name__ == "__main__":
    validate_research_planner_proposal(
        proposal_path="artifacts/research_planner_proposal.json",
        output_path="artifacts/research_planner_validated.json",
    )
    print("research planner proposal validated")
