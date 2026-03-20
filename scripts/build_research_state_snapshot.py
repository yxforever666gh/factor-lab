from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.research_strategy import build_research_state_snapshot


if __name__ == "__main__":
    build_research_state_snapshot(
        db_path="artifacts/factor_lab.db",
        planner_snapshot_path="artifacts/research_planner_snapshot.json",
        candidate_pool_path="artifacts/research_candidate_pool.json",
        proposal_path="artifacts/research_planner_proposal.json",
        output_path="artifacts/research_state_snapshot.json",
        memory_path="artifacts/research_memory.json",
    )
    print("research state snapshot built")
