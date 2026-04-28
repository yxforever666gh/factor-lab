from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.research_candidate_pool import build_research_candidate_pool


if __name__ == "__main__":
    build_research_candidate_pool(
        snapshot_path="artifacts/research_planner_snapshot.json",
        output_path="artifacts/research_candidate_pool.json",
    )
    print("research candidate pool built")
