from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.llm_snapshot import build_snapshot
from factor_lab.llm_agent import run_llm_cycle


if __name__ == "__main__":
    build_snapshot(
        db_path="artifacts/factor_lab.db",
        output_path="artifacts/llm_input_snapshot.json",
    )
    run_llm_cycle(
        snapshot_path="artifacts/llm_input_snapshot.json",
        review_output_path="artifacts/llm_review.md",
        plan_output_path="artifacts/llm_next_batch_proposal.json",
    )
