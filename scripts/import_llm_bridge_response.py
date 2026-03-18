from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.llm_bridge import import_agent_response


if __name__ == "__main__":
    import_agent_response(
        response_path="artifacts/agent_response.json",
        review_output_path="artifacts/llm_review.md",
        plan_output_path="artifacts/llm_next_batch_proposal.json",
        status_output_path="artifacts/llm_status.json",
    )
