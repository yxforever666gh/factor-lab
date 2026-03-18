from pathlib import Path
import sys
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.llm_bridge import write_bridge_status
from factor_lab.llm_recommendation_memory import append_recommendation_history
from factor_lab.llm_recommendation_scoring import build_recommendation_weights


if __name__ == "__main__":
    history = append_recommendation_history(
        plan_path="artifacts/llm_next_batch_proposal.json",
        retrospective_path="artifacts/llm_retrospective.json",
        output_path="artifacts/llm_recommendation_history.json",
    )
    weights = build_recommendation_weights(
        history_path="artifacts/llm_recommendation_history.json",
        output_path="artifacts/llm_recommendation_weights.json",
    )
    write_bridge_status(
        "artifacts/llm_status.json",
        {
            "mode": "openclaw_agent_bridge",
            "status": "recommendation_memory_updated",
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "history_path": "artifacts/llm_recommendation_history.json",
            "weights_path": "artifacts/llm_recommendation_weights.json",
            "history_count": len(history),
            "weights": weights,
        },
    )
    print("recommendation memory updated")
