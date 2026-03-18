from pathlib import Path
import sys
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.llm_bridge import import_agent_response, write_bridge_status


if __name__ == "__main__":
    response = Path("artifacts/agent_response.json")
    if not response.exists():
        write_bridge_status(
            "artifacts/llm_status.json",
            {
                "mode": "openclaw_agent_bridge",
                "status": "awaiting_response",
                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                "request_path": "artifacts/agent_request.json",
                "response_path": "artifacts/agent_response.json",
                "message": "尚未检测到 agent_response.json，请先让 OpenClaw 单 agent 处理请求。",
            },
        )
        print("bridge response not found")
        raise SystemExit(2)

    import_agent_response(
        response_path="artifacts/agent_response.json",
        review_output_path="artifacts/llm_review.md",
        plan_output_path="artifacts/llm_next_batch_proposal.json",
        status_output_path="artifacts/llm_status.json",
    )
    print("bridge response imported")
