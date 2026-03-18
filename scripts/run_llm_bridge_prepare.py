from pathlib import Path
import json
import sys
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.llm_snapshot import build_snapshot
from factor_lab.llm_bridge import build_agent_request, write_bridge_status


if __name__ == "__main__":
    snapshot = build_snapshot(
        db_path="artifacts/factor_lab.db",
        output_path="artifacts/llm_input_snapshot.json",
    )
    build_agent_request(snapshot, "artifacts/agent_request.json")
    write_bridge_status(
        "artifacts/llm_status.json",
        {
            "mode": "openclaw_agent_bridge",
            "status": "request_ready",
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "request_path": "artifacts/agent_request.json",
            "response_path": "artifacts/agent_response.json",
        },
    )
