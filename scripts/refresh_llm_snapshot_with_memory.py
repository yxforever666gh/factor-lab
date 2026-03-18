from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.llm_snapshot import build_snapshot
from factor_lab.llm_bridge import build_agent_request


if __name__ == "__main__":
    snapshot = build_snapshot(
        db_path="artifacts/factor_lab.db",
        output_path="artifacts/llm_input_snapshot.json",
    )
    build_agent_request(snapshot, "artifacts/agent_request.json")
    print("llm snapshot refreshed with recommendation memory")
