from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.llm_snapshot import build_snapshot
from factor_lab.llm_agent import run_llm_cycle
from factor_lab.llm_bridge import write_bridge_status
from factor_lab.heartbeat import append_heartbeat
from datetime import datetime, timezone


if __name__ == "__main__":
    try:
        append_heartbeat("llm_cycle", "started", message="LLM 周期开始执行。")
        build_snapshot(
            db_path="artifacts/factor_lab.db",
            output_path="artifacts/llm_input_snapshot.json",
        )
        run_llm_cycle(
            snapshot_path="artifacts/llm_input_snapshot.json",
            review_output_path="artifacts/llm_review.md",
            plan_output_path="artifacts/llm_next_batch_proposal.json",
        )
        write_bridge_status(
            "artifacts/llm_status.json",
            {
                "mode": "mock_local",
                "status": "completed",
                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                "request_path": "artifacts/llm_input_snapshot.json",
                "response_path": "artifacts/llm_review.md + artifacts/llm_next_batch_proposal.json",
            },
        )
        append_heartbeat("llm_cycle", "finished", summary="LLM 快照、评审与计划已更新。")
    except Exception as exc:
        append_heartbeat("llm_cycle", "failed", message=str(exc))
        raise
