from pathlib import Path
import json
import sys
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.batch import run_batch
from factor_lab.llm_bridge import write_bridge_status
from factor_lab.llm_feedback import summarize_generated_batch_run


if __name__ == "__main__":
    batch_path = Path("artifacts/generated_batch_from_llm.json")
    if not batch_path.exists():
        print("generated batch not found")
        raise SystemExit(2)

    output_dir = "artifacts/llm_generated_batch_run"
    run_batch(str(batch_path), output_dir)
    feedback = summarize_generated_batch_run(output_dir, "artifacts/llm_plan_feedback.json")

    write_bridge_status(
        "artifacts/llm_status.json",
        {
            "mode": "openclaw_agent_bridge",
            "status": "plan_executed",
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "generated_batch_path": str(batch_path),
            "generated_batch_output_dir": output_dir,
            "feedback_path": "artifacts/llm_plan_feedback.json",
            "feedback_summary": feedback.get("batch_summary", []),
        },
    )
    print("generated batch executed")
