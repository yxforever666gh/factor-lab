from pathlib import Path
import sys
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.llm_retrospective import build_retrospective, retrospective_markdown
from factor_lab.llm_bridge import write_bridge_status


if __name__ == "__main__":
    retro = build_retrospective(
        plan_path="artifacts/llm_next_batch_proposal.json",
        feedback_path="artifacts/llm_plan_feedback.json",
        output_path="artifacts/llm_retrospective.json",
    )
    Path("artifacts/llm_retrospective.md").write_text(retrospective_markdown(retro), encoding="utf-8")

    write_bridge_status(
        "artifacts/llm_status.json",
        {
            "mode": "openclaw_agent_bridge",
            "status": "retrospective_ready",
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "feedback_path": "artifacts/llm_plan_feedback.json",
            "retrospective_path": "artifacts/llm_retrospective.json",
            "retrospective": retro,
        },
    )
    print("llm retrospective built")
