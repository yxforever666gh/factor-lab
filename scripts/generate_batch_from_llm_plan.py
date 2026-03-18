from pathlib import Path
import json
import sys
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.plan_validator import validate_plan_file
from factor_lab.proposal_to_batch import generate_batch_from_plan
from factor_lab.llm_bridge import write_bridge_status


if __name__ == "__main__":
    allowed = {item["name"] for item in json.loads(Path("configs/tushare_workflow.json").read_text(encoding="utf-8"))["factors"]}
    result = validate_plan_file("artifacts/llm_next_batch_proposal.json", allowed)

    status_payload = {
        "mode": "openclaw_agent_bridge",
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "plan_validation": result,
    }

    if not result["valid"]:
        status_payload["status"] = "plan_validation_failed"
        write_bridge_status("artifacts/llm_status.json", status_payload)
        print("plan validation failed")
        raise SystemExit(2)

    batch = generate_batch_from_plan(
        result["normalized_plan"],
        base_config_path="configs/tushare_workflow.json",
        output_path="artifacts/generated_batch_from_llm.json",
    )
    status_payload["status"] = "plan_ready"
    status_payload["generated_batch_path"] = "artifacts/generated_batch_from_llm.json"
    write_bridge_status("artifacts/llm_status.json", status_payload)
    print("generated batch from llm plan")
