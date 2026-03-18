from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def build_agent_request(snapshot: dict[str, Any], output_path: str | Path) -> dict[str, Any]:
    payload = {
        "schema_version": "factor_lab.llm_bridge.v1",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "agent_role": "llm_analyst",
        "tasks": ["review", "plan"],
        "snapshot": snapshot,
        "instructions": {
            "review_format": "markdown",
            "plan_format": "json",
            "must_ground_on_snapshot": True,
            "must_not_override_core_metrics": True,
        },
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def import_agent_response(
    response_path: str | Path,
    review_output_path: str | Path,
    plan_output_path: str | Path,
    status_output_path: str | Path,
) -> dict[str, Any]:
    response = json.loads(Path(response_path).read_text(encoding="utf-8"))
    review = response.get("review_markdown", "")
    plan = response.get("next_batch_proposal", {})
    Path(review_output_path).write_text(review, encoding="utf-8")
    Path(plan_output_path).write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")
    status = {
        "mode": "openclaw_agent_bridge",
        "status": "imported",
        "response_path": str(response_path),
        "imported_at_utc": datetime.now(timezone.utc).isoformat(),
        "agent_name": response.get("agent_name", "unknown"),
    }
    Path(status_output_path).write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    return response


def write_bridge_status(status_output_path: str | Path, payload: dict[str, Any]) -> None:
    Path(status_output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
