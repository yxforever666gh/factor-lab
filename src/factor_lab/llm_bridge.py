from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REQUEST_SCHEMA_VERSION = "factor_lab.llm_bridge.v1"
REQUIRED_RESPONSE_KEYS = {"agent_name", "generated_at_utc", "review_markdown", "next_batch_proposal"}
REQUIRED_PLAN_KEYS = {"focus_factors", "keep_as_core_candidates", "review_graveyard", "portfolio_checks", "rationale"}
OPTIONAL_PLAN_KEYS = {"novelty_reason"}


def build_agent_request(snapshot: dict[str, Any], output_path: str | Path) -> dict[str, Any]:
    payload = {
        "schema_version": REQUEST_SCHEMA_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "agent_role": "llm_analyst",
        "tasks": ["review", "plan"],
        "snapshot": snapshot,
        "instructions": {
            "review_format": "markdown",
            "review_required_sections": ["本轮核心结论", "候选池解读", "墓地解读", "组合层观察", "下一轮建议"],
            "plan_format": "json",
            "plan_required_keys": sorted(REQUIRED_PLAN_KEYS),
            "plan_optional_keys": sorted(OPTIONAL_PLAN_KEYS),
            "must_ground_on_snapshot": True,
            "must_not_override_core_metrics": True,
            "planning_hint": "在生成下一轮建议时，参考 snapshot.recommendation_weights、snapshot.recommendation_history_tail、snapshot.recommendation_context、snapshot.paper_portfolio_stability 与 snapshot.conservative_policy；优先考虑历史 decayed_effect_score 更高、recommended_action 更积极、且 fatigue_level 更低的建议模板；对 fatigue_level 高或 cooldown_active=true 的模板，除非出现新信息，否则避免连续重复；若仍继续提该模板，必须提供 novelty_reason；如果 conservative_policy.enabled=true，应优先选择稳定候选、减少 focus_factors、减少墓地复核，并降低新模板尝试。",
        },
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def validate_agent_response(response: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    missing_top = REQUIRED_RESPONSE_KEYS - set(response.keys())
    if missing_top:
        errors.append(f"缺少顶层字段: {', '.join(sorted(missing_top))}")

    review = response.get("review_markdown")
    if not isinstance(review, str) or not review.strip():
        errors.append("review_markdown 不能为空字符串")
    else:
        for section in ["本轮核心结论", "候选池解读", "墓地解读", "组合层观察", "下一轮建议"]:
            if section not in review:
                errors.append(f"review_markdown 缺少章节: {section}")

    plan = response.get("next_batch_proposal")
    if not isinstance(plan, dict):
        errors.append("next_batch_proposal 必须是对象")
    else:
        missing_plan = REQUIRED_PLAN_KEYS - set(plan.keys())
        if missing_plan:
            errors.append(f"next_batch_proposal 缺少字段: {', '.join(sorted(missing_plan))}")

    return errors


def import_agent_response(
    response_path: str | Path,
    review_output_path: str | Path,
    plan_output_path: str | Path,
    status_output_path: str | Path,
) -> dict[str, Any]:
    response = json.loads(Path(response_path).read_text(encoding="utf-8"))
    errors = validate_agent_response(response)
    if errors:
        status = {
            "mode": "openclaw_agent_bridge",
            "status": "validation_failed",
            "response_path": str(response_path),
            "validated_at_utc": datetime.now(timezone.utc).isoformat(),
            "errors": errors,
            "agent_name": response.get("agent_name", "unknown"),
        }
        Path(status_output_path).write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
        raise ValueError("; ".join(errors))

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
        "validation": "passed",
    }
    Path(status_output_path).write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    return response


def write_bridge_status(status_output_path: str | Path, payload: dict[str, Any]) -> None:
    Path(status_output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
