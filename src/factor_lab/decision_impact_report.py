from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.decision_context_builder import build_failure_decision_context, build_planner_decision_context
from factor_lab.llm_provider_router import DecisionProviderRouter

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"



def _read_json(path: str | Path, default: Any) -> Any:
    path = Path(path)
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default



def _normalized_action_rows(rows: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in list(rows or []):
        normalized.append(
            {
                "type": row.get("type"),
                "target": row.get("target"),
                "reason": row.get("reason"),
            }
        )
    return normalized



def _planner_diff(current: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    fields = ["mode", "task_mix", "priority_families", "suppress_families", "recommended_actions", "challenger_queue"]
    diff: dict[str, Any] = {}
    for field in fields:
        current_value = current.get(field)
        baseline_value = baseline.get(field)
        if field == "recommended_actions":
            current_value = _normalized_action_rows(current_value)
            baseline_value = _normalized_action_rows(baseline_value)
        if current_value != baseline_value:
            diff[field] = {"current": current_value, "heuristic_baseline": baseline_value}
    return diff



def _failure_diff(current: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    fields = ["failure_patterns", "should_stop", "should_probe", "should_reroute"]
    diff: dict[str, Any] = {}
    for field in fields:
        current_value = current.get(field)
        baseline_value = baseline.get(field)
        if current_value != baseline_value:
            diff[field] = {"current": current_value, "heuristic_baseline": baseline_value}
    return diff



def build_decision_impact_report(
    *,
    planner_brief_path: str | Path = ARTIFACTS / "planner_agent_brief.json",
    failure_brief_path: str | Path = ARTIFACTS / "failure_analyst_brief.json",
    agent_responses_path: str | Path = ARTIFACTS / "agent_responses.json",
    output_path: str | Path = ARTIFACTS / "decision_impact_report.json",
) -> dict[str, Any]:
    planner_brief = _read_json(planner_brief_path, {})
    failure_brief = _read_json(failure_brief_path, {})
    current = _read_json(agent_responses_path, {})

    heuristic_router = DecisionProviderRouter(provider="heuristic")
    planner_context = build_planner_decision_context(planner_brief) if planner_brief else {}
    failure_context = build_failure_decision_context(failure_brief) if failure_brief else {}
    heuristic_planner = heuristic_router.generate("planner", planner_context) if planner_context else {}
    heuristic_failure = heuristic_router.generate("failure_analyst", failure_context) if failure_context else {}

    planner_current = current.get("planner") or {}
    failure_current = current.get("failure_analyst") or {}
    planner_source = (((planner_current.get("decision_metadata") or {}).get("source")) or planner_current.get("decision_source"))
    failure_source = (((failure_current.get("decision_metadata") or {}).get("source")) or failure_current.get("decision_source"))

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "planner": {
            "current_source": planner_source,
            "heuristic_baseline_source": "heuristic",
            "diff": _planner_diff(planner_current, heuristic_planner),
            "changed": bool(_planner_diff(planner_current, heuristic_planner)),
        },
        "failure_analyst": {
            "current_source": failure_source,
            "heuristic_baseline_source": "heuristic",
            "diff": _failure_diff(failure_current, heuristic_failure),
            "changed": bool(_failure_diff(failure_current, heuristic_failure)),
        },
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload
