from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any


def build_worker_verdict(worker_dir: str | Path, *, run_id: str) -> dict[str, Any]:
    base = Path(worker_dir)
    responses: list[dict[str, Any]] = []
    invalid: list[dict[str, Any]] = []
    for path in sorted(base.glob("*_response.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            invalid.append({"path": str(path), "error": str(exc)})
            continue
        required = {"schema_version", "worker_key", "decision_recommendation", "reason_codes", "requested_actions", "forbidden_actions_observed", "summary"}
        missing = sorted(required - set(payload))
        if missing:
            invalid.append({"path": str(path), "error": "missing_fields", "missing": missing})
            continue
        responses.append({"path": str(path), **payload})

    decisions = Counter(str(row.get("decision_recommendation")) for row in responses)
    consensus_decision = decisions.most_common(1)[0][0] if decisions else "manual_review"
    reason_codes = sorted({str(code) for row in responses for code in (row.get("reason_codes") or [])})
    requested_actions = sorted({str(action) for row in responses for action in (row.get("requested_actions") or [])})
    forbidden_observed = sorted({str(action) for row in responses for action in (row.get("forbidden_actions_observed") or [])})
    controlled_allowed = consensus_decision == "continue_route_with_constraints" and not forbidden_observed

    return {
        "schema_version": 1,
        "run_id": run_id,
        "worker_dir": str(base),
        "worker_count": len(list(base.glob("*_response.json"))),
        "valid_response_count": len(responses),
        "invalid_responses": invalid,
        "decision_counts": dict(decisions),
        "consensus_decision": consensus_decision,
        "reason_codes": reason_codes,
        "requested_actions": requested_actions,
        "forbidden_actions_observed": forbidden_observed,
        "controlled_execution_allowed": controlled_allowed,
        "queue_write_allowed": False,
        "automation_allowed": False,
        "live_trading_enabled": False,
        "worker_summaries": [
            {
                "worker_key": row.get("worker_key"),
                "decision_recommendation": row.get("decision_recommendation"),
                "summary": row.get("summary"),
                "path": row.get("path"),
            }
            for row in responses
        ],
    }


def worker_verdict_to_markdown(verdict: dict[str, Any]) -> str:
    lines = [
        "# Autonomous Strategy Worker Verdict",
        "",
        f"run_id: {verdict.get('run_id')}",
        f"consensus_decision: {verdict.get('consensus_decision')}",
        f"valid_response_count: {verdict.get('valid_response_count')}",
        f"controlled_execution_allowed: {verdict.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {verdict.get('queue_write_allowed')}",
        "",
        "## Reason codes",
    ]
    lines.extend(f"- {code}" for code in verdict.get("reason_codes") or [])
    lines.extend(["", "## Requested actions"])
    lines.extend(f"- {action}" for action in verdict.get("requested_actions") or [])
    lines.extend(["", "## Worker summaries"])
    for row in verdict.get("worker_summaries") or []:
        lines.extend([
            f"### {row.get('worker_key')}",
            f"- decision: {row.get('decision_recommendation')}",
            f"- summary: {row.get('summary')}",
            "",
        ])
    return "\n".join(lines).rstrip() + "\n"


def write_worker_verdict(verdict: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "worker_verdict.json"
    md_path = out / "worker_verdict.md"
    json_path.write_text(json.dumps(verdict, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(worker_verdict_to_markdown(verdict), encoding="utf-8")
    return {"json": json_path, "markdown": md_path}
