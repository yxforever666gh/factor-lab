from __future__ import annotations

import json
from pathlib import Path
from typing import Any

FORBIDDEN_REQUEST_FIELDS = {
    "agent",
    "agent_id",
    "agent_role",
    "legacy_agent_id",
    "llm_fallback_order",
    "model",
    "provider",
    "base_url",
    "api_key",
    "profile",
}


def load_worker_config(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if payload.get("runtime_binding") != "hermes_cli_one_shot":
        raise ValueError("autonomous strategy workers must use hermes_cli_one_shot runtime_binding")
    return payload


def build_worker_requests(
    config: dict[str, Any],
    *,
    run_id: str,
    output_dir: str | Path,
) -> list[dict[str, Any]]:
    base = Path(output_dir)
    requests: list[dict[str, Any]] = []
    global_forbidden = list(config.get("global_forbidden_actions") or [])
    for worker in config.get("workers") or []:
        worker_key = str(worker["worker_key"])
        worker_dir = base / run_id
        output_artifact_path = worker_dir / f"{worker_key}_response.json"
        request_path = worker_dir / f"{worker_key}_request.json"
        prompt_path = worker_dir / f"{worker_key}_prompt.txt"
        request = {
            "schema_version": 1,
            "run_id": run_id,
            "runtime_binding": config["runtime_binding"],
            "model_provider_policy": config.get("model_provider_policy"),
            "factor_lab_authority": config.get("factor_lab_authority"),
            "worker_key": worker_key,
            "purpose": worker.get("purpose"),
            "toolsets": list(worker.get("toolsets") or []),
            "skills": list(worker.get("skills") or []),
            "input_artifacts": list(worker.get("input_artifacts") or []),
            "output_artifact_namespace": worker.get("output_artifact_namespace"),
            "output_artifact_path": str(output_artifact_path),
            "request_path": str(request_path),
            "prompt_path": str(prompt_path),
            "forbidden_actions": _dedupe(global_forbidden + list(worker.get("forbidden_actions") or [])),
            "verification_after": list(worker.get("verification_after") or []),
            "expected_response_schema": {
                "schema_version": 1,
                "worker_key": worker_key,
                "decision_recommendation": "request_data|manual_review|switch_mechanism_route|repair_portfolio_construction|stop_route|continue_route_with_constraints",
                "reason_codes": [],
                "requested_actions": [],
                "forbidden_actions_observed": [],
                "summary": "string",
            },
        }
        _assert_no_forbidden_request_fields(request)
        requests.append(request)
    return requests


def build_worker_prompt(request: dict[str, Any]) -> str:
    input_lines = "\n".join(f"- {path}" for path in request.get("input_artifacts") or []) or "- none"
    forbidden_lines = "\n".join(f"- {action}" for action in request.get("forbidden_actions") or [])
    verification_lines = "\n".join(f"- {step}" for step in request.get("verification_after") or [])
    schema_json = json.dumps(request.get("expected_response_schema") or {}, ensure_ascii=False, indent=2)
    return f"""You are a Hermes CLI one-shot worker for Factor Lab.

Worker key: {request['worker_key']}
Purpose: {request.get('purpose')}
Run id: {request['run_id']}

Input artifacts:
{input_lines}

Output artifact path:
{request['output_artifact_path']}

Forbidden actions:
{forbidden_lines}

Verification after completion:
{verification_lines}

Rules:
- Read the input artifacts before reasoning.
- Write exactly one JSON response artifact to the output artifact path.
- Do not write research queues, enable timers, change systemd, restore broad daemons, auto-promote, relax drawdown limits, or touch live trading.
- Do not pass or change model/provider/profile settings.
- Do not create persistent local Hermes agents.
- If evidence is insufficient, recommend request_data or manual_review instead of inventing metrics.
- Return only after writing the response artifact.

Expected JSON response schema:
```json
{schema_json}
```
"""


def write_worker_requests(requests: list[dict[str, Any]]) -> list[dict[str, Any]]:
    written: list[dict[str, Any]] = []
    for request in requests:
        request_path = Path(request["request_path"])
        prompt_path = Path(request["prompt_path"])
        request_path.parent.mkdir(parents=True, exist_ok=True)
        request_path.write_text(json.dumps(request, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        prompt_path.write_text(build_worker_prompt(request), encoding="utf-8")
        written.append({
            "worker_key": request["worker_key"],
            "request_path": request_path,
            "prompt_path": prompt_path,
            "output_artifact_path": Path(request["output_artifact_path"]),
        })
    return written


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def _assert_no_forbidden_request_fields(payload: dict[str, Any]) -> None:
    bad = set(payload) & FORBIDDEN_REQUEST_FIELDS
    if bad:
        raise ValueError(f"worker request contains forbidden fields: {sorted(bad)}")
