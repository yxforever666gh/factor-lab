from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.hermes_contracts import common_output_contract
from factor_lab.hermes_router import HermesRoute


def write_hermes_briefing(route: HermesRoute, *, task: str, context: dict[str, Any], workdir: str = "/home/admin/factor-lab", allowed_actions: list[str] | None = None) -> dict[str, Any]:
    payload = {
        "request_id": route.request_id,
        "profile_key": route.profile_key,
        "profile_name": route.profile_name,
        "workdir": workdir,
        "task": task,
        "context": context,
        "important_paths": [],
        "output_contract": common_output_contract(route.profile_key, route.request_id),
        "allowed_actions": list(allowed_actions or []),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    route.briefing_path.parent.mkdir(parents=True, exist_ok=True)
    route.briefing_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def build_hermes_prompt(route: HermesRoute, *, workdir: str = "/home/admin/factor-lab") -> str:
    return (
        f"You are the Factor Lab Hermes profile: {route.profile_name}.\n"
        f"Work in {workdir}.\n"
        f"Read this briefing if needed: {route.briefing_path}.\n"
        "Return one JSON object matching the contract. No markdown. No extra text.\n"
        f"The JSON must include request_id={route.request_id} and profile_key={route.profile_key}."
    )
