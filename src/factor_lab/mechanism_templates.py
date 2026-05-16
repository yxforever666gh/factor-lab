from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TEMPLATE_PATH = ROOT / "configs" / "research_mechanism_templates.json"


def load_mechanism_templates(path: str | Path = DEFAULT_TEMPLATE_PATH) -> dict[str, dict[str, Any]]:
    path = Path(path)
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    templates = payload.get("templates") if isinstance(payload, dict) else []
    registry: dict[str, dict[str, Any]] = {}
    for template in templates or []:
        if not isinstance(template, dict):
            continue
        mechanism_id = str(template.get("mechanism_id") or template.get("template_id") or "").strip()
        if mechanism_id:
            normalized = dict(template)
            normalized["mechanism_id"] = mechanism_id
            registry[mechanism_id] = normalized
    return registry


def apply_mechanism_template(
    proposal: dict[str, Any],
    *,
    template_id: str,
    registry: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    registry = registry if registry is not None else load_mechanism_templates()
    template = registry.get(template_id)
    if not template:
        raise KeyError(f"unknown mechanism template: {template_id}")

    enriched = deepcopy(proposal)
    enriched["mechanism_id"] = template["mechanism_id"]
    enriched.setdefault("mechanism_label", template.get("label") or template["mechanism_id"])
    enriched.setdefault("target_family", template.get("target_family"))
    enriched.setdefault("hypothesis", template.get("hypothesis_template") or template.get("hypothesis") or "")
    enriched.setdefault("required_data_fields", list(template.get("required_data_fields") or []))
    enriched.setdefault("expected_regime", template.get("expected_regime"))
    enriched.setdefault("expected_information_gain", list(template.get("expected_information_gain") or []))
    enriched.setdefault("falsification_criteria", list(template.get("falsification_criteria") or []))
    enriched.setdefault("budget_bucket", template.get("budget_bucket") or "mechanism_validation")
    return enriched


def select_mechanism_template_for_family(target_family: str | None, *, registry: dict[str, dict[str, Any]] | None = None) -> dict[str, Any] | None:
    registry = registry if registry is not None else load_mechanism_templates()
    family = str(target_family or "").strip().lower()
    for template in registry.values():
        if str(template.get("target_family") or "").strip().lower() == family:
            return template
    return None
