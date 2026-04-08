from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

from factor_lab.factors import resolve_factor_definitions


def load_base_config(config_path: str | Path) -> dict[str, Any]:
    path = Path(config_path)
    config = json.loads(path.read_text(encoding="utf-8"))
    factor_defs = resolve_factor_definitions(config, config_dir=path.resolve().parent)
    if factor_defs:
        config["factors"] = factor_defs
        config.pop("factor_family_config", None)
    return config


def generate_batch_from_plan(
    plan: dict[str, Any],
    base_config_path: str | Path,
    output_path: str | Path,
) -> dict[str, Any]:
    base = load_base_config(base_config_path)
    factor_map = {item["name"]: item for item in base.get("factors", [])}

    selected_names = [name for name in plan.get("focus_factors", []) if name in factor_map]
    selected_factors = [deepcopy(factor_map[name]) for name in selected_names]

    focused_config = deepcopy(base)
    focused_config["factors"] = selected_factors
    focused_config["output_dir"] = "artifacts/generated_from_llm"

    focused_config_path = Path(output_path).with_name("generated_workflow_from_llm.json")
    focused_config_path.write_text(json.dumps(focused_config, ensure_ascii=False, indent=2), encoding="utf-8")

    batch = {
        "source": "llm_plan",
        "summary": plan.get("rationale", ""),
        "jobs": [
            {
                "name": "llm_focus_batch",
                "config_path": str(focused_config_path),
            }
        ],
    }
    Path(output_path).write_text(json.dumps(batch, ensure_ascii=False, indent=2), encoding="utf-8")
    return batch
