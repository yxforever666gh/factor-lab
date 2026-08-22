from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from factor_lab.value_route_batch import build_value_route_batch

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = ROOT / "artifacts" / "value_route_direction_batch"
DEFAULT_RUN_OUTPUT_ROOT = "artifacts/value_route_direction_diagnostics/runs"


def _select_representative_configs(configs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    preference = {"train": 0, "validation": 1, "test": 2, "recent_monitor": 3}
    for cfg in configs:
        route = str(cfg.get("route_id") or "")
        if not route or route in selected:
            continue
        selected[route] = cfg
    return [selected[key] for key in sorted(selected)]


def _direction_config(base: dict[str, Any], direction: str) -> dict[str, Any]:
    cfg = json.loads(json.dumps(base))
    factor = dict(cfg["factors"][0])
    original_expression = str(factor.get("expression") or "")
    original_name = str(factor.get("name") or cfg.get("route_id"))
    if direction == "inverted":
        factor["name"] = f"{original_name}__inverted"
        factor["expression"] = f"-({original_expression})"
    else:
        factor["name"] = f"{original_name}__original"
        factor["expression"] = original_expression
    cfg["factors"] = [factor]
    cfg["direction"] = direction
    cfg["direction_pair_id"] = str(cfg.get("route_id"))
    cfg["output_dir"] = f"{DEFAULT_RUN_OUTPUT_ROOT}/{cfg['route_id']}_{direction}"
    cfg["write_dataset_csv"] = False
    return cfg


def build_value_route_direction_batch() -> dict[str, Any]:
    base_batch = build_value_route_batch()
    selected = _select_representative_configs(base_batch["configs"])
    configs: list[dict[str, Any]] = []
    for base in selected:
        configs.append(_direction_config(base, "original"))
        configs.append(_direction_config(base, "inverted"))
    return {"schema_version": 1, "source_protocol": base_batch.get("protocol_name"), "configs": configs}


def write_value_route_direction_batch(*, output_dir: str | Path = DEFAULT_OUTPUT_DIR, dry_run: bool = True) -> dict[str, Any]:
    batch = build_value_route_direction_batch()
    if not dry_run:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        manifest_configs = []
        for cfg in batch["configs"]:
            name = f"{cfg['route_id']}_{cfg['direction']}.json"
            path = out / name
            # ASCII-safe JSON remains readable by callers that (incorrectly but
            # commonly) rely on the Windows locale default encoding.
            path.write_text(json.dumps(cfg, ensure_ascii=True, indent=2), encoding="utf-8")
            cfg_with_path = dict(cfg)
            cfg_with_path["config_path"] = path.as_posix()
            manifest_configs.append(cfg_with_path)
        manifest = {**batch, "configs": manifest_configs}
        (out / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=True, indent=2), encoding="utf-8")
        return manifest
    return batch
