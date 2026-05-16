from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from factor_lab.validation_protocols import build_validation_matrix
from factor_lab.value_factor_templates import build_value_route_candidates

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = ROOT / "artifacts" / "value_route_batches"


def build_value_route_batch(
    *,
    available_fields: Iterable[str] | None = None,
    allowed_routes: Iterable[str] | None = None,
    protocol_name: str = "value_factor_default",
) -> dict[str, Any]:
    allowed = set(allowed_routes or ["industry_relative_value", "value_quality_no_distress", "value_momentum_confirmation"])
    candidates = build_value_route_candidates(available_fields=available_fields)
    configs: list[dict[str, Any]] = []
    blocked_by_route: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        route_id = candidate.get("route_id")
        if route_id not in allowed:
            continue
        if candidate.get("status") != "ready":
            blocked_by_route[str(route_id)] = candidate
            continue
        factor = {"name": str(candidate.get("name") or route_id), "expression": str(candidate.get("expression") or "")}
        matrix = build_validation_matrix(factor=factor, protocol_name=protocol_name)
        for run in matrix["runs"]:
            window = run["window_name"]
            horizon = run["horizon"]
            universe = run["universe_limit"]
            expression_slug = factor["name"].replace("::", "_").replace(" ", "_").replace("+", "plus")
            configs.append(
                {
                    "data_source": "tushare",
                    "cache_dir": "artifacts/tushare_cache",
                    "route_id": route_id,
                    "mechanism_id": candidate.get("mechanism_id"),
                    "hypothesis": candidate.get("hypothesis"),
                    "required_data_fields": candidate.get("required_data_fields") or [],
                    "falsification_criteria": candidate.get("falsification_criteria") or [],
                    "validation_protocol_name": protocol_name,
                    "window_name": window,
                    "horizon": horizon,
                    "universe_limit": universe,
                    "rolling_validation": {"window_size": 63, "step_size": 21, "min_pass_rate": 0.5, "max_sign_flips": 2, "min_rank_ic": 0.01},
                    "thresholds": {"min_rank_ic": 0.02, "min_top_bottom_spread": 0.0005},
                    "start_date": run["start_date"],
                    "end_date": run["end_date"],
                    "promotion_eligible": run["promotion_eligible"],
                    "factors": [factor],
                    "output_dir": f"artifacts/value_route_validation/{route_id}_{window}_{horizon}_u{universe}_{expression_slug}",
                }
            )
    return {"schema_version": 1, "protocol_name": protocol_name, "allowed_routes": sorted(allowed), "configs": configs, "blocked": list(blocked_by_route.values())}


def write_value_route_batch(
    *,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    available_fields: Iterable[str] | None = None,
    allowed_routes: Iterable[str] | None = None,
    dry_run: bool = True,
) -> dict[str, Any]:
    batch = build_value_route_batch(available_fields=available_fields, allowed_routes=allowed_routes)
    if not dry_run:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        (out / "batch_manifest.json").write_text(json.dumps(batch, ensure_ascii=False, indent=2), encoding="utf-8")
        for idx, cfg in enumerate(batch["configs"]):
            (out / f"value_route_{idx:03d}_{cfg['route_id']}_{cfg['window_name']}_{cfg['horizon']}.json").write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    return batch
