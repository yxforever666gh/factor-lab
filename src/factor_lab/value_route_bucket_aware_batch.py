from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from factor_lab.value_route_batch import build_value_route_batch

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = ROOT / "artifacts" / "value_route_bucket_aware"

BUCKET_PAIRS = {
    "industry_relative_value": {"long_quantile": 3, "short_quantile": 0},
    "value_quality_no_distress": {"long_quantile": 3, "short_quantile": 0},
    "value_momentum_confirmation": {"long_quantile": 3, "short_quantile": 1},
}


def _representative_by_route() -> dict[str, dict[str, Any]]:
    base = build_value_route_batch()
    reps: dict[str, dict[str, Any]] = {}
    for cfg in base["configs"]:
        route = str(cfg.get("route_id") or "")
        if route in BUCKET_PAIRS and route not in reps:
            reps[route] = cfg
    return reps


def _bucket_config(base: dict[str, Any], *, output_root: str = "artifacts/value_route_bucket_aware/runs") -> dict[str, Any]:
    route = str(base["route_id"])
    cfg = json.loads(json.dumps(base))
    pair = BUCKET_PAIRS[route]
    cfg["portfolio_construction"] = {"mode": "bucket_pair", "quantiles": 5, **pair}
    cfg["thresholds"] = {**(cfg.get("thresholds") or {}), "min_bucket_spread": 0.001}
    cfg["bucket_aware_route"] = True
    cfg["output_dir"] = f"{output_root}/{route}_bucket_aware"
    return cfg


def build_value_route_bucket_aware_batch(*, include_all_routes: bool = False) -> dict[str, Any]:
    reps = _representative_by_route()
    routes = ["value_quality_no_distress"] if not include_all_routes else sorted(BUCKET_PAIRS)
    configs = [_bucket_config(reps[route]) for route in routes if route in reps]
    return {"schema_version": 1, "configs": configs, "include_all_routes": include_all_routes}


def write_value_route_bucket_aware_batch(*, output_dir: str | Path = DEFAULT_OUTPUT_DIR, dry_run: bool = True, include_all_routes: bool = False) -> dict[str, Any]:
    batch = build_value_route_bucket_aware_batch(include_all_routes=include_all_routes)
    if not dry_run:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        manifest_configs = []
        for cfg in batch["configs"]:
            path = out / f"{cfg['route_id']}_bucket_aware.json"
            path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
            saved = dict(cfg)
            saved["config_path"] = str(path)
            manifest_configs.append(saved)
        manifest = {**batch, "configs": manifest_configs}
        (out / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        return manifest
    return batch
