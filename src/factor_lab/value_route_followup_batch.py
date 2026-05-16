from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

from factor_lab.controlled_route_policy import load_controlled_route_policy

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE_DIR = ROOT / "artifacts" / "value_route_bucket_aware"
DEFAULT_OUTPUT_DIR = ROOT / "artifacts" / "value_route_followups"

FOLLOWUP_VARIANTS = ("cost_sensitivity_20bps", "bucket_pair_stricter_tail")


def _load_bucket_config(route_id: str, source_dir: Path = DEFAULT_SOURCE_DIR) -> tuple[Path, dict[str, Any]] | None:
    path = source_dir / f"{route_id}_bucket_aware.json"
    if not path.exists():
        return None
    return path, json.loads(path.read_text(encoding="utf-8"))


def _promoted_routes(route_policy: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    routes = (route_policy or {}).get("routes") or {}
    return sorted(
        (route, row)
        for route, row in routes.items()
        if isinstance(row, dict) and row.get("decision") == "promote"
    )


def _with_followup_metadata(
    cfg: dict[str, Any],
    *,
    route_id: str,
    parent_path: Path,
    policy_row: dict[str, Any],
    followup_type: str,
    expected_new_evidence: str,
    priority_rank: int,
    output_root: str,
) -> dict[str, Any]:
    cfg["route_id"] = route_id
    cfg["source"] = "bucket_aware_promoted_followup"
    cfg["followup_type"] = followup_type
    cfg["expected_new_evidence"] = expected_new_evidence
    cfg["followup_priority_rank"] = priority_rank
    cfg["followup_of"] = {
        "parent_route_id": route_id,
        "parent_config_path": str(parent_path),
        "route_policy_reason": policy_row.get("reason"),
        "route_policy_decision": policy_row.get("decision"),
    }
    cfg["output_dir"] = f"{output_root}/{route_id}__{followup_type}"
    return cfg


def _cost_sensitivity_variant(
    base: dict[str, Any],
    *,
    route_id: str,
    parent_path: Path,
    policy_row: dict[str, Any],
    output_root: str,
) -> dict[str, Any]:
    cfg = deepcopy(base)
    cfg["portfolio_cost_bps_per_turnover"] = 20.0
    return _with_followup_metadata(
        cfg,
        route_id=route_id,
        parent_path=parent_path,
        policy_row=policy_row,
        followup_type="cost_sensitivity_20bps",
        expected_new_evidence="cost_robustness",
        priority_rank=1,
        output_root=output_root,
    )


def _stricter_tail_variant(
    base: dict[str, Any],
    *,
    route_id: str,
    parent_path: Path,
    policy_row: dict[str, Any],
    output_root: str,
) -> dict[str, Any]:
    cfg = deepcopy(base)
    portfolio = dict(cfg.get("portfolio_construction") or {})
    quantiles = int(portfolio.get("quantiles") or 5)
    long_quantile = int(portfolio.get("long_quantile") or 3)
    portfolio["long_quantile"] = min(quantiles - 1, long_quantile + 1)
    cfg["portfolio_construction"] = portfolio
    thresholds = dict(cfg.get("thresholds") or {})
    thresholds["min_bucket_spread"] = max(float(thresholds.get("min_bucket_spread") or 0.0), 0.001)
    cfg["thresholds"] = thresholds
    return _with_followup_metadata(
        cfg,
        route_id=route_id,
        parent_path=parent_path,
        policy_row=policy_row,
        followup_type="bucket_pair_stricter_tail",
        expected_new_evidence="tail_strength",
        priority_rank=2,
        output_root=output_root,
    )


def _followup_variants(
    base: dict[str, Any],
    *,
    route_id: str,
    parent_path: Path,
    policy_row: dict[str, Any],
    output_root: str = "artifacts/value_route_followups/runs",
) -> list[dict[str, Any]]:
    return [
        _cost_sensitivity_variant(base, route_id=route_id, parent_path=parent_path, policy_row=policy_row, output_root=output_root),
        _stricter_tail_variant(base, route_id=route_id, parent_path=parent_path, policy_row=policy_row, output_root=output_root),
    ]


def build_value_route_followup_batch(
    *,
    route_policy: dict[str, Any] | None = None,
    source_dir: str | Path = DEFAULT_SOURCE_DIR,
    output_root: str = "artifacts/value_route_followups/runs",
) -> dict[str, Any]:
    policy = route_policy if route_policy is not None else load_controlled_route_policy()
    configs: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for route, policy_row in _promoted_routes(policy):
        loaded = _load_bucket_config(route, Path(source_dir))
        if loaded is None:
            skipped.append({"route_id": route, "reason": "missing_parent_bucket_aware_config"})
            continue
        parent_path, base = loaded
        configs.extend(
            _followup_variants(
                base,
                route_id=route,
                parent_path=parent_path,
                policy_row=policy_row,
                output_root=output_root,
            )
        )
    configs.sort(key=lambda cfg: (int(cfg.get("followup_priority_rank") or 99), str(cfg.get("route_id") or ""), str(cfg.get("followup_type") or "")))
    return {"schema_version": 1, "configs": configs, "config_count": len(configs), "skipped": skipped}


def write_value_route_followup_batch(
    *,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    dry_run: bool = True,
    route_policy: dict[str, Any] | None = None,
    route_policy_path: str | Path = "artifacts/controlled_route_policy.json",
    source_dir: str | Path = DEFAULT_SOURCE_DIR,
) -> dict[str, Any]:
    policy = route_policy
    if policy is None:
        policy_path = Path(route_policy_path)
        if policy_path.exists():
            policy = json.loads(policy_path.read_text(encoding="utf-8"))
        else:
            policy = load_controlled_route_policy()
    batch = build_value_route_followup_batch(route_policy=policy, source_dir=source_dir)
    out = Path(output_dir)
    result = {**batch, "written": False, "output_dir": str(out)}
    if dry_run:
        return result

    out.mkdir(parents=True, exist_ok=True)
    manifest_configs = []
    for cfg in batch["configs"]:
        path = out / f"{cfg['route_id']}__{cfg['followup_type']}.json"
        path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        saved = dict(cfg)
        saved["config_path"] = str(path)
        manifest_configs.append(saved)
    manifest = {**batch, "configs": manifest_configs, "written": True, "output_dir": str(out)}
    (out / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest
