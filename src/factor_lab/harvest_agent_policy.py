from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = ROOT / "configs/harvest_agent_policy.json"

ALLOWED_MAINLINES = [
    "bucket_aware_oos_followup",
    "defensive_quality_risk_layer",
    "mechanism_data_gap_analysis",
    "direction_sanity_diagnostics",
]

DEFAULT_HARVEST_AGENT_POLICY: dict[str, Any] = {
    "schema_version": 1,
    "enabled": True,
    "mode": "dry_run_first",
    "max_experiments_per_cycle": 2,
    "max_cycles_per_day": 4,
    "cooldown_minutes": 180,
    "allowed_mainlines": ALLOWED_MAINLINES,
    "manual_approval_required_for": [
        "enable_timer",
        "increase_budget",
        "external_data_source",
        "paper_portfolio_promotion",
        "broad_daemon_restore",
        "live_trading",
    ],
    "hard_gates": {
        "require_oos": True,
        "require_cost_adjustment": True,
        "require_duplicate_check": True,
        "require_mechanism_id": True,
        "require_falsification_criteria": True,
    },
    "promotion_thresholds": {
        "min_rank_ic_mean": 0.02,
        "min_rank_ic_ir": 0.15,
        "min_sharpe_net": 0.5,
        "max_drawdown_floor": -0.35,
        "min_coverage": 0.8,
    },
    "live_trading_enabled": False,
    "broad_daemon_restore_allowed": False,
}


def _merge(base: dict[str, Any], overrides: dict[str, Any] | None) -> dict[str, Any]:
    result = copy.deepcopy(base)
    for key, value in (overrides or {}).items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _merge(result[key], value)
        else:
            result[key] = value
    return result


def load_harvest_agent_policy(path: str | Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    p = Path(path)
    overrides: dict[str, Any] = {}
    if p.exists():
        overrides = json.loads(p.read_text(encoding="utf-8"))
    cfg = _merge(DEFAULT_HARVEST_AGENT_POLICY, overrides)
    if cfg.get("live_trading_enabled") or cfg.get("live_trading"):
        raise ValueError("live_trading is forbidden for Harvest Agent")
    if cfg.get("broad_daemon_restore_allowed"):
        raise ValueError("broad_daemon_restore is forbidden for Harvest Agent")
    cfg["live_trading_enabled"] = False
    cfg["broad_daemon_restore_allowed"] = False
    cfg["max_experiments_per_cycle"] = min(2, int(cfg.get("max_experiments_per_cycle") or 2))
    cfg["max_cycles_per_day"] = min(4, int(cfg.get("max_cycles_per_day") or 4))
    cfg["cooldown_minutes"] = max(180, int(cfg.get("cooldown_minutes") or 180))
    for mainline in cfg.get("allowed_mainlines", []):
        if mainline not in ALLOWED_MAINLINES:
            raise ValueError(f"unsupported_mainline: {mainline}")
    return cfg


def validate_mainline(mainline: str, policy: dict[str, Any] | None = None) -> bool:
    cfg = policy or DEFAULT_HARVEST_AGENT_POLICY
    if mainline not in cfg.get("allowed_mainlines", []):
        raise ValueError(f"unsupported_mainline: {mainline}")
    return True
