from __future__ import annotations

import json
import os
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from factor_lab.factors import resolve_factor_definitions
from factor_lab.generated_artifacts import upgrade_generated_batch, upgrade_generated_config

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"
GENERATED_CONFIG_DIR = ARTIFACTS / "generated_opportunity_configs"
GENERATED_BATCH_DIR = ARTIFACTS / "generated_opportunity_batches"
BASE_WORKFLOW_PATH = ROOT / "configs" / "tushare_workflow.json"


def _fingerprint_for_opportunity(opportunity: dict[str, Any]) -> str:
    return f"opportunity::{opportunity.get('opportunity_id')}::{opportunity.get('opportunity_type')}::{json.dumps(opportunity.get('target_candidates') or [], ensure_ascii=False)}"


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_date(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%d")


def _fmt_date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def _sanitize_name(text: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in text)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = int(raw)
    except Exception:
        value = default
    return max(minimum, value)


def _light_cheap_screen_config(config: dict[str, Any]) -> dict[str, Any]:
    tuned = deepcopy(config)
    original_universe_limit = int(tuned.get("universe_limit") or 100)
    tuned["universe_limit"] = min(
        original_universe_limit,
        _env_int("RESEARCH_OPPORTUNITY_LIGHT_UNIVERSE_LIMIT", 30, minimum=10),
    )
    if original_universe_limit > int(tuned.get("universe_limit") or 0):
        tuned["fallback_universe_limit"] = original_universe_limit
    rolling = dict(tuned.get("rolling_validation") or {})
    if rolling:
        rolling["window_size"] = min(
            int(rolling.get("window_size") or 63),
            _env_int("RESEARCH_OPPORTUNITY_LIGHT_ROLLING_WINDOW", 21, minimum=7),
        )
        rolling["step_size"] = min(
            int(rolling.get("step_size") or 21),
            _env_int("RESEARCH_OPPORTUNITY_LIGHT_ROLLING_STEP", 7, minimum=1),
        )
        tuned["rolling_validation"] = rolling
    tuned["validation_mode"] = "light_recent_window"
    tuned["research_profile"] = "opportunity_cheap_screen"
    tuned["refresh_global_risk"] = False
    tuned["refresh_exposure_track"] = False
    return tuned


def _base_workflow_config() -> dict[str, Any]:
    config = _load_json(BASE_WORKFLOW_PATH)
    factor_defs = resolve_factor_definitions(config, config_dir=BASE_WORKFLOW_PATH.resolve().parent)
    if factor_defs:
        config["factors"] = factor_defs
        config.pop("factor_family_config", None)
    return config


def _select_factor_defs(target_candidates: list[str]) -> list[dict[str, Any]]:
    base = _base_workflow_config()
    factor_map = {row["name"]: row for row in base.get("factors", [])}
    selected = [deepcopy(factor_map[name]) for name in target_candidates if name in factor_map]
    if selected:
        return selected
    return [deepcopy(row) for row in base.get("factors", [])[:2]]


def _make_workflow_config(
    opportunity: dict[str, Any],
    *,
    suffix: str,
    start_date: str,
    end_date: str,
    factors: list[dict[str, Any]],
    execution_mode: str,
) -> Path:
    base = _base_workflow_config()
    oid = _sanitize_name(opportunity.get("opportunity_id") or "opportunity")
    config = deepcopy(base)
    config["start_date"] = start_date
    config["end_date"] = end_date
    config["factors"] = deepcopy(factors)
    config["output_dir"] = f"artifacts/opportunity_runs/{oid}/{suffix}"
    config["opportunity_context"] = {
        "opportunity_id": opportunity.get("opportunity_id"),
        "opportunity_type": opportunity.get("opportunity_type"),
        "target_family": opportunity.get("target_family"),
        "target_candidates": list(opportunity.get("target_candidates") or []),
        "question": opportunity.get("question"),
    }
    if execution_mode == "cheap_screen":
        config = _light_cheap_screen_config(config)
    config = upgrade_generated_config(config, source="opportunity_to_tasks")
    path = GENERATED_CONFIG_DIR / f"{oid}__{suffix}.json"
    return _write_json(path, config)


def _is_budget_risky_probe(opportunity: dict[str, Any]) -> bool:
    return (
        (opportunity.get("opportunity_type") == "probe")
        and ((opportunity.get("execution_mode") or "cheap_screen").strip() == "cheap_screen")
        and not list(opportunity.get("target_candidates") or [])
    )


def _build_opportunity_batch(opportunity: dict[str, Any]) -> Path | None:
    otype = opportunity.get("opportunity_type")
    oid = _sanitize_name(opportunity.get("opportunity_id") or "opportunity")
    base = _base_workflow_config()
    end_date = base.get("end_date")
    start_date = base.get("start_date")
    if not start_date or not end_date:
        return None

    end_dt = _parse_date(end_date)
    start_dt = _parse_date(start_date)
    factors = _select_factor_defs(list(opportunity.get("target_candidates") or []))
    jobs: list[dict[str, Any]] = []
    execution_mode = (opportunity.get("execution_mode") or "cheap_screen").strip()

    if otype == "expand":
        jobs.append({"name": "recent_45d", "config_path": str(_make_workflow_config(opportunity, suffix="recent_45d", start_date=_fmt_date(end_dt - timedelta(days=45)), end_date=end_date, factors=factors, execution_mode=execution_mode).relative_to(ROOT))})
        if execution_mode == "full":
            jobs.append({"name": "recent_90d", "config_path": str(_make_workflow_config(opportunity, suffix="recent_90d", start_date=_fmt_date(end_dt - timedelta(days=90)), end_date=end_date, factors=factors, execution_mode=execution_mode).relative_to(ROOT))})
            jobs.append({"name": "expanding_back_180d", "config_path": str(_make_workflow_config(opportunity, suffix="expanding_back_180d", start_date=_fmt_date(start_dt - timedelta(days=180)), end_date=end_date, factors=factors, execution_mode=execution_mode).relative_to(ROOT))})
    elif otype == "recombine":
        hybrid_factors = deepcopy(factors)
        if len(hybrid_factors) >= 2:
            hybrid_name = f"hybrid_{hybrid_factors[0]['name']}_{hybrid_factors[1]['name']}"
            hybrid_expression = f"({hybrid_factors[0]['expression']}) + ({hybrid_factors[1]['expression']})"
            hybrid_factors.append({"name": hybrid_name, "expression": hybrid_expression})
        jobs.append({"name": "recent_hybrid", "config_path": str(_make_workflow_config(opportunity, suffix="recent_hybrid", start_date=_fmt_date(end_dt - timedelta(days=45)), end_date=end_date, factors=hybrid_factors, execution_mode=execution_mode).relative_to(ROOT))})
        if execution_mode == "full":
            jobs.append({"name": "expanding_hybrid", "config_path": str(_make_workflow_config(opportunity, suffix="expanding_hybrid", start_date=_fmt_date(start_dt - timedelta(days=120)), end_date=end_date, factors=hybrid_factors, execution_mode=execution_mode).relative_to(ROOT))})
    elif otype == "probe":
        probe_factors = deepcopy(factors[: min(2, len(factors))]) or deepcopy(factors)
        jobs.append({"name": "probe_recent_30d", "config_path": str(_make_workflow_config(opportunity, suffix="probe_recent_30d", start_date=_fmt_date(end_dt - timedelta(days=30)), end_date=end_date, factors=probe_factors, execution_mode=execution_mode).relative_to(ROOT))})
        if execution_mode == "full":
            jobs.append({"name": "probe_recent_60d", "config_path": str(_make_workflow_config(opportunity, suffix="probe_recent_60d", start_date=_fmt_date(end_dt - timedelta(days=60)), end_date=end_date, factors=probe_factors, execution_mode=execution_mode).relative_to(ROOT))})
    else:
        return None

    batch = upgrade_generated_batch(
        {
            "source": "research_opportunity",
            "opportunity_id": opportunity.get("opportunity_id"),
            "opportunity_type": otype,
            "summary": opportunity.get("question") or otype,
            "jobs": jobs,
        },
        source="opportunity_to_tasks",
    )
    batch_path = GENERATED_BATCH_DIR / f"{oid}.json"
    _write_json(batch_path, batch)
    return batch_path


def map_opportunity_to_task(opportunity: dict[str, Any]) -> dict[str, Any] | None:
    otype = opportunity.get("opportunity_type")
    target_candidates = list(opportunity.get("target_candidates") or [])
    target_family = opportunity.get("target_family")
    expected_gain = list(opportunity.get("expected_knowledge_gain") or [])
    priority_hint = max(1, int(round((1.0 - float(opportunity.get("priority") or 0.5)) * 100)))
    payload_base = {
        "opportunity_id": opportunity.get("opportunity_id"),
        "opportunity_type": otype,
        "question": opportunity.get("question"),
        "hypothesis": opportunity.get("hypothesis"),
        "target_family": target_family,
        "target_candidates": target_candidates,
        "expected_information_gain": expected_gain,
        "source": "research_opportunity",
    }

    if otype in {"confirm", "diagnose"}:
        diagnostic_type = f"opportunity_{otype}"
        return {
            "task_type": "diagnostic",
            "priority": priority_hint,
            "fingerprint": _fingerprint_for_opportunity(opportunity),
            "worker_note": f"validation｜opportunity:{opportunity.get('opportunity_id')}",
            "payload": {
                **payload_base,
                "diagnostic_type": diagnostic_type,
                "focus_factors": target_candidates,
                "reasons": ["opportunity_selected", f"mapped_from:{otype}"],
                "goal": opportunity.get("question") or diagnostic_type,
                "branch_id": opportunity.get("opportunity_id"),
                "stop_if": [],
                "promote_if": [],
                "disconfirm_if": [],
            },
        }

    if otype in {"expand", "recombine", "probe"}:
        if _is_budget_risky_probe(opportunity):
            diagnostic_type = "opportunity_probe_budget_guard"
            return {
                "task_type": "diagnostic",
                "priority": max(priority_hint, 35),
                "fingerprint": _fingerprint_for_opportunity(opportunity) + "::budget_guard",
                "worker_note": f"validation｜opportunity:{opportunity.get('opportunity_id')}｜budget_guard",
                "payload": {
                    **payload_base,
                    "diagnostic_type": diagnostic_type,
                    "focus_factors": target_candidates,
                    "reasons": ["opportunity_selected", "mapped_from:probe", "budget_guard:no_target_candidates"],
                    "goal": opportunity.get("question") or diagnostic_type,
                    "branch_id": opportunity.get("opportunity_id"),
                    "stop_if": [],
                    "promote_if": [],
                    "disconfirm_if": [],
                },
            }
        batch_path = _build_opportunity_batch(opportunity)
        if batch_path is not None:
            return {
                "task_type": "generated_batch",
                "priority": max(priority_hint, 40),
                "fingerprint": _fingerprint_for_opportunity(opportunity),
                "worker_note": f"exploration｜opportunity:{opportunity.get('opportunity_id')}",
                "payload": {
                    **payload_base,
                    "batch_path": str(batch_path),
                    "output_dir": f"artifacts/opportunity_generated_batch_run/{_sanitize_name(opportunity.get('opportunity_id') or 'opportunity')}",
                    "execution_mode": opportunity.get("execution_mode") or "cheap_screen",
                    "goal": opportunity.get("question") or otype,
                    "branch_id": opportunity.get("opportunity_id"),
                    "stop_if": [],
                    "promote_if": [],
                    "disconfirm_if": [],
                },
            }

    return None
