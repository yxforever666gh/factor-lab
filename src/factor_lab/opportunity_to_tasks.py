from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

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


def _base_workflow_config() -> dict[str, Any]:
    return _load_json(BASE_WORKFLOW_PATH)


def _select_factor_defs(target_candidates: list[str]) -> list[dict[str, Any]]:
    base = _base_workflow_config()
    factor_map = {row["name"]: row for row in base.get("factors", [])}
    selected = [deepcopy(factor_map[name]) for name in target_candidates if name in factor_map]
    if selected:
        return selected
    return [deepcopy(row) for row in base.get("factors", [])[:2]]


def _make_workflow_config(opportunity: dict[str, Any], *, suffix: str, start_date: str, end_date: str, factors: list[dict[str, Any]]) -> Path:
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
    path = GENERATED_CONFIG_DIR / f"{oid}__{suffix}.json"
    return _write_json(path, config)


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
        jobs.append({"name": "recent_45d", "config_path": str(_make_workflow_config(opportunity, suffix="recent_45d", start_date=_fmt_date(end_dt - timedelta(days=45)), end_date=end_date, factors=factors).relative_to(ROOT))})
        if execution_mode == "full":
            jobs.append({"name": "recent_90d", "config_path": str(_make_workflow_config(opportunity, suffix="recent_90d", start_date=_fmt_date(end_dt - timedelta(days=90)), end_date=end_date, factors=factors).relative_to(ROOT))})
            jobs.append({"name": "expanding_back_180d", "config_path": str(_make_workflow_config(opportunity, suffix="expanding_back_180d", start_date=_fmt_date(start_dt - timedelta(days=180)), end_date=end_date, factors=factors).relative_to(ROOT))})
    elif otype == "recombine":
        hybrid_factors = deepcopy(factors)
        if len(hybrid_factors) >= 2:
            hybrid_name = f"hybrid_{hybrid_factors[0]['name']}_{hybrid_factors[1]['name']}"
            hybrid_expression = f"({hybrid_factors[0]['expression']}) + ({hybrid_factors[1]['expression']})"
            hybrid_factors.append({"name": hybrid_name, "expression": hybrid_expression})
        jobs.append({"name": "recent_hybrid", "config_path": str(_make_workflow_config(opportunity, suffix="recent_hybrid", start_date=_fmt_date(end_dt - timedelta(days=45)), end_date=end_date, factors=hybrid_factors).relative_to(ROOT))})
        if execution_mode == "full":
            jobs.append({"name": "expanding_hybrid", "config_path": str(_make_workflow_config(opportunity, suffix="expanding_hybrid", start_date=_fmt_date(start_dt - timedelta(days=120)), end_date=end_date, factors=hybrid_factors).relative_to(ROOT))})
    elif otype == "probe":
        probe_factors = deepcopy(factors[: min(2, len(factors))]) or deepcopy(factors)
        jobs.append({"name": "probe_recent_30d", "config_path": str(_make_workflow_config(opportunity, suffix="probe_recent_30d", start_date=_fmt_date(end_dt - timedelta(days=30)), end_date=end_date, factors=probe_factors).relative_to(ROOT))})
        if execution_mode == "full":
            jobs.append({"name": "probe_recent_60d", "config_path": str(_make_workflow_config(opportunity, suffix="probe_recent_60d", start_date=_fmt_date(end_dt - timedelta(days=60)), end_date=end_date, factors=probe_factors).relative_to(ROOT))})
    else:
        return None

    batch = {
        "source": "research_opportunity",
        "opportunity_id": opportunity.get("opportunity_id"),
        "opportunity_type": otype,
        "summary": opportunity.get("question") or otype,
        "jobs": jobs,
    }
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
