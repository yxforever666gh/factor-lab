from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from factor_lab.expression_validation import validate_expression
from factor_lab.feature_schema import TUSHARE_FEATURE_COLUMNS
from factor_lab.promotion_scorecard import build_promotion_scorecard
from factor_lab.storage import ExperimentStore
from factor_lab.generated_artifacts import upgrade_generated_batch, upgrade_generated_config


ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class RobustnessWindowSpec:
    label: str
    days: int | None = None
    expanding: bool = False


DEFAULT_WINDOW_SPECS = [
    RobustnessWindowSpec(label="recent_30d", days=30),
    RobustnessWindowSpec(label="recent_45d", days=45),
    RobustnessWindowSpec(label="recent_60d", days=60),
    RobustnessWindowSpec(label="recent_90d", days=90),
    RobustnessWindowSpec(label="recent_120d", days=120),
    RobustnessWindowSpec(label="expanding", expanding=True),
]


def _parse_date(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%d")


def _format_date(value: datetime) -> str:
    return value.strftime("%Y-%m-%d")


def _default_expanding_start_date(end_date: str) -> str:
    end_dt = _parse_date(end_date)
    anchor = (end_dt - timedelta(days=180)).replace(day=1)
    return _format_date(anchor)


def _slug(text: str) -> str:
    keep = []
    for char in text:
        if char.isalnum() or char in {"_", "-"}:
            keep.append(char)
        else:
            keep.append("_")
    return "".join(keep).strip("_")


def _write_generated_config(config: dict[str, Any], name: str) -> str:
    out_dir = ROOT / "artifacts" / "generated_robustness_configs"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{name}.json"
    payload = upgrade_generated_config(config, source="robustness_batch")
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path.relative_to(ROOT))


def select_robustness_candidates(db_path: str | Path, top_n: int = 5) -> list[dict[str, Any]]:
    payload = build_promotion_scorecard(db_path=db_path, limit=max(20, top_n * 4))
    rows = payload.get("rows") or []
    preferred_order = ["validate_now", "core_candidate", "dedupe_first", "regime_sensitive", "watchlist"]
    selected: list[dict[str, Any]] = []
    seen: set[str] = set()
    suppressed_names: set[str] = set()
    for decision_key in preferred_order:
        for row in rows:
            if row.get("decision_key") != decision_key:
                continue
            factor_name = row.get("factor_name")
            if not factor_name or factor_name in seen or factor_name in suppressed_names:
                continue
            selected.append(row)
            seen.add(factor_name)
            for peer_name in row.get("duplicate_peers") or []:
                if peer_name:
                    suppressed_names.add(peer_name)
            if len(selected) >= top_n:
                return selected
    return selected


def build_robustness_batch(
    *,
    db_path: str | Path,
    base_config_path: str | Path,
    batch_config_path: str | Path,
    batch_output_dir: str,
    plan_output_path: str | Path | None = None,
    top_n: int = 5,
    window_specs: list[RobustnessWindowSpec] | None = None,
    expanding_start_date: str | None = None,
) -> dict[str, Any]:
    db_path = Path(db_path)
    base_config_path = Path(base_config_path)
    batch_config_path = Path(batch_config_path)
    plan_output_path = Path(plan_output_path) if plan_output_path else None
    window_specs = window_specs or DEFAULT_WINDOW_SPECS

    store = ExperimentStore(db_path)
    candidates = {row["name"]: row for row in store.list_factor_candidates(limit=2000)}
    selected_rows = select_robustness_candidates(db_path=db_path, top_n=top_n)
    base_config = json.loads(base_config_path.read_text(encoding="utf-8"))
    end_date = base_config["end_date"]
    expanding_start_date = expanding_start_date or _default_expanding_start_date(end_date)

    jobs: list[dict[str, Any]] = []
    manifest_candidates: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    for row in selected_rows:
        factor_name = row["factor_name"]
        candidate = candidates.get(factor_name)
        if not candidate:
            skipped.append({"factor_name": factor_name, "reason": "candidate_definition_missing"})
            continue

        definition = candidate.get("definition") or {}
        expression = definition.get("expression") or candidate.get("expression") or ""
        if not expression:
            skipped.append({"factor_name": factor_name, "reason": "missing_expression"})
            continue

        validation = validate_expression(expression, available_fields=TUSHARE_FEATURE_COLUMNS)
        if not validation.ok:
            skipped.append({
                "factor_name": factor_name,
                "reason": "invalid_expression",
                "details": validation.errors,
            })
            continue

        manifest_row = {
            "factor_name": factor_name,
            "family": candidate.get("family"),
            "expression": expression,
            "decision_label": row.get("decision_label"),
            "decision_summary": row.get("decision_summary"),
            "latest_final_score": row.get("latest_final_score"),
            "promotion_score": row.get("promotion_score"),
            "window_jobs": [],
        }

        for spec in window_specs:
            if spec.expanding:
                start_date = expanding_start_date
                window_key = f"expanding_{start_date.replace('-', '_')}"
            else:
                start_date = _format_date(_parse_date(end_date) - timedelta(days=int(spec.days or 0)))
                window_key = spec.label

            job_name = f"{_slug(factor_name)}__{window_key}"
            job_output_dir = f"{batch_output_dir}/{job_name}"
            config = deepcopy(base_config)
            config["factors"] = [{"name": factor_name, "expression": expression}]
            config["start_date"] = start_date
            config["end_date"] = end_date
            config["output_dir"] = job_output_dir

            config_path = _write_generated_config(config, job_name)
            jobs.append({"name": job_name, "config_path": config_path})
            manifest_row["window_jobs"].append(
                {
                    "job_name": job_name,
                    "window_label": window_key,
                    "start_date": start_date,
                    "end_date": end_date,
                    "config_path": config_path,
                    "output_dir": job_output_dir,
                }
            )

        manifest_candidates.append(manifest_row)

    batch_payload = upgrade_generated_batch({"jobs": jobs}, source="robustness_batch")
    batch_config_path.parent.mkdir(parents=True, exist_ok=True)
    batch_config_path.write_text(json.dumps(batch_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    plan_payload = {
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "db_path": str(db_path),
        "base_config_path": str(base_config_path),
        "batch_config_path": str(batch_config_path),
        "batch_output_dir": batch_output_dir,
        "top_n": top_n,
        "end_date": end_date,
        "expanding_start_date": expanding_start_date,
        "selected_candidates": manifest_candidates,
        "skipped_candidates": skipped,
        "job_count": len(jobs),
        "window_labels": [spec.label for spec in window_specs],
    }
    if plan_output_path:
        plan_output_path.parent.mkdir(parents=True, exist_ok=True)
        plan_output_path.write_text(json.dumps(plan_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return plan_payload
