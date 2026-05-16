from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.agent_schemas import NOVELTY_SCHEMA_VERSION


SOFT_NOVELTY_CLASSES = {
    "new_mechanism_low_evidence",
    "meaningful_extension_low_confidence",
}


def _clip(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, float(value)))


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def judge_novelty(*, candidate_name: str, quality_row: dict[str, Any] | None, failure_dossier: dict[str, Any] | None, approved_row: dict[str, Any] | None) -> dict[str, Any]:
    quality_row = quality_row or {}
    failure_dossier = failure_dossier or {}
    approved_row = approved_row or {}

    duplicate_count = int(quality_row.get("duplicate_peer_count") or 0)
    refinement_count = int(quality_row.get("refinement_peer_count") or 0)
    high_corr_count = int(quality_row.get("high_corr_peer_count") or 0)
    incremental_value = float(((quality_row.get("quality_scores") or {}).get("incremental_value") or 0.0))
    parent_delta_status = failure_dossier.get("parent_delta_status") or "unknown"
    regime_dependency = failure_dossier.get("regime_dependency") or "unknown"
    approved_state = approved_row.get("lifecycle_state") or approved_row.get("universe_state")
    mechanism_class = quality_row.get("mechanism_novelty_class") or quality_row.get("candidate_status") or quality_row.get("family") or "unknown"
    quality_classification = quality_row.get("quality_classification")

    if duplicate_count >= 2 or (quality_classification == "duplicate-suppress" and parent_delta_status == "non_incremental"):
        novelty_class = "duplicate_like_hard"
        recommended_action = "suppress"
        confidence = 0.92
    elif duplicate_count >= 1 or refinement_count >= 2 or (high_corr_count >= 2 and incremental_value < 10):
        novelty_class = "near_neighbor_soft"
        recommended_action = "keep_validating" if incremental_value >= 6 or parent_delta_status in {"incremental", "unknown"} else "suppress"
        confidence = 0.72
    elif parent_delta_status == "non_incremental":
        novelty_class = "near_neighbor_soft"
        recommended_action = "keep_validating" if incremental_value >= 10 else "suppress"
        confidence = 0.7
    elif incremental_value >= 12 or parent_delta_status == "incremental":
        novelty_class = "meaningful_extension"
        recommended_action = "promote" if quality_row.get("quality_promotion_decision") == "promote" else "keep_validating"
        confidence = 0.74
    elif incremental_value >= 7:
        novelty_class = "meaningful_extension_low_confidence"
        recommended_action = "keep_validating"
        confidence = 0.62
    else:
        novelty_class = "new_mechanism_low_evidence"
        recommended_action = "promote" if approved_state == "approved" else "keep_validating"
        confidence = 0.58

    reasoning = [
        f"duplicate={duplicate_count}",
        f"refinement={refinement_count}",
        f"high_corr={high_corr_count}",
        f"incremental_value={incremental_value:.1f}",
        f"parent_delta={parent_delta_status}",
    ]
    if regime_dependency not in {"", "unknown", None}:
        reasoning.append(f"regime={regime_dependency}")
    if approved_state:
        reasoning.append(f"approved_state={approved_state}")

    return {
        "candidate_name": candidate_name,
        "schema_version": NOVELTY_SCHEMA_VERSION,
        "novelty_class": novelty_class,
        "incrementality_confidence": round(_clip(confidence), 4),
        "parent_delta_judgment": parent_delta_status,
        "recommended_action": recommended_action,
        "soft_route": novelty_class in SOFT_NOVELTY_CLASSES or novelty_class == "near_neighbor_soft",
        "reasoning_summary": "；".join(reasoning),
        "mechanism_context": mechanism_class,
    }


def build_novelty_judgments(snapshot: dict[str, Any]) -> dict[str, Any]:
    promotion_rows = ((snapshot.get("promotion_scorecard") or {}).get("rows") or [])
    quality_map = {row.get("factor_name"): row for row in promotion_rows if row.get("factor_name")}
    failure_rows = snapshot.get("candidate_failure_dossiers") or []
    failure_map = {row.get("candidate_name"): row for row in failure_rows if row.get("candidate_name")}
    approved_rows = ((snapshot.get("approved_universe") or {}).get("rows") or [])
    approved_map = {row.get("factor_name"): row for row in approved_rows if row.get("factor_name")}

    candidate_names = []
    for name in list(quality_map.keys()) + list(failure_map.keys()) + list(approved_map.keys()):
        if name and name not in candidate_names:
            candidate_names.append(name)

    rows = [
        judge_novelty(
            candidate_name=name,
            quality_row=quality_map.get(name),
            failure_dossier=failure_map.get(name),
            approved_row=approved_map.get(name),
        )
        for name in candidate_names
    ]
    rows.sort(key=lambda row: (-float(row.get("incrementality_confidence") or 0.0), row.get("candidate_name") or ""))
    class_counts = Counter(row.get("novelty_class") or "unknown" for row in rows)
    summary = {
        "schema_version": NOVELTY_SCHEMA_VERSION,
        "candidate_count": len(rows),
        "class_counts": dict(class_counts),
        "promote_like_count": len([row for row in rows if row.get("recommended_action") == "promote"]),
        "suppress_like_count": len([row for row in rows if row.get("recommended_action") == "suppress"]),
        "soft_route_count": len([row for row in rows if row.get("soft_route")]),
    }
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "schema_version": NOVELTY_SCHEMA_VERSION,
        "rows": rows,
        "summary": summary,
    }


def write_novelty_judgments(snapshot: dict[str, Any], artifacts_dir: str | Path) -> dict[str, Any]:
    artifacts_dir = Path(artifacts_dir)
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    payload = build_novelty_judgments(snapshot)
    (artifacts_dir / "novelty_judgments.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (artifacts_dir / "novelty_judge_summary.json").write_text(json.dumps(payload.get("summary") or {}, ensure_ascii=False, indent=2), encoding="utf-8")
    calibration = {
        "generated_at_utc": payload.get("generated_at_utc"),
        "schema_version": "factor_lab.novelty_calibration_report.v1",
        "hard_duplicate_count": len([row for row in payload.get("rows") or [] if row.get("novelty_class") == "duplicate_like_hard"]),
        "soft_neighbor_count": len([row for row in payload.get("rows") or [] if row.get("novelty_class") == "near_neighbor_soft"]),
        "soft_route_count": len([row for row in payload.get("rows") or [] if row.get("soft_route")]),
        "meaningful_extension_count": len([row for row in payload.get("rows") or [] if row.get("novelty_class") in {"meaningful_extension", "meaningful_extension_low_confidence"}]),
    }
    (artifacts_dir / "novelty_calibration_report.json").write_text(json.dumps(calibration, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def load_novelty_judgments(artifacts_dir: str | Path) -> dict[str, Any]:
    artifacts_dir = Path(artifacts_dir)
    return _load_json(artifacts_dir / "novelty_judgments.json", {"rows": [], "summary": {}})
