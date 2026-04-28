from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"
STORE_PATH = ARTIFACTS / "research_opportunity_store.json"
EXECUTION_PATH = ARTIFACTS / "opportunity_execution_plan.json"
HIGH_EPISTEMIC_GAIN_MARKERS = {
    "boundary_confirmed",
    "new_branch_opened",
    "repeated_graveyard_confirmed",
    "uncertainty_reduced",
    "stable_candidate_confirmed",
    "candidate_survival_check",
    "exploration_candidate_survived",
}
OPPORTUNITY_LEARNING_PATH = ARTIFACTS / "opportunity_learning.json"
HIGH_MEMORY_RISK_MARKERS = {
    "child-pair_target",
    "root-no_targets",
}


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = float(raw)
    except Exception:
        value = default
    return max(0.0, min(1.0, value))


def _soft_low_confidence_allowed(row: dict[str, Any]) -> bool:
    priority = float(row.get("priority") or 0.0)
    novelty = float(row.get("novelty_score") or 0.0)
    opportunity_type = str(row.get("opportunity_type") or "").strip().lower()
    expected_gain = {
        str(item).strip()
        for item in (row.get("expected_knowledge_gain") or [])
        if item
    }
    return bool(
        expected_gain & HIGH_EPISTEMIC_GAIN_MARKERS
        or (opportunity_type in {"diagnose", "probe", "recombine"} and (priority >= 0.72 or novelty >= 0.65))
        or (priority >= 0.88 and novelty >= 0.55)
    )


def _template_key(row: dict[str, Any]) -> str:
    otype = row.get("opportunity_type") or "unknown"
    family = row.get("target_family") or "none"
    parent = "child" if row.get("parent_opportunity_id") else "root"
    return f"{otype}::{family}::{parent}"


def _target_shape(row: dict[str, Any]) -> str:
    targets = list(row.get("target_candidates") or [])
    if not targets:
        return "no_targets"
    if len(targets) == 1:
        return "single_target"
    if len(targets) == 2:
        return "pair_target"
    return "multi_target"


def _intent_signature(row: dict[str, Any]) -> str:
    expected = sorted([str(x) for x in (row.get("expected_knowledge_gain") or []) if x])
    if not expected:
        return "no_expected_gain"
    return "+".join(expected[:3])


def _pattern_prefix(row: dict[str, Any]) -> str:
    return f"{_template_key(row)}::{_target_shape(row)}::{_intent_signature(row)}::"


def _lookup_learning_meta(learning: dict[str, Any], row: dict[str, Any]) -> dict[str, dict[str, Any]]:
    template_key = _template_key(row)
    pattern_prefix = _pattern_prefix(row)
    patterns = learning.get("patterns") or {}
    matched_pattern = {}
    for key, value in patterns.items():
        if str(key).startswith(pattern_prefix):
            matched_pattern = value or {}
            break
    return {
        "family": (learning.get("families") or {}).get(row.get("target_family") or "", {}) or {},
        "template": (learning.get("templates") or {}).get(template_key, {}) or {},
        "pattern": matched_pattern,
    }


def _has_high_memory_risk(row: dict[str, Any]) -> bool:
    haystack = " ".join([
        str(row.get("opportunity_id") or ""),
        str(row.get("title") or ""),
        str(row.get("question") or ""),
        str(row.get("rationale") or ""),
    ]).lower()
    return any(marker in haystack for marker in HIGH_MEMORY_RISK_MARKERS)


def build_opportunity_metrics(store_path: str | Path | None = None, output_path: str | Path | None = None) -> dict[str, Any]:
    spath = Path(store_path) if store_path else STORE_PATH
    opath = Path(output_path) if output_path else (ARTIFACTS / "opportunity_metrics.json")
    store = json.loads(spath.read_text(encoding="utf-8")) if spath.exists() else {"opportunities": {}}
    items = list((store.get("opportunities") or {}).values())
    total = len(items)
    promoted = len([row for row in items if row.get("state") == "promoted"])
    evaluated = len([row for row in items if row.get("state") == "evaluated"])
    rejected = len([row for row in items if row.get("state") == "rejected"])
    archived = len([row for row in items if row.get("state") == "archived"])
    child_count = len([row for row in items if row.get("parent_opportunity_id")])
    with_evaluation = [row for row in items if row.get("evaluation")]
    high_gain = len([row for row in with_evaluation if (row.get("evaluation") or {}).get("evaluation_label") == "high_gain"])
    payload = {"counts": {"total": total, "promoted": promoted, "evaluated": evaluated, "rejected": rejected, "archived": archived, "child_count": child_count}, "rates": {"success_rate": round(promoted / total, 3) if total else None, "knowledge_gain_rate": round(high_gain / len(with_evaluation), 3) if with_evaluation else None, "branch_growth_rate": round(child_count / total, 3) if total else None}}
    opath.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def build_opportunity_archive_diagnostics(store_path: str | Path | None = None, execution_path: str | Path | None = None, output_path: str | Path | None = None) -> dict[str, Any]:
    spath = Path(store_path) if store_path else STORE_PATH
    epath = Path(execution_path) if execution_path else EXECUTION_PATH
    opath = Path(output_path) if output_path else (ARTIFACTS / "opportunity_archive_diagnostics.json")
    store = json.loads(spath.read_text(encoding="utf-8")) if spath.exists() else {"opportunities": {}}
    execution = json.loads(epath.read_text(encoding="utf-8")) if epath.exists() else {"skipped": []}
    items = list((store.get("opportunities") or {}).values())
    skipped = list(execution.get("skipped") or [])
    skip_reason_by_id = {row.get("opportunity_id"): row.get("reason") for row in skipped if row.get("opportunity_id")}
    archive_counts: dict[str, int] = {}
    archive_samples: list[dict[str, Any]] = []
    funnel = {"proposed": 0, "scheduled": 0, "running": 0, "evaluated": 0, "promoted": 0, "rejected": 0, "archived": 0}
    for row in items:
        state = row.get("state") or "proposed"
        if state in funnel:
            funnel[state] += 1
        if state != "archived":
            continue
        history = list(row.get("history") or [])
        reason = history[-1].get("reason") if history else None
        reason = reason or skip_reason_by_id.get(row.get("opportunity_id")) or "unknown"
        archive_counts[reason] = archive_counts.get(reason, 0) + 1
        archive_samples.append({"opportunity_id": row.get("opportunity_id"), "type": row.get("opportunity_type"), "reason": reason, "priority": row.get("priority"), "novelty": row.get("novelty_score"), "confidence": row.get("confidence"), "target_family": row.get("target_family")})
    payload = {"funnel": funnel, "archive_counts": dict(sorted(archive_counts.items(), key=lambda item: (-item[1], item[0]))), "archive_samples": archive_samples[:20], "skipped_count": len(skipped)}
    opath.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def build_opportunity_review(store_path: str | Path | None = None, output_path: str | Path | None = None) -> dict[str, Any]:
    spath = Path(store_path) if store_path else STORE_PATH
    opath = Path(output_path) if output_path else (ARTIFACTS / "opportunity_review.json")
    store = json.loads(spath.read_text(encoding="utf-8")) if spath.exists() else {"opportunities": {}}
    learning = json.loads(OPPORTUNITY_LEARNING_PATH.read_text(encoding="utf-8")) if OPPORTUNITY_LEARNING_PATH.exists() else {}
    items = list((store.get("opportunities") or {}).values())
    review = {"challenger": [], "auditor": [], "blocks": {}, "downweights": {}}
    hard_block_threshold = _env_float("RESEARCH_OPPORTUNITY_HARD_BLOCK_CONFIDENCE", 0.52)
    review_threshold = _env_float("RESEARCH_OPPORTUNITY_REVIEW_CONFIDENCE", 0.6)
    for row in items[:50]:
        oid = row.get("opportunity_id")
        if not oid:
            continue
        novelty = float(row.get("novelty_score") or 0.0)
        confidence = float(row.get("confidence") or 0.0)
        state = row.get("state")
        learning_meta = _lookup_learning_meta(learning, row)
        family_meta = learning_meta["family"]
        template_meta = learning_meta["template"]
        pattern_meta = learning_meta["pattern"]
        high_memory_risk = _has_high_memory_risk(row)

        if novelty < 0.5:
            review["challenger"].append(f"{oid}: 新颖度偏低，可能仍在重复旧研究问题。")
            review["downweights"][oid] = {"reason": "low_novelty", "delta": 0.08}
        if state == "archived":
            review["auditor"].append(f"{oid}: 已被归档，需检查是否被去重规则过度压制。")

        if high_memory_risk:
            review["auditor"].append(f"{oid}: 命中高内存风险模式，需谨慎进入 generated_batch。")
            existing = review["downweights"].get(oid) or {}
            review["downweights"][oid] = {
                "reason": existing.get("reason") or "high_memory_risk",
                "delta": max(float(existing.get("delta") or 0.0), 0.12),
            }

        cooldown_meta = next((meta for meta in [pattern_meta, template_meta, family_meta] if meta.get("cooldown_active")), None)
        if cooldown_meta:
            reason = str(cooldown_meta.get("cooldown_reason") or "low_yield_cooldown")
            review["auditor"].append(f"{oid}: 最近该模式低产出/高浪费，触发冷却（{reason}）。")
            if not _soft_low_confidence_allowed(row):
                review["blocks"][oid] = {"reason": reason}
            else:
                existing = review["downweights"].get(oid) or {}
                review["downweights"][oid] = {
                    "reason": existing.get("reason") or reason,
                    "delta": max(float(existing.get("delta") or 0.0), 0.16),
                }

        if int(pattern_meta.get("recent_resource_exhaustion_count") or 0) > 0 or int(template_meta.get("recent_resource_exhaustion_count") or 0) > 0:
            existing = review["downweights"].get(oid) or {}
            review["downweights"][oid] = {
                "reason": existing.get("reason") or "recent_resource_exhaustion",
                "delta": max(float(existing.get("delta") or 0.0), 0.12),
            }

        if confidence < review_threshold:
            review["auditor"].append(f"{oid}: 置信度偏低，进入执行前应谨慎。")
            if confidence < hard_block_threshold:
                review["blocks"].setdefault(oid, {"reason": "low_confidence_hard_block"})
            elif _soft_low_confidence_allowed(row):
                existing = review["downweights"].get(oid) or {}
                review["downweights"][oid] = {
                    "reason": existing.get("reason") or "soft_low_confidence",
                    "delta": max(float(existing.get("delta") or 0.0), 0.06),
                }
            else:
                review["blocks"].setdefault(oid, {"reason": "low_confidence"})
    opath.write_text(json.dumps(review, ensure_ascii=False, indent=2), encoding="utf-8")
    return review
