from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"
STORE_PATH = ARTIFACTS / "research_opportunity_store.json"
EXECUTION_PATH = ARTIFACTS / "opportunity_execution_plan.json"


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
    items = list((store.get("opportunities") or {}).values())
    review = {"challenger": [], "auditor": [], "blocks": {}, "downweights": {}}
    for row in items[:50]:
        oid = row.get("opportunity_id")
        if not oid:
            continue
        novelty = float(row.get("novelty_score") or 0.0)
        confidence = float(row.get("confidence") or 0.0)
        state = row.get("state")
        if novelty < 0.5:
            review["challenger"].append(f"{oid}: 新颖度偏低，可能仍在重复旧研究问题。")
            review["downweights"][oid] = {"reason": "low_novelty", "delta": 0.08}
        if state == "archived":
            review["auditor"].append(f"{oid}: 已被归档，需检查是否被去重规则过度压制。")
        if confidence < 0.6:
            review["auditor"].append(f"{oid}: 置信度偏低，进入执行前应谨慎。")
            review["blocks"][oid] = {"reason": "low_confidence"}
    opath.write_text(json.dumps(review, ensure_ascii=False, indent=2), encoding="utf-8")
    return review
