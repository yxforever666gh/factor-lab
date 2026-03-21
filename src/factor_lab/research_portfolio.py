from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def build_research_portfolio_plan(snapshot: dict[str, Any], opportunity_learning: dict[str, Any], output_path: str | Path | None = None) -> dict[str, Any]:
    families = dict(opportunity_learning.get("families") or {})
    patterns = dict(opportunity_learning.get("patterns") or {})
    flow_state = snapshot.get("research_flow_state") or {}

    horizon = []
    family_allocations = []
    for family, meta in sorted(families.items(), key=lambda item: (-float((item[1] or {}).get("epistemic_value_score") or 0.0), item[0])):
        family_allocations.append({
            "family": family,
            "epistemic_value_score": float(meta.get("epistemic_value_score") or 0.0),
            "recommended_action": meta.get("recommended_action") or "keep",
            "negative_informative_count": int(meta.get("negative_informative_count") or 0),
            "uncertainty_reduction_count": int(meta.get("uncertainty_reduction_count") or 0),
        })

    top_patterns = sorted(
        [
            {"pattern": key, **(meta or {})}
            for key, meta in patterns.items()
            if float((meta or {}).get("epistemic_value_score") or 0.0) > 0
        ],
        key=lambda row: (-float(row.get("epistemic_value_score") or 0.0), row.get("pattern") or ""),
    )[:5]

    # Simple 3-step horizon plan: validate core, exploit productive pattern, then probe frontier.
    horizon.append({
        "slot": 1,
        "focus": "stabilize_core_validation",
        "goal": "先保证核心稳定候选与高价值诊断主线继续可解释。",
        "budget_mix": {"validation": 0.5, "exploration": 0.3, "invalidation": 0.2},
        "families": [row["family"] for row in family_allocations[:2]],
    })
    horizon.append({
        "slot": 2,
        "focus": "pattern_exploitation",
        "goal": "优先利用已证明具有 epistemic value 的模式，继续压缩高价值搜索空间。",
        "budget_mix": {"validation": 0.3, "exploration": 0.4, "invalidation": 0.3},
        "patterns": [row.get("pattern") for row in top_patterns[:2]],
    })
    horizon.append({
        "slot": 3,
        "focus": "frontier_probe",
        "goal": "保留一部分预算给边缘高分/高新颖度方向，避免研究空间塌缩。",
        "budget_mix": {"validation": 0.2, "exploration": 0.5, "invalidation": 0.3},
        "patterns": [row.get("pattern") for row in top_patterns[2:4]],
    })

    payload = {
        "generated_at_utc": _iso_now(),
        "flow_state": flow_state,
        "family_allocations": family_allocations,
        "top_patterns": top_patterns,
        "horizon": horizon,
    }
    out = Path(output_path) if output_path else (ARTIFACTS / "research_portfolio_plan.json")
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload
