from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def build_retrospective(
    plan_path: str | Path,
    feedback_path: str | Path,
    output_path: str | Path,
) -> dict[str, Any]:
    plan = json.loads(Path(plan_path).read_text(encoding="utf-8"))
    feedback = json.loads(Path(feedback_path).read_text(encoding="utf-8"))

    focus_factors = set(plan.get("focus_factors", []))
    core_candidates = set(plan.get("keep_as_core_candidates", []))

    candidate_presence = set((feedback.get("batch_comparison", {}) or {}).get("candidate_presence", {}).keys())
    graveyard_presence = set((feedback.get("batch_comparison", {}) or {}).get("graveyard_presence", {}).keys())

    survived = sorted(focus_factors & candidate_presence)
    dropped = sorted(focus_factors & graveyard_presence)
    missed = sorted(focus_factors - candidate_presence - graveyard_presence)
    core_preserved = sorted(core_candidates & candidate_presence)
    core_lost = sorted(core_candidates - candidate_presence)

    effectiveness = "neutral"
    reason = "建议带来了候选与墓地混合结果。"
    if core_candidates and core_candidates.issubset(candidate_presence):
        effectiveness = "positive"
        reason = "核心候选在执行后的 generated batch 中继续存活。"
    if core_lost:
        effectiveness = "negative"
        reason = "部分核心候选未在执行结果中存活。"

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "effectiveness": effectiveness,
        "reason": reason,
        "focus_factors": sorted(focus_factors),
        "survived_as_candidates": survived,
        "fell_to_graveyard": dropped,
        "unclassified": missed,
        "core_candidates_preserved": core_preserved,
        "core_candidates_lost": core_lost,
        "next_action_hint": (
            "继续沿当前候选主线做小步扩展。"
            if effectiveness == "positive"
            else "降低该建议优先级，并复盘失败项。"
            if effectiveness == "negative"
            else "保留该建议，但需进一步验证。"
        ),
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def retrospective_markdown(retro: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# LLM 建议效果回溯",
            "",
            f"- 效果判断：{retro.get('effectiveness', '-')}",
            f"- 原因：{retro.get('reason', '-')}",
            f"- 保留下来的候选：{', '.join(retro.get('survived_as_candidates', [])) or '无'}",
            f"- 落入墓地：{', '.join(retro.get('fell_to_graveyard', [])) or '无'}",
            f"- 核心候选保留：{', '.join(retro.get('core_candidates_preserved', [])) or '无'}",
            f"- 核心候选丢失：{', '.join(retro.get('core_candidates_lost', [])) or '无'}",
            f"- 下一步提示：{retro.get('next_action_hint', '-')}",
        ]
    )
