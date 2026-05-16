from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from factor_lab.exploration_pools import NEW_MECHANISM_POOL, OLD_SPACE_POOL, classify_exploration_pool

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"


def _read_json(path: str | Path, default: Any) -> Any:
    path = Path(path)
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _current_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _pool_for_row(row: dict[str, Any]) -> str:
    payload = row.get("candidate_generation_context") or row.get("payload") or row
    return classify_exploration_pool(row.get("source") or payload.get("source"), payload)


def _safe_ratio(num: int, den: int) -> float | None:
    if den <= 0:
        return None
    return round(num / den, 6)


def _promotion_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    quality_counts: dict[str, int] = {}
    decision_counts: dict[str, int] = {}
    medium_long_survivors = 0
    neutralized_survivors = 0
    incremental_survivors = 0
    implementable_survivors = 0
    for row in rows:
        quality = row.get("quality_classification") or "unknown"
        decision = row.get("quality_promotion_decision") or "unknown"
        quality_counts[quality] = int(quality_counts.get(quality) or 0) + 1
        decision_counts[decision] = int(decision_counts.get(decision) or 0) + 1
        if int(row.get("window_count") or 0) >= 4 and decision in {"promote", "keep_validating"}:
            medium_long_survivors += 1
        retention = row.get("retention_industry")
        neutralized_rank_ic_mean = row.get("neutralized_rank_ic_mean")
        if retention is not None and neutralized_rank_ic_mean is not None and float(retention) >= 0.15 and float(neutralized_rank_ic_mean) > 0.0:
            neutralized_survivors += 1
        if row.get("net_metric") is not None and float(row.get("net_metric") or 0.0) > 0.0:
            incremental_survivors += 1
        if row.get("turnover_daily") is not None and float(row.get("turnover_daily") or 999.0) <= 0.35:
            implementable_survivors += 1
    total = len(rows)
    return {
        "total_rows": total,
        "quality_classification_counts": quality_counts,
        "promotion_decision_counts": decision_counts,
        "medium_long_survival_rate": _safe_ratio(medium_long_survivors, total),
        "neutralized_survival_rate": _safe_ratio(neutralized_survivors, total),
        "incremental_value_survival_rate": _safe_ratio(incremental_survivors, total),
        "implementability_pass_rate": _safe_ratio(implementable_survivors, total),
    }


def _lifecycle_window_summary(candidate_lifecycle: dict[str, Any], cutoff: datetime) -> dict[str, dict[str, int]]:
    summary: dict[str, dict[str, int]] = {}
    for candidate_name, payload in (candidate_lifecycle or {}).items():
        history = list((payload or {}).get("history") or [])
        meta = {
            "keep_validating": 0,
            "stable_alpha_candidate": 0,
            "do_not_promote": 0,
        }
        for row in history:
            updated_at = _parse_iso(row.get("updated_at_utc"))
            if updated_at is None or updated_at < cutoff:
                continue
            next_state = row.get("next_state")
            action = row.get("action")
            if next_state == "validating" or action == "hold":
                meta["keep_validating"] += 1
            if next_state == "stable_candidate" or action == "promote":
                meta["stable_alpha_candidate"] += 1
            if next_state in {"provisional", "graveyard"} or action in {"demote", "suppress"}:
                meta["do_not_promote"] += 1
        if any(meta.values()):
            summary[candidate_name] = meta
    return summary


def _window_attribution(
    *,
    cutoff: datetime,
    candidate_generation_history: list[dict[str, Any]],
    candidate_lifecycle: dict[str, Any],
    promotion_rows_by_name: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    proposals = [row for row in candidate_generation_history if (_parse_iso(row.get("updated_at_utc")) or datetime.now(timezone.utc)) >= cutoff]
    lifecycle_summary = _lifecycle_window_summary(candidate_lifecycle, cutoff)

    by_pool: dict[str, dict[str, Any]] = {
        OLD_SPACE_POOL: {"proposal_count": 0, "keep_validating": 0, "stable_alpha_candidate": 0, "do_not_promote": 0, "duplicate_suppress": 0, "families": {}},
        NEW_MECHANISM_POOL: {"proposal_count": 0, "keep_validating": 0, "stable_alpha_candidate": 0, "do_not_promote": 0, "duplicate_suppress": 0, "families": {}},
    }
    pool_seen_candidates: dict[str, set[str]] = {
        OLD_SPACE_POOL: set(),
        NEW_MECHANISM_POOL: set(),
    }
    family_seen_candidates: dict[tuple[str, str], set[str]] = {}
    proposed_names: list[str] = []

    for row in proposals:
        candidate_name = row.get("candidate_id")
        if not candidate_name:
            continue
        proposed_names.append(candidate_name)
        pool_name = _pool_for_row(row)
        bucket = by_pool.setdefault(pool_name, {"proposal_count": 0, "keep_validating": 0, "stable_alpha_candidate": 0, "do_not_promote": 0, "duplicate_suppress": 0, "families": {}})
        lifecycle = lifecycle_summary.get(candidate_name) or {}
        promotion_row = promotion_rows_by_name.get(candidate_name) or {}
        family = promotion_row.get("family") or row.get("target_family") or "unknown"

        if candidate_name not in pool_seen_candidates.setdefault(pool_name, set()):
            pool_seen_candidates[pool_name].add(candidate_name)
            bucket["proposal_count"] += 1
            bucket["keep_validating"] += 1 if lifecycle.get("keep_validating") else 0
            bucket["stable_alpha_candidate"] += 1 if lifecycle.get("stable_alpha_candidate") else 0
            bucket["do_not_promote"] += 1 if lifecycle.get("do_not_promote") else 0
            if promotion_row.get("quality_classification") == "duplicate-suppress":
                bucket["duplicate_suppress"] += 1

        family_bucket = bucket["families"].setdefault(family, {"proposal_count": 0, "stable_alpha_candidate": 0, "keep_validating": 0, "effective_candidate_count": 0})
        family_key = (pool_name, family)
        if candidate_name not in family_seen_candidates.setdefault(family_key, set()):
            family_seen_candidates[family_key].add(candidate_name)
            family_bucket["proposal_count"] += 1
            family_bucket["stable_alpha_candidate"] += 1 if lifecycle.get("stable_alpha_candidate") else 0
            family_bucket["keep_validating"] += 1 if lifecycle.get("keep_validating") else 0
            family_bucket["effective_candidate_count"] += 1 if (lifecycle.get("stable_alpha_candidate") or lifecycle.get("keep_validating")) else 0

    for bucket in by_pool.values():
        proposal_count = int(bucket.get("proposal_count") or 0)
        bucket["proposal_to_keep_validating"] = _safe_ratio(int(bucket.get("keep_validating") or 0), proposal_count)
        bucket["proposal_to_stable_alpha_candidate"] = _safe_ratio(int(bucket.get("stable_alpha_candidate") or 0), proposal_count)
        bucket["do_not_promote_ratio"] = _safe_ratio(int(bucket.get("do_not_promote") or 0), proposal_count)
        bucket["duplicate_suppress_ratio"] = _safe_ratio(int(bucket.get("duplicate_suppress") or 0), proposal_count)
        bucket["family_effective_output_rate"] = {
            family: _safe_ratio(int(meta.get("effective_candidate_count") or 0), int(meta.get("proposal_count") or 0))
            for family, meta in (bucket.get("families") or {}).items()
        }

    unique_proposed = sorted(set(proposed_names))
    proposal_rows = [promotion_rows_by_name[name] for name in unique_proposed if name in promotion_rows_by_name]
    promotion_summary = _promotion_summary(proposal_rows)

    total_proposals = len(unique_proposed)
    keep_validating = len([name for name in unique_proposed if (lifecycle_summary.get(name) or {}).get("keep_validating")])
    stable_alpha = len([name for name in unique_proposed if (lifecycle_summary.get(name) or {}).get("stable_alpha_candidate")])
    do_not_promote = len([name for name in unique_proposed if (lifecycle_summary.get(name) or {}).get("do_not_promote")])
    duplicate_suppress = len([name for name in unique_proposed if (promotion_rows_by_name.get(name) or {}).get("quality_classification") == "duplicate-suppress"])

    return {
        "proposal_count": total_proposals,
        "keep_validating_count": keep_validating,
        "stable_alpha_candidate_net_new": stable_alpha,
        "do_not_promote_count": do_not_promote,
        "duplicate_suppress_count": duplicate_suppress,
        "proposal_to_keep_validating": _safe_ratio(keep_validating, total_proposals),
        "proposal_to_stable_alpha_candidate": _safe_ratio(stable_alpha, total_proposals),
        "do_not_promote_ratio": _safe_ratio(do_not_promote, total_proposals),
        "duplicate_suppress_ratio": _safe_ratio(duplicate_suppress, total_proposals),
        "quality_distribution": promotion_summary,
        "by_pool": by_pool,
    }


def _build_markdown_report(payload: dict[str, Any]) -> str:
    lines = [
        "# Factor Quality Attribution Report",
        "",
        f"Generated at: {payload.get('generated_at_utc')}",
        "",
        "## Current snapshot",
        f"- proposal_count: {payload['current_snapshot']['generation'].get('proposal_count')}",
        f"- candidate_generation_task_count: {payload['current_snapshot']['generation'].get('candidate_generation_task_count')}",
        f"- stable-alpha-candidate count: {payload['current_snapshot']['final_conversion'].get('stable_alpha_candidate_count')}",
        f"- keep_validating count: {payload['current_snapshot']['final_conversion'].get('keep_validating_count')}",
        f"- do_not_promote count: {payload['current_snapshot']['final_conversion'].get('do_not_promote_count')}",
        f"- duplicate_suppress count: {payload['current_snapshot']['final_conversion'].get('duplicate_suppress_count')}",
        f"- planner decision source: {(payload['current_snapshot'].get('decision_layer') or {}).get('planner_source')}",
        f"- failure analyst decision source: {(payload['current_snapshot'].get('decision_layer') or {}).get('failure_analyst_source')}",
        "",
        "## Observation windows",
    ]
    for label in ("48h", "7d"):
        window = (payload.get("observation_windows") or {}).get(label) or {}
        lines.extend(
            [
                f"### {label}",
                f"- proposal_count: {window.get('proposal_count')}",
                f"- proposal->keep_validating: {window.get('proposal_to_keep_validating')}",
                f"- proposal->stable-alpha-candidate: {window.get('proposal_to_stable_alpha_candidate')}",
                f"- do_not_promote_ratio: {window.get('do_not_promote_ratio')}",
                f"- duplicate_suppress_ratio: {window.get('duplicate_suppress_ratio')}",
                f"- medium_long_survival_rate: {(window.get('quality_distribution') or {}).get('medium_long_survival_rate')}",
                f"- neutralized_survival_rate: {(window.get('quality_distribution') or {}).get('neutralized_survival_rate')}",
                f"- incremental_value_survival_rate: {(window.get('quality_distribution') or {}).get('incremental_value_survival_rate')}",
                "",
            ]
        )
        for pool_name, pool in ((window.get("by_pool") or {}).items()):
            lines.append(f"- {pool_name}: proposals={pool.get('proposal_count')}, stable={pool.get('stable_alpha_candidate')}, keep_validating={pool.get('keep_validating')}")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def build_research_attribution(
    *,
    memory_path: str | Path = ARTIFACTS / "research_memory.json",
    learning_path: str | Path = ARTIFACTS / "research_learning.json",
    candidate_pool_path: str | Path = ARTIFACTS / "research_candidate_pool.json",
    candidate_generation_plan_path: str | Path = ARTIFACTS / "candidate_generation_plan.json",
    promotion_scorecard_path: str | Path = ARTIFACTS / "promotion_scorecard.json",
    portfolio_stability_path: str | Path = ARTIFACTS / "paper_portfolio" / "portfolio_stability_score.json",
    agent_responses_path: str | Path = ARTIFACTS / "agent_responses.json",
    output_path: str | Path = ARTIFACTS / "research_attribution.json",
    report_path: str | Path = ARTIFACTS / "factor_quality_observation_report.md",
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    memory = _read_json(memory_path, {})
    learning = _read_json(learning_path, {})
    candidate_pool = _read_json(candidate_pool_path, {})
    candidate_generation_plan = _read_json(candidate_generation_plan_path, {})
    promotion_scorecard = _read_json(promotion_scorecard_path, {})
    portfolio_stability = _read_json(portfolio_stability_path, {})
    agent_responses = _read_json(agent_responses_path, {})

    current_proposals = list(candidate_generation_plan.get("proposals") or [])
    current_generated_tasks = [
        row for row in (candidate_pool.get("tasks") or [])
        if (row.get("payload") or {}).get("source") == "candidate_generation"
    ]
    current_generation_by_pool = {
        OLD_SPACE_POOL: {
            "proposal_count": len([row for row in current_proposals if _pool_for_row(row) == OLD_SPACE_POOL]),
            "task_count": len([row for row in current_generated_tasks if ((row.get("payload") or {}).get("exploration_pool") or OLD_SPACE_POOL) == OLD_SPACE_POOL]),
        },
        NEW_MECHANISM_POOL: {
            "proposal_count": len([row for row in current_proposals if _pool_for_row(row) == NEW_MECHANISM_POOL]),
            "task_count": len([row for row in current_generated_tasks if ((row.get("payload") or {}).get("exploration_pool") or OLD_SPACE_POOL) == NEW_MECHANISM_POOL]),
        },
    }

    promotion_rows = list((promotion_scorecard.get("rows") or []))
    promotion_rows_by_name = {row.get("factor_name"): row for row in promotion_rows if row.get("factor_name")}
    current_quality = _promotion_summary(promotion_rows)

    current_final_conversion = {
        "keep_validating_count": int((current_quality.get("promotion_decision_counts") or {}).get("keep_validating") or 0),
        "stable_alpha_candidate_count": int((current_quality.get("quality_classification_counts") or {}).get("stable-alpha-candidate") or 0),
        "do_not_promote_count": int((current_quality.get("promotion_decision_counts") or {}).get("do_not_promote") or 0),
        "duplicate_suppress_count": int((current_quality.get("quality_classification_counts") or {}).get("duplicate-suppress") or 0),
    }

    candidate_generation_history = list((memory.get("candidate_generation_history") or learning.get("candidate_generation_history") or []))
    candidate_lifecycle = dict(memory.get("candidate_lifecycle") or {})

    observation_windows = {
        "48h": _window_attribution(
            cutoff=now - timedelta(hours=48),
            candidate_generation_history=candidate_generation_history,
            candidate_lifecycle=candidate_lifecycle,
            promotion_rows_by_name=promotion_rows_by_name,
        ),
        "7d": _window_attribution(
            cutoff=now - timedelta(days=7),
            candidate_generation_history=candidate_generation_history,
            candidate_lifecycle=candidate_lifecycle,
            promotion_rows_by_name=promotion_rows_by_name,
        ),
    }

    payload = {
        "generated_at_utc": _current_iso(),
        "current_snapshot": {
            "generation": {
                "proposal_count": len(current_proposals),
                "candidate_generation_task_count": len(current_generated_tasks),
                "failure_question_card_count": len(learning.get("failure_question_cards") or []),
                "pool_budgets": ((candidate_generation_plan.get("quality_throttle") or {}).get("pool_budgets") or {}),
                "by_pool": current_generation_by_pool,
            },
            "quality_distribution": current_quality,
            "final_conversion": current_final_conversion,
            "portfolio_margin_proxy": {
                "stability_score": portfolio_stability.get("stability_score"),
                "label": portfolio_stability.get("label"),
            },
            "decision_layer": {
                "planner_source": (((agent_responses.get("planner") or {}).get("decision_metadata") or {}).get("source") or ((agent_responses.get("planner") or {}).get("decision_source"))),
                "failure_analyst_source": (((agent_responses.get("failure_analyst") or {}).get("decision_metadata") or {}).get("source") or ((agent_responses.get("failure_analyst") or {}).get("decision_source"))),
                "planner_schema_valid": (((agent_responses.get("planner") or {}).get("decision_metadata") or {}).get("schema_valid")),
                "failure_analyst_schema_valid": (((agent_responses.get("failure_analyst") or {}).get("decision_metadata") or {}).get("schema_valid")),
            },
        },
        "observation_windows": observation_windows,
    }

    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    Path(report_path).write_text(_build_markdown_report(payload), encoding="utf-8")
    return payload
