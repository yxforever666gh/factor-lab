from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

FORBIDDEN_CORE_LOGIC_TERMS = [
    "RSI",
    "MACD",
    "布林",
    "Bollinger",
    "KDJ",
    "均线金叉",
    "均线死叉",
    "MA cross",
    "grid",
    "网格",
    "martingale",
    "马丁格尔",
    "turtle",
    "海龟",
]

DIRECT_STRATEGY_TERMS = [
    "buy_rule",
    "sell_rule",
    "position_size",
    "rebalance_rule",
    "portfolio_weight",
    "order_generation",
]

SAFETY_FLAGS = {
    "strategy_generation_allowed": False,
    "backtest_allowed": False,
    "controlled_execution_allowed": False,
    "queue_write_allowed": False,
    "timer_enable_allowed": False,
    "daemon_restore_allowed": False,
    "auto_promotion_allowed": False,
    "live_trading_allowed": False,
}

REQUIRED_PHENOMENON_FIELDS = [
    "phenomenon_id",
    "title",
    "mechanism_source",
    "participants",
    "participant_constraints",
    "behavioral_story",
    "temporary_mispricing_reason",
    "why_not_immediately_arbitraged",
    "observable_variables",
    "prediction_target",
    "expected_horizon",
    "failure_conditions",
    "minimal_verification_question",
]


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _contains_forbidden_core_logic(phenomenon: dict[str, Any]) -> list[str]:
    core_text = " ".join(
        _as_text(phenomenon.get(key))
        for key in [
            "title",
            "mechanism_source",
            "behavioral_story",
            "temporary_mispricing_reason",
            "minimal_verification_question",
            "core_logic",
        ]
    ).lower()
    return [term for term in FORBIDDEN_CORE_LOGIC_TERMS if term.lower() in core_text]


def _contains_direct_strategy_output(phenomenon: dict[str, Any]) -> list[str]:
    found = [term for term in DIRECT_STRATEGY_TERMS if term in phenomenon]
    # Be conservative with free-text matching: phenomenon artifacts may say
    # “不是买入规则” when translating an indicator into mechanism language.
    # Direct strategy fields are always rejected; textual English imperative
    # patterns are also rejected, but negated Chinese explanatory text is not.
    text = " ".join(
        _as_text(phenomenon.get(key))
        for key in ["behavioral_story", "temporary_mispricing_reason", "minimal_verification_question", "core_logic"]
    ).lower()
    textual_terms = ["buy when", "sell when", "open position", "close position"]
    found.extend(term for term in textual_terms if term in text)
    return sorted(set(found))


def validate_phenomenon(phenomenon: dict[str, Any]) -> dict[str, Any]:
    """Validate a market phenomenon candidate.

    This deliberately rejects strategies disguised as phenomena. The output is
    artifact-friendly so later review steps can render exact failure reasons.
    """

    reason_codes: list[str] = []
    missing_fields = [field for field in REQUIRED_PHENOMENON_FIELDS if not phenomenon.get(field)]
    reason_codes.extend(f"missing_{field}" for field in missing_fields)

    for list_field in ["participants", "participant_constraints", "observable_variables", "failure_conditions"]:
        value = phenomenon.get(list_field)
        if not isinstance(value, list) or not value:
            code = f"missing_{list_field}"
            if code not in reason_codes:
                reason_codes.append(code)

    forbidden = _contains_forbidden_core_logic(phenomenon)
    if forbidden:
        reason_codes.append("forbidden_indicator_core_logic")

    direct_strategy = _contains_direct_strategy_output(phenomenon)
    if direct_strategy:
        reason_codes.append("direct_strategy_rule_detected")

    decision = "candidate" if not reason_codes else "reject"
    return {
        "phenomenon_id": phenomenon.get("phenomenon_id"),
        "decision": decision,
        "reason_codes": reason_codes,
        "missing_fields": missing_fields,
        "forbidden_core_logic_terms": forbidden,
        "direct_strategy_terms": direct_strategy,
    }


def score_phenomenon(phenomenon: dict[str, Any]) -> dict[str, float]:
    scores = phenomenon.get("scores") or {}
    mechanism_strength = float(scores.get("mechanism_strength", 0))
    observability = float(scores.get("observability", 0))
    testability = float(scores.get("testability", 0))
    tradability_potential = float(scores.get("tradability_potential", 0))
    novelty = float(scores.get("novelty", 0))
    crowding_risk = float(scores.get("crowding_risk", 0))
    overfit_risk = float(scores.get("overfit_risk", 0))
    cost_sensitivity = float(scores.get("cost_sensitivity", 0))
    total = (
        2.0 * mechanism_strength
        + 1.5 * observability
        + 1.5 * testability
        + 1.5 * tradability_potential
        + 1.0 * novelty
        - 1.5 * crowding_risk
        - 2.0 * overfit_risk
        - 1.0 * cost_sensitivity
    )
    return {
        "mechanism_strength": mechanism_strength,
        "observability": observability,
        "testability": testability,
        "tradability_potential": tradability_potential,
        "novelty": novelty,
        "crowding_risk": crowding_risk,
        "overfit_risk": overfit_risk,
        "cost_sensitivity": cost_sensitivity,
        "total_score": total,
    }


def normalize_phenomenon(phenomenon: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(phenomenon)
    normalized["scores"] = score_phenomenon(phenomenon)
    normalized["hard_gate_decision"] = validate_phenomenon(phenomenon)["decision"]
    return normalized


def build_candidates_report(*, run_id: str, market: str, phenomena: list[dict[str, Any]]) -> dict[str, Any]:
    normalized = [normalize_phenomenon(item) for item in phenomena]
    return {
        "schema_version": 1,
        "run_id": run_id,
        "market": market,
        "mode": "research_artifact_only",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "phenomena": normalized,
        **SAFETY_FLAGS,
    }


def candidates_to_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Market Phenomenon Candidates",
        "",
        f"run_id: {report.get('run_id')}",
        f"market: {report.get('market')}",
        f"mode: {report.get('mode')}",
        f"strategy_generation_allowed: {report.get('strategy_generation_allowed')}",
        f"backtest_allowed: {report.get('backtest_allowed')}",
        f"queue_write_allowed: {report.get('queue_write_allowed')}",
        "",
        "## Phenomena",
    ]
    for item in report.get("phenomena") or []:
        scores = item.get("scores") or {}
        lines.extend(
            [
                "",
                f"### {item.get('phenomenon_id')}: {item.get('title')}",
                f"- mechanism_source: {item.get('mechanism_source')}",
                f"- hard_gate_decision: {item.get('hard_gate_decision')}",
                f"- total_score: {scores.get('total_score')}",
                f"- participants: {', '.join(item.get('participants') or [])}",
                f"- observable_variables: {', '.join(item.get('observable_variables') or [])}",
                f"- minimal_verification_question: {item.get('minimal_verification_question')}",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def write_candidates_report(report: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "phenomenon_candidates.json"
    markdown_path = out / "phenomenon_candidates.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(candidates_to_markdown(report), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
