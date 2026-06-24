from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.market_phenomena_schema import SAFETY_FLAGS


def _common_verdict_fields(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "phenomenon_id": result.get("phenomenon_id"),
        "title": result.get("title"),
        "minimal_result_status": result.get("result_status"),
        "target_group": result.get("target_group"),
        "spread_vs_control": result.get("spread_vs_control"),
        "usable_row_count": result.get("usable_row_count"),
        "usable_ticker_count": result.get("usable_ticker_count"),
        "strategy_design_allowed": False,
        "human_approval_required_for_strategy_phase": True,
        **SAFETY_FLAGS,
    }


def verdict_for_experiment_result(result: dict[str, Any]) -> dict[str, Any]:
    status = result.get("result_status")
    spread = result.get("spread_vs_control")
    base = _common_verdict_fields(result)
    if status in {"blocked_missing_columns", "blocked_missing_data"}:
        base.update(
            {
                "verdict": "blocked_missing_data",
                "reason_codes": ["minimal_verification_blocked_missing_data"],
                "what_was_learned": ["The phenomenon cannot be evaluated until required fields are available or derivable."],
                "what_failed": [f"missing_columns={result.get('missing_columns') or []}"],
                "do_not_repeat": ["do not force a verification result when required fields are missing"],
                "next_research_question": "Can missing fields be derived safely, or should this phenomenon stay in data request backlog?",
            }
        )
        return base
    if status == "insufficient_sample":
        base.update(
            {
                "verdict": "blocked_insufficient_sample",
                "reason_codes": ["minimal_verification_insufficient_sample"],
                "what_was_learned": ["The current feature frame does not provide enough usable rows/tickers for a reliable minimal check."],
                "what_failed": ["sample gate failed"],
                "do_not_repeat": ["do not interpret insufficient-sample diagnostics as alpha evidence"],
                "next_research_question": "Which broader feature frame or universe should be used for this phenomenon?",
            }
        )
        return base
    if status == "pass" and spread is not None and spread > 0:
        base.update(
            {
                "verdict": "supported_for_further_research",
                "reason_codes": ["positive_spread_vs_control", "minimal_verification_passed", "strategy_phase_still_requires_human_approval"],
                "what_was_learned": [
                    f"Target group {result.get('target_group')} showed positive spread_vs_control={spread} in the minimal distribution check.",
                    "This supports further research on the phenomenon, not immediate strategy generation.",
                ],
                "what_failed": [],
                "do_not_repeat": ["do not convert this directly into a strategy before regime/risk robustness review"],
                "next_research_question": "Does the phenomenon survive industry, size, regime, turnover, and drawdown-sensitivity splits?",
            }
        )
        return base
    base.update(
        {
            "verdict": "rejected_failed_verification",
            "reason_codes": ["negative_or_zero_spread_vs_control", "minimal_verification_failed"],
            "what_was_learned": [
                f"Target group {result.get('target_group')} did not beat controls in the minimal distribution check.",
                f"Observed spread_vs_control={spread}.",
            ],
            "what_failed": ["minimal verification did not support the phenomenon"],
            "do_not_repeat": ["不要把这个现象简化成静态指标组合后继续生成策略", "do not repeat without a changed market mechanism or regime condition"],
            "next_research_question": "Is there a stricter regime condition or different participant constraint that would make this phenomenon testable again?",
        }
    )
    return base


def build_phenomenon_verdict_report(*, run_id: str, minimal_result_report: dict[str, Any]) -> dict[str, Any]:
    verdicts = [verdict_for_experiment_result(item) for item in minimal_result_report.get("results") or []]
    summary = Counter(item["verdict"] for item in verdicts)
    return {
        "schema_version": 1,
        "run_id": run_id,
        "mode": "phenomenon_verdict_artifact_only",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_minimal_result_run_id": minimal_result_report.get("run_id"),
        "verdicts": verdicts,
        "summary": dict(sorted(summary.items())),
        **SAFETY_FLAGS,
    }


def phenomenon_verdict_to_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Market Phenomenon Verdict",
        "",
        f"run_id: {report.get('run_id')}",
        f"mode: {report.get('mode')}",
        f"strategy_generation_allowed: {report.get('strategy_generation_allowed')}",
        f"backtest_allowed: {report.get('backtest_allowed')}",
        f"queue_write_allowed: {report.get('queue_write_allowed')}",
        "",
        "## Summary",
    ]
    for key, value in (report.get("summary") or {}).items():
        lines.append(f"- {key}: {value}")
    lines.append("")
    lines.append("## Verdicts")
    for item in report.get("verdicts") or []:
        lines.extend([
            "",
            f"### {item.get('phenomenon_id')}: {item.get('title')}",
            f"- verdict: {item.get('verdict')}",
            f"- spread_vs_control: {item.get('spread_vs_control')}",
            f"- strategy_design_allowed: {item.get('strategy_design_allowed')}",
            f"- next_research_question: {item.get('next_research_question')}",
            "- reason_codes:",
        ])
        lines.extend(f"  - {code}" for code in item.get("reason_codes") or [])
        lines.append("- do_not_repeat:")
        lines.extend(f"  - {entry}" for entry in item.get("do_not_repeat") or [])
    return "\n".join(lines).rstrip() + "\n"


def write_phenomenon_verdict_report(report: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "phenomenon_verdict.json"
    markdown_path = out / "phenomenon_verdict.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(phenomenon_verdict_to_markdown(report), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
