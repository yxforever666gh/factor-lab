from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.market_phenomena_data import build_data_feasibility_review, update_data_requests
from factor_lab.market_phenomena_experiment_plan import build_minimal_verification_plan, write_minimal_verification_plan
from factor_lab.market_phenomena_experiment_runner import build_minimal_verification_result, write_minimal_verification_result
from factor_lab.market_phenomena_generator import build_seed_candidates_report
from factor_lab.market_phenomena_memory import empty_phenomena_memory, upsert_phenomenon_verdict, write_phenomena_memory
from factor_lab.market_phenomena_novelty import build_novelty_review, write_novelty_review
from factor_lab.market_phenomena_quality import build_quality_review, write_quality_review
from factor_lab.market_phenomena_schema import SAFETY_FLAGS, write_candidates_report
from factor_lab.market_phenomena_verdict import build_phenomenon_verdict_report, write_phenomenon_verdict_report


def build_research_loop_report(
    *,
    run_id: str,
    candidates: dict[str, Any],
    quality_review: dict[str, Any],
    novelty_review: dict[str, Any],
    data_feasibility: dict[str, Any],
    minimal_plan: dict[str, Any],
    minimal_result: dict[str, Any],
    verdict: dict[str, Any],
    artifacts: dict[str, str],
) -> dict[str, Any]:
    summary = {
        "phenomenon_count": len(candidates.get("phenomena") or []),
        "quality_keep": (quality_review.get("summary") or {}).get("keep", 0),
        "novelty_keep": (novelty_review.get("summary") or {}).get("keep", 0),
        "ready_for_minimal_verification": (data_feasibility.get("summary") or {}).get("ready_for_minimal_verification", 0),
        "planned": (minimal_plan.get("summary") or {}).get("planned", 0),
        "experiment_count": (minimal_result.get("summary") or {}).get("experiment_count", 0),
    }
    summary.update(verdict.get("summary") or {})
    return {
        "schema_version": 1,
        "run_id": run_id,
        "mode": "market_phenomena_research_loop_artifact_only",
        "loop_status": "completed",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "description": "Market phenomenon research loop; not automatic strategy, not backtest, not queue, not daemon.",
        "steps_completed": [
            "candidates",
            "quality_review",
            "novelty_review",
            "data_feasibility",
            "minimal_verification_plan",
            "minimal_verification_result",
            "phenomenon_verdict",
            "memory_update",
        ],
        "summary": summary,
        "artifacts": artifacts,
        **SAFETY_FLAGS,
    }


def run_research_loop(
    *,
    run_id: str,
    feature_frame: pd.DataFrame,
    market: str = "cn_equity_daily",
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    candidates = build_seed_candidates_report(run_id=f"{run_id}_candidates", market=market)
    quality = build_quality_review(run_id=f"{run_id}_quality", candidates_report=candidates)
    memory = empty_phenomena_memory()
    novelty = build_novelty_review(run_id=f"{run_id}_novelty", quality_review=quality, candidates_report=candidates, memory=memory)
    data = build_data_feasibility_review(run_id=f"{run_id}_data", candidates_report=candidates, novelty_review=novelty)
    plan = build_minimal_verification_plan(run_id=f"{run_id}_minimal_plan", data_feasibility_review=data)
    result = build_minimal_verification_result(run_id=f"{run_id}_minimal_result", plan_report=plan, feature_frame=feature_frame)
    verdict = build_phenomenon_verdict_report(run_id=f"{run_id}_verdict", minimal_result_report=result)
    for item in verdict.get("verdicts") or []:
        memory = upsert_phenomenon_verdict(memory, item)
    data_requests = update_data_requests({"schema_version": 1, "requests": []}, data)

    artifacts = {
        "phenomenon_candidates": "phenomenon_candidates.json",
        "phenomenon_quality_review": "phenomenon_quality_review.json",
        "phenomenon_novelty_review": "phenomenon_novelty_review.json",
        "phenomenon_data_feasibility": "phenomenon_data_feasibility.json",
        "minimal_verification_plan": "minimal_verification_plan.json",
        "minimal_verification_result": "minimal_verification_result.json",
        "phenomenon_verdict": "phenomenon_verdict.json",
        "market_phenomena_memory": "market_phenomena_memory.json",
        "market_phenomena_data_requests": "market_phenomena_data_requests.json",
    }

    if output_dir is not None:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        artifacts = {
            "phenomenon_candidates": str(write_candidates_report(candidates, out)["json"]),
            "phenomenon_quality_review": str(write_quality_review(quality, out)["json"]),
            "phenomenon_novelty_review": str(write_novelty_review(novelty, out)["json"]),
            "phenomenon_data_feasibility": str(_write_json_md(data, out, "phenomenon_data_feasibility", _data_feasibility_markdown(data))["json"]),
            "minimal_verification_plan": str(write_minimal_verification_plan(plan, out)["json"]),
            "minimal_verification_result": str(write_minimal_verification_result(result, out)["json"]),
            "phenomenon_verdict": str(write_phenomenon_verdict_report(verdict, out)["json"]),
        }
        memory_paths = write_phenomena_memory(memory, out / "market_phenomena_memory.json", out / "market_phenomena_lessons.md")
        artifacts["market_phenomena_memory"] = str(memory_paths["json"])
        requests_path = out / "market_phenomena_data_requests.json"
        requests_path.write_text(json.dumps(data_requests, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        artifacts["market_phenomena_data_requests"] = str(requests_path)

    return build_research_loop_report(
        run_id=run_id,
        candidates=candidates,
        quality_review=quality,
        novelty_review=novelty,
        data_feasibility=data,
        minimal_plan=plan,
        minimal_result=result,
        verdict=verdict,
        artifacts=artifacts,
    )


def _data_feasibility_markdown(data: dict[str, Any]) -> str:
    from factor_lab.market_phenomena_data import data_feasibility_to_markdown

    return data_feasibility_to_markdown(data)


def _write_json_md(payload: dict[str, Any], output_dir: Path, stem: str, markdown: str) -> dict[str, Path]:
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(markdown, encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}


def research_loop_report_to_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Market Phenomena Research Loop",
        "",
        f"run_id: {report.get('run_id')}",
        f"loop_status: {report.get('loop_status')}",
        "scope: artifact-only; not automatic strategy, not backtest, not queue, not daemon",
        f"strategy_generation_allowed: {report.get('strategy_generation_allowed')}",
        f"backtest_allowed: {report.get('backtest_allowed')}",
        f"queue_write_allowed: {report.get('queue_write_allowed')}",
        "",
        "## Steps completed",
    ]
    lines.extend(f"- {step}" for step in report.get("steps_completed") or [])
    lines.append("")
    lines.append("## Summary")
    for key, value in (report.get("summary") or {}).items():
        lines.append(f"- {key}: {value}")
    lines.append("")
    lines.append("## Artifacts")
    for key, value in (report.get("artifacts") or {}).items():
        lines.append(f"- {key}: {value}")
    return "\n".join(lines).rstrip() + "\n"


def write_research_loop_report(report: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "research_loop.json"
    markdown_path = out / "research_loop.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(research_loop_report_to_markdown(report), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
