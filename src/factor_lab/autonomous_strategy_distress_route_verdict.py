from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def build_distress_route_verdict(*, run_id: str, pit_preflight: dict[str, Any], distress_screen: dict[str, Any]) -> dict[str, Any]:
    if not pit_preflight.get("ready_for_proxy_distress_screen"):
        verdict = "request_data"
        reasons = ["pit_preflight_not_ready"]
    elif distress_screen.get("overall_status") == "manual_review" and distress_screen.get("recommended_next_step") == "manual_review_distress_repaired_screen":
        verdict = "manual_review_before_controlled_backtest"
        reasons = ["distress_screen_repair_candidate_passed"]
    else:
        verdict = "stop_route"
        reasons = ["distress_screen_failed", "bounded_proxy_distress_filters_did_not_repair_drawdown"]
    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "distress_route_verdict",
        "route_id": "quality_cashflow_distress_filter",
        "verdict": verdict,
        "reason_codes": reasons,
        "pit_preflight_decision": pit_preflight.get("decision"),
        "distress_screen_status": distress_screen.get("overall_status"),
        "distress_screen_recommended_next_step": distress_screen.get("recommended_next_step"),
        "best_candidate": distress_screen.get("best_candidate"),
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "blocked_actions": ["controlled_backtest", "queue_write", "timer_enable", "broad_daemon_restore", "auto_promotion", "live_trading"],
    }


def write_distress_route_verdict(verdict: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out=Path(output_dir); out.mkdir(parents=True, exist_ok=True)
    jp=out/'quality_cashflow_distress_route_verdict.json'; mp=out/'quality_cashflow_distress_route_verdict.md'
    jp.write_text(json.dumps(verdict,ensure_ascii=False,indent=2,sort_keys=True),encoding='utf-8')
    mp.write_text(f"# Quality Cashflow Distress Route Verdict\n\nverdict: {verdict.get('verdict')}\nreason_codes: {', '.join(verdict.get('reason_codes') or [])}\ncontrolled_execution_allowed: {verdict.get('controlled_execution_allowed')}\nqueue_write_allowed: {verdict.get('queue_write_allowed')}\n",encoding='utf-8')
    return {'json':jp,'markdown':mp}
