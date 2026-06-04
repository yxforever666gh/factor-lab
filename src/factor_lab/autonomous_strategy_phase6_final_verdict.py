from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

BLOCKED_ACTIONS = [
    "controlled_backtest",
    "queue_write",
    "timer_enable",
    "broad_daemon_restore",
    "auto_promotion",
    "live_trading",
]


def build_phase6_final_verdict(
    *,
    run_id: str,
    controller_state: dict[str, Any],
    pit_overlay_diagnostic: dict[str, Any],
    proxy_pit_alignment: dict[str, Any],
) -> dict[str, Any]:
    controller_ready = controller_state.get("current_state") == "proxy_pit_alignment_passed"
    pit_overlay_passed = pit_overlay_diagnostic.get("decision") == "prepare_proxy_pit_alignment_review" and not pit_overlay_diagnostic.get("low_after_overlay")
    pit_alignment_passed = proxy_pit_alignment.get("decision") == "prepare_proxy_cheap_screen_plan"
    completed = controller_ready and pit_overlay_passed and pit_alignment_passed
    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "phase6_final_verdict",
        "phase": 6,
        "phase_status": "completed" if completed else "blocked",
        "final_state": controller_state.get("current_state"),
        "controller_ready": controller_ready,
        "pit_cache_extended": pit_overlay_passed,
        "pit_overlay_coverage_passed": pit_overlay_passed,
        "pit_overlay_coverage": pit_overlay_diagnostic.get("overlay_coverage") or {},
        "pit_alignment_passed": pit_alignment_passed,
        "pit_alignment_usable_coverage": proxy_pit_alignment.get("usable_coverage"),
        "completed_items": [
            "Hermes-native controller one-shot",
            "PIT cache extension plan",
            "autonomous chunked PIT cache extension runner",
            "combined PIT financial cache",
            "PIT overlay diagnostic",
            "proxy PIT alignment review",
        ] if completed else [],
        "remaining_phase6_items": [] if completed else ["resolve blocked controller/PIT state"],
        "next_phase": "phase7_proxy_cheap_screen_plan" if completed else "continue_phase6_blocker_resolution",
        "recommended_next_step": "write_proxy_cheap_screen_plan" if completed else "inspect_phase6_blockers",
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "blocked_actions": BLOCKED_ACTIONS,
    }


def phase6_final_verdict_to_markdown(verdict: dict[str, Any]) -> str:
    lines = [
        "# Phase 6 Final Verdict",
        "",
        f"phase_status: {verdict.get('phase_status')}",
        f"final_state: {verdict.get('final_state')}",
        f"next_phase: {verdict.get('next_phase')}",
        f"recommended_next_step: {verdict.get('recommended_next_step')}",
        f"controlled_execution_allowed: {verdict.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {verdict.get('queue_write_allowed')}",
        f"timer_enable_allowed: {verdict.get('timer_enable_allowed')}",
        "",
        "## Completion checks",
        f"- controller_ready: {verdict.get('controller_ready')}",
        f"- pit_cache_extended: {verdict.get('pit_cache_extended')}",
        f"- pit_overlay_coverage_passed: {verdict.get('pit_overlay_coverage_passed')}",
        f"- pit_alignment_passed: {verdict.get('pit_alignment_passed')}",
        f"- pit_alignment_usable_coverage: {verdict.get('pit_alignment_usable_coverage')}",
        "",
        "## PIT overlay coverage",
    ]
    for field, value in (verdict.get("pit_overlay_coverage") or {}).items():
        lines.append(f"- {field}: {value}")
    lines += ["", "## Completed items"]
    lines.extend(f"- {item}" for item in verdict.get("completed_items") or [])
    lines += ["", "## Blocked actions"]
    lines.extend(f"- {action}" for action in verdict.get("blocked_actions") or [])
    return "\n".join(lines).rstrip() + "\n"


def write_phase6_final_verdict(verdict: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "phase6_final_verdict.json"
    markdown_path = out / "phase6_final_verdict.md"
    json_path.write_text(json.dumps(verdict, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(phase6_final_verdict_to_markdown(verdict), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
