from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SAFE_ACTIONS = {
    "write_proxy_workstream_report": ["python", "scripts/write_autonomous_strategy_proxy_workstream_report.py"],
    "write_proxy_route_verdict": ["python", "scripts/write_autonomous_strategy_proxy_route_verdict.py"],
    "run_proxy_cheap_screen_execution": ["python", "scripts/run_autonomous_strategy_proxy_cheap_screen.py"],
    "write_proxy_cheap_screen_plan": ["python", "scripts/write_autonomous_strategy_proxy_cheap_screen_plan.py"],
    "prove_proxy_report_date_alignment": ["python", "scripts/write_autonomous_strategy_proxy_pit_alignment.py"],
    "write_proxy_cheap_screen_plan": ["python", "scripts/write_autonomous_strategy_proxy_cheap_screen_plan.py"],
    "run_autonomous_pit_cache_extension": ["python", "scripts/run_autonomous_strategy_pit_cache_extension.py", "--max-tickers", "10"],
    "prepare_pit_cache_extension_plan": ["python", "scripts/write_autonomous_strategy_pit_cache_extension_plan.py"],
    "request_new_mechanism_or_revisit_risk_model": ["python", "scripts/write_autonomous_strategy_next_workstream_decision.py"],
}

FORBIDDEN_ACTION_WORDS = ["backtest", "queue", "timer", "daemon", "live_trading", "auto_promotion"]
BLOCKED_ACTIONS = ["controlled_backtest", "queue_write", "timer_enable", "broad_daemon_restore", "auto_promotion", "live_trading"]


def load_controller_state(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def dispatch_once(*, run_id: str, controller_state: dict[str, Any], workdir: str | Path = ".", max_seconds: int = 300) -> dict[str, Any]:
    action = controller_state.get("recommended_next_step")
    allowed = controller_state.get("controlled_execution_allowed") is False and controller_state.get("queue_write_allowed") is False and controller_state.get("timer_enable_allowed") is False
    if not allowed:
        status = "blocked_unsafe_controller_state"
        command = None
        result = None
    elif action not in SAFE_ACTIONS:
        status = "blocked_no_registered_safe_action"
        command = None
        result = None
    elif any(word in str(action) for word in FORBIDDEN_ACTION_WORDS):
        status = "blocked_forbidden_action_name"
        command = None
        result = None
    else:
        command = SAFE_ACTIONS[action]
        proc = subprocess.run(command, cwd=workdir, text=True, capture_output=True, timeout=max_seconds)
        status = "completed" if proc.returncode == 0 else "failed"
        result = {"returncode": proc.returncode, "stdout": proc.stdout[-4000:], "stderr": proc.stderr[-4000:]}
    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "autonomous_strategy_dispatch_once",
        "controller_state": controller_state.get("current_state"),
        "recommended_next_step": action,
        "dispatch_status": status,
        "command": command,
        "result": result,
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "blocked_actions": BLOCKED_ACTIONS,
    }


def write_dispatch_report(report: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "dispatch_once.json"
    markdown_path = out / "dispatch_once.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    lines = [
        "# Autonomous Strategy Dispatch Once",
        "",
        f"controller_state: {report.get('controller_state')}",
        f"recommended_next_step: {report.get('recommended_next_step')}",
        f"dispatch_status: {report.get('dispatch_status')}",
        f"command: {report.get('command')}",
        f"controlled_execution_allowed: {report.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {report.get('queue_write_allowed')}",
        "",
        "## Blocked actions",
    ]
    lines.extend(f"- {action}" for action in report.get("blocked_actions") or [])
    markdown_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
