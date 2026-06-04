#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from factor_lab.hermes_correction_state import build_hermes_correction_state, write_state_files
from scripts.inspect_hermes_correction_status import ROOT, determine_correction_status, known_artifact_payloads

DEFAULT_JSON_PATH = Path("artifacts/hermes_correction/current_state.json")
DEFAULT_MARKDOWN_PATH = Path("artifacts/hermes_correction/current_state.md")


def _correction_id(now_iso: str) -> str:
    safe = now_iso.replace(":", "").replace("+00:00", "Z")
    return f"portfolio-simulation-drawdown-blocker-{safe}"


def _next_agent_role(next_action: str) -> str:
    if next_action in {"run_risk_reduction_controlled_executor", "write_risk_reduction_plan"}:
        return "implementer"
    if next_action == "score_risk_reduction_results":
        return "verifier"
    if next_action in {"manual_review_before_admission", "write_blocker_report_or_request_new_mechanism"}:
        return "reviewer"
    return "diagnostician"


def build_state_from_artifacts(*, root: str | Path = ROOT, created_at_utc: str | None = None) -> dict[str, Any]:
    now = created_at_utc or datetime.now(timezone.utc).isoformat()
    source_artifacts = known_artifact_payloads(root)
    diagnosis = determine_correction_status(source_artifacts)
    next_action = str(diagnosis.get("next_action") or "run_diagnostician_agent")
    return build_hermes_correction_state(
        correction_id=_correction_id(now),
        created_at_utc=now,
        failure_target="portfolio_simulation_drawdown_blocker",
        source_artifacts=source_artifacts,
        diagnosis=diagnosis,
        allowed_actions=[
            "read_artifacts",
            "write_correction_artifacts",
            "run_targeted_pytest",
            "run_py_compile",
        ],
        forbidden_actions=[
            "no_workflow_queue_write",
            "no_systemd_change",
            "no_timer_enable",
            "no_broad_daemon_restore",
            "no_auto_promotion",
            "no_local_persistent_hermes_cli_agent",
            "no_factor_lab_provider_model_profile_change",
        ],
        next_agent_role=_next_agent_role(next_action),
    )


def write_hermes_correction_state(
    *,
    root: str | Path = ROOT,
    json_path: str | Path | None = None,
    markdown_path: str | Path | None = None,
) -> dict[str, Any]:
    base = Path(root)
    payload = build_state_from_artifacts(root=base)
    json_out = Path(json_path) if json_path is not None else base / DEFAULT_JSON_PATH
    markdown_out = Path(markdown_path) if markdown_path is not None else base / DEFAULT_MARKDOWN_PATH
    write_state_files(payload, json_path=json_out, markdown_path=markdown_out)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write conservative Hermes correction state artifacts.")
    parser.add_argument("--root", default=str(ROOT))
    parser.add_argument("--json-path", default=None)
    parser.add_argument("--markdown-path", default=None)
    args = parser.parse_args(argv)
    payload = write_hermes_correction_state(root=args.root, json_path=args.json_path, markdown_path=args.markdown_path)
    print(
        json.dumps(
            {
                "correction_status": payload.get("diagnosis", {}).get("correction_status"),
                "next_action": payload.get("diagnosis", {}).get("next_action"),
                "next_agent_role": payload.get("next_agent_role"),
                "json_path": args.json_path or str(Path(args.root) / DEFAULT_JSON_PATH),
                "markdown_path": args.markdown_path or str(Path(args.root) / DEFAULT_MARKDOWN_PATH),
                "manual_review_required": payload.get("manual_review_required"),
                "queue_write_allowed": payload.get("queue_write_allowed"),
                "automation_allowed": payload.get("automation_allowed"),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
