from __future__ import annotations

import json
from pathlib import Path

from factor_lab.autonomous_strategy_worker_verdict import build_worker_verdict, worker_verdict_to_markdown, write_worker_verdict


def write_response(base: Path, worker_key: str, decision: str, reasons: list[str]) -> None:
    (base / f"{worker_key}_response.json").write_text(json.dumps({
        "schema_version": 1,
        "worker_key": worker_key,
        "decision_recommendation": decision,
        "reason_codes": reasons,
        "requested_actions": ["write_blocker_report"],
        "forbidden_actions_observed": [],
        "summary": f"{worker_key} summary",
    }), encoding="utf-8")


def test_build_worker_verdict_uses_consensus_request_data(tmp_path):
    write_response(tmp_path, "factor_lab_diagnostician", "request_data", ["drawdown_blocker"])
    write_response(tmp_path, "factor_lab_mechanism_researcher", "request_data", ["new_data_required"])
    write_response(tmp_path, "factor_lab_data_steward", "request_data", ["missing_fields"])
    write_response(tmp_path, "factor_lab_reviewer", "request_data", ["worker_consensus_request_data"])

    verdict = build_worker_verdict(tmp_path, run_id="x")

    assert verdict["schema_version"] == 1
    assert verdict["run_id"] == "x"
    assert verdict["worker_count"] == 4
    assert verdict["valid_response_count"] == 4
    assert verdict["consensus_decision"] == "request_data"
    assert verdict["controlled_execution_allowed"] is False
    assert verdict["queue_write_allowed"] is False
    assert "drawdown_blocker" in verdict["reason_codes"]


def test_worker_verdict_markdown_and_write(tmp_path):
    write_response(tmp_path, "factor_lab_diagnostician", "request_data", ["drawdown_blocker"])
    verdict = build_worker_verdict(tmp_path, run_id="x")

    markdown = worker_verdict_to_markdown(verdict)
    assert "Autonomous Strategy Worker Verdict" in markdown
    assert "consensus_decision: request_data" in markdown

    paths = write_worker_verdict(verdict, tmp_path)
    assert paths["json"].exists()
    assert paths["markdown"].exists()
    payload = json.loads(paths["json"].read_text())
    assert payload["consensus_decision"] == "request_data"
