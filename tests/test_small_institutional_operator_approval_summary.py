import json
from pathlib import Path

from factor_lab.small_institutional_operator_approval_summary import (
    build_operator_approval_summary,
    operator_approval_summary_to_markdown,
    write_operator_approval_summary,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _artifact_paths(tmp_path: Path) -> dict[str, Path]:
    gate_path = _write_json(
        tmp_path / "manual_approval_gate.json",
        {
            "gate_status": "blocked_pending_manual_approval",
            "human_approval_present": False,
            "risk_relaxation_allowed": False,
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
            "automation_allowed": False,
            "automated_rerun_allowed": False,
            "live_trading_enabled": False,
        },
    )
    manual_review_path = _write_json(
        tmp_path / "repair_blocker_manual_review.json",
        {
            "review_status": "blocked_manual_review_required",
            "primary_issue": "drawdown_risk_too_high",
            "repair_status": "blocked_no_drawdown_safe_candidate",
            "candidate_count": 0,
            "best_available_max_drawdown": -0.478256,
            "drawdown_gap_to_limit": 0.128256,
            "recommended_manual_decision": {
                "decision_required": True,
                "dimension": "holding_count",
                "value": "50",
                "automated_rerun_allowed": False,
            },
            "safety": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
            },
        },
    )
    status_path = _write_json(
        tmp_path / "status.json",
        {
            "next_action": "repair_simulated_portfolio_construction",
            "paper_live_promotion_readiness": {"live_trading_enabled": False},
        },
    )
    return {"gate_path": gate_path, "manual_review_path": manual_review_path, "status_path": status_path}


def test_build_operator_approval_summary_compacts_gate_review_and_status(tmp_path):
    paths = _artifact_paths(tmp_path)

    payload = build_operator_approval_summary(**paths)

    assert payload["summary_status"] == "blocked_pending_manual_approval"
    assert payload["approval_required"] is True
    assert payload["required_decision_axis"] == "holding_count=50"
    assert payload["primary_blocker"] == "drawdown_risk_too_high"
    assert payload["best_available_max_drawdown"] == -0.478256
    assert payload["drawdown_gap_to_limit"] == 0.128256
    assert payload["next_action"] == "repair_simulated_portfolio_construction"
    assert payload["safety"] == {
        "queue_write_allowed": False,
        "broad_daemon_allowed": False,
        "automation_allowed": False,
        "automated_rerun_allowed": False,
        "live_trading_enabled": False,
    }


def test_operator_approval_summary_markdown_includes_required_human_checklist(tmp_path):
    payload = build_operator_approval_summary(**_artifact_paths(tmp_path))

    markdown = operator_approval_summary_to_markdown(payload)

    assert "Operator Approval Summary" in markdown
    assert "approved candidate id OR approved risk relaxation" in markdown
    assert "explicit scope" in markdown
    assert "explicit reason" in markdown
    assert "no queue/broad daemon/live trading permission" in markdown
    assert "holding_count=50" in markdown


def test_write_operator_approval_summary_writes_json_and_markdown(tmp_path):
    paths = _artifact_paths(tmp_path)
    json_path = tmp_path / "operator_approval_summary.json"
    markdown_path = tmp_path / "operator_approval_summary.md"

    payload = write_operator_approval_summary(json_path=json_path, markdown_path=markdown_path, **paths)

    assert payload["summary_status"] == "blocked_pending_manual_approval"
    assert json.loads(json_path.read_text(encoding="utf-8"))["required_decision_axis"] == "holding_count=50"
    assert "no queue/broad daemon/live trading permission" in markdown_path.read_text(encoding="utf-8")
