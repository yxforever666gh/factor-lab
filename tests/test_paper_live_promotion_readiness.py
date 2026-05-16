import json

from factor_lab.paper_live_promotion_readiness import (
    build_paper_live_promotion_readiness,
    evaluate_paper_live_promotion_readiness,
    paper_live_promotion_readiness_to_markdown,
    write_paper_live_promotion_readiness,
)


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _status(min_observations=1):
    return {
        "decision": "ready_for_portfolio_mvp",
        "next_action": "paper_live_promotion_readiness_review",
        "runtime_safety": {"safe": True, "would_run_count": 0, "claimable_workflow_count": 0},
        "policy": {"paper_live_min_observations": min_observations},
    }


def _constraint(status="pass"):
    return {"constraint_status": status, "violations": [] if status == "pass" else ["single_name_weight_cap_breached"], "warnings": []}


def _portfolio():
    return {"strategy_name": "small_institutional_value_sleeve_mvp", "as_of_date": "2021-12-28", "position_count": 72}


def _tracking(observation_count=1):
    return {
        "tracking_status": "ok",
        "portfolio_return": {"portfolio_forward_return": 0.01842, "matched_position_count": 72, "missing_position_count": 0},
        "observation_count": observation_count,
    }


def _dry_run(would_run_count=0):
    return {"would_run_count": would_run_count, "blocked_count": 0, "claimable_workflow_count": would_run_count}


def _audit(recommendations=None):
    return {"recommendations": recommendations or ["pause_broad_daemon", "allow_controlled_only_daemon"]}


def test_readiness_blocks_when_constraint_hardening_is_missing_or_failed():
    missing = evaluate_paper_live_promotion_readiness(_status(), {}, _portfolio(), _tracking(), _dry_run(), _audit())
    failed = evaluate_paper_live_promotion_readiness(_status(), _constraint("fail"), _portfolio(), _tracking(), _dry_run(), _audit())

    assert missing["readiness_status"] == "blocked"
    assert "constraint_hardening_not_passed" in missing["blockers"]
    assert failed["readiness_status"] == "blocked"
    assert "constraint_hardening_not_passed" in failed["blockers"]


def test_readiness_waits_when_paper_observation_count_is_below_minimum():
    result = evaluate_paper_live_promotion_readiness(
        _status(min_observations=3),
        _constraint("pass"),
        _portfolio(),
        _tracking(observation_count=1),
        _dry_run(),
        _audit(),
    )

    assert result["readiness_status"] == "wait"
    assert "insufficient_paper_observations" in result["warnings"]
    assert result["checks"]["paper_observations"]["actual"] == 1
    assert result["checks"]["paper_observations"]["minimum"] == 3


def test_readiness_can_be_ready_for_manual_approval_without_enabling_live_trading():
    result = evaluate_paper_live_promotion_readiness(
        _status(min_observations=1),
        _constraint("pass"),
        _portfolio(),
        _tracking(observation_count=1),
        _dry_run(),
        _audit(),
    )

    assert result["readiness_status"] == "ready_for_manual_approval"
    assert result["blockers"] == []
    assert result["manual_approval_required"] is True
    assert result["live_trading_enabled"] is False


def test_readiness_blocks_when_runtime_is_not_safe_or_has_claimable_workflows():
    unsafe = evaluate_paper_live_promotion_readiness(_status(), _constraint("pass"), _portfolio(), _tracking(), _dry_run(), _audit(["restore_broad_daemon"]))
    claimable = evaluate_paper_live_promotion_readiness(_status(), _constraint("pass"), _portfolio(), _tracking(), _dry_run(2), _audit())

    assert unsafe["readiness_status"] == "blocked"
    assert "runtime_not_controlled_safe" in unsafe["blockers"]
    assert claimable["readiness_status"] == "blocked"
    assert "claimable_workflows_not_empty" in claimable["blockers"]


def test_build_readiness_loads_inputs_and_markdown(tmp_path):
    status = _write_json(tmp_path / "status.json", _status())
    constraint = _write_json(tmp_path / "constraint.json", _constraint("pass"))
    portfolio = _write_json(tmp_path / "portfolio.json", _portfolio())
    tracking = _write_json(tmp_path / "tracking.json", _tracking())
    dry_run = _write_json(tmp_path / "dry_run.json", _dry_run())
    audit = _write_json(tmp_path / "audit.json", _audit())

    payload = build_paper_live_promotion_readiness(
        status_path=status,
        constraint_hardening_path=constraint,
        portfolio_path=portfolio,
        retrospective_tracking_path=tracking,
        dry_run_path=dry_run,
        runtime_audit_path=audit,
    )

    assert payload["readiness_status"] == "ready_for_manual_approval"
    markdown = paper_live_promotion_readiness_to_markdown(payload)
    assert "Paper/Live Promotion Readiness" in markdown
    assert "ready_for_manual_approval" in markdown


def test_write_readiness_writes_json_and_markdown(tmp_path):
    status = _write_json(tmp_path / "status.json", _status())
    constraint = _write_json(tmp_path / "constraint.json", _constraint("pass"))
    portfolio = _write_json(tmp_path / "portfolio.json", _portfolio())
    tracking = _write_json(tmp_path / "tracking.json", _tracking())
    dry_run = _write_json(tmp_path / "dry_run.json", _dry_run())
    audit = _write_json(tmp_path / "audit.json", _audit())
    json_path = tmp_path / "paper_live_promotion_readiness.json"
    markdown_path = tmp_path / "paper_live_promotion_readiness.md"

    payload = write_paper_live_promotion_readiness(
        status_path=status,
        constraint_hardening_path=constraint,
        portfolio_path=portfolio,
        retrospective_tracking_path=tracking,
        dry_run_path=dry_run,
        runtime_audit_path=audit,
        json_path=json_path,
        markdown_path=markdown_path,
    )

    assert payload["readiness_status"] == "ready_for_manual_approval"
    assert json.loads(json_path.read_text(encoding="utf-8"))["readiness_status"] == "ready_for_manual_approval"
    assert "manual approval" in markdown_path.read_text(encoding="utf-8").lower()
