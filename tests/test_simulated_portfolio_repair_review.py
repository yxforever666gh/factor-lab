import json

from factor_lab.simulated_portfolio_repair_review import (
    build_repair_blocker_review,
    load_repair_diagnostics,
    repair_review_to_markdown,
    write_repair_blocker_review,
)


def test_load_repair_diagnostics_returns_empty_for_missing_file(tmp_path):
    assert load_repair_diagnostics(tmp_path / "missing.json") == {}


def test_build_repair_blocker_review_surfaces_blocked_no_safe_candidate():
    repair = {
        "repair_status": "blocked_no_drawdown_safe_candidate",
        "candidate_count": 0,
        "recommended_candidate": None,
        "automation_allowed": False,
        "best_available_max_drawdown": -0.478256,
        "drawdown_gap_to_limit": 0.128256,
    }

    review = build_repair_blocker_review(repair)

    assert review["review_status"] == "manual_review_required"
    assert review["primary_blocker"] == "blocked_no_drawdown_safe_candidate"
    assert review["best_available_max_drawdown"] == -0.478256
    assert review["drawdown_gap_to_limit"] == 0.128256
    assert review["automation_allowed"] is False
    assert review["manual_decision_required"] is True
    assert review["recommended_action"] == "manual_review_drawdown_tradeoff_before_any_automation"


def test_build_repair_blocker_review_handles_missing_repair_diagnostics():
    review = build_repair_blocker_review({})

    assert review["review_status"] == "missing_repair_diagnostics"
    assert review["primary_blocker"] == "missing_repair_diagnostics"
    assert review["automation_allowed"] is False
    assert review["manual_decision_required"] is True


def test_write_repair_blocker_review_writes_json_and_markdown(tmp_path):
    repair_path = tmp_path / "portfolio_construction_repair.json"
    json_path = tmp_path / "repair_blocker_review.json"
    markdown_path = tmp_path / "repair_blocker_review.md"
    repair_path.write_text(
        json.dumps(
            {
                "repair_status": "blocked_no_drawdown_safe_candidate",
                "candidate_count": 0,
                "automation_allowed": False,
                "best_available_max_drawdown": -0.478256,
                "drawdown_gap_to_limit": 0.128256,
            }
        ),
        encoding="utf-8",
    )

    payload = write_repair_blocker_review(
        repair_path=repair_path,
        json_path=json_path,
        markdown_path=markdown_path,
    )

    assert payload["manual_decision_required"] is True
    assert json.loads(json_path.read_text(encoding="utf-8"))["primary_blocker"] == "blocked_no_drawdown_safe_candidate"
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "Simulated Portfolio Repair Blocker Review" in markdown
    assert "manual_decision_required: True" in markdown


def test_repair_review_markdown_includes_blocker_and_drawdown_gap():
    markdown = repair_review_to_markdown(
        {
            "generated_at_utc": "2026-05-21T00:00:00+00:00",
            "review_status": "manual_review_required",
            "primary_blocker": "blocked_no_drawdown_safe_candidate",
            "best_available_max_drawdown": -0.478256,
            "drawdown_gap_to_limit": 0.128256,
            "automation_allowed": False,
            "manual_decision_required": True,
            "recommended_action": "manual_review_drawdown_tradeoff_before_any_automation",
        }
    )

    assert "blocked_no_drawdown_safe_candidate" in markdown
    assert "drawdown_gap_to_limit" in markdown
    assert "manual_review_drawdown_tradeoff" in markdown
