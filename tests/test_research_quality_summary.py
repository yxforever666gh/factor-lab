import json
from pathlib import Path

from factor_lab.research_quality_summary import build_research_quality_summary, write_research_quality_summary


def test_build_research_quality_summary_combines_gate_budget_coverage_and_value_routes(tmp_path):
    gate_log = tmp_path / "research_gate_decisions.jsonl"
    gate_log.write_text(
        '\n'.join(
            [
                json.dumps({"decision": "block", "reasons": ["missing required data fields: debt_to_asset"], "budget_bucket": "mechanism_validation"}),
                json.dumps({"decision": "allow", "reasons": [], "budget_bucket": "robustness_validation"}),
            ]
        ),
        encoding="utf-8",
    )

    summary = build_research_quality_summary(
        gate_decision_path=gate_log,
        available_fields={"industry", "book_yield", "earnings_yield", "roe", "momentum_60_skip_5", "pb", "pe_ttm"},
    )

    assert summary["gate_decisions"]["by_decision"]["block"] == 1
    assert summary["gate_decisions"]["by_budget_bucket"]["mechanism_validation"] == 1
    assert summary["data_coverage"]["summary"]["total_templates"] >= 1
    assert any(route["route_id"] == "industry_relative_value" for route in summary["value_research_routes"]["ready"])


def test_write_research_quality_summary_writes_json_and_markdown(tmp_path):
    json_path = tmp_path / "research_quality_summary.json"
    md_path = tmp_path / "research_quality_summary.md"

    payload = write_research_quality_summary(json_path=json_path, markdown_path=md_path, available_fields={"industry", "book_yield", "roe"})

    assert json.loads(json_path.read_text(encoding="utf-8"))["schema_version"] == payload["schema_version"]
    text = md_path.read_text(encoding="utf-8")
    assert "Research Quality Summary" in text
    assert "Value research routes" in text


def test_research_quality_summary_includes_controlled_runtime_ledger(tmp_path):
    ledger_summary = tmp_path / "controlled_run_ledger_summary.json"
    ledger_summary.write_text(json.dumps({
        "total": 3,
        "by_status": {"finished": 2, "failed": 1},
        "main_blockers": {"coverage_too_low": 1, "too_many_split_failures": 2},
        "route_summary": {"value_quality_no_distress": {"run_count": 2, "pass_gate_count": 1}},
    }), encoding="utf-8")

    payload = build_research_quality_summary(
        available_fields={"industry", "book_yield", "earnings_yield", "roe", "momentum_60_skip_5", "pb", "pe_ttm"},
        controlled_ledger_summary_path=ledger_summary,
    )

    assert payload["controlled_runtime"]["total"] == 3
    assert payload["controlled_runtime"]["main_blockers"]["too_many_split_failures"] == 2
    assert "value_quality_no_distress" in payload["controlled_runtime"]["route_summary"]


def test_research_quality_summary_includes_value_sleeve_decision(tmp_path):
    sleeve_path = tmp_path / "value_sleeve_decision.json"
    sleeve_path.write_text(json.dumps({
        "decision": "collapse_to_value_sleeve_with_primary_route",
        "primary_route": "value_quality_no_distress",
        "confirmation_route": "value_momentum_confirmation",
        "recommended_next_action": "implement_value_sleeve_policy_after_user_go",
    }), encoding="utf-8")
    payload = build_research_quality_summary(
        available_fields={"industry", "book_yield", "earnings_yield", "roe", "momentum_60_skip_5", "pb", "pe_ttm"},
        value_sleeve_decision_path=sleeve_path,
    )
    assert payload["value_sleeve_validation"]["decision"] == "collapse_to_value_sleeve_with_primary_route"

    json_path = tmp_path / "summary.json"
    md_path = tmp_path / "summary.md"
    write_research_quality_summary(
        json_path=json_path,
        markdown_path=md_path,
        available_fields={"industry", "book_yield", "earnings_yield", "roe", "momentum_60_skip_5", "pb", "pe_ttm"},
        value_sleeve_decision_path=sleeve_path,
    )
    text = md_path.read_text(encoding="utf-8")
    assert "Value sleeve validation" in text
    assert "Broad daemon restoration remains forbidden" in text


def test_research_quality_summary_includes_value_sleeve_policy(tmp_path):
    policy_path = tmp_path / "value_sleeve_policy.json"
    policy_path.write_text(json.dumps({
        "decision": "collapse_to_value_sleeve_with_primary_route",
        "primary_route": "value_quality_no_distress",
        "confirmation_route": "value_momentum_confirmation",
        "low_weight_route": "industry_relative_value",
        "routes": {
            "value_quality_no_distress": {"role": "primary", "action": "prioritize_primary"},
            "value_momentum_confirmation": {"role": "confirmation", "action": "confirmation_only"},
            "industry_relative_value": {"role": "low_weight_core_value", "action": "cap_or_skip_duplicate"},
        },
    }), encoding="utf-8")

    payload = build_research_quality_summary(
        available_fields={"industry", "book_yield", "earnings_yield", "roe", "momentum_60_skip_5", "pb", "pe_ttm"},
        value_sleeve_policy_path=policy_path,
    )

    assert payload["value_sleeve_policy"]["decision"] == "collapse_to_value_sleeve_with_primary_route"
    assert payload["value_sleeve_policy"]["routes"]["value_quality_no_distress"]["role"] == "primary"

    json_path = tmp_path / "summary.json"
    md_path = tmp_path / "summary.md"
    write_research_quality_summary(
        json_path=json_path,
        markdown_path=md_path,
        available_fields={"industry", "book_yield", "earnings_yield", "roe", "momentum_60_skip_5", "pb", "pe_ttm"},
        value_sleeve_policy_path=policy_path,
    )
    text = md_path.read_text(encoding="utf-8")
    assert "Value sleeve policy" in text
    assert "Primary route: value_quality_no_distress" in text
    assert "Confirmation route: value_momentum_confirmation" in text
    assert "Low-weight route: industry_relative_value" in text
