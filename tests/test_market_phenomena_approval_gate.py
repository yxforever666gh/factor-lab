from __future__ import annotations

import json

from factor_lab.market_phenomena_approval_gate import (
    build_strategy_design_approval_stub,
    validate_strategy_design_approval_stub,
    strategy_design_approval_stub_to_markdown,
    write_strategy_design_approval_stub,
)


def strategy_design_spec():
    return {
        "run_id": "spec",
        "phenomenon_id": "p1",
        "supported_horizons": [20],
        "review_scope": ["20d"],
        "strategy_design_review_allowed": True,
        "strategy_generation_allowed": False,
        "human_approval_required": True,
        "production_execution_allowed": False,
        "queue_write_allowed": False,
        "live_trading_allowed": False,
        "auto_promotion_allowed": False,
    }


def test_approval_stub_defaults_to_pending_and_blocks_prototype_generation():
    report = build_strategy_design_approval_stub(run_id="approval", strategy_design_spec=strategy_design_spec())
    stub = report["strategy_design_approval_stub"]
    gate = report["strategy_design_prototype_gate"]
    assert report["mode"] == "strategy_design_approval_gate"
    assert stub["approval_status"] == "pending_human_review"
    assert stub["requested_horizons"] == [20]
    assert stub["approved_horizons"] == []
    assert gate["prototype_generation_allowed"] is False
    assert gate["production_execution_allowed"] is False
    assert gate["queue_write_allowed"] is False


def test_approval_stub_can_record_explicit_approval_without_opening_production():
    report = build_strategy_design_approval_stub(
        run_id="approval",
        strategy_design_spec=strategy_design_spec(),
        approval_status="approved",
        approved_horizons=[20],
        reviewer="human",
    )
    stub = report["strategy_design_approval_stub"]
    gate = report["strategy_design_prototype_gate"]
    assert stub["approval_status"] == "approved"
    assert gate["prototype_generation_allowed"] is True
    assert gate["approved_horizons"] == [20]
    assert gate["production_execution_allowed"] is False
    assert gate["live_trading_allowed"] is False


def test_approval_stub_rejects_unrequested_horizon_approval():
    report = build_strategy_design_approval_stub(
        run_id="approval",
        strategy_design_spec=strategy_design_spec(),
        approval_status="approved",
        approved_horizons=[60],
    )
    validation = validate_strategy_design_approval_stub(report)
    assert validation["decision"] == "reject"
    assert "approved_horizon_not_requested_60" in validation["reason_codes"]


def test_approval_stub_blocks_if_review_not_allowed():
    spec = strategy_design_spec()
    spec["strategy_design_review_allowed"] = False
    report = build_strategy_design_approval_stub(run_id="approval", strategy_design_spec=spec)
    assert report["strategy_design_prototype_gate"]["prototype_generation_allowed"] is False
    assert "strategy_design_review_not_allowed" in report["strategy_design_prototype_gate"]["block_reasons"]


def test_validate_rejects_open_queue_gate():
    report = build_strategy_design_approval_stub(run_id="approval", strategy_design_spec=strategy_design_spec())
    report["strategy_design_prototype_gate"]["queue_write_allowed"] = True
    validation = validate_strategy_design_approval_stub(report)
    assert validation["decision"] == "reject"
    assert "prototype_gate_not_closed_queue_write_allowed" in validation["reason_codes"]


def test_approval_stub_markdown_and_writes(tmp_path):
    report = build_strategy_design_approval_stub(run_id="approval", strategy_design_spec=strategy_design_spec())
    assert validate_strategy_design_approval_stub(report)["decision"] == "keep"
    markdown = strategy_design_approval_stub_to_markdown(report)
    assert "Strategy Design Approval Gate" in markdown
    assert "pending_human_review" in markdown
    paths = write_strategy_design_approval_stub(report, tmp_path)
    assert paths["approval_stub_json"].exists()
    assert paths["prototype_gate_json"].exists()
    payload = json.loads(paths["prototype_gate_json"].read_text(encoding="utf-8"))
    assert payload["prototype_generation_allowed"] is False
