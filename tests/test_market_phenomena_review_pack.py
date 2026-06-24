from __future__ import annotations

import json

from factor_lab.market_phenomena_review_pack import (
    build_human_review_pack,
    validate_human_review_pack,
    human_review_pack_to_markdown,
    write_human_review_pack,
)


def iteration_plan():
    return {
        "run_id": "plan_v2",
        "phenomenon_id": "p1",
        "title": "资产负债表修复后的价值陷阱脱离",
        "mechanism_hypothesis": {"claim": "delayed repricing after balance sheet repair"},
        "participant_logic": {"participants": ["forced_seller", "risk_budgeted_institution"], "constraint": "capital_constraint"},
        "risk_cost_constraints": {"liquidity_turnover_filter": {}, "drawdown_guard": {}},
        "production_boundaries": {"live_trading_allowed": False, "queue_write_allowed": False, "timer_enable_allowed": False, "daemon_restore_allowed": False, "auto_promotion_allowed": False},
    }


def router():
    return {
        "run_id": "router",
        "phenomenon_id": "p1",
        "supported_horizons": [20],
        "rejected_horizons": [
            {"horizon": 5, "reason": "oos_failed_negative_spread"},
            {"horizon": 60, "reason": "risk_gate_failed"},
            {"horizon": 120, "reason": "oos_failed_negative_spread"},
        ],
        "strategy_design_review_gate": {
            "strategy_design_review_allowed": True,
            "strategy_generation_allowed": False,
            "human_approval_required": True,
            "auto_promotion_allowed": False,
            "production_execution_allowed": False,
            "queue_write_allowed": False,
            "review_scope": ["20d"],
        },
        "queue_write_allowed": False,
    }


def deeper_oos_report():
    return {
        "run_id": "deep",
        "results": [
            {"horizon": 20, "split": "validation", "status": "pass", "cost_adjusted_spread_vs_control": 0.005, "target_worst_forward_return": -0.24},
            {"horizon": 20, "split": "oos", "status": "pass", "cost_adjusted_spread_vs_control": 0.003, "target_worst_forward_return": -0.23},
            {"horizon": 5, "split": "oos", "status": "fail", "cost_adjusted_spread_vs_control": -0.001, "target_worst_forward_return": -0.19},
        ],
        "queue_write_allowed": False,
    }


def test_review_pack_builds_spec_for_supported_horizon_without_production_permissions():
    pack = build_human_review_pack(run_id="review", iteration_plan=iteration_plan(), horizon_router=router(), deeper_oos_report=deeper_oos_report())
    spec = pack["strategy_design_spec"]
    assert pack["mode"] == "human_review_pack"
    assert spec["supported_horizons"] == [20]
    assert spec["review_scope"] == ["20d"]
    assert spec["strategy_design_review_allowed"] is True
    assert spec["strategy_generation_allowed"] is False
    assert spec["production_execution_allowed"] is False
    assert spec["queue_write_allowed"] is False


def test_review_pack_includes_evidence_rejections_and_review_questions():
    pack = build_human_review_pack(run_id="review", iteration_plan=iteration_plan(), horizon_router=router(), deeper_oos_report=deeper_oos_report())
    assert pack["rejected_horizons"][0]["reason"] == "oos_failed_negative_spread"
    assert any(item["horizon"] == 20 and item["split"] == "oos" for item in pack["evidence_summary"])
    checklist = pack["strategy_design_review_checklist"]
    assert any("20d" in item for item in checklist["questions"])
    assert any("train tail risk" in item for item in checklist["questions"])


def test_review_pack_blocks_when_gate_disallows_review():
    r = router()
    r["supported_horizons"] = []
    r["strategy_design_review_gate"]["strategy_design_review_allowed"] = False
    r["strategy_design_review_gate"]["review_scope"] = []
    pack = build_human_review_pack(run_id="review", iteration_plan=iteration_plan(), horizon_router=r, deeper_oos_report=deeper_oos_report())
    assert pack["strategy_design_spec"]["strategy_design_review_allowed"] is False
    assert pack["strategy_design_review_checklist"]["decision_required"] == "reject_or_request_more_research"


def test_validate_rejects_open_gates():
    pack = build_human_review_pack(run_id="review", iteration_plan=iteration_plan(), horizon_router=router(), deeper_oos_report=deeper_oos_report())
    pack["strategy_design_spec"]["queue_write_allowed"] = True
    validation = validate_human_review_pack(pack)
    assert validation["decision"] == "reject"
    assert "spec_gate_not_closed_queue_write_allowed" in validation["reason_codes"]


def test_review_pack_markdown_and_writes(tmp_path):
    pack = build_human_review_pack(run_id="review", iteration_plan=iteration_plan(), horizon_router=router(), deeper_oos_report=deeper_oos_report())
    assert validate_human_review_pack(pack)["decision"] == "keep"
    markdown = human_review_pack_to_markdown(pack)
    assert "Human Review Pack" in markdown
    assert "20d" in markdown
    paths = write_human_review_pack(pack, tmp_path)
    assert paths["review_markdown"].exists()
    assert paths["spec_json"].exists()
    assert paths["checklist_markdown"].exists()
    payload = json.loads(paths["spec_json"].read_text(encoding="utf-8"))
    assert payload["supported_horizons"] == [20]
