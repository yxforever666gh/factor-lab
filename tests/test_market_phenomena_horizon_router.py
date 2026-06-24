from __future__ import annotations

import json

from factor_lab.market_phenomena_horizon_router import (
    build_supported_horizon_router,
    validate_supported_horizon_router,
    supported_horizon_router_to_markdown,
    write_supported_horizon_router,
)


def deeper_report():
    results = []
    for horizon, statuses in {
        5: {"validation": "pass", "oos": "fail"},
        20: {"train": "fail", "validation": "pass", "oos": "pass"},
        60: {"validation": "fail", "oos": "fail"},
        120: {"validation": "pass", "oos": "fail"},
    }.items():
        for split, status in statuses.items():
            spread = 0.003 if status == "pass" else (-0.001 if horizon in {5, 120} and split == "oos" else 0.006)
            results.append(
                {
                    "horizon": horizon,
                    "split": split,
                    "status": status,
                    "spread_vs_control": spread,
                    "cost_adjusted_spread_vs_control": spread,
                    "target_worst_forward_return": -0.20 if status == "pass" else (-0.50 if spread > 0 else -0.19),
                }
            )
    return {
        "run_id": "deep",
        "phenomenon_id": "p1",
        "mode": "deeper_oos_horizon_report",
        "results": results,
        "queue_write_allowed": False,
        "live_trading_allowed": False,
        "strategy_generation_allowed": False,
    }


def test_router_supports_only_validation_and_oos_passing_horizons():
    report = build_supported_horizon_router(run_id="router", deeper_oos_report=deeper_report())
    assert report["mode"] == "supported_horizon_router"
    assert report["supported_horizons"] == [20]
    assert report["strategy_design_review_gate"]["strategy_design_review_allowed"] is True
    assert report["strategy_design_review_gate"]["human_approval_required"] is True
    assert report["strategy_design_review_gate"]["auto_promotion_allowed"] is False
    assert report["production_execution_allowed"] is False


def test_router_rejects_horizons_with_reasons():
    report = build_supported_horizon_router(run_id="router", deeper_oos_report=deeper_report())
    rejected = {item["horizon"]: item for item in report["rejected_horizons"]}
    assert rejected[5]["reason"] == "oos_failed_negative_spread"
    assert rejected[60]["reason"] == "risk_gate_failed"
    assert rejected[120]["reason"] == "oos_failed_negative_spread"


def test_router_blocks_strategy_review_when_no_supported_horizon():
    deep = deeper_report()
    for item in deep["results"]:
        item["status"] = "fail"
        item["spread_vs_control"] = -0.01
        item["cost_adjusted_spread_vs_control"] = -0.01
    report = build_supported_horizon_router(run_id="router", deeper_oos_report=deep)
    assert report["supported_horizons"] == []
    assert report["strategy_design_review_gate"]["strategy_design_review_allowed"] is False
    assert report["strategy_design_review_gate"]["next_action"] == "mutate_or_reject_horizons"


def test_validate_rejects_open_production_gates():
    report = build_supported_horizon_router(run_id="router", deeper_oos_report=deeper_report())
    report["queue_write_allowed"] = True
    validation = validate_supported_horizon_router(report)
    assert validation["decision"] == "reject"
    assert "gate_not_closed_queue_write_allowed" in validation["reason_codes"]


def test_router_markdown_and_writes(tmp_path):
    report = build_supported_horizon_router(run_id="router", deeper_oos_report=deeper_report())
    assert validate_supported_horizon_router(report)["decision"] == "keep"
    markdown = supported_horizon_router_to_markdown(report)
    assert "Supported Horizon Router" in markdown
    assert "20d" in markdown
    paths = write_supported_horizon_router(report, tmp_path)
    assert paths["router_json"].exists()
    assert paths["gate_json"].exists()
    payload = json.loads(paths["gate_json"].read_text(encoding="utf-8"))
    assert payload["strategy_design_review_allowed"] is True
