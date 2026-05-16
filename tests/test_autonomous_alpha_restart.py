from __future__ import annotations

from factor_lab.autonomous_alpha_restart import (
    BENCHMARK_ROUTE,
    build_historical_alpha_map,
    build_mechanism_candidates,
    build_restart_boundary,
    scan_evidence_runs,
    score_mechanism_candidates,
)


def test_restart_boundary_closes_failed_routes() -> None:
    doc = build_restart_boundary()
    assert doc["workflow_execution_allowed_in_this_plan"] is False
    assert "value_trap_filter_quality_confirmation" in doc["closed_routes"]
    assert "cashflow_value_trap" in doc["closed_routes"]


def test_historical_alpha_map_uses_scanned_runs(tmp_path) -> None:
    run_dir = tmp_path / "artifacts" / "value_route_followups" / "runs" / "value_quality_no_distress__cost_sensitivity_20bps"
    run_dir.mkdir(parents=True)
    (run_dir / "results.json").write_text(
        '[{"factor_name":"value_quality_no_distress::x","expression":"x","rank_ic_mean":0.03,"rank_ic_ir":0.2,"top_bottom_spread_mean":-0.001,"sharpe_net":-2,"observations":100}]'
    )
    (run_dir / "bucket_aware_portfolio_results.json").write_text(
        '[{"spread_mean":0.006225,"pass_gate":true,"observations":100}]'
    )
    runs = scan_evidence_runs(tmp_path / "artifacts")
    alpha_map = build_historical_alpha_map(runs)
    route = next(row for row in alpha_map["routes"] if row["route_id"] == BENCHMARK_ROUTE)
    assert route["bucket_successes"] == 1
    assert route["max_bucket_spread"] == 0.006225


def test_candidate_scoring_selects_low_crowding_probe() -> None:
    candidates = build_mechanism_candidates({})
    scores = score_mechanism_candidates(candidates)
    selected = scores["selected_probe_candidate"]
    assert selected is not None
    assert selected["candidate_id"] == "value_quality_low_crowding_confirmation"
    assert selected["score"] >= 70
    blocked = {row["candidate_id"]: row for row in scores["scores"]}
    assert blocked["cashflow_value_trap_reopen"]["decision"] == "not_selected"
    assert "cashflow_closure_policy" in blocked["cashflow_value_trap_reopen"]["hard_blocks"]
