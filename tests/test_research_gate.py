from factor_lab.research_gate import evaluate_research_gate


def valid_hypothesis():
    return {
        "hypothesis_id": "value_trap_filter_quality_confirmation",
        "economic_logic": "行业内低估股票中，极端便宜组表现不强，可能因为混入现金流差、高负债、盈利恶化的价值陷阱。",
        "required_fields": ["pb", "industry", "operating_cashflow_to_profit", "debt_to_assets"],
        "required_pit_features": ["operating_cashflow_to_profit", "debt_to_assets", "netprofit_yoy"],
        "expected_mechanism": "过滤价值陷阱，只保留低估但基本面没有恶化的股票。",
        "failure_modes": ["coverage不足"],
        "stop_rules": {"max_pairwise_corr": 0.9, "require_cost_adjusted_pass": True},
        "max_variants": 3,
        "min_coverage": 0.7,
        "pit_requirements": {"require_ann_date_asof": True, "forbid_end_date_only": True},
    }


def test_gate_allows_mechanism_with_pit_rules():
    decision = evaluate_research_gate(valid_hypothesis())
    assert decision.decision == "allow_preflight"
    assert decision.to_dict()["max_variants"] == 3
    assert "operating_cashflow_to_profit" in decision.to_dict()["required_pit_features"]


def test_gate_blocks_missing_economic_logic():
    h = valid_hypothesis()
    h["economic_logic"] = ""
    decision = evaluate_research_gate(h)
    assert decision.decision == "block"
    assert "economic_logic_too_thin" in decision.reasons


def test_gate_blocks_legacy_low_information_recombination():
    h = valid_hypothesis()
    h["required_fields"] = ["pe", "pb", "momentum_20"]
    h["required_pit_features"] = []
    decision = evaluate_research_gate(h)
    assert decision.decision == "block"
    assert "low_information_legacy_field_recombination" in decision.reasons
    assert "missing_required_pit_features" in decision.reasons


def test_gate_blocks_missing_pit_rules_and_too_many_variants():
    h = valid_hypothesis()
    h["max_variants"] = 10
    h["pit_requirements"] = {"require_ann_date_asof": False}
    decision = evaluate_research_gate(h)
    assert "max_variants_above_3" in decision.reasons
    assert "pit_asof_not_required" in decision.reasons


def test_gate_blocks_unknown_pit_feature_metadata():
    h = valid_hypothesis()
    h["required_pit_features"] = ["fake_pit_feature"]
    decision = evaluate_research_gate(h)
    assert decision.decision == "block"
    assert "unknown_pit_features:fake_pit_feature" in decision.reasons
