import json

from factor_lab.harvest_mechanism_routes import load_mechanism_routes, select_mechanism_route


def test_load_mechanism_routes_from_config(tmp_path):
    config = tmp_path / "routes.json"
    config.write_text(
        json.dumps(
            {
                "routes": [
                    {
                        "mechanism_id": "low_volatility_value_quality",
                        "allowed_signals": ["industry_relative_earnings_yield"],
                        "required_fields": ["volatility_20", "roe"],
                        "default_filters": [{"field": "volatility_20", "operator": "<=", "quantile": 0.6}],
                        "rationale": "reduce drawdown",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    routes = load_mechanism_routes(config)

    assert routes["low_volatility_value_quality"]["allowed_signals"] == ["industry_relative_earnings_yield"]


def test_select_route_uses_failure_classes():
    route = select_mechanism_route({"failure_classes": ["drawdown_too_high", "weak_risk_adjusted_return"]})

    assert route["mechanism_id"] == "low_volatility_value_quality"
    assert "volatility_20" in route["required_fields"]
    assert route["allowed_signals"]


def test_select_route_prefers_cost_robust_when_zero_cost_best_only():
    route = select_mechanism_route({"failure_classes": ["zero_cost_best_only"]})

    assert route["mechanism_id"] == "cost_robust_value_quality"
    assert any(f.get("field") == "turnover" for f in route["default_filters"])
