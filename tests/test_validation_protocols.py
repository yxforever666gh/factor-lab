from factor_lab.validation_protocols import build_validation_matrix, load_validation_protocols


def test_load_validation_protocols_defines_oos_windows_and_universes():
    protocols = load_validation_protocols()
    protocol = protocols["alpha_candidate_default"]

    assert protocol["windows"]["train"] == ["2020-01-01", "2021-12-31"]
    assert protocol["windows"]["validation"] == ["2022-01-01", "2023-12-31"]
    assert protocol["windows"]["test"] == ["2024-01-01", "2025-12-31"]
    assert protocol["recent_is_monitor_only"] is True
    assert 100 in protocol["universes"]


def test_build_validation_matrix_expands_windows_universes_and_horizons():
    matrix = build_validation_matrix(
        factor={"name": "value_quality", "expression": "industry_relative_book_yield + roe"},
        protocol_name="value_factor_default",
    )

    assert matrix["protocol_name"] == "value_factor_default"
    assert {row["window_name"] for row in matrix["runs"]} >= {"train", "validation", "test"}
    assert {row["horizon"] for row in matrix["runs"]} >= {"60d", "120d"}
    assert all(row["factor"]["name"] == "value_quality" for row in matrix["runs"])
    assert all(row["promotion_eligible"] is False for row in matrix["runs"] if row["window_name"] == "recent")
