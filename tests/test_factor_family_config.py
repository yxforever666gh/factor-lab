from pathlib import Path

from factor_lab.factors import expand_factor_family_config, resolve_factor_definitions


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_expand_factor_family_config_contains_first_batch_variants():
    rows = expand_factor_family_config(REPO_ROOT / "configs" / "factor_families_v1.json")
    names = {row["name"] for row in rows}
    assert {"mom_20", "mom_60", "mom_120", "mom_60_skip_5", "book_yield", "earnings_yield", "book_yield_plus_earnings_yield", "earnings_yield_over_pb"}.issubset(names)


def test_resolve_factor_definitions_uses_family_config_when_factors_missing():
    rows = resolve_factor_definitions({"factor_family_config": "factor_families_v1.json"}, config_dir=REPO_ROOT / "configs")
    assert len(rows) >= 8
    lookup = {row["name"]: row for row in rows}
    assert lookup["mom_60_skip_5"]["family"] == "momentum"
    assert lookup["earnings_yield"]["allow_in_portfolio"] is False
