import pytest

from factor_lab.portfolio_construction_config import parse_portfolio_construction


def test_default_portfolio_construction_is_extreme_top_bottom():
    cfg = parse_portfolio_construction({})

    assert cfg["mode"] == "top_bottom"


def test_bucket_pair_config_parses_quantiles():
    cfg = parse_portfolio_construction({
        "portfolio_construction": {
            "mode": "bucket_pair",
            "quantiles": 5,
            "long_quantile": 3,
            "short_quantile": 0,
        }
    })

    assert cfg == {"mode": "bucket_pair", "quantiles": 5, "long_quantile": 3, "short_quantile": 0}


def test_invalid_bucket_pair_config_blocks():
    with pytest.raises(ValueError, match="out of range"):
        parse_portfolio_construction({
            "portfolio_construction": {"mode": "bucket_pair", "quantiles": 5, "long_quantile": 5, "short_quantile": 0}
        })
