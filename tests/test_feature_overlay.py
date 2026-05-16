import pandas as pd

from factor_lab.feature_overlay import apply_feature_overlay, normalize_overlay_frame


def test_normalize_overlay_accepts_ts_code_alias():
    overlay = pd.DataFrame({"date": ["2020-01-02"], "ts_code": ["000001.SZ"], "x": [1.0]})
    out = normalize_overlay_frame(overlay)
    assert out["ticker"].iloc[0] == "000001.SZ"
    assert out["date"].iloc[0] == "2020-01-02"


def test_apply_feature_overlay_merges_requested_columns_only():
    base = pd.DataFrame({"date": ["2020-01-02", "2020-01-03"], "ticker": ["000001.SZ", "000002.SZ"], "close": [1, 2]})
    overlay = pd.DataFrame({"date": ["20200102"], "ticker": ["000001.SZ"], "low_margin_crowding": [0.5], "ignored": [9]})
    out = apply_feature_overlay(base, overlay, columns=["low_margin_crowding"])
    assert "low_margin_crowding" in out.columns
    assert "ignored" not in out.columns
    assert out["low_margin_crowding"].iloc[0] == 0.5
    assert pd.isna(out["low_margin_crowding"].iloc[1])


def test_feature_overlay_normalizes_integer_yyyymmdd_dates():
    base = pd.DataFrame({"date": ["2020-07-06"], "ticker": ["000002.SZ"], "close": [1]})
    overlay = pd.DataFrame({"date": [20200706], "ticker": ["000002.SZ"], "event_signal": [1.0]})

    out = apply_feature_overlay(base, overlay, columns=["event_signal"])

    assert out["event_signal"].iloc[0] == 1.0
