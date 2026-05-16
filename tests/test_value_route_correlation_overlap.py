
import pandas as pd

from factor_lab.value_route_correlation_overlap import pair_metrics, ROUTE_BUCKETS, reconstruct_route_frame


def _frame(vals):
    return pd.DataFrame({"date":["2020-01-01"]*10+["2020-01-02"]*10,"ticker":[f"T{i}" for i in range(10)]*2,"factor_value":vals*2,"forward_return_5d":[v/100 for v in vals]*2})


def test_identical_signals_have_high_corr_and_overlap():
    f=_frame(list(range(10)))
    m=pair_metrics("industry_relative_value", f, "value_quality_no_distress", f.copy())
    assert m["factor_score_spearman_corr"] == 1.0
    assert m["long_bucket_overlap_mean"] == 1.0
    assert m["common_dates"] == 2


def test_reconstruct_live_route_frame():
    f, meta = reconstruct_route_frame("value_quality_no_distress")
    assert meta["status"] == "ok"
    assert {"date","ticker","forward_return_5d","factor_value"}.issubset(f.columns)
    assert len(f) > 0
